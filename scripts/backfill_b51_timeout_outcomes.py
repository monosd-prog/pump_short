#!/usr/bin/env python3
"""
Backfill task for B51 (read-only):
- Read TIMEOUT rows from outcomes_v3.csv
- If details_payload.path_1m exists -> infer TP/SL hits and reconstructed MFE/MAE
- If no path_1m but symbol+opened_ts+outcome_time_utc -> mark candidate_from_klines
- Do NOT modify original datasets; produce artifacts under reports/B51/
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import sys
from typing import Any, Dict, List

import pandas as pd

ROOT = Path.cwd()
REPORT_DIR = ROOT / "reports" / "B51"
DATASET_GLOB = ROOT / "datasets"

def find_outcome_files() -> List[Path]:
    return sorted(DATASET_GLOB.glob("**/outcomes_v3.csv"))

def load_outcomes(paths: List[Path]) -> pd.DataFrame:
    frames = []
    for p in paths:
        try:
            df = pd.read_csv(p, low_memory=False)
            df["__source_file"] = str(p)
            frames.append(df)
        except Exception:
            try:
                df = pd.read_csv(p, header=0, low_memory=False, dtype=str)
                df["__source_file"] = str(p)
                frames.append(df)
            except Exception:
                print(f"WARN: failed to read {p}", file=sys.stderr)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True, sort=False)
    return out

def parse_details_payload(payload_raw: Any) -> Dict[str, Any]:
    if payload_raw is None:
        return {}
    if isinstance(payload_raw, dict):
        return payload_raw
    if not isinstance(payload_raw, str):
        return {}
    try:
        return json.loads(payload_raw)
    except Exception:
        return {}

def infer_from_path(path_list: List[Dict[str, Any]], entry_price: float, tp: float, sl: float, side: str):
    # path_list expected as list of dicts with keys 't','h','l' or 'high'/'low'
    lows = []
    highs = []
    for p in path_list:
        if not isinstance(p, dict):
            continue
        # support variants
        h = p.get("h") or p.get("high") or p.get("H")
        l = p.get("l") or p.get("low") or p.get("L")
        try:
            if h is not None:
                highs.append(float(h))
            if l is not None:
                lows.append(float(l))
        except Exception:
            continue
    mfe = 0.0
    mae = 0.0
    tp_hit = False
    sl_hit = False
    if lows:
        min_low = min(lows)
        max_high = max(highs) if highs else min_low
        if side.lower() == "short":
            mfe = (entry_price - min_low) / entry_price * 100.0 if entry_price else 0.0
            mae = (max_high - entry_price) / entry_price * 100.0 if entry_price else 0.0
            if min_low <= tp:
                tp_hit = True
            if max_high >= sl:
                sl_hit = True
        else:
            mfe = (max_high - entry_price) / entry_price * 100.0 if entry_price else 0.0
            mae = (entry_price - min_low) / entry_price * 100.0 if entry_price else 0.0
            if max_high >= tp:
                tp_hit = True
            if min_low <= sl:
                sl_hit = True
    return {"tp_hit": tp_hit, "sl_hit": sl_hit, "mfe_pct": mfe, "mae_pct": mae}

def classify_row(r: pd.Series) -> Dict[str, Any]:
    details = parse_details_payload(r.get("details_payload") or r.get("details_json") or "")
    path = None
    if isinstance(details, dict):
        path = details.get("path_1m") or details.get("path") or details.get("path1m")
    entry = r.get("entry") or r.get("entry_price") or r.get("paper_entry_price")
    tp = r.get("tp") or r.get("tp_price") or r.get("paper_tp_price")
    sl = r.get("sl") or r.get("sl_price") or r.get("paper_sl_price")
    side = r.get("side") or "short"
    try:
        entry_f = float(entry) if entry not in (None, "", "nan") else 0.0
    except Exception:
        entry_f = 0.0
    try:
        tp_f = float(tp) if tp not in (None, "", "nan") else 0.0
    except Exception:
        tp_f = 0.0
    try:
        sl_f = float(sl) if sl not in (None, "", "nan") else 0.0
    except Exception:
        sl_f = 0.0

    out = {
        "backfill_label": "unknown",
        "backfill_source": "none",
        "backfill_confidence": "low",
        "reconstructed_mfe_pct": None,
        "reconstructed_mae_pct": None,
    }
    if path and isinstance(path, list) and len(path) > 0:
        res = infer_from_path(path, entry_f, tp_f, sl_f, side)
        if res["tp_hit"] and not res["sl_hit"]:
            out["backfill_label"] = "TP_hit_before_timeout"
        elif res["sl_hit"] and not res["tp_hit"]:
            out["backfill_label"] = "SL_hit_before_timeout"
        elif res["tp_hit"] and res["sl_hit"]:
            # conflict: both in path; mark as conflict_neither (requires manual inspect)
            out["backfill_label"] = "conflict_both_hit"
        else:
            out["backfill_label"] = "neither_hit"
        out["backfill_source"] = "path_1m"
        out["backfill_confidence"] = "high"
        out["reconstructed_mfe_pct"] = res["mfe_pct"]
        out["reconstructed_mae_pct"] = res["mae_pct"]
        return out

    # no path: check if candidate for klines
    has_symbol = bool(r.get("symbol"))
    has_opened = bool(r.get("opened_ts") or r.get("entry_time_utc") or r.get("entry_time"))
    has_outcome_time = bool(r.get("outcome_time_utc") or r.get("exit_time_utc") or r.get("hit_time_utc"))
    if has_symbol and has_opened and has_outcome_time:
        out["backfill_label"] = "candidate_from_klines"
        out["backfill_source"] = "klines"
        out["backfill_confidence"] = "medium"
        return out

    out["backfill_label"] = "unknown_no_path"
    out["backfill_source"] = "none"
    out["backfill_confidence"] = "low"
    return out

def main():
    paths = find_outcome_files()
    if not paths:
        print("No outcomes_v3.csv files found under datasets/ — nothing to backfill.")
        sys.exit(0)
    df = load_outcomes(paths)
    if df.empty:
        print("No outcome rows loaded.")
        sys.exit(0)
    df_timeout = df[df.get("outcome", "").astype(str).str.upper() == "TIMEOUT"].copy()
    if df_timeout.empty:
        print("No TIMEOUT rows to backfill.")
        sys.exit(0)
    results = []
    for _, r in df_timeout.iterrows():
        rec = classify_row(r)
        rec_row = {
            "trade_id": r.get("trade_id") or r.get("tradeId") or "",
            "event_id": r.get("event_id") or r.get("eventId") or "",
            "run_id": r.get("run_id") or "",
            "symbol": r.get("symbol") or "",
            "strategy": r.get("strategy") or "",
            "opened_ts": r.get("opened_ts") or r.get("entry_time_utc") or "",
            "outcome_time_utc": r.get("outcome_time_utc") or r.get("exit_time_utc") or r.get("hit_time_utc") or "",
            "entry": r.get("entry") or r.get("entry_price") or "",
            "tp": r.get("tp") or r.get("tp_price") or "",
            "sl": r.get("sl") or r.get("sl_price") or "",
            "backfill_label": rec["backfill_label"],
            "backfill_source": rec["backfill_source"],
            "backfill_confidence": rec["backfill_confidence"],
            "reconstructed_mfe_pct": rec["reconstructed_mfe_pct"],
            "reconstructed_mae_pct": rec["reconstructed_mae_pct"],
            "__source_file": r.get("__source_file", ""),
        }
        results.append(rec_row)
    out_df = pd.DataFrame(results)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(REPORT_DIR / "B51_timeout_backfill.csv", index=False)
    # === Enriched backfill for candidates with no path but timestamps ===
    enriched_rows: List[Dict[str, Any]] = []
    # Prepare original dataframe for lookup of raw numeric mfe/mae if present
    # iterate candidates: those marked as candidate_from_klines OR strict suspects (mfe==0 & mae==0 & no path)
    candidates = out_df[(out_df["backfill_source"] == "klines")].copy()
    # also include strict suspects based on original source dataframe values
    # find original rows matching criteria
    try:
        # reload full outcomes to get mfe/mae numeric if available
        full_paths = find_outcome_files()
        full_df = load_outcomes(full_paths)
    except Exception:
        full_df = pd.DataFrame()

    # helper to check path presence in original details
    def original_has_path(trade_id: str, event_id: str, src_file: str) -> bool:
        if full_df.empty:
            return False
        cond = False
        sub = full_df
        if trade_id:
            sub = sub[(sub.get("trade_id") == trade_id) | (sub.get("tradeId") == trade_id)]
        if event_id:
            sub = sub[(sub.get("event_id") == event_id) | (sub.get("eventId") == event_id)]
        if src_file:
            sub = sub[sub.get("__source_file") == src_file]
        for _, r in sub.head(10).iterrows():
            det = parse_details_payload(r.get("details_payload") or r.get("details_json") or "")
            if isinstance(det, dict):
                p = det.get("path_1m") or det.get("path")
                if isinstance(p, list) and len(p) > 0:
                    return True
        return False

    for _, row in candidates.iterrows():
        trade_id = row.get("trade_id") or ""
        event_id = row.get("event_id") or ""
        src_file = row.get("__source_file") or ""
        # strict suspect: if original mfe/mae are zero and no original path
        is_strict = False
        if not full_df.empty:
            cond = (full_df.get("trade_id") == trade_id) | (full_df.get("tradeId") == trade_id)
            sub = full_df[cond]
            if len(sub):
                try:
                    mfe_v = float(sub.iloc[0].get("mfe_pct") or 0.0)
                    mae_v = float(sub.iloc[0].get("mae_pct") or 0.0)
                except Exception:
                    mfe_v = 0.0
                    mae_v = 0.0
                has_path_orig = original_has_path(trade_id, event_id, src_file)
                if mfe_v == 0 and mae_v == 0 and not has_path_orig:
                    is_strict = True
        # proceed with klines fetch
        symbol = row.get("symbol") or ""
        opened_ts = row.get("opened_ts") or ""
        outcome_time = row.get("outcome_time_utc") or ""
        entry = row.get("entry") or ""
        tp = row.get("tp") or ""
        sl = row.get("sl") or ""
        try:
            entry_f = float(entry) if entry not in (None, "", "nan") else 0.0
        except Exception:
            entry_f = 0.0
        try:
            tp_f = float(tp) if tp not in (None, "", "nan") else 0.0
        except Exception:
            tp_f = 0.0
        try:
            sl_f = float(sl) if sl not in (None, "", "nan") else 0.0
        except Exception:
            sl_f = 0.0

        enriched = {
            "trade_id": trade_id,
            "event_id": event_id,
            "symbol": symbol,
            "opened_ts": opened_ts,
            "outcome_time_utc": outcome_time,
            "entry": entry_f,
            "tp": tp_f,
            "sl": sl_f,
            "original_outcome": "TIMEOUT",
            "reconstructed_outcome": "unknown",
            "mfe_pct_new": None,
            "mae_pct_new": None,
            "backfill_source": "none",
            "confidence": "low",
        }

        # fetch klines
        try:
            from short_pump.bybit_api import get_klines_1m_safe as get_klines_1m
            kl = get_klines_1m("linear", symbol, limit=300)
            if kl is None or kl.empty:
                # unable to fetch
                enriched["backfill_source"] = "klines_fetch_failed"
                enriched_rows.append(enriched)
                continue
            # normalize columns
            if "ts" not in kl.columns:
                # try common timestamp column names
                possible = [c for c in ("timestamp", "time", "open_time") if c in kl.columns]
                if possible:
                    kl = kl.rename(columns={possible[0]: "ts"})
            kl["ts"] = pd.to_datetime(kl["ts"])
            # filter to window
            try:
                start = pd.to_datetime(opened_ts)
                end = pd.to_datetime(outcome_time)
                window = kl[(kl["ts"] >= start) & (kl["ts"] <= end)]
            except Exception:
                window = kl
            if window.empty:
                enriched["backfill_source"] = "klines_no_window"
                enriched_rows.append(enriched)
                continue
            min_low = float(window["low"].min())
            max_high = float(window["high"].max())
            if row.get("side","").strip().lower() == "short":
                mfe_new = (entry_f - min_low) / entry_f * 100.0 if entry_f else 0.0
                mae_new = (max_high - entry_f) / entry_f * 100.0 if entry_f else 0.0
                tp_hit = min_low <= tp_f
                sl_hit = max_high >= sl_f
            else:
                mfe_new = (max_high - entry_f) / entry_f * 100.0 if entry_f else 0.0
                mae_new = (entry_f - min_low) / entry_f * 100.0 if entry_f else 0.0
                tp_hit = max_high >= tp_f
                sl_hit = min_low <= sl_f
            if tp_hit and not sl_hit:
                recon = "TP_hit_before_timeout"
            elif sl_hit and not tp_hit:
                recon = "SL_hit_before_timeout"
            elif tp_hit and sl_hit:
                recon = "conflict_both_hit"
            else:
                recon = "neither_hit"
            enriched.update(
                {
                    "reconstructed_outcome": recon,
                    "mfe_pct_new": mfe_new,
                    "mae_pct_new": mae_new,
                    "backfill_source": "klines_fetch",
                    "confidence": "high",
                }
            )
        except Exception as e:
            enriched["backfill_source"] = "klines_fetch_error"
            enriched["confidence"] = "low"
            enriched["error"] = str(e)
        enriched_rows.append(enriched)

    # save enriched results
    if enriched_rows:
        enriched_df = pd.DataFrame(enriched_rows)
        enriched_df.to_csv(REPORT_DIR / "B51_timeout_backfill_enriched.csv", index=False)
    else:
        pd.DataFrame().to_csv(REPORT_DIR / "B51_timeout_backfill_enriched.csv", index=False)
    # summary
    total = len(out_df)
    counts = out_df["backfill_label"].value_counts().to_dict()
    with open(REPORT_DIR / "B51_backfill_summary.md", "w", encoding="utf-8") as f:
        f.write("# B51 Backfill Summary\n\n")
        f.write(f"Total TIMEOUT rows processed: {total}\n\n")
        f.write("Breakdown by inferred label:\n\n")
        for k, v in counts.items():
            pct = v / total * 100 if total else 0.0
            f.write(f"- {k}: {v} ({pct:.2f}%)\n")
        f.write("\nExamples CSV (first 20 rows):\n\n")
        sample = out_df.head(20)
        f.write(sample.to_csv(index=False))
    # print short summary
    print("B51 backfill complete.")
    print("Total TIMEOUT:", total)
    for k, v in counts.items():
        pct = v / total * 100 if total else 0.0
        print(f" - {k}: {v} ({pct:.2f}%)")
    print("Reports written to:", REPORT_DIR)

if __name__ == "__main__":
    main()

