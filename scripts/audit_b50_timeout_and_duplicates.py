#!/usr/bin/env python3
"""
Audit for TASK_B50: TIMEOUT backfill feasibility and duplicate detection.

Outputs:
 - reports/B50_summary.md
 - reports/B50_timeout_samples.csv (sample up to 50 rows)
 - reports/B50_duplicates.csv

Usage:
 PYTHONPATH=. /path/to/python3 scripts/audit_b50_timeout_and_duplicates.py
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import sys
from typing import List

import pandas as pd

ROOT = Path.cwd()
REPORT_DIR = ROOT / "reports" / "B50"
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
            # try tolerant read
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

def parse_details_payload(row) -> dict:
    payload = row.get("details_payload") or row.get("details_json") or row.get("details") or ""
    if not payload or not isinstance(payload, str):
        return {}
    try:
        return json.loads(payload)
    except Exception:
        return {}

def timeout_audit(df: pd.DataFrame) -> pd.DataFrame:
    df_timeout = df[df.get("outcome", "").astype(str).str.upper() == "TIMEOUT"].copy()
    if df_timeout.empty:
        return df_timeout
    categories = []
    for _, r in df_timeout.iterrows():
        details = parse_details_payload(r)
        path = None
        if isinstance(details, dict):
            path = details.get("path_1m") or details.get("path")
        # check category
        if path:
            try:
                if isinstance(path, list) and len(path) > 0:
                    categories.append("backfillable_from_path")
                    continue
            except Exception:
                pass
        # check timestamps for possible klines fetch
        has_symbol = bool(r.get("symbol"))
        has_opened = bool(r.get("opened_ts") or r.get("entry_time_utc") or r.get("entry_time"))
        has_outcome_time = bool(r.get("outcome_time_utc") or r.get("exit_time_utc") or r.get("hit_time_utc"))
        if not path and has_symbol and has_opened and has_outcome_time:
            categories.append("backfillable_from_klines")
        else:
            categories.append("unknown_no_path")
    df_timeout["b50_category"] = categories
    return df_timeout

def duplicate_audit(df: pd.DataFrame) -> pd.DataFrame:
    # Primary key: trade_id
    df_dup = df.copy()
    df_dup["_trade_id"] = df_dup.get("trade_id") if "trade_id" in df_dup.columns else df_dup.get("tradeId")
    df_dup["_event_id"] = df_dup.get("event_id") if "event_id" in df_dup.columns else df_dup.get("eventId")
    # count by trade_id
    dup_by_trade = df_dup.groupby("_trade_id").size().reset_index(name="cnt").query("cnt>1")
    # conflicting outcomes for same trade_id
    conflict_rows = []
    for tid in dup_by_trade["_trade_id"].dropna().unique():
        sub = df_dup[df_dup["_trade_id"] == tid]
        outs = sub.get("outcome").astype(str).str.upper().unique().tolist()
        if len(outs) > 1:
            conflict_rows.append(tid)
    # exact duplicate full-row detection
    df_dup["_row_tuple"] = df_dup.apply(lambda r: tuple((k, str(r.get(k, ""))) for k in sorted(r.index)), axis=1)
    exact_dupes = df_dup[df_dup.duplicated(subset=["_row_tuple"], keep=False)].copy()
    # composite key duplicates
    comp_cols = ["run_id", "symbol", "outcome_time_utc", "outcome", "side"]
    comp_present = [c for c in comp_cols if c in df_dup.columns]
    comp_dupes = pd.DataFrame()
    if comp_present:
        comp_dupes = df_dup.groupby(comp_present).size().reset_index(name="cnt").query("cnt>1")
    # assemble result summary as DataFrame
    summary = {
        "n_rows": len(df_dup),
        "n_trade_id_duplicates": int(len(dup_by_trade)),
        "n_conflicting_trade_id_outcomes": int(len(conflict_rows)),
        "n_exact_duplicate_rows": int(exact_dupes.shape[0]),
        "n_composite_duplicates": int(len(comp_dupes)) if not comp_dupes.empty else 0,
        "conflict_trade_ids_sample": conflict_rows[:200],
    }
    return df_dup, summary, dup_by_trade, exact_dupes

def write_reports(df_timeout: pd.DataFrame, dup_summary: dict, dup_by_trade: pd.DataFrame, exact_dupes: pd.DataFrame):
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    total_timeout = len(df_timeout)
    counts = df_timeout["b50_category"].value_counts().to_dict() if not df_timeout.empty else {}
    with open(REPORT_DIR / "B50_summary.md", "w", encoding="utf-8") as f:
        f.write("# B50 Audit Summary\n\n")
        f.write(f"Total TIMEOUT rows: {total_timeout}\n\n")
        f.write("Category breakdown:\n\n")
        for k, v in counts.items():
            pct = v / total_timeout * 100 if total_timeout else 0.0
            f.write(f"- {k}: {v} ({pct:.2f}%)\n")
        f.write("\nDuplicate summary:\n\n")
        for k, v in dup_summary.items():
            f.write(f"- {k}: {v}\n")
    # timeout samples
    if not df_timeout.empty:
        sample = df_timeout.head(50)
        sample.to_csv(REPORT_DIR / "B50_timeout_samples.csv", index=False)
    else:
        pd.DataFrame().to_csv(REPORT_DIR / "B50_timeout_samples.csv", index=False)
    # duplicates export
    if not exact_dupes.empty:
        exact_dupes.to_csv(REPORT_DIR / "B50_duplicates.csv", index=False)
    else:
        # export trade-id duplicate list
        dup_by_trade.to_csv(REPORT_DIR / "B50_duplicates.csv", index=False)

def main():
    paths = find_outcome_files()
    if not paths:
        print("No outcomes_v3.csv files found under datasets/ — nothing to audit.")
        sys.exit(0)
    df = load_outcomes(paths)
    if df.empty:
        print("No outcome rows loaded.")
        sys.exit(0)
    df_timeout = timeout_audit(df)
    df_dup, dup_summary, dup_by_trade, exact_dupes = duplicate_audit(df)
    write_reports(df_timeout, dup_summary, dup_by_trade, exact_dupes)
    # print short summary
    total_timeout = len(df_timeout)
    counts = df_timeout["b50_category"].value_counts().to_dict() if not df_timeout.empty else {}
    total_dupes = int(dup_summary.get("n_exact_duplicate_rows", 0))
    print("B50 audit complete.")
    print("Total TIMEOUT:", total_timeout)
    for k, v in counts.items():
        pct = v / total_timeout * 100 if total_timeout else 0.0
        print(f" - {k}: {v} ({pct:.2f}%)")
    print("Duplicate rows (exact):", total_dupes)
    print("Reports written to:", REPORT_DIR)

if __name__ == "__main__":
    main()

