#!/usr/bin/env python3
"""
Targeted backfill for 11 confirmed false-TIMEOUT outcomes caused by stale-klines bug.
Updates only outcomes_v3.csv partitions. Does NOT modify CSV schema.

Usage:
    python3 scripts/backfill_false_timeouts.py             # dry-run (default)
    python3 scripts/backfill_false_timeouts.py --apply      # apply changes with backup
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
import time as _time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pandas as pd

CANDIDATES: List[Dict[str, str]] = [
    {"run_id": "20260320_173110", "symbol": "PIPPINUSDT",     "date": "20260320", "strategy": "short_pump", "expected": "SL_hit"},
    {"run_id": "20260321_030932", "symbol": "GODSUSDT",       "date": "20260321", "strategy": "short_pump", "expected": "SL_hit"},
    {"run_id": "20260321_113910", "symbol": "UAIUSDT",        "date": "20260321", "strategy": "short_pump", "expected": "TP_hit"},
    {"run_id": "20260322_075210", "symbol": "NTRNUSDT",       "date": "20260322", "strategy": "short_pump", "expected": "TP_hit"},
    {"run_id": "20260322_095432", "symbol": "BANANAS31USDT",  "date": "20260322", "strategy": "short_pump", "expected": "TP_hit"},
    {"run_id": "20260322_104446", "symbol": "UAIUSDT",        "date": "20260322", "strategy": "short_pump", "expected": "TP_hit"},
    {"run_id": "20260322_142115", "symbol": "SIRENUSDT",      "date": "20260322", "strategy": "short_pump", "expected": "SL_hit"},
    {"run_id": "20260322_155010", "symbol": "BANANAS31USDT",  "date": "20260322", "strategy": "short_pump", "expected": "TP_hit"},
    {"run_id": "20260322_181620", "symbol": "SIRENUSDT",      "date": "20260322", "strategy": "short_pump", "expected": "TP_hit"},
    {"run_id": "20260322_194645", "symbol": "1000TURBOUSDT",  "date": "20260322", "strategy": "short_pump", "expected": "TP_hit"},
    {"run_id": "20260322_225230", "symbol": "CYSUSDT",        "date": "20260322", "strategy": "short_pump", "expected": "SL_hit"},
]

OUTCOME_WATCH_MINUTES = 15
DATA_DIR = _ROOT / "datasets"
REPORTS_DIR = _ROOT / "reports"
KLINE_FETCH_DELAY_SEC = 0.35


def _fetch_klines_1m(symbol: str, start_ms: int, end_ms: int) -> pd.DataFrame:
    from short_pump.bybit_api import get_klines_1m_range
    return get_klines_1m_range("linear", symbol, start_ms=start_ms, end_ms=end_ms, limit=200)


def _recompute_outcome(
    entry_price: float,
    tp_price: float,
    sl_price: float,
    entry_ts: pd.Timestamp,
    klines: pd.DataFrame,
) -> Dict[str, Any]:
    """Recompute SHORT outcome from fresh klines using same logic as outcome_tracker."""
    future = klines[klines["ts"] >= entry_ts].copy()
    if future.empty:
        return {"outcome": "TIMEOUT", "changed": False}

    mfe = 0.0
    mae = 0.0
    path_1m: list[dict] = []

    for _, row in future.iterrows():
        hi = float(row["high"])
        lo = float(row["low"])
        ts = row["ts"]

        path_1m.append({"t": pd.Timestamp(ts).isoformat(), "h": hi, "l": lo})

        mfe = max(mfe, (entry_price - lo) / entry_price * 100.0)
        mae = max(mae, (hi - entry_price) / entry_price * 100.0)

        tp_hit = lo <= tp_price
        sl_hit = hi >= sl_price

        if tp_hit and sl_hit:
            exit_price = sl_price
            outcome = "SL_hit"
            hold_sec = (pd.Timestamp(ts) - entry_ts).total_seconds()
            pnl_pct = (entry_price - exit_price) / entry_price * 100.0
            return {
                "outcome": outcome, "exit_price": exit_price, "pnl_pct": pnl_pct,
                "hold_seconds": hold_sec, "mfe_pct": mfe, "mae_pct": mae,
                "path_1m": path_1m, "hit_ts": pd.Timestamp(ts).isoformat(), "changed": True,
            }

        if tp_hit:
            hold_sec = (pd.Timestamp(ts) - entry_ts).total_seconds()
            pnl_pct = (entry_price - tp_price) / entry_price * 100.0
            return {
                "outcome": "TP_hit", "exit_price": tp_price, "pnl_pct": pnl_pct,
                "hold_seconds": hold_sec, "mfe_pct": mfe, "mae_pct": mae,
                "path_1m": path_1m, "hit_ts": pd.Timestamp(ts).isoformat(), "changed": True,
            }

        if sl_hit:
            hold_sec = (pd.Timestamp(ts) - entry_ts).total_seconds()
            pnl_pct = (entry_price - sl_price) / entry_price * 100.0
            return {
                "outcome": "SL_hit", "exit_price": sl_price, "pnl_pct": pnl_pct,
                "hold_seconds": hold_sec, "mfe_pct": mfe, "mae_pct": mae,
                "path_1m": path_1m, "hit_ts": pd.Timestamp(ts).isoformat(), "changed": True,
            }

    return {"outcome": "TIMEOUT", "changed": False, "path_1m": path_1m, "mfe_pct": mfe, "mae_pct": mae}


def _outcomes_v3_path(date: str, strategy: str) -> Path:
    return DATA_DIR / f"date={date}" / f"strategy={strategy}" / "mode=live" / "outcomes_v3.csv"


def _backup_file(path: Path, ts_str: str) -> Path:
    bak = path.parent / f"{path.name}.bak.{ts_str}"
    shutil.copy2(path, bak)
    return bak


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill false TIMEOUT outcomes")
    parser.add_argument("--apply", action="store_true", help="Apply changes (default: dry-run)")
    args = parser.parse_args()

    apply = args.apply
    ts_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    audit_path = REPORTS_DIR / f"backfill_false_timeouts_audit_{ts_str}.jsonl"

    corrected = 0
    unchanged = 0
    skipped = 0
    backups_created: List[str] = []
    backed_up_files: set[str] = set()

    by_partition: Dict[str, List[Dict[str, str]]] = {}
    for c in CANDIDATES:
        key = f"{c['date']}|{c['strategy']}"
        by_partition.setdefault(key, []).append(c)

    audit_entries: List[Dict[str, Any]] = []

    for part_key, candidates in sorted(by_partition.items()):
        date_str, strategy = part_key.split("|")
        csv_path = _outcomes_v3_path(date_str, strategy)
        if not csv_path.exists():
            print(f"SKIP partition {csv_path}: not found")
            skipped += len(candidates)
            continue

        df = pd.read_csv(csv_path, dtype=str)
        modified = False

        for c in candidates:
            run_id = c["run_id"]
            symbol = c["symbol"]
            expected = c["expected"]

            mask = (
                (df["run_id"] == run_id)
                & (df["symbol"] == symbol)
                & (df["strategy"] == strategy)
            )
            idx_list = df.index[mask].tolist()

            if not idx_list:
                print(f"SKIP {run_id} {symbol}: no matching row")
                skipped += 1
                continue

            if len(idx_list) > 1 and "event_id" in df.columns:
                for ix in idx_list:
                    if run_id in str(df.at[ix, "event_id"]):
                        idx_list = [ix]
                        break

            if len(idx_list) > 1:
                print(f"SKIP {run_id} {symbol}: {len(idx_list)} matches, ambiguous")
                skipped += 1
                continue

            idx = idx_list[0]
            row = df.loc[idx]

            if str(row.get("outcome", "")) != "TIMEOUT":
                print(f"SKIP {run_id} {symbol}: already {row.get('outcome')}")
                unchanged += 1
                continue

            entry_price = float(row["entry"])
            tp_price = float(row["tp"])
            sl_price = float(row["sl"])

            entry_ts_str = ""
            try:
                details = json.loads(str(row.get("details_json", "{}")))
                entry_ts_str = details.get("entry_time_utc", "")
            except Exception:
                pass
            if not entry_ts_str:
                entry_ts_str = str(row.get("opened_ts") or row.get("outcome_time_utc", ""))
            if not entry_ts_str:
                print(f"SKIP {run_id} {symbol}: no entry timestamp")
                skipped += 1
                continue

            entry_ts = pd.Timestamp(entry_ts_str)
            if entry_ts.tzinfo is None:
                entry_ts = entry_ts.tz_localize("UTC")

            start_ms = int(entry_ts.timestamp() * 1000)
            end_ms = int((entry_ts + pd.Timedelta(minutes=OUTCOME_WATCH_MINUTES + 1)).timestamp() * 1000)

            print(f"FETCH {run_id} {symbol}: {entry_ts.strftime('%H:%M:%S')} +{OUTCOME_WATCH_MINUTES}m ...", end=" ")
            try:
                klines = _fetch_klines_1m(symbol, start_ms, end_ms)
                _time.sleep(KLINE_FETCH_DELAY_SEC)
            except Exception as e:
                print(f"FAILED ({e})")
                skipped += 1
                continue

            if klines is None or klines.empty:
                print("no klines")
                skipped += 1
                continue

            future = klines[klines["ts"] >= entry_ts]
            print(f"{len(future)} candles →", end=" ")

            result = _recompute_outcome(entry_price, tp_price, sl_price, entry_ts, klines)

            if not result.get("changed"):
                print("confirmed TIMEOUT")
                unchanged += 1
                continue

            new_outcome = result["outcome"]
            match_str = "✓" if new_outcome == expected else "≠"
            print(f"{new_outcome} (expected {expected} {match_str})")

            old_snapshot = row.to_dict()

            df.at[idx, "outcome"] = new_outcome
            df.at[idx, "pnl_pct"] = str(result["pnl_pct"])
            df.at[idx, "exit_price"] = str(result["exit_price"])
            df.at[idx, "hold_seconds"] = str(result["hold_seconds"])
            df.at[idx, "mfe_pct"] = str(result["mfe_pct"])
            df.at[idx, "mae_pct"] = str(result["mae_pct"])

            try:
                det = json.loads(str(old_snapshot.get("details_json", "{}")))
                det["timeout"] = False
                det["tp_hit"] = new_outcome == "TP_hit"
                det["sl_hit"] = new_outcome == "SL_hit"
                det["path_1m"] = result.get("path_1m", [])
                det["exit_price"] = result["exit_price"]
                det["mfe_pct"] = result["mfe_pct"]
                det["mae_pct"] = result["mae_pct"]
                det["hold_seconds"] = result["hold_seconds"]
                if "details_payload" in det:
                    try:
                        dp = json.loads(det["details_payload"])
                        dp["tp_hit"] = new_outcome == "TP_hit"
                        dp["sl_hit"] = new_outcome == "SL_hit"
                        dp["path_1m"] = result.get("path_1m", [])
                        det["details_payload"] = json.dumps(dp, ensure_ascii=False)
                    except Exception:
                        pass
                df.at[idx, "details_json"] = json.dumps(det, ensure_ascii=False)
            except Exception as e:
                print(f"  WARN: details_json update failed: {e}")

            new_snapshot = df.loc[idx].to_dict()

            audit_entries.append({
                "run_id": run_id,
                "symbol": symbol,
                "strategy": strategy,
                "date": date_str,
                "old_outcome": old_snapshot.get("outcome"),
                "new_outcome": new_outcome,
                "old_pnl_pct": old_snapshot.get("pnl_pct"),
                "new_pnl_pct": str(result["pnl_pct"]),
                "old_exit_price": old_snapshot.get("exit_price"),
                "new_exit_price": str(result["exit_price"]),
                "old": old_snapshot,
                "new": new_snapshot,
                "backfill_ts": datetime.now(timezone.utc).isoformat(),
                "reason": "stale_klines_false_timeout",
            })

            modified = True
            corrected += 1

        if modified and apply:
            if str(csv_path) not in backed_up_files:
                bak = _backup_file(csv_path, ts_str)
                backups_created.append(str(bak))
                backed_up_files.add(str(csv_path))
            df.to_csv(csv_path, index=False)
            print(f"  WRITTEN {csv_path}")

    if audit_entries:
        with open(audit_path, "w", encoding="utf-8") as f:
            for entry in audit_entries:
                f.write(json.dumps(entry, ensure_ascii=False, default=str) + "\n")

    mode = "APPLY" if apply else "DRY-RUN"
    print(f"\n{'='*40}")
    print(f"{mode} SUMMARY")
    print(f"{'='*40}")
    print(f"Corrected:  {corrected}")
    print(f"Unchanged:  {unchanged}")
    print(f"Skipped:    {skipped}")
    if backups_created:
        print(f"Backups:    {len(backups_created)}")
        for b in backups_created:
            print(f"  {b}")
    if audit_entries:
        print(f"Audit:      {audit_path}")


if __name__ == "__main__":
    main()
