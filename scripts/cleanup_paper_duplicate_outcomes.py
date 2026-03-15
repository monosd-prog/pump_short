#!/usr/bin/env python3
"""
Remove duplicate final outcomes for the same paper trade (mode=paper only).

Symptom: one trade had both SL_hit/TP_hit and TIMEOUT written. This script:
- Scans date=*/strategy=*/mode=paper/outcomes_v3.csv
- Groups rows by trade_id (fallback event_id)
- For each group with multiple final outcomes (TP_hit, SL_hit, TIMEOUT):
  keeps the first non-TIMEOUT final outcome, or the first by outcome_time_utc;
  drops duplicate TIMEOUT when TP_hit/SL_hit exists
- Writes back only mode=paper; safe and idempotent (re-run leaves correct state).

Usage:
  PYTHONPATH=. python3 scripts/cleanup_paper_duplicate_outcomes.py --data-dir /root/pump_short/datasets --dry-run
  PYTHONPATH=. python3 scripts/cleanup_paper_duplicate_outcomes.py --data-dir /root/pump_short/datasets [--backup]
"""

from __future__ import annotations

import argparse
import csv
import os
import shutil
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

FINAL_OUTCOMES = ("TP_hit", "SL_hit", "TIMEOUT")


def _normalize_outcome(o: str) -> str:
    s = (o or "").strip()
    if s in FINAL_OUTCOMES:
        return s
    return s or ""


def _key_for_row(row: dict) -> str:
    tid = (row.get("trade_id") or row.get("tradeId") or "").strip()
    if tid:
        return f"t:{tid}"
    eid = (row.get("event_id") or "").strip()
    return f"e:{eid}" if eid else ""


def _is_final_outcome(row: dict) -> bool:
    return _normalize_outcome(row.get("outcome") or "") in FINAL_OUTCOMES


def _outcome_priority(outcome: str) -> int:
    """Lower = prefer to keep. TP_hit/SL_hit before TIMEOUT."""
    o = (outcome or "").strip()
    if o == "TIMEOUT":
        return 2
    if o in ("TP_hit", "SL_hit"):
        return 1
    return 0


def _choose_keep_indices(rows: list[dict], final_indices: list[int]) -> set[int]:
    """From rows and indices of final-outcome rows, return set of indices to keep (one per trade)."""
    if len(final_indices) <= 1:
        return set(final_indices)
    # Prefer non-TIMEOUT; then first by outcome_time_utc
    best_idx = final_indices[0]
    best_pri = _outcome_priority(rows[best_idx].get("outcome"))
    best_ts = (rows[best_idx].get("outcome_time_utc") or "").strip()
    for i in final_indices[1:]:
        r = rows[i]
        pri = _outcome_priority(r.get("outcome"))
        ts = (r.get("outcome_time_utc") or "").strip()
        if pri < best_pri or (pri == best_pri and ts and (not best_ts or ts < best_ts)):
            best_pri = pri
            best_ts = ts
            best_idx = i
    return {best_idx}


def run_cleanup(data_dir: str | Path, *, dry_run: bool = False, backup: bool = False) -> tuple[int, int]:
    """
    Scan mode=paper outcome files, remove duplicate final outcomes per trade.
    Returns (n_files_modified, n_rows_removed).
    """
    data_dir = Path(data_dir)
    if not data_dir.is_dir():
        print(f"ERROR: not a directory: {data_dir}")
        return 0, 0
    n_files = 0
    n_removed = 0
    for date_dir in sorted(data_dir.glob("date=*")):
        if not date_dir.is_dir():
            continue
        for strategy_dir in date_dir.iterdir():
            if not strategy_dir.is_dir() or not strategy_dir.name.startswith("strategy="):
                continue
            mode_dir = strategy_dir / "mode=paper"
            if not mode_dir.is_dir():
                continue
            path = mode_dir / "outcomes_v3.csv"
            if not path.is_file():
                continue
            rows = []
            try:
                with open(path, "r", newline="", encoding="utf-8", errors="replace") as f:
                    r = csv.DictReader(f)
                    fieldnames = r.fieldnames or []
                    rows = list(r)
            except Exception as e:
                print(f"  skip {path}: {e}")
                continue
            if not rows:
                continue
            by_key: dict[str, list[int]] = {}
            for i, row in enumerate(rows):
                if not _is_final_outcome(row):
                    continue
                k = _key_for_row(row)
                if not k:
                    continue
                by_key.setdefault(k, []).append(i)
            to_drop = set()
            for k, indices in by_key.items():
                if len(indices) <= 1:
                    continue
                to_keep = _choose_keep_indices(rows, indices)
                for i in indices:
                    if i not in to_keep:
                        to_drop.add(i)
            if not to_drop:
                continue
            new_rows = [row for i, row in enumerate(rows) if i not in to_drop]
            n_removed += len(to_drop)
            if dry_run:
                print(f"  [dry-run] would remove {len(to_drop)} row(s) from {path}")
                n_files += 1
                continue
            if backup:
                shutil.copy2(path, str(path) + ".bak")
            with open(path, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
                w.writeheader()
                w.writerows(new_rows)
            print(f"  cleaned {path} (removed {len(to_drop)} duplicate(s))")
            n_files += 1
    return n_files, n_removed


def main() -> None:
    ap = argparse.ArgumentParser(description="Remove duplicate final outcomes for same paper trade (mode=paper only)")
    ap.add_argument("--data-dir", default=None, help="Datasets root")
    ap.add_argument("--dry-run", action="store_true", help="Do not write; only report")
    ap.add_argument("--backup", action="store_true", help="Create .bak before overwriting")
    args = ap.parse_args()
    data_dir = args.data_dir or os.getenv("DATASET_BASE_DIR") or os.path.join(_ROOT, "datasets")
    n_files, n_removed = run_cleanup(data_dir, dry_run=args.dry_run, backup=args.backup)
    print(f"Done: {n_files} file(s) updated, {n_removed} duplicate row(s) removed")
    if args.dry_run and n_removed:
        print("Run without --dry-run to apply.")


if __name__ == "__main__":
    main()
