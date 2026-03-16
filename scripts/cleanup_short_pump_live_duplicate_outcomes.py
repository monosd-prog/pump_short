#!/usr/bin/env python3
"""
Cleanup duplicate core outcomes (TP_hit/SL_hit/TIMEOUT) for short_pump live.

Analysis-safe:
- dry-run mode (default)
- apply mode with optional backup

Logic:
- Reads datasets/date=*/strategy=short_pump/mode=live/outcomes_v3.csv
- For each file:
  - groups core outcomes by trade_id (fallback event_id)
  - if multiple rows in a group:
      - if outcomes/pnl/exit_price are identical, keep the earliest by outcome_time_utc
      - if they differ (true conflict), log and skip (do not modify)
"""

from __future__ import annotations

import argparse
import csv
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple

CORE_OUTCOMES = ("TP_hit", "SL_hit", "TIMEOUT")


def _find_dataset_root(data_dir: Path) -> Path:
    data_dir = data_dir.resolve()
    sub = data_dir / "datasets"
    if sub.is_dir():
        return sub
    return data_dir


def _collect_outcomes_paths(root: Path) -> List[Path]:
    paths: List[Path] = []
    for date_dir in sorted(root.glob("date=*")):
        if not date_dir.is_dir():
            continue
        base = date_dir / "strategy=short_pump" / "mode=live"
        p = base / "outcomes_v3.csv"
        if p.is_file():
            paths.append(p)
    return paths


def _read_rows(path: Path) -> Tuple[List[Dict[str, str]], List[str]]:
    with open(path, "r", newline="", encoding="utf-8", errors="replace") as f:
        r = csv.DictReader(f)
        rows = list(r)
        fieldnames = list(r.fieldnames or [])
    return rows, fieldnames


def _is_core(row: Dict[str, str]) -> bool:
    o = (row.get("outcome") or "").strip()
    return o in CORE_OUTCOMES


def _group_key(row: Dict[str, str]) -> str:
    tid = (row.get("trade_id") or row.get("tradeId") or "").strip()
    if tid:
        return f"t:{tid}"
    eid = (row.get("event_id") or "").strip()
    if eid:
        return f"e:{eid}"
    return ""


def _same_outcome_payload(a: Dict[str, str], b: Dict[str, str]) -> bool:
    """Check if two rows are semantically the same final outcome."""
    keys = ["outcome", "pnl_r", "pnl_pct", "exit_price"]
    for k in keys:
        if (a.get(k) or "").strip() != (b.get(k) or "").strip():
            return False
    return True


def _choose_keep_indices(rows: List[Dict[str, str]], indices: List[int]) -> Tuple[bool, List[int]]:
    """
    Decide which indices to keep for one duplicate group.
    Returns (conflict, indices_to_keep).
    - If all rows have same outcome/pnl/exit_price: keep earliest by outcome_time_utc.
    - Else: conflict=True, keep all (caller will skip modifications).
    """
    base = rows[indices[0]]
    if not all(_same_outcome_payload(base, rows[i]) for i in indices[1:]):
        return True, indices[:]  # conflict

    # All equal by payload; keep earliest by outcome_time_utc
    best_idx = indices[0]
    best_ts = (rows[best_idx].get("outcome_time_utc") or "").strip()
    for i in indices[1:]:
        ts = (rows[i].get("outcome_time_utc") or "").strip()
        if ts and (not best_ts or ts < best_ts):
            best_idx = i
            best_ts = ts
    return False, [best_idx]


def _backup(path: Path) -> Path:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    backup = path.with_suffix(f".{ts}.bak")
    shutil.copy2(path, backup)
    return backup


def run_cleanup(data_dir: str | Path, *, dry_run: bool = True, backup: bool = False) -> Tuple[int, int, int]:
    """
    Run cleanup across all short_pump live outcomes_v3.
    Returns (files_modified, rows_removed, conflict_groups).
    """
    root = _find_dataset_root(Path(data_dir))
    paths = _collect_outcomes_paths(root)
    files_modified = 0
    rows_removed = 0
    conflicts = 0

    for path in paths:
        rows, fieldnames = _read_rows(path)
        if not rows:
            continue

        by_key: Dict[str, List[int]] = {}
        for idx, row in enumerate(rows):
            if not _is_core(row):
                continue
            k = _group_key(row)
            if not k:
                continue
            by_key.setdefault(k, []).append(idx)

        to_drop: set[int] = set()
        for k, idxs in by_key.items():
            if len(idxs) <= 1:
                continue
            conflict, keep = _choose_keep_indices(rows, idxs)
            if conflict:
                print(f"[CONFLICT] {path} group={k} indices={idxs}")
                conflicts += 1
                continue
            keep_set = set(keep)
            for i in idxs:
                if i not in keep_set:
                    to_drop.add(i)

        if not to_drop:
            continue

        if dry_run:
            print(f"[DRY-RUN] {path} would remove {len(to_drop)} duplicate core outcome row(s)")
            files_modified += 1
            rows_removed += len(to_drop)
            continue

        if backup:
            bak = _backup(path)
            print(f"[BACKUP] {path} -> {bak}")

        new_rows = [row for i, row in enumerate(rows) if i not in to_drop]
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            w.writeheader()
            w.writerows(new_rows)
        print(f"[APPLY] {path} removed {len(to_drop)} duplicate core outcome row(s)")
        files_modified += 1
        rows_removed += len(to_drop)

    return files_modified, rows_removed, conflicts


def main() -> None:
    ap = argparse.ArgumentParser(description="Cleanup duplicate core outcomes for short_pump live")
    ap.add_argument("--data-dir", default="/root/pump_short/datasets", help="Datasets root")
    ap.add_argument("--apply", action="store_true", help="Apply changes (default: dry-run)")
    ap.add_argument("--backup", action="store_true", help="Backup files before applying (only with --apply)")
    args = ap.parse_args()

    files_modified, rows_removed, conflicts = run_cleanup(
        args.data_dir,
        dry_run=not args.apply,
        backup=args.apply and args.backup,
    )
    mode = "APPLY" if args.apply else "DRY-RUN"
    print(f"SUMMARY [{mode}]: files_modified={files_modified} rows_removed={rows_removed} conflict_groups={conflicts}")
    if conflicts:
        print("NOTE: conflict groups were NOT modified. Inspect [CONFLICT] lines above.")


if __name__ == "__main__":
    main()

