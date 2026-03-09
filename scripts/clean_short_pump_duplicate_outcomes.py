#!/usr/bin/env python3
"""
Historical cleanup: remove duplicate short_pump live outcomes.

Scope: strategy=short_pump, mode=live, outcomes_v3.csv only.
Duplicate resolution by event_id:
  - If group has non-TIMEOUT (TP_hit/SL_hit) + TIMEOUT → keep non-TIMEOUT, drop TIMEOUT
  - If multiple TIMEOUT-only → keep one
  - If multiple non-TIMEOUT (ambiguous) → warn and skip

Usage:
  python3 scripts/clean_short_pump_duplicate_outcomes.py --root /root/pump_short/datasets
  python3 scripts/clean_short_pump_duplicate_outcomes.py --root /root/pump_short/datasets --apply
"""
from __future__ import annotations

import argparse
import csv
import shutil
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


NON_TIMEOUT_OUTCOMES = ("TP_hit", "SL_hit")
DEFAULT_ROOT = "/root/pump_short/datasets"


def _event_id(row: dict) -> str:
    return (row.get("event_id") or row.get("eventId") or "").strip()


def _outcome_val(row: dict) -> str:
    return (row.get("outcome") or row.get("end_reason") or "").strip()


def _is_timeout(row: dict) -> bool:
    o = _outcome_val(row).upper()
    return o == "TIMEOUT"


def _is_non_timeout(row: dict) -> bool:
    o = _outcome_val(row)
    return o in NON_TIMEOUT_OUTCOMES


def _find_outcome_files(base: Path) -> list[Path]:
    base = Path(base).resolve()
    candidates = [base, base / "datasets"]
    found: list[Path] = []
    for root in candidates:
        if not root.exists():
            continue
        for d in sorted(root.glob("date=*")):
            if not d.is_dir():
                continue
            p = d / "strategy=short_pump" / "mode=live" / "outcomes_v3.csv"
            if p.exists():
                found.append(p)
    return found


def _read_csv(path: Path) -> tuple[list[dict], list[str]]:
    with open(path, "r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        fieldnames = list(r.fieldnames or [])
        rows = list(r)
    return rows, fieldnames


def _write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def _backup(path: Path) -> Path:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    backup = path.with_suffix(f".{ts}.bak")
    shutil.copy2(path, backup)
    return backup


def _resolve_duplicates(rows: list[dict]) -> tuple[list[dict], list[dict], list[str]]:
    """
    Group by event_id, decide which rows to keep and drop.
    Returns (rows_to_keep, rows_to_drop, warnings).
    """
    by_event: dict[str, list[tuple[int, dict]]] = defaultdict(list)
    no_event_indices: set[int] = set()
    for i, r in enumerate(rows):
        eid = _event_id(r)
        if eid:
            by_event[eid].append((i, r))
        else:
            no_event_indices.add(i)

    keep_indices: set[int] = set(no_event_indices)
    drop_indices: set[int] = set()
    warnings: list[str] = []

    for event_id, items in by_event.items():
        if len(items) <= 1:
            keep_indices.add(items[0][0])
            continue

        non_timeout = [(i, r) for i, r in items if _is_non_timeout(r)]
        timeout = [(i, r) for i, r in items if _is_timeout(r)]
        other = [(i, r) for i, r in items if not _is_non_timeout(r) and not _is_timeout(r)]

        if len(non_timeout) > 1:
            warnings.append(f"event_id={event_id}: multiple non-TIMEOUT outcomes {[_outcome_val(r) for _, r in non_timeout]} — skip group")
            keep_indices.update(i for i, _ in items)
            continue

        if non_timeout:
            keep_indices.add(non_timeout[0][0])
            drop_indices.update(i for i, _ in timeout)
            if other:
                drop_indices.update(i for i, _ in other)
            continue

        if timeout:
            keep_indices.add(timeout[0][0])
            drop_indices.update(i for i, _ in timeout[1:])
            if other:
                drop_indices.update(i for i, _ in other)
            continue

        if other:
            keep_indices.add(other[0][0])
            drop_indices.update(i for i, _ in other[1:])

    rows_to_keep = [r for i, r in enumerate(rows) if i not in drop_indices]
    rows_to_drop = [r for i, r in enumerate(rows) if i in drop_indices]
    return rows_to_keep, rows_to_drop, warnings


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Clean duplicate short_pump live outcomes (dry-run by default)"
    )
    parser.add_argument("--root", type=Path, default=Path(DEFAULT_ROOT), help="Datasets root")
    parser.add_argument("--apply", action="store_true", help="Apply changes (default: dry-run)")
    args = parser.parse_args()

    files = _find_outcome_files(args.root)
    if not files:
        print(f"No short_pump mode=live outcomes_v3.csv found under {args.root}", file=sys.stderr)
        return 1

    total_duplicate_groups = 0
    total_rows_dropped = 0
    total_warnings = 0

    for path in files:
        rows, fieldnames = _read_csv(path)
        if not rows:
            continue

        rows_to_keep, rows_to_drop, warnings = _resolve_duplicates(rows)
        for w in warnings:
            print(f"WARN {path.name}: {w}", file=sys.stderr)
            total_warnings += 1

        if not rows_to_drop:
            continue

        date_part = path.parent.parent.parent.name
        dup_event_ids = {_event_id(r) for r in rows_to_drop if _event_id(r)}
        print(f"\n--- {date_part} {path.name} ---")
        print(f"  duplicate_groups_found: {len(dup_event_ids)}")
        print(f"  rows_to_drop: {len(rows_to_drop)}")
        for r in rows_to_drop:
            print(f"    event_id={_event_id(r)} outcome={_outcome_val(r)}")

        total_duplicate_groups += 1
        total_rows_dropped += len(rows_to_drop)

        if args.apply:
            backup = _backup(path)
            _write_csv(path, rows_to_keep, fieldnames)
            print(f"  backup: {backup}")
            print(f"  applied: wrote {len(rows_to_keep)} rows")
        else:
            print(f"  (dry-run; use --apply to write)")

    print("\n--- Summary ---")
    print(f"  files_scanned: {len(files)}")
    print(f"  files_with_duplicates: {total_duplicate_groups}")
    print(f"  total_rows_to_drop: {total_rows_dropped}")
    print(f"  warnings: {total_warnings}")
    if total_rows_dropped and not args.apply:
        print("  (dry-run; use --apply to write)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
