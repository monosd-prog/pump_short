#!/usr/bin/env python3
"""
Historical normalization: fix short_pump live events/trades rows where path is mode=live
but row fields are mode=paper/source_mode=paper.

Scope: strategy=short_pump only, mode=live path only.
Files: events_v3.csv, trades_v3.csv, events.csv, trades.csv (optional).
Does NOT touch outcomes.

Usage:
  python3 scripts/normalize_short_pump_live_rows.py --root /root/pump_short/datasets
  python3 scripts/normalize_short_pump_live_rows.py --root /root/pump_short/datasets --apply
"""
from __future__ import annotations

import argparse
import csv
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path


TARGET_FILES = ("events_v3.csv", "trades_v3.csv", "events.csv", "trades.csv")
DEFAULT_ROOT = "/root/pump_short/datasets"


def _find_live_files(base: Path) -> list[Path]:
    base = Path(base).resolve()
    candidates = [base, base / "datasets"]
    found: list[Path] = []
    for root in candidates:
        if not root.exists():
            continue
        for d in sorted(root.glob("date=*")):
            if not d.is_dir():
                continue
            live_dir = d / "strategy=short_pump" / "mode=live"
            if not live_dir.is_dir():
                continue
            for fname in TARGET_FILES:
                p = live_dir / fname
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


def _normalize_row(row: dict, fieldnames: list[str]) -> tuple[bool, dict]:
    """
    If mode != live or source_mode != live, rewrite to live.
    Returns (changed, row_dict).
    Handles absent source_mode: update mode only, skip source_mode.
    """
    changed = False
    out = dict(row)

    has_mode = "mode" in fieldnames
    has_source_mode = "source_mode" in fieldnames

    if has_mode:
        m = (row.get("mode") or "").strip().lower()
        if m != "live":
            out["mode"] = "live"
            changed = True

    if has_source_mode:
        sm = (row.get("source_mode") or "").strip().lower()
        if sm != "live":
            out["source_mode"] = "live"
            changed = True

    return changed, out


def _process_file(path: Path, apply: bool) -> tuple[int, int, str | None]:
    """
    Process one CSV file. Return (rows_found, rows_normalized, backup_path or None).
    """
    rows, fieldnames = _read_csv(path)
    rows_found = len(rows)
    if not rows:
        return rows_found, 0, None

    normalized: list[dict] = []
    rows_normalized = 0
    for r in rows:
        changed, new_row = _normalize_row(r, fieldnames)
        if changed:
            rows_normalized += 1
        normalized.append(new_row)

    if rows_normalized == 0:
        return rows_found, 0, None

    backup_path: str | None = None
    if apply:
        backup = _backup(path)
        backup_path = str(backup)
        _write_csv(path, normalized, fieldnames)

    return rows_found, rows_normalized, backup_path


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Normalize short_pump live rows: mode=paper→live, source_mode=paper→live (dry-run by default)"
    )
    parser.add_argument("--root", type=Path, default=Path(DEFAULT_ROOT), help="Datasets root")
    parser.add_argument("--apply", action="store_true", help="Apply changes (default: dry-run)")
    args = parser.parse_args()

    files = _find_live_files(args.root)
    if not files:
        print(f"No short_pump mode=live files found under {args.root}", file=sys.stderr)
        return 1

    total_normalized = 0
    files_changed = 0

    for path in files:
        rows_found, rows_normalized, backup_path = _process_file(path, args.apply)
        if rows_normalized > 0:
            files_changed += 1
            total_normalized += rows_normalized
            try:
                rel = path.relative_to(Path(args.root).resolve())
            except ValueError:
                rel = path
            print(f"\n--- {rel} ---")
            print(f"  rows_found: {rows_found}")
            print(f"  rows_normalized: {rows_normalized}")
            if args.apply and backup_path:
                print(f"  backup: {backup_path}")
                print(f"  applied: wrote {rows_found} rows")
            else:
                print(f"  (dry-run; use --apply to write)")

    print("\n--- Summary ---")
    print(f"  files_scanned: {len(files)}")
    print(f"  files_changed: {files_changed}")
    print(f"  total_rows_normalized: {total_normalized}")
    if total_normalized and not args.apply:
        print("  (dry-run; use --apply to write)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
