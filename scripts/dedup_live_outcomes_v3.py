#!/usr/bin/env python3
"""
Safe deduplication of live outcomes_v3.csv.

Removes duplicate rows by trade_id (or fallback: strategy+run_id+event_id+symbol+outcome).
Keeps first occurrence. Creates backup before modify.

Scope: datasets/date=*/strategy=*/mode=live/outcomes_v3.csv only.

Usage:
  python3 scripts/dedup_live_outcomes_v3.py --root /root/pump_short
  python3 scripts/dedup_live_outcomes_v3.py --root . --apply
"""
from __future__ import annotations

import argparse
import csv
import shutil
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from trading.config import DATASET_BASE_DIR


def _dedup_key(row: dict) -> str:
    """Primary: trade_id. Fallback: strategy|run_id|event_id|symbol|outcome."""
    tid = (row.get("trade_id") or row.get("tradeId") or "").strip()
    if tid:
        return f"trade_id:{tid}"
    s = (row.get("strategy") or "").strip()
    r = (row.get("run_id") or "").strip()
    e = (row.get("event_id") or row.get("eventId") or "").strip()
    sym = (row.get("symbol") or "").strip()
    o = (row.get("outcome") or row.get("end_reason") or "").strip()
    return f"fallback:{s}|{r}|{e}|{sym}|{o}"


def _find_live_outcome_files(root: Path) -> list[tuple[Path, str, str]]:
    """Find outcomes_v3.csv under date=*/strategy=*/mode=live. Return (path, date, strategy)."""
    root = Path(root).resolve()
    candidates = [root, root / "datasets"]
    found: list[tuple[Path, str, str]] = []
    for base in candidates:
        if not base.exists():
            continue
        for date_dir in sorted(base.glob("date=*")):
            if not date_dir.is_dir():
                continue
            date_val = date_dir.name.replace("date=", "")
            for strat_dir in date_dir.iterdir():
                if not strat_dir.is_dir() or not strat_dir.name.startswith("strategy="):
                    continue
                strategy = strat_dir.name.replace("strategy=", "")
                live_dir = strat_dir / "mode=live"
                p = live_dir / "outcomes_v3.csv"
                if p.exists():
                    found.append((p, date_val, strategy))
    return found


def _read_csv(path: Path) -> tuple[list[dict], list[str]]:
    with open(path, "r", newline="", encoding="utf-8", errors="replace") as f:
        r = csv.DictReader(f)
        fieldnames = list(r.fieldnames or [])
        rows = list(r)
    return rows, fieldnames


def _write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def _rel_path(path: Path, root: Path) -> str:
    """Safe relative path; fallback to full path if not under root."""
    try:
        return str(path.resolve().relative_to(root.resolve()))
    except ValueError:
        return str(path.resolve())


def _deduplicate(rows: list[dict]) -> tuple[list[dict], int]:
    """
    Deduplicate by key. Keep first occurrence.
    Returns (deduplicated_rows, n_removed).
    """
    seen: set[str] = set()
    kept: list[dict] = []
    removed = 0
    for r in rows:
        k = _dedup_key(r)
        if k in seen:
            removed += 1
            continue
        seen.add(k)
        kept.append(r)
    return kept, removed


def run(root: Path, apply: bool) -> dict:
    root = Path(root).resolve()
    files_found = _find_live_outcome_files(root)
    files_changed = 0
    total_before = 0
    total_after = 0
    total_removed = 0
    by_strategy_date: dict[str, dict] = defaultdict(lambda: {"before": 0, "after": 0, "removed": 0})

    for path, date_val, strategy in files_found:
        rows, fieldnames = _read_csv(path)
        total_before += len(rows)
        by_strategy_date[f"{date_val}/{strategy}"]["before"] = len(rows)

        kept, removed = _deduplicate(rows)
        total_removed += removed
        total_after += len(kept)
        by_strategy_date[f"{date_val}/{strategy}"]["after"] = len(kept)
        by_strategy_date[f"{date_val}/{strategy}"]["removed"] = removed

        if removed > 0 and apply:
            ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            backup = path.parent / f"outcomes_v3.csv.bak_{ts}"
            shutil.copy2(path, backup)
            _write_csv(path, kept, fieldnames)
            files_changed += 1
            print(f"  APPLIED | {_rel_path(path, root)} | backup={backup.name} | removed={removed}", flush=True)
        elif removed > 0:
            print(f"  DRY-RUN | {_rel_path(path, root)} | would_remove={removed}", flush=True)

    return {
        "files_scanned": len(files_found),
        "files_changed": files_changed,
        "rows_before": total_before,
        "rows_after": total_after,
        "duplicates_removed": total_removed,
        "by_strategy_date": dict(by_strategy_date),
    }


def _print_summary(s: dict, apply: bool) -> None:
    print("\n--- Dedup Live Outcomes v3 ---")
    print(f"  mode:              {'APPLY' if apply else 'DRY-RUN'}")
    print(f"  files scanned:     {s['files_scanned']}")
    print(f"  files changed:     {s['files_changed']}")
    print(f"  rows before:       {s['rows_before']}")
    print(f"  rows after:        {s['rows_after']}")
    print(f"  duplicates removed:{s['duplicates_removed']}")
    by_sd = s.get("by_strategy_date") or {}
    if by_sd:
        print("\n--- By date/strategy ---")
        for k in sorted(by_sd.keys()):
            v = by_sd[k]
            if v.get("removed", 0) > 0:
                print(f"  {k}: before={v['before']} after={v['after']} removed={v['removed']}")
    print()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Dedup live outcomes_v3.csv by trade_id (dry-run by default)"
    )
    parser.add_argument("--root", type=Path, default=Path(DATASET_BASE_DIR), help="Datasets root")
    parser.add_argument("--apply", action="store_true", help="Apply changes (default: dry-run)")
    args = parser.parse_args()
    root_path = Path(args.root).resolve()
    summary = run(root_path, apply=args.apply)
    _print_summary(summary, apply=args.apply)
    return 0


if __name__ == "__main__":
    sys.exit(main())
