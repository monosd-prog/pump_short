#!/usr/bin/env python3
"""
Backfill risk_profile for existing short_pump_fast0 mode=paper outcomes (TP_hit, SL_hit, TIMEOUT)
that were written without risk_profile (root cause: fast0_sampler paper path did not set summary["risk_profile"]).

Uses events (same date/strategy/mode) to get liq_long_usd_30s and dist_to_peak_pct, then
get_risk_profile("short_pump_fast0", ...) to set risk_profile. Only updates rows where
risk_profile is empty/NaN. Safe to run multiple times (idempotent).

Usage:
  PYTHONPATH=. python3 scripts/backfill_fast0_paper_risk_profile.py --data-dir /root/pump_short/datasets
  PYTHONPATH=. python3 scripts/backfill_fast0_paper_risk_profile.py --data-dir /root/pump_short/datasets --dry-run
  PYTHONPATH=. python3 scripts/backfill_fast0_paper_risk_profile.py --data-dir /root/pump_short/datasets --backup
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

PAPER_CORE_OUTCOMES = ("TP_hit", "SL_hit", "TIMEOUT")
STRATEGY = "short_pump_fast0"
MODE = "paper"


def _empty_risk_profile(val) -> bool:
    if val is None:
        return True
    s = str(val).strip()
    return s == "" or s.lower() in ("nan", "none")


def _load_events_for_date(events_path: str):
    """Load events CSV into list of dicts keyed by event_id. Returns {} on error."""
    if not os.path.isfile(events_path):
        return {}
    try:
        with open(events_path, "r", newline="", encoding="utf-8", errors="replace") as f:
            r = csv.DictReader(f)
            rows = list(r)
        by_ev = {}
        for row in rows:
            eid = (row.get("event_id") or "").strip()
            if eid:
                by_ev[eid] = row
        return by_ev
    except Exception:
        return {}


def _get_risk_profile_for_row(event_row: dict) -> str:
    try:
        from trading.risk_profile import get_risk_profile
        liq = event_row.get("liq_long_usd_30s")
        dist = event_row.get("dist_to_peak_pct")
        try:
            liq_f = float(liq) if liq is not None and str(liq).strip() not in ("", "nan") else 0.0
        except (TypeError, ValueError):
            liq_f = 0.0
        try:
            dist_f = float(dist) if dist is not None and str(dist).strip() not in ("", "nan") else None
        except (TypeError, ValueError):
            dist_f = None
        rp, _, _ = get_risk_profile(
            "short_pump_fast0",
            liq_long_usd_30s=liq_f,
            dist_to_peak_pct=dist_f,
        )
        return (rp or "").strip()
    except Exception:
        return ""


def run_backfill(
    data_dir: str | Path,
    *,
    dry_run: bool = False,
    backup: bool = False,
) -> tuple[int, int]:
    """
    Scan date=*/strategy=short_pump_fast0/mode=paper/, fix outcome rows missing risk_profile.
    Returns (n_files_updated, n_rows_updated).
    """
    data_dir = Path(data_dir)
    if not data_dir.is_dir():
        print(f"ERROR: not a directory: {data_dir}")
        return 0, 0

    n_files = 0
    n_rows = 0
    date_dirs = sorted(data_dir.glob("date=*"))
    for date_dir in date_dirs:
        if not date_dir.is_dir():
            continue
        strategy_dir = date_dir / f"strategy={STRATEGY}"
        if not strategy_dir.is_dir():
            continue
        mode_dir = strategy_dir / f"mode={MODE}"
        if not mode_dir.is_dir():
            continue
        outcomes_path = mode_dir / "outcomes_v3.csv"
        if not outcomes_path.is_file():
            continue

        events_path = mode_dir / "events_v2.csv"
        if not events_path.is_file():
            events_path = mode_dir / "events_v3.csv"
        events_by_id = _load_events_for_date(str(events_path))

        with open(outcomes_path, "r", newline="", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames or []
            rows = list(reader)

        if "risk_profile" not in fieldnames:
            fieldnames = list(fieldnames) + ["risk_profile"]

        updated_any = False
        file_rows_updated = 0
        for row in rows:
            strat = (row.get("strategy") or "").strip()
            mode_val = (row.get("mode") or "").strip().lower()
            outcome = (row.get("outcome") or "").strip()
            if strat != STRATEGY or mode_val != MODE:
                continue
            if outcome not in PAPER_CORE_OUTCOMES:
                continue
            if not _empty_risk_profile(row.get("risk_profile")):
                continue
            event_id = (row.get("event_id") or "").strip()
            event_row = events_by_id.get(event_id) if event_id else None
            if not event_row:
                continue
            rp = _get_risk_profile_for_row(event_row)
            if not rp:
                continue
            row["risk_profile"] = rp
            updated_any = True
            file_rows_updated += 1
            n_rows += 1

        if updated_any and not dry_run:
            if backup:
                shutil.copy2(outcomes_path, str(outcomes_path) + ".bak")
            with open(outcomes_path, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
                w.writeheader()
                w.writerows(rows)
            n_files += 1
            print(f"  updated {outcomes_path} ({file_rows_updated} rows)")
        elif updated_any and dry_run:
            print(f"  [dry-run] would update {outcomes_path} ({file_rows_updated} rows)")
            n_files += 1

    return n_files, n_rows


def main() -> None:
    ap = argparse.ArgumentParser(description="Backfill risk_profile for short_pump_fast0 paper outcomes (TP/SL/TIMEOUT)")
    ap.add_argument("--data-dir", default=None, help="Datasets root (e.g. /root/pump_short/datasets)")
    ap.add_argument("--dry-run", action="store_true", help="Do not write; only report what would be updated")
    ap.add_argument("--backup", action="store_true", help="Create .bak copy before overwriting CSV")
    args = ap.parse_args()
    data_dir = args.data_dir or os.getenv("DATASET_BASE_DIR") or os.path.join(_ROOT, "datasets")
    n_files, n_rows = run_backfill(data_dir, dry_run=args.dry_run, backup=args.backup)
    print(f"Done: {n_files} file(s) updated, {n_rows} row(s) with risk_profile set")
    if args.dry_run and n_rows:
        print("Run without --dry-run to apply changes.")


if __name__ == "__main__":
    main()
