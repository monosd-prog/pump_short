#!/usr/bin/env python3
"""
Safe normalization for events_v3.csv:
- Re-align columns to the current EVENT_FIELDS_V3 order.
- Backfill empty factor columns from payload_json (via factor_report._fill_from_payload_json).

This is an offline dataset hygiene step (no trading logic changes).
"""

from __future__ import annotations

import argparse
import datetime as dt
import shutil
from pathlib import Path

import pandas as pd

from analytics.factor_report import _fill_from_payload_json
from analytics.load import _read_csv
from common.dataset_schema import EVENT_FIELDS_V3


def _parse_date_dir(p: Path) -> dt.date | None:
    # expected: date=YYYYMMDD
    if p.name.startswith("date="):
        v = p.name.split("=", 1)[1]
        if len(v) == 8 and v.isdigit():
            return dt.datetime.strptime(v, "%Y%m%d").date()
    return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", required=True, help="datasets root directory (contains date=YYYYMMDD/...)")
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--strategies", default=",".join(["short_pump", "short_pump_fast0"]))
    ap.add_argument("--modes", default="live,paper")
    args = ap.parse_args()

    base_dir = Path(args.data_dir)
    strategies = [s.strip() for s in args.strategies.split(",") if s.strip()]
    modes = [m.strip() for m in args.modes.split(",") if m.strip()]

    now = dt.datetime.now(dt.timezone.utc).date()
    start = now - dt.timedelta(days=max(0, int(args.days) - 1))
    end = now

    date_dirs = []
    for p in base_dir.glob("date=*"):
        d = _parse_date_dir(p)
        if d is None:
            continue
        if start <= d <= end:
            date_dirs.append((d, p))
    date_dirs.sort(key=lambda x: x[0])

    if not date_dirs:
        raise SystemExit(f"No date dirs found in range {start}..{end} under {base_dir}")

    print(f"[NORMALIZE_EVENTS_V3] range={start}..{end} strategies={strategies} modes={modes}")

    updated = 0
    for d, date_dir in date_dirs:
        date_str = f"{d.year:04d}{d.month:02d}{d.day:02d}"
        for strat in strategies:
            for mode in modes:
                path = date_dir / f"strategy={strat}" / f"mode={mode}" / "events_v3.csv"
                if not path.exists():
                    continue

                # Read using project loader (handles legacy headers).
                df = _read_csv(path)
                if df.empty:
                    continue

                # Backfill missing values from payload_json (factor_report fallback).
                df2 = _fill_from_payload_json(df)

                # Force canonical column ordering & schema.
                df2 = df2.reindex(columns=EVENT_FIELDS_V3)
                df2 = df2.fillna("")

                backup = path.with_suffix(path.suffix + ".bak_norm")
                if not backup.exists():
                    shutil.copy2(path, backup)

                df2.to_csv(path, index=False)
                updated += 1
                print(f"  updated {path.relative_to(base_dir)} rows={len(df2)}")

    print(f"[NORMALIZE_EVENTS_V3] done updated_files={updated}")


if __name__ == "__main__":
    main()

