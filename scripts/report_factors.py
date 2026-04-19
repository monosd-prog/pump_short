#!/usr/bin/env python3
"""
Factor report CLI.

Usage (from pump_short root):
  PYTHONPATH=. python3 scripts/report_factors.py --days 30 --data-dir /root/pump_short/datasets
  PYTHONPATH=. python3 scripts/report_factors.py --days 30 --strategies short_pump short_pump_fast0

CLI:
- вызывает analytics.factor_report.save_factor_report_files
- печатает краткий summary
- печатает пути к TXT и JSON отчётам
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# Ensure pump_short root on path
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from analytics.factor_report import save_factor_report_files


def main() -> None:
    parser = argparse.ArgumentParser(description="Factor report (single factors, combinations, paper candidates)")
    parser.add_argument(
        "--data-dir",
        type=str,
        default=os.getenv("FACTOR_REPORT_DATA_ROOT", "/root/pump_short/datasets"),
        help="Datasets root (default: /root/pump_short/datasets or $FACTOR_REPORT_DATA_ROOT)",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Days window (lookback from datasets date=* folders)",
    )
    parser.add_argument(
        "--strategies",
        type=str,
        nargs="*",
        default=None,
        help="Strategies to include (default: short_pump, short_pump_fast0, short_pump_premium, short_pump_wick)",
    )
    parser.add_argument(
        "--mode",
        type=str,
        default="live",
        help="Datasets mode to read (live|paper|lab). Default: live",
    )
    parser.add_argument(
        "--min-date",
        type=str,
        default="20260401",
        help=(
            "Fresh canonical window: only dates >= YYYYMMDD (default: 20260401). "
            "Use 'all' to disable and fall back to --days window."
        ),
    )
    parser.add_argument(
        "--fresh",
        action="store_true",
        help="Shortcut: use fresh canonical window from 20260401 (same as --min-date 20260401)",
    )
    args = parser.parse_args()

    base_dir = args.data_dir
    days = int(args.days)
    strategies = args.strategies
    mode = str(args.mode)

    # Resolve min_date: --fresh forces 20260401; 'all' disables fresh filter
    raw_min_date = "20260401" if args.fresh else str(args.min_date)
    min_date: str | None = None if raw_min_date.strip().lower() == "all" else raw_min_date

    if mode.strip().lower() == "lab":
        print("[LAB MODE V1] WARNING: factor report on mode=lab is candle-only.")
        print("[LAB MODE V1] WARNING: delta/cvd/liq/oi/funding/spread/orderbook/context/stage are NOT reconstructed unless historical sources are added.")
        print("[LAB MODE V1] TIP: use audit_feature_contract.py --mode lab to inspect coverage.")
        print("")

    if min_date:
        print(f"[FACTOR REPORT v2] Fresh canonical window: date >= {min_date}")
    else:
        print(f"[FACTOR REPORT v2] Legacy mode: days={days}")
    print("")

    txt_path, json_path, summary = save_factor_report_files(
        base_dir=base_dir,
        days=days,
        strategies=strategies,
        mode=mode,
        min_date=min_date,
    )

    print(summary)
    print("")
    print(f"TXT report : {txt_path}")
    print(f"JSON report: {json_path}")


if __name__ == "__main__":
    main()
