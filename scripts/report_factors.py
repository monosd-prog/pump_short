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
        help="Strategies to include (default: short_pump, short_pump_fast0)",
    )
    args = parser.parse_args()

    base_dir = args.data_dir
    days = int(args.days)
    strategies = args.strategies

    txt_path, json_path, summary = save_factor_report_files(
        base_dir=base_dir,
        days=days,
        strategies=strategies,
    )

    print(summary)
    print("")
    print(f"TXT report : {txt_path}")
    print(f"JSON report: {json_path}")


if __name__ == "__main__":
    main()
