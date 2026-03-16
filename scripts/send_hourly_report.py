#!/usr/bin/env python3
"""
Hourly report sender: invokes daily_tg_report.py with investor-report options.

Uses subprocess.run to call daily_tg_report.py (same generator as manual run).
Telegram credentials are loaded by daily_tg_report from /root/pump_short/.env
or environment (TG_BOT_TOKEN/TELEGRAM_BOT_TOKEN, TG_CHAT_ID/TELEGRAM_CHAT_ID).

Default config (can override via CLI):
- strategy: short_pump (use --strategy short_pump_fast0 for FAST0)
- days: 0 (auto full period from date=* folders) or N; fallback 30 if no date=* found
- rolling: 20
- maturity-threshold: 50
- Default: text-only (--investor-text-only, no PNGs). Use --charts for full report with 4 PNGs.

Usage:
    python3 scripts/send_hourly_report.py [--dry-run] [--strategy X] [--days N|0] [--rolling N] [--no-charts|--charts]
    # Hourly TG report for FAST0 (full dataset period):
    python3 scripts/send_hourly_report.py --strategy short_pump_fast0 --days 0
"""

import argparse
import os
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Optional

# Default config
DEFAULT_STRATEGY = "short_pump"
DEFAULT_DAYS = 0  # 0 = auto full period from date=*; fallback 30 if no folders
FALLBACK_DAYS = 30
DEFAULT_ROLLING = 20
DEFAULT_MATURITY_THRESHOLD = 50
DEFAULT_DATA_ROOT = "/root/pump_short/datasets"


def _find_roots(base_dir: Path) -> list[Path]:
    base_dir = Path(base_dir).resolve()
    candidates = [base_dir, base_dir / "datasets"]
    found = []
    for candidate in candidates:
        if not candidate.exists() or "/datasets/datasets" in str(candidate):
            continue
        date_dirs = [d for d in candidate.glob("date=*") if d.is_dir()]
        if date_dirs:
            found.append(candidate)
    return found


def _days_for_full_period(root_path: Path) -> Optional[int]:
    """Scan root for date=YYYYMMDD folders; return (today - earliest + 1) or None if none."""
    roots = _find_roots(root_path)
    if not roots:
        return None
    root = roots[0]
    today = date.today()
    dates_found = []
    for d in root.glob("date=*"):
        if not d.is_dir():
            continue
        try:
            # name is "date=YYYYMMDD"
            part = d.name.split("=", 1)[-1]
            dt = datetime.strptime(part, "%Y%m%d").date()
            dates_found.append(dt)
        except ValueError:
            continue
    if not dates_found:
        return None
    earliest = min(dates_found)
    return (today - earliest).days + 1


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run daily_tg_report.py with investor-report options (subprocess)"
    )
    parser.add_argument("--dry-run", action="store_true", help="Print command and exit without running")
    parser.add_argument("--strategy", type=str, default=DEFAULT_STRATEGY, help=f"Strategy (default: {DEFAULT_STRATEGY})")
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS,
                        help="Report window in days; 0=auto full period from date=* (default: 0)")
    parser.add_argument("--rolling", type=int, default=DEFAULT_ROLLING, help=f"Rolling window (default: {DEFAULT_ROLLING})")
    parser.add_argument("--maturity-threshold", type=int, default=DEFAULT_MATURITY_THRESHOLD,
                        help=f"Maturity threshold (default: {DEFAULT_MATURITY_THRESHOLD})")
    parser.add_argument("--no-charts", action="store_true",
                        help="Text-only (default): no PNG charts (uses --investor-text-only)")
    parser.add_argument("--charts", action="store_true",
                        help="Include 4 PNG charts (uses --investor-report)")
    parser.add_argument("--root", type=str, default=DEFAULT_DATA_ROOT,
                        help=f"Data root (default: {DEFAULT_DATA_ROOT})")
    args = parser.parse_args()

    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent
    root_path = Path(args.root)

    days = args.days
    if days == 0:
        auto_days = _days_for_full_period(root_path)
        if auto_days is not None:
            days = auto_days
        else:
            days = FALLBACK_DAYS
            print(f"WARNING: No date=* folders under {root_path}; using days={days}", file=sys.stderr)

    cmd = [
        sys.executable,
        "scripts/daily_tg_report.py",
        "--root", args.root,
        "--strategy", args.strategy,
        "--days", str(days),
        "--rolling", str(args.rolling),
        "--maturity-threshold", str(args.maturity_threshold),
    ]

    if args.charts:
        cmd.append("--investor-report")
    else:
        cmd.append("--investor-text-only")

    env = os.environ.copy()
    env["PYTHONPATH"] = str(project_root)

    if args.dry_run:
        print("=== DRY RUN ===")
        print(f"cwd: {project_root}")
        print(f"PYTHONPATH={env.get('PYTHONPATH')}")
        print("Command:", " ".join(cmd))
        sys.exit(0)

    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(
        cmd,
        cwd=str(project_root),
        env=env,
    )

    sys.exit(result.returncode)


if __name__ == "__main__":
    main()
