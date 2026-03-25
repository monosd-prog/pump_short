#!/usr/bin/env python3
"""
auto_debug.py — TASK_B54_AUTO_DEBUG_PIPELINE  (MVP iteration 1)

Automatic log analysis for pump_short trading bot on VPS.
Read-only. No code changes. No service restarts. Stdout only.

Usage:
  # From project root:
  python3 scripts/auto_debug.py

  # With custom signal-gap threshold:
  python3 scripts/auto_debug.py --hours-threshold 4

  # Custom project root (e.g. on VPS):
  python3 scripts/auto_debug.py --root /root/pump_short

  # Suppress state detail section:
  python3 scripts/auto_debug.py --no-state-detail

  # Verbose: show collected raw data summary:
  python3 scripts/auto_debug.py --verbose
"""
from __future__ import annotations

import argparse
import sys
import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Path bootstrap: allow running as `python3 scripts/auto_debug.py` from
# the project root without installing the package.
# ---------------------------------------------------------------------------
_HERE = Path(__file__).resolve()
_PROJECT_ROOT = _HERE.parent.parent  # scripts/ → project root
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from scripts.debug_modules.log_reader import collect as collect_raw
from scripts.debug_modules.state_checker import build as build_state
from scripts.debug_modules.issue_detector import detect
from scripts.debug_modules.report_builder import build as build_report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Auto-debug pipeline for pump_short trading bot. "
            "Read-only. Prints diagnosis to stdout."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--hours-threshold",
        type=float,
        default=2.0,
        metavar="N",
        help=(
            "Hours without new log activity before a NO_SIGNAL issue is raised. "
            "Default: 2.0"
        ),
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=None,
        metavar="PATH",
        help="Project root directory. Auto-detected if not provided.",
    )
    parser.add_argument(
        "--journal-lines",
        type=int,
        default=500,
        metavar="N",
        help="Number of journalctl lines to fetch. Default: 500.",
    )
    parser.add_argument(
        "--no-state-detail",
        action="store_true",
        default=False,
        help="Omit the State Detail section from the report.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        default=False,
        help="Print a brief raw-data summary before the main report.",
    )
    return parser.parse_args()


def _print_verbose_summary(raw: dict) -> None:
    """Print a brief collection summary when --verbose is used."""
    print("── COLLECTION SUMMARY (--verbose) " + "─" * 37)
    print(f"  project_root    : {raw.get('project_root')}")
    print(f"  datasets_dir    : {raw.get('datasets_dir')}")
    print(f"  logs_dir        : {raw.get('logs_dir')}")
    print(f"  log files found : {len(raw.get('log_files', []))}")
    print(f"  journal lines   : {len(raw.get('journal_lines', []))}")
    journal_err = raw.get("journal_error")
    if journal_err:
        print(f"  journal error   : {journal_err}")
    print(f"  state_json      : {'present' if raw.get('state_json') else 'missing/error'}")
    state_err = raw.get("state_parse_error")
    if state_err:
        print(f"  state error     : {state_err}")
    print(f"  queue lines     : {len(raw.get('queue_lines', []))}")
    print(f"  lock exists     : {raw.get('lock_exists')}")
    print(f"  processing exists: {raw.get('processing_exists')}")
    print(f"  trades rows     : {len(raw.get('trades_rows', []))}")
    svc_err = raw.get("service_status_error")
    if svc_err:
        print(f"  service error   : {svc_err}")
    print("─" * 72)
    print()


def main() -> None:
    args = parse_args()

    # Step 1: Collect raw data from all sources
    raw = collect_raw(
        project_root=args.root,
        journal_lines=args.journal_lines,
    )

    if args.verbose:
        _print_verbose_summary(raw)

    # Step 2: Build state report (factual summary)
    state = build_state(raw, no_signal_threshold_hours=args.hours_threshold)

    # Step 3: Detect issues
    issues = detect(raw, state)

    # Step 4: Build and print report
    report = build_report(
        raw,
        state,
        issues,
        include_state_detail=not args.no_state_detail,
    )
    print(report)

    # Exit with non-zero status if CRITICAL issues found (useful for CI/alerting)
    critical_count = sum(1 for i in issues if i.severity == "CRITICAL")
    if critical_count > 0:
        sys.exit(2)
    high_count = sum(1 for i in issues if i.severity == "HIGH")
    if high_count > 0:
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
