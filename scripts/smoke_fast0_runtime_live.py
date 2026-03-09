#!/usr/bin/env python3
"""
Runtime smoke for REAL short_pump_fast0 flow.
Runs run_fast0_for_symbol(..., mode="live") and verifies that events_v3/trades_v3
rows in the live path have mode=live and source_mode=live in the row content.
"""

import csv
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

os.environ.setdefault("FAST0_POLL_SECONDS", "2")
os.environ.setdefault("FAST0_WINDOW_SEC", "60")

from common.io_dataset import ensure_dataset_files
from short_pump.fast0_sampler import STRATEGY, run_fast0_for_symbol


def _check_mode_in_rows(path: Path, run_id: str, label: str) -> tuple[int, int]:
    """Return (total_our_rows, bad_rows) for rows with run_id."""
    if not path.exists():
        return 0, 0
    total, bad = 0, 0
    with path.open(newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            if run_id not in row.get("run_id", ""):
                continue
            total += 1
            m = (row.get("mode") or "").strip().lower()
            sm = (row.get("source_mode") or "").strip().lower()
            if m != "live" or sm != "live":
                bad += 1
    return total, bad


def main():
    symbol = os.getenv("SYMBOL", "BTCUSDT")
    max_ticks = int(os.getenv("FAST0_SMOKE_TICKS", "10"))
    run_id = time.strftime("%Y%m%d_%H%M%S") + "_runtime_live"
    pump_ts = datetime.now(timezone.utc).isoformat()
    base_dir = os.getenv("FAST0_BASE_DIR") or os.getenv("DATASETS_ROOT") or "/root/pump_short/datasets"

    now_utc = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    ensure_dataset_files(STRATEGY, "live", now_utc, schema_version=3, base_dir=base_dir)
    print(f"Runtime smoke: running fast0 for {symbol}, max_ticks={max_ticks}, base_dir={base_dir}")

    run_fast0_for_symbol(
        symbol=symbol,
        run_id=run_id,
        pump_ts=pump_ts,
        mode="live",
        max_ticks=max_ticks,
        base_dir=base_dir,
    )

    day = datetime.now(timezone.utc).strftime("%Y%m%d")
    base_path = Path(base_dir.rstrip("/")) / f"date={day}" / f"strategy={STRATEGY}" / "mode=live"
    events_path = base_path / "events_v3.csv"
    trades_path = base_path / "trades_v3.csv"

    if not events_path.exists():
        print(f"FAIL: events_v3.csv not found: {events_path}")
        sys.exit(1)

    ev_total, ev_bad = _check_mode_in_rows(events_path, run_id, "events")
    tr_total, tr_bad = _check_mode_in_rows(trades_path, run_id, "trades")

    if ev_total < max_ticks:
        print(f"FAIL: expected >= {max_ticks} event rows for run_id={run_id}, got {ev_total}")
        sys.exit(1)
    if ev_bad > 0:
        print(f"FAIL: {ev_bad}/{ev_total} event rows have mode/source_mode != live in {events_path}")
        sys.exit(1)
    if tr_total > 0 and tr_bad > 0:
        print(f"FAIL: {tr_bad}/{tr_total} trade rows have mode/source_mode != live in {trades_path}")
        sys.exit(1)

    print(f"OK: events {ev_total} rows all mode=live, source_mode=live")
    if tr_total > 0:
        print(f"OK: trades {tr_total} rows all mode=live, source_mode=live")
    print("OK: smoke_fast0_runtime_live passed")
    sys.exit(0)


if __name__ == "__main__":
    main()
