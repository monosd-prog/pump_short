#!/usr/bin/env python3
"""
Smoke test for FAST_FROM_PUMP (fast0 sampler).
Runs sampler for one symbol for 10 ticks and verifies N rows in short_pump_fast0 events file.
"""

import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Ensure project root on path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Force fast0 params for smoke
os.environ.setdefault("FAST0_POLL_SECONDS", "2")
os.environ.setdefault("FAST0_WINDOW_SEC", "60")

from common.io_dataset import ensure_dataset_files
from short_pump.fast0_sampler import STRATEGY, run_fast0_for_symbol


def main():
    symbol = os.getenv("SYMBOL", "BTCUSDT")
    max_ticks = int(os.getenv("FAST0_SMOKE_TICKS", "10"))
    run_id = time.strftime("%Y%m%d_%H%M%S") + "_smoke"
    pump_ts = datetime.now(timezone.utc).isoformat()

    now_utc = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    ensure_dataset_files(STRATEGY, "live", now_utc, schema_version=3)
    print(f"Smoke: running fast0 sampler for {symbol}, max_ticks={max_ticks}")
    run_fast0_for_symbol(
        symbol=symbol,
        run_id=run_id,
        pump_ts=pump_ts,
        mode="live",
        max_ticks=max_ticks,
    )

    # Verify rows written (datasets/date=YYYYMMDD/strategy=short_pump_fast0/mode=live/)
    day = datetime.now(timezone.utc).strftime("%Y%m%d")
    base_dir = Path("datasets") / f"date={day}" / f"strategy={STRATEGY}" / "mode=live"
    events_path = base_dir / "events_v3.csv"
    if not events_path.exists():
        print(f"FAIL: events file not found: {events_path}")
        sys.exit(1)

    lines = events_path.read_text().strip().split("\n")
    # header + data rows
    data_rows = [l for l in lines[1:] if l.strip()]
    # Filter by our run_id
    our_rows = [r for r in data_rows if run_id in r]

    print(f"Found {len(our_rows)} rows for run_id={run_id} in {events_path}")
    if len(our_rows) < max_ticks:
        print(f"FAIL: expected at least {max_ticks} rows, got {len(our_rows)}")
        sys.exit(1)
    print("OK: smoke_fast0 passed")
    sys.exit(0)


if __name__ == "__main__":
    main()
