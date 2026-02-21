#!/usr/bin/env python3
"""
E2E smoke test for fast0: events, trades, outcomes.
Runs with relaxed entry thresholds to force ENTRY_OK, short outcome watch for quick exit.
"""

import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Relax thresholds to force ENTRY_OK
os.environ.setdefault("FAST0_ENTRY_CONTEXT_MIN", "0.0")
os.environ.setdefault("FAST0_ENTRY_DIST_MIN", "0.0")
os.environ.setdefault("FAST0_ENTRY_CVD30S_MAX", "999")
os.environ.setdefault("FAST0_ENTRY_CVD1M_MAX", "999")
os.environ.setdefault("FAST0_ENTRY_MIN_TICK", "1")
os.environ.setdefault("FAST0_POLL_SECONDS", "2")
os.environ.setdefault("FAST0_WINDOW_SEC", "60")
# Short outcome watch for smoke
os.environ.setdefault("FAST0_OUTCOME_WATCH_SEC", "10")
os.environ.setdefault("FAST0_OUTCOME_POLL_SEC", "1")

base_dir = os.getenv("FAST0_BASE_DIR") or os.getenv("DATASETS_ROOT") or "/root/pump_short/datasets"
_watch_sec = int(os.getenv("FAST0_OUTCOME_WATCH_SEC", "10"))

from common.io_dataset import ensure_dataset_files
from short_pump.fast0_sampler import STRATEGY, run_fast0_for_symbol


def main():
    symbol = os.getenv("SYMBOL", "BTCUSDT")
    max_ticks = int(os.getenv("FAST0_SMOKE_TICKS", "10"))
    run_id = time.strftime("%Y%m%d_%H%M%S") + "_e2e"
    pump_ts = datetime.now(timezone.utc).isoformat()

    now_utc = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    ensure_dataset_files(STRATEGY, "live", now_utc, schema_version=3, base_dir=base_dir)

    print(f"E2E smoke: running fast0 for {symbol}, max_ticks={max_ticks}, base_dir={base_dir}")
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
    outcomes_path = base_path / "outcomes_v3.csv"

    ok = True
    if not events_path.exists():
        print(f"FAIL: events_v3.csv not found at {events_path}")
        ok = False
    else:
        lines = events_path.read_text().strip().split("\n")
        data_rows = [l for l in lines[1:] if l.strip()]
        our_events = [r for r in data_rows if run_id in r]
        print(f"OK: events_v3.csv has {len(our_events)} rows for run_id")

    if not trades_path.exists():
        print(f"FAIL: trades_v3.csv not found at {trades_path}")
        ok = False
    else:
        lines = trades_path.read_text().strip().split("\n")
        data_rows = [l for l in lines[1:] if l.strip()]
        our_trades = [r for r in data_rows if run_id in r]
        print(f"OK: trades_v3.csv has {len(our_trades)} rows for run_id")

    # Outcomes: watcher runs FAST0_OUTCOME_WATCH_SEC (10s in smoke)
    if not outcomes_path.exists():
        print(f"WARN: outcomes_v3.csv not found yet (watcher may still be running)")
        time.sleep(_watch_sec + 5)
        if outcomes_path.exists():
            lines = outcomes_path.read_text().strip().split("\n")
            data_rows = [l for l in lines[1:] if l.strip()]
            our_outcomes = [r for r in data_rows if run_id in r]
            print(f"OK: outcomes_v3.csv has {len(our_outcomes)} rows for run_id")
        else:
            print(f"FAIL: outcomes_v3.csv still missing after 15s wait")
            ok = False
    else:
        lines = outcomes_path.read_text().strip().split("\n")
        data_rows = [l for l in lines[1:] if l.strip()]
        our_outcomes = [r for r in data_rows if run_id in r]
        print(f"OK: outcomes_v3.csv has {len(our_outcomes)} rows for run_id")

    if ok:
        print("OK: smoke_fast0_e2e passed")
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
