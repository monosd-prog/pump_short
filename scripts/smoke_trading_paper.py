#!/usr/bin/env python3
"""Smoke test: enqueue 2 sample signals (short_pump + fast0), run runner --once, print state and last CSV row."""
from __future__ import annotations

import csv
import json
import os
import sys
from pathlib import Path

# Project root
_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))
os.chdir(_ROOT)

from short_pump.signals import Signal

from trading.config import LOG_PATH, STATE_PATH
from trading.queue import enqueue_signal
from trading.runner import run_once
from trading.state import load_state


def main() -> None:
    # Sample signals with entry/tp/sl (required for runner)
    entry1, tp1, sl1 = 100_000.0, 98_000.0, 102_000.0
    sig1 = Signal(
        strategy="short_pump",
        symbol="BTCUSDT",
        side="SHORT",
        ts_utc="2026-02-05 12:00:00+0000",
        run_id="smoke_run_1",
        event_id="e1",
        entry_price=entry1,
        tp_price=tp1,
        sl_price=sl1,
        tp_pct=-2.0,
        sl_pct=2.0,
        stage=4,
        dist_to_peak_pct=5.0,
        context_score=0.7,
        cvd_30s=None,
        cvd_1m=None,
        liq_long_usd_30s=None,
        liq_short_usd_30s=None,
        extras={},
    )
    entry2, tp2, sl2 = 50_000.0, 49_000.0, 51_000.0
    sig2 = Signal(
        strategy="short_pump_fast0",
        symbol="ETHUSDT",
        side="SHORT",
        ts_utc="2026-02-05 12:01:00+0000",
        run_id="smoke_run_2",
        event_id="e2",
        entry_price=entry2,
        tp_price=tp2,
        sl_price=sl2,
        tp_pct=-2.0,
        sl_pct=2.0,
        stage=None,
        dist_to_peak_pct=1.5,
        context_score=0.65,
        cvd_30s=None,
        cvd_1m=None,
        liq_long_usd_30s=None,
        liq_short_usd_30s=None,
        extras={},
    )
    enqueue_signal(sig1)
    enqueue_signal(sig2)
    print("Enqueued 2 signals (short_pump, short_pump_fast0)")

    run_once()
    print("Ran runner --once")

    state = load_state()
    print("State:", json.dumps(state, indent=2))

    if Path(LOG_PATH).exists():
        with open(LOG_PATH, "r", newline="", encoding="utf-8") as f:
            rows = list(csv.reader(f))
        if len(rows) >= 2:
            print("Last CSV row:", rows[-1])
        else:
            print("CSV rows:", rows)
    else:
        print("No CSV file yet:", LOG_PATH)
    print("Smoke OK.")


if __name__ == "__main__":
    main()
