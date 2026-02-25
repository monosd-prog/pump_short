#!/usr/bin/env python3
"""Smoke test: build 1 sample Signal, call enqueue_signal, print last line of queue file."""
from __future__ import annotations

import os
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))
os.chdir(_ROOT)

from short_pump.signals import Signal

from trading.queue import enqueue_signal

# Use a dedicated path so we don't depend on AUTO_TRADING_ENABLE and don't touch main queue
SMOKE_QUEUE = _ROOT / "datasets" / "signals_queue_smoke.jsonl"


def main() -> None:
    sig = Signal(
        strategy="short_pump",
        symbol="BTCUSDT",
        side="SHORT",
        ts_utc="2026-02-05 14:00:00+0000",
        run_id="smoke_enqueue_1",
        event_id="ev1",
        entry_price=101_000.0,
        tp_price=99_000.0,
        sl_price=103_000.0,
        tp_pct=-1.98,
        sl_pct=1.98,
        stage=4,
        dist_to_peak_pct=4.0,
        context_score=0.72,
        cvd_30s=None,
        cvd_1m=None,
        liq_long_usd_30s=None,
        liq_short_usd_30s=None,
        extras={},
    )
    enqueue_signal(sig, queue_path=str(SMOKE_QUEUE))
    print("Enqueued 1 signal to", SMOKE_QUEUE)

    if SMOKE_QUEUE.exists():
        lines = SMOKE_QUEUE.read_text(encoding="utf-8").strip().splitlines()
        if lines:
            print("Last line:", lines[-1])
        else:
            print("File empty")
    else:
        print("File not found")
    print("Smoke OK.")


if __name__ == "__main__":
    main()
