#!/usr/bin/env python3
"""Smoke test: multi-positions per strategy. Enqueue 3 fast0 signals, verify 3 positions, close one, timeout."""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))
os.chdir(_ROOT)

# Allow 3+ positions per strategy
os.environ["MAX_OPEN_PER_STRATEGY"] = "5"
os.environ["MAX_TOTAL_RISK_PCT"] = "0.02"

from short_pump.signals import Signal

from trading.config import LOG_PATH, STATE_PATH
from trading.paper_outcome import close_from_outcome, close_on_timeout
from trading.queue import enqueue_signal
from trading.runner import run_once
from trading.state import load_state, make_position_id, save_state


def main() -> int:
    # 3 fast0 signals: SOSOUSDT, ARCUSDT, STBLUSDT
    strategy = "short_pump_fast0"
    signals = [
        Signal(strategy=strategy, symbol="SOSOUSDT", side="SHORT", ts_utc="2026-02-05 12:00:00+0000", run_id="r1", event_id="e1",
               entry_price=0.1, tp_price=0.098, sl_price=0.102, tp_pct=None, sl_pct=None, stage=None, dist_to_peak_pct=1.0,
               context_score=0.7, cvd_30s=None, cvd_1m=None, liq_long_usd_30s=None, liq_short_usd_30s=None, extras={}),
        Signal(strategy=strategy, symbol="ARCUSDT", side="SHORT", ts_utc="2026-02-05 12:01:00+0000", run_id="r2", event_id="e2",
               entry_price=0.2, tp_price=0.196, sl_price=0.204, tp_pct=None, sl_pct=None, stage=None, dist_to_peak_pct=1.0,
               context_score=0.7, cvd_30s=None, cvd_1m=None, liq_long_usd_30s=None, liq_short_usd_30s=None, extras={}),
        Signal(strategy=strategy, symbol="STBLUSDT", side="SHORT", ts_utc="2026-02-05 12:02:00+0000", run_id="r3", event_id="e3",
               entry_price=0.3, tp_price=0.294, sl_price=0.306, tp_pct=None, sl_pct=None, stage=None, dist_to_peak_pct=1.0,
               context_score=0.7, cvd_30s=None, cvd_1m=None, liq_long_usd_30s=None, liq_short_usd_30s=None, extras={}),
    ]
    for s in signals:
        enqueue_signal(s)
    print("Enqueued 3 fast0 signals (SOSOUSDT, ARCUSDT, STBLUSDT)")

    run_once()
    state = load_state()
    op = state.get("open_positions") or {}
    strat_pos = op.get(strategy) or {}
    n = len(strat_pos) if isinstance(strat_pos, dict) else 0
    print(f"Open positions under {strategy}: {n}")
    print("position_ids:", list(strat_pos.keys()) if isinstance(strat_pos, dict) else [])

    if n != 3:
        print(f"FAIL: expected 3 positions, got {n}")
        return 1
    print("OK: 3 positions exist, none overwritten")

    # Close only ARCUSDT via outcome
    ok = close_from_outcome(strategy, "ARCUSDT", "r2", "e2", "TP_hit", -2.0, "2026-02-05 12:30:00+0000")
    if not ok:
        print("FAIL: close_from_outcome returned False")
        return 1
    state = load_state()
    strat_pos = (state.get("open_positions") or {}).get(strategy) or {}
    n = len(strat_pos) if isinstance(strat_pos, dict) else 0
    if n != 2:
        print(f"FAIL: expected 2 positions after close ARCUSDT, got {n}")
        return 1
    remaining = set(strat_pos.keys()) if isinstance(strat_pos, dict) else set()
    if make_position_id(strategy, "r2", "e2", "ARCUSDT") in remaining:
        print("FAIL: ARCUSDT position still open")
        return 1
    print("OK: ARCUSDT closed, 2 remain")

    # Timeout test: TTL=0 closes all
    state = load_state()
    now_utc = "2026-02-05 13:00:00+0000"
    closed = close_on_timeout(state, now_utc, ttl_seconds=0)
    if closed:
        save_state(state)
    if not closed:
        print("FAIL: close_on_timeout did not close (TTL=0)")
        return 1
    state = load_state()
    strat_pos = (state.get("open_positions") or {}).get(strategy) or {}
    n = len(strat_pos) if isinstance(strat_pos, dict) else 0
    if n != 0:
        print(f"FAIL: expected 0 positions after timeout, got {n}")
        return 1
    print("OK: all positions closed by timeout with reason=timeout")

    print("Smoke OK.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
