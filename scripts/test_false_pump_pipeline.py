#!/usr/bin/env python3
"""One-shot smoke test for false_pump pipeline: enqueue -> runner pickup -> state check -> cleanup."""
from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Project root
_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))
os.chdir(_ROOT)

from trading.queue import enqueue_signal
from trading.state import load_state, make_position_id, save_state

try:
    # Backward-compatible import path if project later exposes trading.signal.Signal
    from trading.signal import Signal  # type: ignore
except Exception:
    from short_pump.signals import Signal


def _build_test_signal(run_id: str, event_id: str, symbol: str) -> Signal:
    # Conservative synthetic prices for SHORT test signal.
    entry = 65000.0
    tp = 64350.0
    sl = 65650.0
    ts_utc = datetime.now(timezone.utc).isoformat()
    return Signal(
        strategy="false_pump",
        symbol=symbol,
        side="SHORT",
        ts_utc=ts_utc,
        run_id=run_id,
        event_id=event_id,
        entry_price=entry,
        tp_price=tp,
        sl_price=sl,
        tp_pct=-1.0,
        sl_pct=1.0,
        stage=4,
        dist_to_peak_pct=4.0,
        context_score=0.67,
        cvd_30s=-0.2,
        cvd_1m=-0.2,
        liq_long_usd_30s=0.0,
        liq_short_usd_30s=0.0,
        extras={
            "test": True,
            "test_name": "false_pump_pipeline",
            "note": "one-shot test signal",
        },
    )


def _cleanup_position_if_exists(position_id: str) -> bool:
    state = load_state()
    open_positions = state.get("open_positions") or {}
    strategy_positions = open_positions.get("false_pump")
    if not isinstance(strategy_positions, dict) or position_id not in strategy_positions:
        return False

    del strategy_positions[position_id]
    if not strategy_positions:
        open_positions.pop("false_pump", None)

    last_signal_ids = state.get("last_signal_ids") or {}
    if isinstance(last_signal_ids.get("false_pump"), list):
        last_signal_ids["false_pump"] = [x for x in last_signal_ids["false_pump"] if x != position_id]

    save_state(state)
    return True


def main() -> None:
    run_id = f"test_fp_{int(time.time())}"
    event_id = str(int(time.time() * 1000))
    symbol = "BTCUSDT"  # test symbol (real market symbol)

    signal = _build_test_signal(run_id, event_id, symbol)
    enqueue_signal(signal)
    print(json.dumps({
        "step": "enqueued",
        "strategy": signal.strategy,
        "symbol": symbol,
        "run_id": run_id,
        "event_id": event_id,
    }, ensure_ascii=False))

    wait_sec = 20
    print(json.dumps({"step": "wait", "seconds": wait_sec}, ensure_ascii=False))
    time.sleep(wait_sec)

    position_id = make_position_id("false_pump", run_id, event_id, symbol)
    state = load_state()
    pos = ((state.get("open_positions") or {}).get("false_pump") or {}).get(position_id)

    if not pos:
        print(json.dumps({
            "status": "FAIL",
            "reason": "позиция не найдена",
            "position_id": position_id,
        }, ensure_ascii=False))
        raise SystemExit(1)

    mode = str(pos.get("mode") or "")
    assert mode == "paper", f"Expected paper mode, got: {mode!r}"

    print(json.dumps({
        "status": "OK",
        "position_found": True,
        "trade_id": position_id,
        "mode": mode,
        "risk_profile": pos.get("risk_profile"),
    }, ensure_ascii=False))

    removed = _cleanup_position_if_exists(position_id)
    print(json.dumps({
        "step": "cleanup",
        "removed": removed,
        "position_id": position_id,
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
