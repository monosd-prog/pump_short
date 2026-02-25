#!/usr/bin/env python3
"""Smoke: verify format_fast0_outcome_message produces output with symbol, run_id, res."""
from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

from notifications.tg_format import format_fast0_outcome_message


def main() -> int:
    msg = format_fast0_outcome_message(
        symbol="SOSOUSDT",
        run_id="20260205_123456",
        event_id="20260205_123456_fast0_3_a1b2c3d4",
        res="TP_hit",
        entry_price=0.0450,
        tp_price=0.0445,
        sl_price=0.0455,
        exit_price=0.0445,
        pnl_pct=1.11,
        hold_seconds=45.0,
        dist_to_peak_pct=1.5,
        context_score=0.72,
    )
    assert "SOSOUSDT" in msg, f"symbol missing: {msg}"
    assert "20260205_123456" in msg, f"run_id missing: {msg}"
    assert "TP_hit" in msg, f"res missing: {msg}"
    assert "entry=" in msg
    assert "tp=" in msg
    assert "sl=" in msg
    assert "pnl=" in msg
    assert "hold=" in msg
    assert "short_pump_fast0" in msg
    print("format_fast0_outcome_message: OK")
    print(msg)
    return 0


if __name__ == "__main__":
    sys.exit(main())
