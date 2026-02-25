#!/usr/bin/env python3
"""Regression smoke: PnL sign for SHORT/LONG in trading/paper.simulate_close."""
from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

from trading.paper import simulate_close


def main() -> int:
    risk_usd = 2.5

    # SHORT: entry=1.0, sl=1.01 (above), tp=0.988 (below)
    short_pos = {
        "entry": 1.0,
        "sl": 1.01,
        "tp": 0.988,
        "side": "SHORT",
        "risk_usd": risk_usd,
    }
    pnl_r_tp, pnl_usd_tp = simulate_close(short_pos, 0.988, "tp", "")
    pnl_r_sl, pnl_usd_sl = simulate_close(short_pos, 1.01, "sl", "")
    print(f"SHORT TP: pnl_r={pnl_r_tp:.2f} pnl_usd={pnl_usd_tp:.2f} (expect +1.2, +3.0)")
    print(f"SHORT SL: pnl_r={pnl_r_sl:.2f} pnl_usd={pnl_usd_sl:.2f} (expect -1.0, -2.5)")
    if abs(pnl_r_tp - 1.2) > 0.01 or abs(pnl_usd_tp - 3.0) > 0.01:
        print("FAIL: SHORT TP expectations not met")
        return 1
    if abs(pnl_r_sl - (-1.0)) > 0.01 or abs(pnl_usd_sl - (-2.5)) > 0.01:
        print("FAIL: SHORT SL expectations not met")
        return 1

    # LONG: entry=1.0, sl=0.99 (below), tp=1.012 (above)
    long_pos = {
        "entry": 1.0,
        "sl": 0.99,
        "tp": 1.012,
        "side": "LONG",
        "risk_usd": risk_usd,
    }
    pnl_r_tp, pnl_usd_tp = simulate_close(long_pos, 1.012, "tp", "")
    pnl_r_sl, pnl_usd_sl = simulate_close(long_pos, 0.99, "sl", "")
    print(f"LONG TP:  pnl_r={pnl_r_tp:.2f} pnl_usd={pnl_usd_tp:.2f} (expect +1.2, +3.0)")
    print(f"LONG SL:  pnl_r={pnl_r_sl:.2f} pnl_usd={pnl_usd_sl:.2f} (expect -1.0, -2.5)")
    if abs(pnl_r_tp - 1.2) > 0.01 or abs(pnl_usd_tp - 3.0) > 0.01:
        print("FAIL: LONG TP expectations not met")
        return 1
    if abs(pnl_r_sl - (-1.0)) > 0.01 or abs(pnl_usd_sl - (-2.5)) > 0.01:
        print("FAIL: LONG SL expectations not met")
        return 1

    print("Smoke OK.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
