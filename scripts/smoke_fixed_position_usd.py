"""Smoke: FIXED_POSITION_USD sizing (fixed notional, lot rounding, min checks)."""
from __future__ import annotations

import os
import sys
from pathlib import Path

_repo_root = Path(__file__).resolve().parents[1]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

os.environ["FIXED_POSITION_USD"] = "7"
os.environ["EXECUTION_MODE"] = "paper"

from trading.config import FIXED_POSITION_USD
from trading.instrument import InstrumentLimits, clear_instrument_limits_override, set_instrument_limits_override
from trading.risk import calc_position_size


def main() -> None:
    assert FIXED_POSITION_USD == 7.0, f"FIXED_POSITION_USD expected 7, got {FIXED_POSITION_USD}"

    symbol = "BTCUSDT"
    equity = 1000.0
    entry = 1.0
    sl = 0.99
    stop_pct = abs(entry - sl) / entry  # 0.01

    # (a) FIXED_POSITION_USD=7, price=1.0 -> qty=7 (after rounding), entry accepted
    clear_instrument_limits_override()
    notional, risk_usd, reject = calc_position_size(
        entry, sl, equity, symbol, 0.0025, stop_pct, risk_usd_override=None
    )
    assert reject is None, f"expected no reject, got {reject}"
    assert 6.9 <= notional <= 7.1, f"notional expected ~7, got {notional}"
    assert notional / entry >= 6.9, f"qty expected ~7, got {notional/entry}"
    print("OK: FIXED_POSITION_USD=7 price=1.0 -> notional~7, qty~7, entry accepted")

    # (b) FIXED_POSITION_USD=7, minNotional=10 -> entry rejected
    set_instrument_limits_override(
        symbol,
        InstrumentLimits(min_qty=0.001, min_notional_usd=10.0, lot_step=0.001, qty_precision=3),
    )
    notional, risk_usd, reject = calc_position_size(
        entry, sl, equity, symbol, 0.0025, stop_pct, risk_usd_override=None
    )
    assert reject == "FIXED_SIZE_BELOW_MIN", f"expected FIXED_SIZE_BELOW_MIN, got {reject}"
    assert notional == 0.0, f"expected notional=0 on reject, got {notional}"
    print("OK: FIXED_POSITION_USD=7 minNotional=10 -> entry rejected (FIXED_SIZE_BELOW_MIN)")

    clear_instrument_limits_override()
    print("smoke_fixed_position_usd: FIXED_POSITION_USD sizing OK")


if __name__ == "__main__":
    main()
