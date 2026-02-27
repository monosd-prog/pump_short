"""Instrument metadata (minQty, minNotional, lot step). Stub for paper/smoke; plug Bybit for live."""
from __future__ import annotations

import math
from typing import NamedTuple

# Optional registry for smoke tests: patch this to inject limits
_INSTRUMENT_LIMITS_OVERRIDE: dict[str, "InstrumentLimits"] = {}


class InstrumentLimits(NamedTuple):
    min_qty: float
    min_notional_usd: float
    lot_step: float
    qty_precision: int


def get_instrument_limits(symbol: str) -> InstrumentLimits:
    """
    Return minQty, minNotional (USD), lot step, qty precision for symbol.
    Uses override registry if set (for smoke tests); else sensible defaults.
    """
    if symbol in _INSTRUMENT_LIMITS_OVERRIDE:
        return _INSTRUMENT_LIMITS_OVERRIDE[symbol]
    # Defaults (Bybit linear USDT typical: minNotional ~5-6, minQty ~0.001, lotStep ~0.001)
    return InstrumentLimits(
        min_qty=0.001,
        min_notional_usd=5.0,
        lot_step=0.001,
        qty_precision=3,
    )


def set_instrument_limits_override(symbol: str, limits: InstrumentLimits) -> None:
    """For smoke tests: inject limits without Bybit."""
    _INSTRUMENT_LIMITS_OVERRIDE[symbol] = limits


def clear_instrument_limits_override(symbol: str | None = None) -> None:
    """For smoke tests: clear override(s)."""
    if symbol is None:
        _INSTRUMENT_LIMITS_OVERRIDE.clear()
    else:
        _INSTRUMENT_LIMITS_OVERRIDE.pop(symbol, None)


def round_qty_down(qty: float, lot_step: float, qty_precision: int) -> float:
    """Round qty DOWN to lot step and precision."""
    if lot_step <= 0:
        lot_step = 0.001
    steps = math.floor(qty / lot_step)
    raw = steps * lot_step
    # apply decimal precision
    factor = 10**qty_precision
    return math.floor(raw * factor) / factor
