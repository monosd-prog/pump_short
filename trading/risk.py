"""Risk calculations and validation. No exchange; configurable bounds."""
from __future__ import annotations

import logging
from typing import Any, Tuple

from trading.config import (
    FIXED_POSITION_USD,
    MAX_LEVERAGE,
    MAX_OPEN_PER_STRATEGY,
    MAX_RISK_USD_PER_TRADE,
    MAX_TOTAL_RISK_PCT,
    RISK_PCT,
    STOP_DISTANCE_MAX_PCT,
    STOP_DISTANCE_MIN_PCT,
)
from trading.instrument import get_instrument_limits, round_qty_down
from trading.state import count_open_positions, total_risk_usd

logger = logging.getLogger(__name__)


def calc_risk_usd(equity: float, risk_pct: float) -> float:
    """Risk per trade in USD (1R)."""
    return equity * risk_pct


def calc_stop_distance_pct(entry: float, sl: float) -> float:
    """Absolute distance from entry to SL as fraction of entry: |sl - entry| / entry."""
    if entry <= 0:
        return 0.0
    return abs(sl - entry) / entry


def calc_notional_usd(risk_usd: float, stop_distance_pct: float) -> float:
    """Notional size in USD such that 1R = risk_usd. notional * stop_distance_pct = risk_usd."""
    if stop_distance_pct <= 0:
        return 0.0
    return risk_usd / stop_distance_pct


def calc_margin_usd(notional_usd: float, leverage: int) -> float:
    """Margin required for notional at given leverage."""
    if leverage <= 0:
        return 0.0
    return notional_usd / leverage


def calc_position_size(
    entry_price: float,
    sl_price: float,
    equity: float,
    symbol: str,
    risk_pct: float,
    stop_distance_pct: float,
    *,
    risk_usd_override: float | None = None,
) -> Tuple[float, float, str | None]:
    """
    Compute notional_usd and risk_usd. Returns (notional_usd, risk_usd, reject_reason).
    reject_reason is None on success.
    If FIXED_POSITION_USD > 0: fixed sizing with lot rounding and min checks.
    Else: risk-based sizing (RISK_PCT).
    """
    if FIXED_POSITION_USD > 0:
        notional_usd = FIXED_POSITION_USD
        raw_qty = notional_usd / entry_price if entry_price > 0 else 0.0
        limits = get_instrument_limits(symbol)
        final_qty = round_qty_down(raw_qty, limits.lot_step, limits.qty_precision)
        if final_qty < limits.min_qty:
            logger.info(
                "FIXED_SIZE_BELOW_MIN | symbol=%s reason=qty_below_minQty raw_qty=%.6f final_qty=%.6f minQty=%.6f minNotional=%.2f",
                symbol, raw_qty, final_qty, limits.min_qty, limits.min_notional_usd,
            )
            return 0.0, 0.0, "FIXED_SIZE_BELOW_MIN"
        result_notional = final_qty * entry_price
        if result_notional < limits.min_notional_usd:
            logger.info(
                "FIXED_SIZE_BELOW_MIN | symbol=%s reason=notional_below_minNotional notional=%.2f minNotional=%.2f",
                symbol, result_notional, limits.min_notional_usd,
            )
            return 0.0, 0.0, "FIXED_SIZE_BELOW_MIN"
        risk_usd = result_notional * stop_distance_pct
        logger.info(
            "FIXED_POSITION_SIZING | notional_usd=%.2f | price=%.6f | raw_qty=%.6f | final_qty=%.6f | minQty=%.6f | minNotional=%.2f",
            result_notional, entry_price, raw_qty, final_qty, limits.min_qty, limits.min_notional_usd,
        )
        return result_notional, risk_usd, None

    # Risk-based
    risk_usd = risk_usd_override if risk_usd_override is not None else calc_risk_usd(equity, risk_pct)
    notional_usd = calc_notional_usd(risk_usd, stop_distance_pct)
    return notional_usd, risk_usd, None


def calc_qty_and_notional_from_risk(risk_usd: float, entry_price: float, sl_price: float) -> tuple[float, float]:
    """
    Position size from risk and SL distance: qty = risk_usd / abs(entry - sl), notional = qty * entry.
    Returns (qty, notional_usd). Returns (0, 0) if entry/sl invalid.
    """
    if entry_price <= 0:
        return 0.0, 0.0
    sl_dist = abs(entry_price - sl_price)
    if sl_dist < 1e-12:
        return 0.0, 0.0
    qty = risk_usd / sl_dist
    notional_usd = qty * entry_price
    return qty, notional_usd


def validate_notional_leverage(
    notional_usd: float,
    equity_usd: float,
    leverage: int,
) -> tuple[bool, str]:
    """True if notional <= equity * leverage (max exposure). Else (False, reason)."""
    if equity_usd <= 0 or leverage <= 0:
        return False, "invalid equity or leverage"
    max_notional = equity_usd * leverage
    if notional_usd > max_notional:
        return False, f"notional {notional_usd:.2f} > max {max_notional:.2f} (equity*leverage)"
    return True, ""


def risk_usd_for_live(equity: float) -> float:
    """Risk per trade for LIVE: cap at MAX_RISK_USD_PER_TRADE."""
    from_pct = equity * RISK_PCT
    return min(from_pct, MAX_RISK_USD_PER_TRADE) if MAX_RISK_USD_PER_TRADE > 0 else from_pct


def validate_stop_distance(stop_distance_pct: float) -> tuple[bool, str]:
    """Validate stop is within [STOP_DISTANCE_MIN_PCT, STOP_DISTANCE_MAX_PCT]. Return (ok, reason)."""
    if stop_distance_pct < STOP_DISTANCE_MIN_PCT:
        return False, f"stop_distance_pct {stop_distance_pct:.4f} < min {STOP_DISTANCE_MIN_PCT}"
    if stop_distance_pct > STOP_DISTANCE_MAX_PCT:
        return False, f"stop_distance_pct {stop_distance_pct:.4f} > max {STOP_DISTANCE_MAX_PCT}"
    return True, ""


def can_open(
    strategy: str,
    state: dict[str, Any],
    equity: float,
    risk_pct: float,
    max_total_risk_pct: float,
) -> bool:
    """
    True if we can open a new position:
    - count of open positions for strategy < MAX_OPEN_PER_STRATEGY
    - total risk_usd across ALL positions + new risk <= max_total_risk_pct * equity
    """
    n_for_strategy = count_open_positions(state, strategy)
    if n_for_strategy >= MAX_OPEN_PER_STRATEGY:
        logger.debug("can_open=false: strategy=%s count=%d >= MAX_OPEN_PER_STRATEGY=%d", strategy, n_for_strategy, MAX_OPEN_PER_STRATEGY)
        return False
    total_risk = total_risk_usd(state)
    new_risk = risk_pct * equity
    if total_risk + new_risk > max_total_risk_pct * equity:
        logger.debug(
            "can_open=false: total_risk %.2f + new %.2f > max %.2f",
            total_risk, new_risk, max_total_risk_pct * equity,
        )
        return False
    return True
