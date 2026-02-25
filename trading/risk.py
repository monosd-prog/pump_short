"""Risk calculations and validation. No exchange; configurable bounds."""
from __future__ import annotations

import logging
from typing import Any

from trading.config import (
    MAX_OPEN_PER_STRATEGY,
    MAX_TOTAL_RISK_PCT,
    RISK_PCT,
    STOP_DISTANCE_MAX_PCT,
    STOP_DISTANCE_MIN_PCT,
)

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
    True if we can open a new position: max 1 per strategy and total risk cap.
    total_risk = risk_pct * equity * num_open_positions must not exceed max_total_risk_pct * equity.
    """
    open_positions = state.get("open_positions") or {}
    if strategy in open_positions:
        logger.debug("can_open=false: strategy=%s already has open position", strategy)
        return False
    n_open = len(open_positions)
    if n_open >= 1 and MAX_OPEN_PER_STRATEGY == 1:
        # We allow multiple strategies but 1 per strategy; so we only block if this strategy is open
        pass
    total_risk_current = n_open * risk_pct * equity
    total_risk_after = (n_open + 1) * risk_pct * equity
    if total_risk_after > max_total_risk_pct * equity:
        logger.debug(
            "can_open=false: total_risk_after %.2f > max %.2f",
            total_risk_after, max_total_risk_pct * equity,
        )
        return False
    return True
