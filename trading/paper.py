"""Paper execution: simulate open/close, PnL in R. No exchange API."""
from __future__ import annotations

import logging
from typing import Any, Literal

logger = logging.getLogger(__name__)

# Position status
STATUS_OPEN = "open"
STATUS_CLOSED = "closed"


def simulate_open(
    signal: Any,
    qty_notional_usd: float,
    risk_usd: float,
    leverage: int,
    opened_ts: str,
) -> dict[str, Any]:
    """
    Build position dict from signal and notional/risk. No exchange call.
    """
    entry = float(signal.entry_price) if signal.entry_price is not None else 0.0
    sl = float(signal.sl_price) if signal.sl_price is not None else 0.0
    tp = float(signal.tp_price) if signal.tp_price is not None else 0.0
    position = {
        "strategy": signal.strategy,
        "symbol": signal.symbol,
        "side": (signal.side or "SHORT").upper(),
        "entry": entry,
        "sl": sl,
        "tp": tp,
        "opened_ts": opened_ts,
        "notional_usd": qty_notional_usd,
        "risk_usd": risk_usd,
        "leverage": leverage,
        "status": STATUS_OPEN,
        "run_id": getattr(signal, "run_id", ""),
        "event_id": getattr(signal, "event_id", "") or "",
    }
    return position


def simulate_close(
    position: dict[str, Any],
    exit_price: float,
    reason: str,
    ts_utc: str,
) -> tuple[float, float]:
    """
    Compute PnL in R and USD. Returns (pnl_r, pnl_usd).
    risk_per_unit = abs(entry - sl). Guard if zero -> return 0.
    SHORT: pnl_r = (entry - exit) / risk_per_unit
    LONG:  pnl_r = (exit - entry) / risk_per_unit
    pnl_usd = pnl_r * risk_usd (keep sign)
    """
    entry = position["entry"]
    sl = position["sl"]
    side = (position.get("side") or "SHORT").upper()
    risk_usd = position.get("risk_usd", 0.0)
    risk_per_unit = abs(entry - sl)
    if risk_per_unit < 1e-12:
        logger.warning("simulate_close: zero risk_per_unit (entry=%.4f sl=%.4f), pnl_r=0", entry, sl)
        return 0.0, 0.0
    if side == "SHORT":
        pnl_r = (entry - exit_price) / risk_per_unit
    else:
        pnl_r = (exit_price - entry) / risk_per_unit
    pnl_usd = pnl_r * risk_usd
    return pnl_r, pnl_usd


def decide_close_on_price(
    position: dict[str, Any],
    last_price: float,
) -> tuple[bool, Literal["sl", "tp", ""]]:
    """
    Decide if position should close due to SL/TP hit.
    SHORT: hit SL if last_price >= sl; hit TP if last_price <= tp.
    LONG:  hit SL if last_price <= sl; hit TP if last_price >= tp.
    Returns (should_close, "sl"|"tp"|"").
    """
    side = (position.get("side") or "SHORT").upper()
    sl = position["sl"]
    tp = position["tp"]

    if side == "SHORT":
        if last_price >= sl:
            return True, "sl"
        if last_price <= tp:
            return True, "tp"
    else:
        if last_price <= sl:
            return True, "sl"
        if last_price >= tp:
            return True, "tp"
    return False, ""
