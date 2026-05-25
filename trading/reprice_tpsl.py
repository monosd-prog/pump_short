"""Reprice TP/SL from actual fill (avgPrice) after live open."""
from __future__ import annotations

import logging
from typing import Any, Optional, Tuple

logger = logging.getLogger(__name__)

REPRICE_DRIFT_THRESHOLD = 0.001


def _side_upper(side: str) -> str:
    return (side or "SHORT").strip().upper()


def _pct_from_signal(
    signal_entry: float,
    price: float,
    pct_attr: Optional[float],
    fallback_num: float,
) -> float:
    if pct_attr is not None and pct_attr > 0:
        return float(pct_attr)
    if signal_entry <= 0:
        return 0.0
    return abs(fallback_num) / signal_entry


def compute_repriced_tpsl(
    signal_entry: float,
    actual_entry: float,
    sl_price: float,
    tp_price: float,
    side: str,
    *,
    sl_pct: Optional[float] = None,
    tp_pct: Optional[float] = None,
    drift_threshold: float = REPRICE_DRIFT_THRESHOLD,
) -> Optional[Tuple[float, float]]:
    """
    Return (new_sl, new_tp) from actual_entry when drift > threshold, else None.
    """
    if signal_entry <= 0 or actual_entry <= 0:
        return None
    price_drift = abs(actual_entry - signal_entry) / signal_entry
    if price_drift <= drift_threshold:
        return None

    sl_p = _pct_from_signal(signal_entry, sl_price, sl_pct, float(sl_price) - signal_entry)
    tp_p = _pct_from_signal(signal_entry, tp_price, tp_pct, signal_entry - float(tp_price))
    side_u = _side_upper(side)
    if side_u in ("SHORT", "SELL"):
        new_sl = actual_entry * (1.0 + sl_p)
        new_tp = actual_entry * (1.0 - tp_p)
    else:
        new_sl = actual_entry * (1.0 - sl_p)
        new_tp = actual_entry * (1.0 + tp_p)
    return new_sl, new_tp


def maybe_reprice_position_after_fill(
    signal: Any,
    position: dict[str, Any],
    broker: Any,
    *,
    exec_mode: str,
    notional_usd: Optional[float] = None,
) -> None:
    """Update exchange TP/SL and position dict when fill drifts from signal entry (live only)."""
    signal_entry = float(getattr(signal, "entry_price", 0) or 0)
    actual_entry = float(position.get("entry", signal_entry) or signal_entry)
    sl_price = float(getattr(signal, "sl_price", 0) or position.get("sl", 0) or 0)
    tp_price = float(getattr(signal, "tp_price", 0) or position.get("tp", 0) or 0)
    if signal_entry <= 0 or sl_price <= 0 or tp_price <= 0:
        return

    repriced = compute_repriced_tpsl(
        signal_entry,
        actual_entry,
        sl_price,
        tp_price,
        str(getattr(signal, "side", "SHORT")),
        sl_pct=getattr(signal, "sl_pct", None),
        tp_pct=getattr(signal, "tp_pct", None),
    )
    if repriced is None:
        return

    new_sl, new_tp = repriced
    price_drift = abs(actual_entry - signal_entry) / signal_entry
    logger.info(
        "REPRICE_TPSL | symbol=%s signal_entry=%.6f actual_entry=%.6f drift=%.4f%% "
        "old_sl=%.6f new_sl=%.6f old_tp=%.6f new_tp=%.6f mode=%s",
        getattr(signal, "symbol", ""),
        signal_entry,
        actual_entry,
        price_drift * 100.0,
        sl_price,
        new_sl,
        tp_price,
        new_tp,
        exec_mode,
    )

    if (exec_mode or "").strip().lower() != "live":
        return

    try:
        from trading.bybit_live import _round_price_to_tick

        symbol = str(getattr(signal, "symbol", "") or position.get("symbol", ""))
        side = _side_upper(getattr(signal, "side", "SHORT"))
        position_idx = int(position.get("position_idx", 0) or 0)
        limits = broker.get_instrument_limits(symbol)
        tick_size = float(limits.get("tick_size") or 0.0001)
        notional = float(notional_usd or position.get("notional_usd") or 0)
        qty = notional / actual_entry if actual_entry > 0 else 0.0
        tp_r = _round_price_to_tick(new_tp, tick_size)
        sl_r = _round_price_to_tick(new_sl, tick_size)
        tpsl_ok, _ = broker._set_tpsl_with_retry(
            symbol=symbol,
            position_idx=position_idx,
            side=side,
            entry_price=actual_entry,
            tp=tp_r,
            sl=sl_r,
            tick_size=tick_size,
            qty=qty,
        )
        if tpsl_ok:
            position["sl"] = sl_r
            position["tp"] = tp_r
        else:
            logger.warning(
                "REPRICE_TPSL failed | symbol=%s reason=set_tpsl_with_retry returned false",
                symbol,
            )
    except Exception as exc:
        logger.warning("REPRICE_TPSL failed | symbol=%s: %s", getattr(signal, "symbol", ""), exc)
