"""Enqueue v2 Signal via v1 trading.queue."""
from __future__ import annotations

import logging
from typing import Optional

from pump_v2.core.market_context import MarketContext
from pump_v2.core.strategy_base import Signal
from pump_v2.execution.entry_gate import EntryGate, get_default_gate
from pump_v2.execution.signal_adapter import v2_signal_to_queue_dict

logger = logging.getLogger(__name__)


def maybe_enqueue_v2_signal(
    signal: Optional[Signal],
    ctx: MarketContext,
    gate: Optional[EntryGate] = None,
) -> bool:
    """
    Enqueue v2 Signal when cooldown allows.

    Returns True if enqueue ran. Never raises — errors logged as warning.
    """
    if signal is None:
        return False

    _gate = gate or get_default_gate()

    if not _gate.allow_and_record(signal.symbol):
        logger.debug(
            "pump_v2 enqueue skipped (cooldown): symbol=%s profile=%s",
            signal.symbol,
            signal.metadata.get("risk_profile"),
        )
        return False

    try:
        from trading.queue import enqueue_signal_dict

        q_dict = v2_signal_to_queue_dict(signal, ctx)
        enqueue_signal_dict(q_dict)
        logger.info(
            "pump_v2 ENQUEUED | symbol=%s profile=%s stage=%s dist=%.2f ctx=%.2f",
            signal.symbol,
            signal.metadata.get("risk_profile"),
            signal.metadata.get("stage"),
            float(signal.metadata.get("dist_to_peak_pct") or 0),
            float(signal.metadata.get("context_score") or 0),
        )
        return True
    except Exception as exc:
        logger.warning("pump_v2 enqueue error: %s", exc)
        return False
