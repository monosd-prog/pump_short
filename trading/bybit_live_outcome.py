"""Resolve LIVE trade outcome from Bybit (closed PnL / executions) instead of candle touch logic."""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any, Optional

logger = logging.getLogger(__name__)

LIVE_OUTCOME_POLL_SEC = float((__import__("os").getenv("LIVE_OUTCOME_POLL_SEC") or "5").replace(",", "."))
LIVE_OUTCOME_TIMEOUT_SEC = int((__import__("os").getenv("LIVE_OUTCOME_TIMEOUT_SEC") or "120").replace(",", "."))


def _parse_ts_to_ms(ts: str) -> Optional[int]:
    """Parse opened_ts (ISO or common str) to milliseconds. Return None on failure."""
    if not ts or not str(ts).strip():
        return None
    s = str(ts).strip().replace("Z", "+00:00").replace("+0000", "+00:00")
    for fmt in (
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S",
    ):
        try:
            dt = datetime.strptime(s, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        except ValueError:
            continue
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except ValueError:
        return None


def resolve_live_outcome(
    symbol: str,
    order_id: str,
    position_idx: int,
    opened_ts: str,
    entry_price: float,
    tp_price: float,
    sl_price: float,
    side: str,
    *,
    broker: Any = None,
    poll_sec: float = LIVE_OUTCOME_POLL_SEC,
    timeout_sec: int = LIVE_OUTCOME_TIMEOUT_SEC,
    raise_on_network_error: bool = False,
) -> Optional[dict[str, Any]]:
    """
    Resolve LIVE trade outcome from Bybit closed PnL.
    Returns {status, exit_price, exit_ts, reason, pnl_pct} or None if not yet resolved.
    status: "TP_hit" | "SL_hit" | "manual" | "unknown"
    raise_on_network_error: propagate Timeout/ConnectionError for retry logic in caller.
    """
    if not broker:
        try:
            from trading.broker import get_broker
            broker = get_broker("live", dry_run_live=False)
        except Exception as e:
            logger.warning("resolve_live_outcome: no broker %s", e)
            return None

    opened_ms = _parse_ts_to_ms(opened_ts)
    if opened_ms is None:
        logger.warning("resolve_live_outcome: unparseable opened_ts=%s", opened_ts)
        return None

    side_upper = (side or "").strip().upper()
    want_side = "Sell" if side_upper in ("SHORT", "SELL") else "Buy"
    entry_f = float(entry_price)
    tp_f = float(tp_price)
    sl_f = float(sl_price)

    start = time.monotonic()
    while (time.monotonic() - start) < timeout_sec:
        end_ms = int(time.time() * 1000)
        records = broker.get_closed_pnl(
            symbol,
            start_time_ms=opened_ms - 60_000,
            end_time_ms=end_ms + 60_000,
            limit=100,
            raise_on_network_error=raise_on_network_error,
        )
        for rec in records:
            rec_side = rec.get("side", "")
            if rec_side != want_side:
                continue
            avg_entry = float(rec.get("avgEntryPrice") or 0)
            avg_exit = float(rec.get("avgExitPrice") or 0)
            if avg_entry <= 0 or avg_exit <= 0:
                continue
            if abs(avg_entry - entry_f) / max(entry_f, 1e-9) > 0.01:
                continue
            updated_ms = int(rec.get("updatedTime") or 0)
            if updated_ms < opened_ms:
                continue
            closed_pnl = float(rec.get("closedPnl") or 0)
            exit_ts = datetime.fromtimestamp(updated_ms / 1000.0, tz=timezone.utc).isoformat()
            if want_side == "Sell":
                status = "TP_hit" if avg_exit < entry_f else "SL_hit"
            else:
                status = "TP_hit" if avg_exit > entry_f else "SL_hit"
            if abs(avg_exit - tp_f) / max(tp_f, 1e-9) < 0.005:
                status = "TP_hit"
            elif abs(avg_exit - sl_f) / max(sl_f, 1e-9) < 0.005:
                status = "SL_hit"
            pnl_pct = (entry_f - avg_exit) / entry_f * 100.0 if want_side == "Sell" else (avg_exit - entry_f) / entry_f * 100.0
            return {
                "status": status,
                "exit_price": avg_exit,
                "exit_ts": exit_ts,
                "reason": status,
                "pnl_pct": pnl_pct,
                "closed_pnl": closed_pnl,
                "order_id": rec.get("orderId", ""),
            }
        time.sleep(poll_sec)

    return None
