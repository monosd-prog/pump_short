"""Resolve LIVE trade outcome from Bybit (closed PnL / executions) instead of candle touch logic."""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any, Optional

logger = logging.getLogger(__name__)

LIVE_OUTCOME_POLL_SEC = float((__import__("os").getenv("LIVE_OUTCOME_POLL_SEC") or "3").replace(",", "."))
LIVE_OUTCOME_TIMEOUT_SEC = int((__import__("os").getenv("LIVE_OUTCOME_TIMEOUT_SEC") or "120").replace(",", "."))
# Entry price match tolerance: 2% (slippage). When position gone on exchange, use 2.5%.
_ENTRY_TOLERANCE_PCT = 0.02
_ENTRY_TOLERANCE_RELAXED_PCT = 0.025


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


def _match_closed_record(
    rec: dict,
    want_side: str,
    entry_f: float,
    tp_f: float,
    sl_f: float,
    opened_ms: int,
    entry_tolerance_pct: float,
) -> Optional[dict[str, Any]]:
    """Check if closed-pnl record matches our position. Returns outcome dict or None."""
    rec_side = rec.get("side", "")
    if rec_side != want_side:
        return None
    avg_entry = float(rec.get("avgEntryPrice") or 0)
    avg_exit = float(rec.get("avgExitPrice") or 0)
    if avg_entry <= 0 or avg_exit <= 0:
        return None
    if abs(avg_entry - entry_f) / max(entry_f, 1e-9) > entry_tolerance_pct:
        return None
    updated_ms = int(rec.get("updatedTime") or 0)
    if updated_ms < opened_ms:
        return None
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
    When position is gone from exchange (get_open_position returns None), uses relaxed entry
    tolerance to find closed-pnl record. Returns {status, exit_price, exit_ts, reason, pnl_pct}
    or None if not yet resolved.
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
    iter_count = 0
    while (time.monotonic() - start) < timeout_sec:
        iter_count += 1
        # If position is gone on exchange, use relaxed entry tolerance (slippage / fill variance)
        position_open = True
        if hasattr(broker, "get_open_position"):
            try:
                pos = broker.get_open_position(symbol, side)
                position_open = pos is not None and float(pos.get("size") or 0) > 0
            except Exception:
                pass
        entry_tol = _ENTRY_TOLERANCE_RELAXED_PCT if not position_open else _ENTRY_TOLERANCE_PCT

        end_ms = int(time.time() * 1000)
        records = broker.get_closed_pnl(
            symbol,
            start_time_ms=opened_ms - 60_000,
            end_time_ms=end_ms + 60_000,
            limit=100,
            raise_on_network_error=raise_on_network_error,
        )
        n_records = len(records)
        n_side = sum(1 for r in records if r.get("side") == want_side)
        n_after_time = sum(
            1 for r in records
            if r.get("side") == want_side and int(r.get("updatedTime") or 0) >= opened_ms
        )
        logger.debug(
            "LIVE_OUTCOME_CHECK | symbol=%s iter=%d position_open=%s n_closed_pnl=%d n_side=%d n_after_open=%d entry_tol=%.2f%%",
            symbol, iter_count, position_open, n_records, n_side, n_after_time, entry_tol * 100,
        )

        for rec in records:
            result = _match_closed_record(
                rec, want_side, entry_f, tp_f, sl_f, opened_ms, entry_tol,
            )
            if result:
                logger.info(
                    "LIVE_OUTCOME_RESOLVED | symbol=%s status=%s exit=%.4f closed_pnl=%.4f by=%s",
                    symbol, result["status"], result["exit_price"], result.get("closed_pnl", 0),
                    "closed_pnl" + ("_relaxed" if not position_open else ""),
                )
                return result
        time.sleep(poll_sec)

    logger.info(
        "LIVE_OUTCOME_PENDING | symbol=%s side=%s entry=%.4f iter=%d no_match (position_open=%s)",
        symbol, want_side, entry_f, iter_count, position_open if iter_count else "unknown",
    )
    return None
