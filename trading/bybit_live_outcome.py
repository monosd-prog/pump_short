"""Resolve LIVE trade outcome from Bybit (closed PnL / order history / executions). Cascade fallback."""
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
# TP/SL level tolerance: exit must cross level to classify as TP_hit/SL_hit (0.5% relative).
_EXIT_LEVEL_TOLERANCE_PCT = 0.005
_TIME_WINDOW_START_MS = 5 * 60 * 1000


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
    *,
    symbol: str = "",
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
    status = _classify_exit_price(avg_exit, entry_f, tp_f, sl_f, want_side, closed_pnl=closed_pnl, symbol=symbol)
    pnl_pct = (entry_f - avg_exit) / entry_f * 100.0 if want_side == "Sell" else (avg_exit - entry_f) / entry_f * 100.0
    if status == "EARLY_EXIT":
        logger.info(
            "LIVE_OUTCOME_CLASSIFIED_AS_OTHER | symbol=%s exit=%.6f entry=%.6f tp=%.6f sl=%.6f status=EARLY_EXIT",
            symbol or "?", avg_exit, entry_f, tp_f, sl_f,
        )
    return {
        "status": status,
        "exit_price": avg_exit,
        "exit_ts": exit_ts,
        "reason": status,
        "pnl_pct": pnl_pct,
        "closed_pnl": closed_pnl,
        "order_id": rec.get("orderId", ""),
    }


def _classify_exit_price(
    exit_price: float,
    entry: float,
    tp: float,
    sl: float,
    want_side: str,
    *,
    closed_pnl: Optional[float] = None,
    symbol: str = "",
) -> str:
    """
    Low-level helper: classify outcome from exit_price using TP/SL level boundaries only.
    TP_hit only if exit crossed TP; SL_hit only if exit crossed SL.
    If exit is between TP and SL: EARLY_EXIT (manual/other close).

    IMPORTANT:
    - PnL / UI labels / commissions do NOT influence classification.
    - Only relative position of exit_price vs TP/SL with tolerance is used.
    """
    tol = _EXIT_LEVEL_TOLERANCE_PCT
    tp_tol = tp * (1 + tol) if want_side == "Sell" else tp * (1 - tol)
    sl_tol = sl * (1 - tol) if want_side == "Sell" else sl * (1 + tol)

    # Sanity: TP_hit = profit. SHORT profit iff exit < entry; LONG profit iff exit > entry.
    # If exit is in loss zone, never classify as TP_hit (handles invalid tp>entry for short / tp<entry for long).
    if want_side == "Sell":
        if exit_price > entry:
            # Loss: cannot be TP_hit
            if exit_price >= sl_tol:
                logger.debug(
                    "LIVE_OUTCOME_CLASSIFY_BY_LEVELS | source=levels symbol=%s exit=%.6f sl=%.6f SHORT SL_hit (exit>=sl-tol, loss_zone)",
                    symbol or "?", exit_price, sl,
                )
                return "SL_hit"
            logger.info(
                "LIVE_OUTCOME_INVALID_TP_ZONE | symbol=%s exit=%.6f entry=%.6f tp=%.6f SHORT exit>entry (loss) tp_zone_invalid -> EARLY_EXIT",
                symbol or "?", exit_price, entry, tp,
            )
            return "EARLY_EXIT"
        if exit_price <= tp_tol:
            logger.debug(
                "LIVE_OUTCOME_CLASSIFY_BY_LEVELS | source=levels closed_pnl_based=0 symbol=%s exit=%.6f tp=%.6f SHORT TP_hit (exit<=tp+tol)",
                symbol or "?", exit_price, tp,
            )
            return "TP_hit"
        if exit_price >= sl_tol:
            logger.debug(
                "LIVE_OUTCOME_CLASSIFY_BY_LEVELS | source=levels closed_pnl_based=0 symbol=%s exit=%.6f sl=%.6f SHORT SL_hit (exit>=sl-tol)",
                symbol or "?", exit_price, sl,
            )
            return "SL_hit"
    else:
        if exit_price < entry:
            # Loss: cannot be TP_hit
            if exit_price <= sl_tol:
                logger.debug(
                    "LIVE_OUTCOME_CLASSIFY_BY_LEVELS | source=levels symbol=%s exit=%.6f sl=%.6f LONG SL_hit (exit<=sl+tol, loss_zone)",
                    symbol or "?", exit_price, sl,
                )
                return "SL_hit"
            logger.info(
                "LIVE_OUTCOME_INVALID_TP_ZONE | symbol=%s exit=%.6f entry=%.6f tp=%.6f LONG exit<entry (loss) tp_zone_invalid -> EARLY_EXIT",
                symbol or "?", exit_price, entry, tp,
            )
            return "EARLY_EXIT"
        if exit_price >= tp_tol:
            logger.debug(
                "LIVE_OUTCOME_CLASSIFY_BY_LEVELS | source=levels closed_pnl_based=0 symbol=%s exit=%.6f tp=%.6f LONG TP_hit (exit>=tp-tol)",
                symbol or "?", exit_price, tp,
            )
            return "TP_hit"
        if exit_price <= sl_tol:
            logger.debug(
                "LIVE_OUTCOME_CLASSIFY_BY_LEVELS | source=levels closed_pnl_based=0 symbol=%s exit=%.6f sl=%.6f LONG SL_hit (exit<=sl+tol)",
                symbol or "?", exit_price, sl,
            )
            return "SL_hit"

    gross_fav = (exit_price < entry) if want_side == "Sell" else (exit_price > entry)
    if closed_pnl is not None and closed_pnl < 0 and gross_fav:
        logger.info(
            "LIVE_OUTCOME_NET_NEGATIVE_DUE_TO_FEES | symbol=%s exit=%.6f entry=%.6f tp=%.6f sl=%.6f closed_pnl=%.4f",
            symbol or "?", exit_price, entry, tp, sl, closed_pnl,
        )
    logger.info(
        "LIVE_OUTCOME_BETWEEN_LEVELS | symbol=%s exit=%.6f entry=%.6f tp=%.6f sl=%.6f -> EARLY_EXIT",
        symbol or "?", exit_price, entry, tp, sl,
    )
    return "EARLY_EXIT"


def classify_live_outcome_by_levels(
    *,
    exit_price: float,
    entry: float,
    tp: float,
    sl: float,
    side: str,
    closed_pnl: Optional[float] = None,
    symbol: str = "",
) -> str:
    """
    Public helper used by all live outcome paths.

    Rules:
    - SHORT (side in {\"SHORT\",\"Sell\"}):
      - TP_hit  if exit_price <= tp + tolerance
      - SL_hit  if exit_price >= sl - tolerance
      - EARLY_EXIT if tp < exit_price < sl
    - LONG (side in {\"LONG\",\"Buy\"}) зеркально.

    Net PnL / комиссии / UI label \"Lose\" не переопределяют level-based статус.
    """
    want_side = "Sell" if (side or "").strip().upper() in ("SHORT", "SELL") else "Buy"
    return _classify_exit_price(
        exit_price=exit_price,
        entry=entry,
        tp=tp,
        sl=sl,
        want_side=want_side,
        closed_pnl=closed_pnl,
        symbol=symbol,
    )


def _resolve_via_fallback(
    broker: Any,
    symbol: str,
    side: str,
    entry: float,
    tp: float,
    sl: float,
    opened_ms: int,
    end_ms: int,
) -> Optional[dict[str, Any]]:
    """
    Cascade fallback when closed-pnl returns nothing. Try order history, then execution list.
    Returns outcome dict or None.
    """
    want_side = "Sell" if (side or "").strip().upper() in ("SHORT", "SELL") else "Buy"
    close_side = "Buy" if want_side == "Sell" else "Sell"
    start_ms = opened_ms - _TIME_WINDOW_START_MS
    if end_ms - start_ms > 7 * 24 * 3600 * 1000:
        end_ms = start_ms + 7 * 24 * 3600 * 1000 - 1000

    if hasattr(broker, "get_order_history"):
        try:
            orders = broker.get_order_history(
                symbol, start_time_ms=start_ms, end_time_ms=end_ms,
                order_status="Filled", limit=50, max_pages=5, raise_on_network_error=False,
            )
            reduce_only = [
                o for o in orders
                if (o.get("reduceOnly") in (True, "true", "True", 1, "1"))
                and o.get("side") == close_side
            ]
            in_time = []
            for o in reduce_only:
                um = int(o.get("updatedTime") or o.get("createdTime") or 0)
                if um >= opened_ms and um <= end_ms:
                    ap = float(o.get("avgPrice") or 0)
                    if ap > 0:
                        in_time.append((ap, um))
            if in_time:
                if len(in_time) == 1:
                    avg_exit, um = in_time[0]
                else:
                    in_time_sorted = sorted(in_time, key=lambda x: x[1], reverse=True)
                    avg_exit, um = in_time_sorted[0]
                    logger.debug("LIVE_OUTCOME_FALLBACK | order_history multiple_reduceOnly symbol=%s using_latest", symbol)
                exit_ts = datetime.fromtimestamp(um / 1000.0, tz=timezone.utc).isoformat()
                status = _classify_exit_price(avg_exit, entry, tp, sl, want_side, symbol=symbol)
                pnl_pct = (entry - avg_exit) / entry * 100.0 if want_side == "Sell" else (avg_exit - entry) / entry * 100.0
                logger.info(
                    "LIVE_OUTCOME_ORDER_HISTORY_FOUND | symbol=%s status=%s exit=%.4f source=order_history",
                    symbol, status, avg_exit,
                )
                return {"status": status, "exit_price": avg_exit, "exit_ts": exit_ts, "pnl_pct": pnl_pct, "closed_pnl": 0.0}
        except Exception as e:
            logger.debug("LIVE_OUTCOME_FALLBACK | order_history error symbol=%s: %s", symbol, e)

    if hasattr(broker, "get_executions_all_pages"):
        try:
            execs = broker.get_executions_all_pages(
                symbol, start_time_ms=start_ms, end_time_ms=end_ms,
                limit=100, max_pages=5, raise_on_network_error=False,
            )
            closing = []
            for ex in execs:
                cs = ex.get("closedSize")
                eq_raw = ex.get("execQty") or ex.get("closedSize") or 0
                try:
                    cs_val = float(cs) if cs else 0
                except (TypeError, ValueError):
                    cs_val = 0
                try:
                    eq_val = float(eq_raw) if eq_raw else 0
                except (TypeError, ValueError):
                    eq_val = 0
                if ex.get("side") == close_side and (cs_val > 0 or eq_val > 0):
                    ep = float(ex.get("execPrice") or 0)
                    et = int(ex.get("execTime") or 0)
                    if ep > 0 and opened_ms <= et <= end_ms:
                        closing.append({"execPrice": ep, "execQty": max(cs_val, eq_val), "execTime": et})
            if closing:
                total_qty = sum(c["execQty"] for c in closing)
                if total_qty > 0:
                    avg_exit = sum(c["execPrice"] * c["execQty"] for c in closing) / total_qty
                    latest_et = max(c["execTime"] for c in closing)
                    exit_ts = datetime.fromtimestamp(latest_et / 1000.0, tz=timezone.utc).isoformat()
                    status = _classify_exit_price(avg_exit, entry, tp, sl, want_side, symbol=symbol)
                    pnl_pct = (entry - avg_exit) / entry * 100.0 if want_side == "Sell" else (avg_exit - entry) / entry * 100.0
                    logger.info(
                        "LIVE_OUTCOME_EXECUTIONS_FOUND | symbol=%s status=%s exit=%.4f n_fills=%d source=execution_list",
                        symbol, status, avg_exit, len(closing),
                    )
                    return {"status": status, "exit_price": avg_exit, "exit_ts": exit_ts, "pnl_pct": pnl_pct, "closed_pnl": 0.0}
        except Exception as e:
            logger.debug("LIVE_OUTCOME_FALLBACK | execution_list error symbol=%s: %s", symbol, e)
    return None


def resolve_live_outcome_final_reconcile(
    broker: Any,
    symbol: str,
    side: str,
    opened_ts: str,
    entry: float,
    tp: float,
    sl: float,
    *,
    raise_on_network_error: bool = False,
) -> Optional[dict[str, Any]]:
    """
    One-shot final reconciliation before TIMEOUT. Use when position reached TTL and we want
    to avoid false TIMEOUT if position already closed on exchange.
    Returns outcome dict (TP_hit/SL_hit) or None. If position still open, returns None.
    """
    opened_ms = _parse_ts_to_ms(opened_ts)
    if opened_ms is None:
        return None
    position_open = True
    if hasattr(broker, "get_open_position"):
        try:
            pos = broker.get_open_position(symbol, side)
            position_open = pos is not None and float(pos.get("size") or 0) > 0
        except Exception:
            pass
    if position_open:
        logger.debug("LIVE_OUTCOME_FINAL_RECONCILE | symbol=%s position_still_open skip", symbol)
        return None

    logger.info("LIVE_OUTCOME_FINAL_RECONCILE | symbol=%s position_gone attempting_resolve", symbol)
    want_side = "Sell" if (side or "").strip().upper() in ("SHORT", "SELL") else "Buy"
    entry_tol = _ENTRY_TOLERANCE_RELAXED_PCT
    end_ms = int(__import__("time").time() * 1000)

    records = []
    if hasattr(broker, "get_closed_pnl_all_pages"):
        try:
            records = broker.get_closed_pnl_all_pages(
                symbol,
                start_time_ms=opened_ms - 60_000,
                end_time_ms=end_ms + 60_000,
                limit=100,
                max_pages=5,
                raise_on_network_error=raise_on_network_error,
            )
        except Exception as e:
            logger.debug("LIVE_OUTCOME_FINAL_RECONCILE | get_closed_pnl_all_pages failed: %s", e)
    if not records and hasattr(broker, "get_closed_pnl"):
        records = broker.get_closed_pnl(
            symbol, start_time_ms=opened_ms - 60_000, end_time_ms=end_ms + 60_000,
            limit=100, raise_on_network_error=raise_on_network_error,
        )

    for rec in records:
        result = _match_closed_record(rec, want_side, entry, tp, sl, opened_ms, entry_tol, symbol=symbol)
        if result:
            logger.info(
                "LIVE_OUTCOME_FINAL_RECONCILE | symbol=%s CLOSED_PNL_FOUND status=%s",
                symbol, result["status"],
            )
            return result

    fb = _resolve_via_fallback(broker, symbol, side, entry, tp, sl, opened_ms, end_ms)
    if fb:
        return fb
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
        if hasattr(broker, "get_closed_pnl_all_pages"):
            try:
                records = broker.get_closed_pnl_all_pages(
                    symbol,
                    start_time_ms=opened_ms - 60_000,
                    end_time_ms=end_ms + 60_000,
                    limit=100,
                    max_pages=5,
                    raise_on_network_error=raise_on_network_error,
                )
            except Exception as e:
                logger.debug("LIVE_OUTCOME_CHECK | get_closed_pnl_all_pages failed symbol=%s: %s", symbol, e)
                records = broker.get_closed_pnl(
                    symbol, start_time_ms=opened_ms - 60_000, end_time_ms=end_ms + 60_000,
                    limit=100, raise_on_network_error=raise_on_network_error,
                )
        else:
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
        if position_open:
            logger.debug("LIVE_OUTCOME_OPEN_FOUND | symbol=%s iter=%d", symbol, iter_count)
        logger.debug(
            "LIVE_OUTCOME_CHECK | symbol=%s iter=%d position_open=%s n_closed_pnl=%d n_side=%d n_after_open=%d entry_tol=%.2f%%",
            symbol, iter_count, position_open, n_records, n_side, n_after_time, entry_tol * 100,
        )

        for rec in records:
            result = _match_closed_record(
                rec, want_side, entry_f, tp_f, sl_f, opened_ms, entry_tol, symbol=symbol,
            )
            if result:
                logger.info(
                    "LIVE_OUTCOME_CLOSED_PNL_FOUND | symbol=%s status=%s exit=%.4f closed_pnl=%.4f",
                    symbol, result["status"], result["exit_price"], result.get("closed_pnl", 0),
                )
                logger.info(
                    "LIVE_OUTCOME_RESOLVED | symbol=%s status=%s exit=%.4f by=closed_pnl",
                    symbol, result["status"], result["exit_price"],
                )
                return result

        # Position gone but closed-pnl has no match: cascade fallback
        if not position_open:
            fb = _resolve_via_fallback(
                broker, symbol, side, entry_f, tp_f, sl_f, opened_ms, end_ms,
            )
            if fb:
                logger.info(
                    "LIVE_OUTCOME_RESOLVED | symbol=%s status=%s exit=%.4f by=fallback",
                    symbol, fb["status"], fb.get("exit_price", 0),
                )
                return fb
        time.sleep(poll_sec)

    logger.info(
        "LIVE_OUTCOME_PENDING | symbol=%s side=%s entry=%.4f iter=%d no_match (position_open=%s)",
        symbol, want_side, entry_f, iter_count, position_open if iter_count else "unknown",
    )
    return None
