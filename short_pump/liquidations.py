# short_pump/liquidations.py
from __future__ import annotations

import json
import os
import socket
import threading
import time
from collections import defaultdict, deque
from typing import Deque, Dict, Optional, Set, Tuple

from short_pump.logging_utils import get_logger, log_exception, log_info, log_warning

logger = get_logger(__name__)

try:
    import websocket  # type: ignore
    from websocket import WebSocketTimeoutException, WebSocketConnectionClosedException  # type: ignore
except Exception:  # pragma: no cover - runtime optional
    websocket = None
    WebSocketTimeoutException = Exception  # type: ignore
    WebSocketConnectionClosedException = Exception  # type: ignore


_lock = threading.Lock()
_events_short: Dict[str, Deque[Tuple[int, float, float]]] = defaultdict(deque)
_events_long: Dict[str, Deque[Tuple[int, float, float]]] = defaultdict(deque)
_started = False
_max_age_sec = 600.0  # keep up to 10 minutes
_subscribed_symbols: Set[str] = set()
_desired_symbols: Set[str] = set()
_last_heartbeat = 0.0
_rx_total = 0
_rx_json_ok = 0
_rx_topic_liq = 0
_rx_events_total = 0
_last_raw_ts: Optional[float] = None
_last_rx_wall = 0.0
_last_event_ts_ms: Optional[int] = None
_last_symbol: Optional[str] = None
_last_ws_op: Optional[str] = None
_last_ws_topic: Optional[str] = None
_reconnects_total = 0
_debug_msg_total = 0
_ping_sent_count = 0
_last_disconnect_reason: Optional[str] = None
_last_no_data_log_ts = 0.0
_last_liq_zero_window_log_ts: Dict[str, float] = {}
_last_liq_after_event_log_ts: Dict[str, float] = {}

def _env_flag(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")


_LIQ_WS_DEBUG = _env_flag("LIQ_WS_DEBUG", False)


def _normalize_symbol(symbol: str) -> str:
    return (symbol or "").strip().upper()


def _endpoint_for_category(category: str) -> str:
    c = (category or "linear").strip().lower()
    if c in ("linear", "inverse"):
        return "wss://stream.bybit.com/v5/public/linear"
    if c == "spot":
        return "wss://stream.bybit.com/v5/public/spot"
    if c == "option":
        return "wss://stream.bybit.com/v5/public/option"
    return "wss://stream.bybit.com/v5/public/linear"


def _add_event(symbol: str, ts_ms: int, qty: float, price: float, side: str) -> None:
    if not symbol or qty <= 0 or price <= 0:
        return
    sym = _normalize_symbol(symbol)
    with _lock:
        if side == "short":
            _events_short[sym].append((ts_ms, qty, price))
        elif side == "long":
            _events_long[sym].append((ts_ms, qty, price))
        _purge_locked(sym, now=ts_ms)


def _purge_locked(symbol: str, now: float) -> None:
    q_short = _events_short.get(symbol)
    q_long = _events_long.get(symbol)
    cutoff_ms = int(now) - int(_max_age_sec * 1000)
    while q_short and q_short[0][0] < cutoff_ms:
        q_short.popleft()
    while q_long and q_long[0][0] < cutoff_ms:
        q_long.popleft()


def get_liq_stats(symbol: str, now_ts: float, window_seconds: int, side: str = "short") -> Tuple[int, float]:
    """Return (count, qty_sum) for liquidations over window_seconds."""
    sym = _normalize_symbol(symbol)
    if now_ts < 10**11:
        now_ms = int(now_ts * 1000)
    else:
        now_ms = int(now_ts)
    with _lock:
        _purge_locked(sym, now=now_ms)
        q = _events_short.get(sym) if side == "short" else _events_long.get(sym)
        if not q:
            return 0, 0.0
        cutoff_ms = now_ms - int(window_seconds * 1000)
        count = 0
        qty_sum = 0.0
        for ts_ms, qty, _price in q:
            if ts_ms >= cutoff_ms:
                count += 1
                qty_sum += qty
        if count == 0 and q:
            now_wall = time.time()
            last_dbg = _last_liq_zero_window_log_ts.get(sym, 0.0)
            if _LIQ_WS_DEBUG or (now_wall - last_dbg >= 60):
                last_event_ts_ms_for_symbol = q[-1][0] if q else None
                computed_age_ms = (
                    int(now_ms - int(last_event_ts_ms_for_symbol))
                    if last_event_ts_ms_for_symbol is not None
                    else None
                )
                log_info(
                    logger,
                    "LIQ_STATS_ZERO_WINDOW_DEBUG",
                    symbol=sym,
                    step="LIQ_STATS",
                    extra={
                        "side": side,
                        "now_ms": now_ms,
                        "window_ms": int(window_seconds * 1000),
                        "events_in_buffer": len(q),
                        "last_event_ts_ms_for_symbol": last_event_ts_ms_for_symbol,
                        "computed_age_ms": computed_age_ms,
                    },
                )
                _last_liq_zero_window_log_ts[sym] = now_wall
        return count, float(qty_sum)
def get_liq_health() -> Dict[str, Optional[int]]:
    with _lock:
        xrp_short = _events_short.get("XRPUSDT")
        xrp_long = _events_long.get("XRPUSDT")
        xrp_last_short_ts_ms = xrp_short[-1][0] if xrp_short else None
        xrp_last_long_ts_ms = xrp_long[-1][0] if xrp_long else None
        return {
            "rx_total": _rx_total,
            "rx_json_ok": _rx_json_ok,
            "rx_topic_liq": _rx_topic_liq,
            "rx_events_total": _rx_events_total,
            "last_raw_ts": _last_raw_ts,
            "last_event_ts_ms": _last_event_ts_ms,
            "last_symbol": _last_symbol,
            "last_ws_op": _last_ws_op,
            "last_ws_topic": _last_ws_topic,
            "subscribed_symbols_count": len(_subscribed_symbols),
            "reconnects_total": _reconnects_total,
            "ping_sent_count": _ping_sent_count,
            "xrp_last_short_ts_ms": xrp_last_short_ts_ms,
            "xrp_last_long_ts_ms": xrp_last_long_ts_ms,
        }


def register_symbol(symbol: str) -> None:
    sym = _normalize_symbol(symbol)
    if not sym:
        return
    with _lock:
        _desired_symbols.add(sym)


def unregister_symbol(symbol: str) -> None:
    sym = _normalize_symbol(symbol)
    if not sym:
        return
    with _lock:
        _desired_symbols.discard(sym)


def start_liquidation_listener(category: str) -> None:
    global _started
    if _started:
        return
    _started = True

    if websocket is None:
        log_warning(logger, "websocket-client not available; liquidation listener disabled", step="LIQ_WS")
        return

    url = _endpoint_for_category(category)
    log_info(logger, "Starting liquidation listener", step="LIQ_WS", extra={"url": url})
    raw_force = os.getenv("FORCE_LIQ_SYMBOLS", "")
    if raw_force:
        for sym in [s.strip().upper() for s in raw_force.split(",") if s.strip()]:
            register_symbol(sym)

    def _run() -> None:
        global _last_heartbeat
        global _rx_total, _rx_json_ok, _rx_topic_liq, _rx_events_total, _last_raw_ts, _last_rx_wall, _last_event_ts_ms, _last_symbol, _reconnects_total, _debug_msg_total
        global _ping_sent_count, _last_disconnect_reason, _last_no_data_log_ts, _last_ws_op, _last_ws_topic

        def _maybe_log_health(now: float, url: str) -> None:
            global _last_heartbeat
            if now - _last_heartbeat >= 60:
                health = get_liq_health()
                log_info(
                    logger,
                    "LIQ_WS_HEALTH",
                    step="LIQ_WS",
                    extra={"url": url, **health},
                )
                _last_heartbeat = now
            if _rx_topic_liq > 0 and _rx_events_total == 0 and (now - _last_rx_wall >= 60):
                log_info(
                    logger,
                    "LIQ_WS_PARSE_SUSPECT",
                    step="LIQ_WS",
                    extra={
                        "url": url,
                        "rx_topic_liq": _rx_topic_liq,
                        "rx_events_total": _rx_events_total,
                        "last_raw_ts": _last_raw_ts,
                        "last_ws_op": _last_ws_op,
                        "last_ws_topic": _last_ws_topic,
                    },
                )
        backoff = 1.0
        conn_id = 0
        while True:
            try:
                reason = "startup" if conn_id == 0 else (_last_disconnect_reason or "unknown")
                log_info(logger, "LIQ_WS_RECONNECTING", step="LIQ_WS", extra={"reason": reason, "conn_id": conn_id + 1})
                conn_id += 1
                ws = websocket.WebSocket()
                ws.connect(url, timeout=10)
                ws.settimeout(10)
                with _lock:
                    _subscribed_symbols.clear()
                    desired_snapshot = sorted(_desired_symbols)
                desired_topics = [f"allLiquidation.{sym}" for sym in desired_snapshot]
                log_info(
                    logger,
                    "LIQ_WS_CONNECT_CTX",
                    step="LIQ_WS",
                    extra={
                        "category": (category or "linear").strip().lower(),
                        "url": url,
                        "conn_id": conn_id,
                        "topics_count": len(desired_topics),
                        "topics_first": desired_topics[0] if desired_topics else None,
                        "topics_last": desired_topics[-1] if desired_topics else None,
                    },
                )
                log_info(logger, "WS connected", step="LIQ_WS", extra={"url": url, "conn_id": conn_id})
                log_info(
                    logger,
                    "LIQ_WS_DEBUG_ON",
                    step="LIQ_WS",
                    extra={"conn_id": conn_id, "env": os.getenv("LIQ_WS_DEBUG"), "parsed": _LIQ_WS_DEBUG},
                )

                backoff = 1.0
                msg_idx = 0
                last_ping_wall = 0.0
                _ping_sent_count = 0
                _last_rx_wall = time.time()
                debug_event_window_start = 0.0
                debug_event_count = 0
                last_reconcile = 0.0
                last_empty_log = 0.0
                while True:
                    now_wall = time.time()
                    if now_wall - last_reconcile >= 3:
                        with _lock:
                            desired = set(_desired_symbols)
                            subscribed = set(_subscribed_symbols)
                        to_add = sorted(desired - subscribed)
                        to_remove = sorted(subscribed - desired)
                        if to_add:
                            args = [f"allLiquidation.{sym}" for sym in to_add]
                            if not all(arg.startswith("allLiquidation.") for arg in args):
                                log_warning(
                                    logger,
                                    "LIQ_WS_SUBSCRIBE_INVALID",
                                    step="LIQ_WS",
                                    extra={"args": args},
                                )
                            else:
                                ws.send(json.dumps({"op": "subscribe", "args": args}))
                                log_info(
                                    logger,
                                    "LIQ_WS_SUBSCRIBE_SENT",
                                    step="LIQ_WS",
                                    extra={
                                        "count": len(args),
                                        "first": args[0] if args else None,
                                        "last": args[-1] if args else None,
                                    },
                                )
                                with _lock:
                                    _subscribed_symbols.update(to_add)
                                log_info(
                                    logger,
                                    "LIQ_WS_SUBSCRIBE",
                                    step="LIQ_WS",
                                    extra={"added": len(to_add), "symbols": to_add},
                                )
                        if to_remove:
                            args = [f"allLiquidation.{sym}" for sym in to_remove]
                            if not all(arg.startswith("allLiquidation.") for arg in args):
                                log_warning(
                                    logger,
                                    "LIQ_WS_UNSUBSCRIBE_INVALID",
                                    step="LIQ_WS",
                                    extra={"args": args},
                                )
                            else:
                                ws.send(json.dumps({"op": "unsubscribe", "args": args}))
                                log_info(
                                    logger,
                                    "LIQ_WS_UNSUBSCRIBE_SENT",
                                    step="LIQ_WS",
                                    extra={
                                        "count": len(args),
                                        "first": args[0] if args else None,
                                        "last": args[-1] if args else None,
                                    },
                                )
                                with _lock:
                                    for sym in to_remove:
                                        _subscribed_symbols.discard(sym)
                                log_info(
                                    logger,
                                    "LIQ_WS_UNSUBSCRIBE",
                                    step="LIQ_WS",
                                    extra={"removed": len(to_remove), "symbols": to_remove},
                                )
                        if not desired and (now_wall - last_empty_log >= 60):
                            log_info(
                                logger,
                                "LIQ_WS_NO_SYMBOLS",
                                step="LIQ_WS",
                                extra={"conn_id": conn_id},
                            )
                            last_empty_log = now_wall
                        last_reconcile = now_wall

                    if now_wall - last_ping_wall >= 20:
                        try:
                            ping_method = "json"
                            if hasattr(ws, "ping"):
                                ws.ping()
                                ping_method = "frame"
                            else:
                                ws.send(json.dumps({"op": "ping"}))
                            log_info(
                                logger,
                                "LIQ_WS_PING",
                                step="LIQ_WS",
                                extra={"conn_id": conn_id, "method": ping_method},
                            )
                            _ping_sent_count += 1
                        except Exception:
                            pass
                        last_ping_wall = now_wall

                    try:
                        _maybe_log_health(time.time(), url)
                        raw = ws.recv()
                    except (WebSocketTimeoutException, socket.timeout, TimeoutError):
                        _last_disconnect_reason = "timeout_continue"
                        if _LIQ_WS_DEBUG:
                            log_info(logger, "LIQ_WS_TIMEOUT_CONTINUE", step="LIQ_WS", extra={"conn_id": conn_id})
                        now = time.time()
                        _maybe_log_health(now, url)
                        if now - _last_rx_wall >= 60 and (now - _last_no_data_log_ts >= 60):
                            log_info(
                                logger,
                                "LIQ_WS_NO_DATA_60S",
                                step="LIQ_WS",
                                extra={
                                    "conn_id": conn_id,
                                    "subscribed_symbols_count": len(_subscribed_symbols),
                                    "ping_sent_count": _ping_sent_count,
                                    "last_raw_ts": _last_raw_ts,
                                    "last_ws_op": _last_ws_op,
                                    "last_ws_topic": _last_ws_topic,
                                },
                            )
                            _last_no_data_log_ts = now
                        continue

                    msg_idx += 1
                    _last_raw_ts = time.time()
                    if not raw:
                        continue
                    _rx_total += 1
                    _last_rx_wall = time.time()
                    if _LIQ_WS_DEBUG and _debug_msg_total < 10:
                        if isinstance(raw, (bytes, bytearray)):
                            raw_text = raw.decode("utf-8", errors="replace")
                        else:
                            raw_text = str(raw)
                        log_info(
                            logger,
                            "LIQ_WS_RAW",
                            step="LIQ_WS",
                            extra={
                                "conn_id": conn_id,
                                "msg_idx": msg_idx,
                                "len": len(raw_text),
                                "liq_symbol": _last_symbol,
                                "raw": raw_text[:500],
                            },
                        )
                        _debug_msg_total += 1
                    try:
                        msg = json.loads(raw)
                        _rx_json_ok += 1
                    except Exception:
                        continue
                    if isinstance(msg, dict):
                        if msg.get("op"):
                            _last_ws_op = str(msg.get("op"))
                        if msg.get("topic"):
                            _last_ws_topic = str(msg.get("topic"))
                    if isinstance(msg, dict) and (msg.get("op") in ("subscribe", "unsubscribe") or "success" in msg):
                        log_info(
                            logger,
                            "LIQ_WS_ACK",
                            step="LIQ_WS",
                            extra={
                                "op": msg.get("op"),
                                "success": msg.get("success"),
                                "ret_msg": msg.get("ret_msg"),
                                "req_id": msg.get("req_id"),
                            },
                        )
                    if isinstance(msg, dict) and msg.get("op") == "pong":
                        server_ts = None
                        data = msg.get("data")
                        if isinstance(data, list) and data:
                            server_ts = data[0]
                        log_info(
                            logger,
                            "LIQ_WS_PONG",
                            step="LIQ_WS",
                            extra={"server_ts": server_ts, "conn_id": conn_id},
                        )
                    topic = msg.get("topic") or msg.get("op") or ""
                    topic = topic if isinstance(topic, str) else ""
                    if topic.startswith("allLiquidation."):
                        _rx_topic_liq += 1

                    data = msg.get("data")
                    if isinstance(data, dict):
                        items = [data]
                    elif isinstance(data, list):
                        items = data
                    else:
                        items = []
                    if not items:
                        continue
                    for item in items:
                        if not isinstance(item, dict):
                            continue
                        symbol = item.get("s") or (topic.split(".", 1)[1] if "." in topic else "")
                        side_raw = item.get("S") or ""
                        side = side_raw.strip().lower()
                        # For Bybit allLiquidation: Buy closes SHORT, Sell closes LONG.
                        if side not in ("buy", "sell"):
                            continue
                        price = item.get("p") or item.get("price") or 0
                        qty = item.get("v") or item.get("size") or item.get("qty") or item.get("q") or 0
                        ts_candidates = (
                            item.get("ts"),
                            item.get("T"),
                            item.get("ctime"),
                            item.get("updatedTime"),
                            msg.get("ts"),
                            msg.get("ctime"),
                            msg.get("creationTime"),
                        )
                        try:
                            price_f = float(price)
                            qty_f = float(qty)
                        except Exception:
                            continue
                        ts_ms = 0
                        ts_fallback = False
                        for ts_candidate in ts_candidates:
                            if ts_candidate is None:
                                continue
                            try:
                                candidate_ms = int(float(ts_candidate))
                            except Exception:
                                continue
                            if candidate_ms <= 0:
                                continue
                            if candidate_ms < 10**11:
                                candidate_ms *= 1000
                            if candidate_ms <= 0:
                                continue
                            ts_ms = candidate_ms
                            break
                        if ts_ms <= 0:
                            ts_ms = int(time.time() * 1000)
                            ts_fallback = True

                        _rx_events_total += 1
                        _last_event_ts_ms = ts_ms
                        _last_symbol = str(symbol) if symbol else None
                        liq_side = "short" if side == "buy" else "long"
                        _add_event(str(symbol), ts_ms, qty_f, price_f, liq_side)
                        now_ms = int(time.time() * 1000)
                        age_ms = int(now_ms - ts_ms)
                        log_info(
                            logger,
                            "LIQ_WS_EVENT",
                            symbol=str(symbol),
                            step="LIQ_WS",
                            extra={
                                "side": side_raw,
                                "size": qty_f,
                                "price": price_f,
                                "ts_ms": ts_ms,
                                "ts_fallback": ts_fallback,
                                "now_ms": now_ms,
                                "age_ms": age_ms,
                            },
                        )
                        sym_norm = _normalize_symbol(str(symbol))
                        now_ts_stats = time.time()
                        long_30s_count, long_30s_usd = get_liq_stats(sym_norm, now_ts_stats, 30, side="long")
                        short_30s_count, short_30s_usd = get_liq_stats(sym_norm, now_ts_stats, 30, side="short")
                        long_60s_count, long_60s_usd = get_liq_stats(sym_norm, now_ts_stats, 60, side="long")
                        short_60s_count, short_60s_usd = get_liq_stats(sym_norm, now_ts_stats, 60, side="short")
                        with _lock:
                            events_in_buffer_long = len(_events_long.get(sym_norm) or [])
                            events_in_buffer_short = len(_events_short.get(sym_norm) or [])
                        last_after_event_log_ts = _last_liq_after_event_log_ts.get(sym_norm, 0.0)
                        if (now_ts_stats - last_after_event_log_ts) >= 2.0:
                            log_info(
                                logger,
                                "LIQ_STATS_AFTER_EVENT",
                                symbol=sym_norm,
                                step="LIQ_WS",
                                side_raw=side_raw,
                                liq_side_selected=liq_side,
                                now_ms=now_ms,
                                ts_ms=ts_ms,
                                age_ms=age_ms,
                                long_30s_count=long_30s_count,
                                long_30s_usd=long_30s_usd,
                                short_30s_count=short_30s_count,
                                short_30s_usd=short_30s_usd,
                                long_60s_count=long_60s_count,
                                long_60s_usd=long_60s_usd,
                                short_60s_count=short_60s_count,
                                short_60s_usd=short_60s_usd,
                                events_in_buffer_long=events_in_buffer_long,
                                events_in_buffer_short=events_in_buffer_short,
                            )
                            _last_liq_after_event_log_ts[sym_norm] = now_ts_stats
                        if _LIQ_WS_DEBUG:
                            log_info(
                                logger,
                                "LIQ_WS_EVENT",
                                symbol=str(symbol),
                                step="LIQ_WS",
                                extra={
                                    "count": 1,
                                },
                            )
                        if _LIQ_WS_DEBUG:
                            now = time.time()
                            if now - debug_event_window_start >= 60:
                                debug_event_window_start = now
                                debug_event_count = 0
                            if debug_event_count < 5:
                                log_info(
                                    logger,
                                    "LIQ_WS_EVENT",
                                    step="LIQ_WS",
                                    extra={
                                        "symbol": str(symbol),
                                        "ts_ms": ts_ms,
                                        "side": side_raw,
                                        "qty": qty_f,
                                        "price": price_f,
                                    },
                                )
                                debug_event_count += 1

            except (WebSocketTimeoutException, TimeoutError):
                now = time.time()
                _maybe_log_health(now, url)
                continue
            except WebSocketConnectionClosedException as e:
                _reconnects_total += 1
                close_code = getattr(ws, "close_status", None)
                close_reason = getattr(ws, "close_reason", None)
                _last_disconnect_reason = "closed"
                log_info(
                    logger,
                    "LIQ_WS_RECONNECT_CONTEXT",
                    step="LIQ_WS",
                    extra={"conn_id": conn_id, "last_ws_topic": _last_ws_topic, "last_ws_op": _last_ws_op},
                )
                log_exception(
                    logger,
                    "WS disconnected; reconnecting",
                    step="LIQ_WS",
                    extra={
                        "exc_type": type(e).__name__,
                        "exc_repr": repr(e),
                        "close_code": close_code,
                        "close_reason": close_reason,
                        "conn_id": conn_id,
                    },
                )
                time.sleep(backoff)
                backoff = min(backoff * 2.0, 30.0)
                continue
            except Exception as e:
                _reconnects_total += 1
                close_code = None
                close_reason = None
                try:
                    close_code = getattr(ws, "close_status", None)
                    close_reason = getattr(ws, "close_reason", None)
                except Exception:
                    pass
                _last_disconnect_reason = "exception"
                log_info(
                    logger,
                    "LIQ_WS_RECONNECT_CONTEXT",
                    step="LIQ_WS",
                    extra={"conn_id": conn_id, "last_ws_topic": _last_ws_topic, "last_ws_op": _last_ws_op},
                )
                log_exception(
                    logger,
                    "Liquidation listener error; reconnecting",
                    step="LIQ_WS",
                    extra={
                        "exc_type": type(e).__name__,
                        "exc_repr": repr(e),
                        "close_code": close_code,
                        "close_reason": close_reason,
                        "conn_id": conn_id,
                    },
                )
                time.sleep(backoff)
                backoff = min(backoff * 2.0, 30.0)
            else:
                _last_disconnect_reason = "loop_exit"

    t = threading.Thread(target=_run, name="liq_listener", daemon=True)
    t.start()
