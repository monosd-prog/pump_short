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
_pending_symbols: Set[str] = set()
_last_heartbeat = 0.0
_rx_total = 0
_rx_json_ok = 0
_rx_topic_liq = 0
_rx_events_total = 0
_last_raw_ts: Optional[float] = None
_last_rx_wall = 0.0
_last_event_ts_ms: Optional[int] = None
_last_symbol: Optional[str] = None
_reconnects_total = 0
_debug_msg_total = 0
_ping_sent_count = 0
_last_disconnect_reason: Optional[str] = None
_last_no_data_log_ts = 0.0

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
        return count, float(qty_sum)
def get_liq_health() -> Dict[str, Optional[int]]:
    with _lock:
        return {
            "rx_total": _rx_total,
            "rx_json_ok": _rx_json_ok,
            "rx_topic_liq": _rx_topic_liq,
            "rx_events_total": _rx_events_total,
            "last_raw_ts": _last_raw_ts,
            "last_event_ts_ms": _last_event_ts_ms,
            "last_symbol": _last_symbol,
            "subscribed_symbols_count": len(_subscribed_symbols),
            "reconnects_total": _reconnects_total,
            "ping_sent_count": _ping_sent_count,
        }


def register_symbol(symbol: str) -> None:
    sym = _normalize_symbol(symbol)
    if not sym:
        return
    with _lock:
        if sym in _subscribed_symbols:
            return
        _pending_symbols.add(sym)


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

    def _run() -> None:
        global _last_heartbeat
        global _rx_total, _rx_json_ok, _rx_topic_liq, _rx_events_total, _last_raw_ts, _last_rx_wall, _last_event_ts_ms, _last_symbol, _reconnects_total, _debug_msg_total
        global _ping_sent_count, _last_disconnect_reason, _last_no_data_log_ts

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
                while True:
                    # subscribe to new symbols
                    args = []
                    with _lock:
                        if _pending_symbols:
                            for sym in list(_pending_symbols):
                                topic = f"allLiquidation.{sym}"
                                args.append(topic)
                                if _LIQ_WS_DEBUG:
                                    args.append(f"tickers.{sym}")
                                _subscribed_symbols.add(sym)
                                _pending_symbols.remove(sym)
                    if args:
                        sub_msg = {"op": "subscribe", "args": args}
                        ws.send(json.dumps(sub_msg))
                        log_info(logger, "WS subscribed", step="LIQ_WS", extra={"url": url, "args": args, "conn_id": conn_id})

                    now_wall = time.time()
                    if now_wall - last_ping_wall >= 25:
                        try:
                            if hasattr(ws, "ping"):
                                ws.ping()
                            else:
                                ws.send('{"op":"ping"}')
                            log_info(logger, "LIQ_WS_PING", step="LIQ_WS", extra={"conn_id": conn_id})
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
                    topic = msg.get("topic") or msg.get("op") or ""
                    topic = topic if isinstance(topic, str) else ""
                    if topic.startswith("allLiquidation."):
                        _rx_topic_liq += 1

                    data = msg.get("data")
                    if not data or not isinstance(data, list):
                        continue
                    for item in data:
                        if not isinstance(item, dict):
                            continue
                        symbol = item.get("s") or (topic.split(".", 1)[1] if "." in topic else "")
                        side_raw = item.get("S") or ""
                        side = side_raw.strip().lower()
                        # For Bybit: Buy = liquidation of LONG, Sell = liquidation of SHORT
                        if side not in ("buy", "sell"):
                            continue
                        price = item.get("p") or item.get("price") or 0
                        qty = item.get("v") or item.get("size") or item.get("qty") or item.get("q") or 0
                        ts = item.get("T") or msg.get("ts") or 0
                        try:
                            price_f = float(price)
                            qty_f = float(qty)
                        except Exception:
                            continue
                        try:
                            ts_ms = int(ts)
                        except Exception:
                            ts_ms = int(time.time() * 1000)
                        if ts_ms < 10**11:
                            ts_ms = int(ts_ms * 1000)

                        _rx_events_total += 1
                        _last_event_ts_ms = ts_ms
                        _last_symbol = str(symbol) if symbol else None
                        liq_side = "long" if side == "buy" else "short"
                        _add_event(str(symbol), ts_ms, qty_f, price_f, liq_side)
                        if _LIQ_WS_DEBUG:
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

            except (WebSocketTimeoutException, TimeoutError):
                now = time.time()
                _maybe_log_health(now, url)
                continue
            except WebSocketConnectionClosedException as e:
                _reconnects_total += 1
                close_code = getattr(ws, "close_status", None)
                close_reason = getattr(ws, "close_reason", None)
                _last_disconnect_reason = "closed"
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
