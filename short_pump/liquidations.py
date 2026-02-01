# short_pump/liquidations.py
from __future__ import annotations

import json
import threading
import time
from collections import defaultdict, deque
from typing import Deque, Dict, Set, Tuple

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
_events_short: Dict[str, Deque[Tuple[float, float]]] = defaultdict(deque)
_events_long: Dict[str, Deque[Tuple[float, float]]] = defaultdict(deque)
_started = False
_max_age_sec = 600.0  # keep up to 10 minutes
_subscribed_symbols: Set[str] = set()
_pending_symbols: Set[str] = set()
_last_heartbeat = 0.0


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


def _add_event(symbol: str, ts: float, usd: float, side: str) -> None:
    if not symbol or usd <= 0:
        return
    sym = _normalize_symbol(symbol)
    with _lock:
        if side == "short":
            _events_short[sym].append((ts, usd))
        elif side == "long":
            _events_long[sym].append((ts, usd))
        _purge_locked(sym, now=ts)


def _purge_locked(symbol: str, now: float) -> None:
    q_short = _events_short.get(symbol)
    q_long = _events_long.get(symbol)
    cutoff = now - _max_age_sec
    while q_short and q_short[0][0] < cutoff:
        q_short.popleft()
    while q_long and q_long[0][0] < cutoff:
        q_long.popleft()


def get_liq_stats(symbol: str, window_seconds: int, side: str = "short") -> Tuple[int, float]:
    """Return (count, usd_sum) for liquidations over window_seconds."""
    sym = _normalize_symbol(symbol)
    now = time.time()
    with _lock:
        _purge_locked(sym, now=now)
        q = _events_short.get(sym) if side == "short" else _events_long.get(sym)
        if not q:
            return 0, 0.0
        cutoff = now - float(window_seconds)
        count = 0
        usd_sum = 0.0
        for ts, usd in q:
            if ts >= cutoff:
                count += 1
                usd_sum += usd
        return count, float(usd_sum)


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
        backoff = 1.0
        while True:
            try:
                ws = websocket.WebSocket()
                ws.connect(url, timeout=10)
                ws.settimeout(10)
                log_info(logger, "WS connected", step="LIQ_WS", extra={"url": url})

                backoff = 1.0
                while True:
                    # subscribe to new symbols
                    args = []
                    with _lock:
                        if _pending_symbols:
                            for sym in list(_pending_symbols):
                                topic = f"allLiquidation.{sym}"
                                args.append(topic)
                                _subscribed_symbols.add(sym)
                                _pending_symbols.remove(sym)
                    if args:
                        sub_msg = {"op": "subscribe", "args": args}
                        ws.send(json.dumps(sub_msg))
                        log_info(logger, "WS subscribed", step="LIQ_WS", extra={"url": url, "args": args})

                    raw = ws.recv()
                    if not raw:
                        continue
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue

                    data = msg.get("data")
                    if not data:
                        continue
                    if isinstance(data, dict):
                        items = [data]
                    else:
                        items = data

                    for item in items:
                        if not isinstance(item, dict):
                            continue
                        symbol = item.get("symbol") or item.get("s")
                        side = (item.get("side") or item.get("S") or "").lower()
                        # For Bybit: Buy = liquidation of LONG, Sell = liquidation of SHORT
                        if side not in ("buy", "sell"):
                            continue

                        price = item.get("price") or item.get("p")
                        qty = item.get("size") or item.get("qty") or item.get("q")
                        ts = item.get("time") or item.get("ts") or item.get("timestamp") or item.get("T")

                        try:
                            price_f = float(price)
                            qty_f = float(qty)
                        except Exception:
                            continue

                        usd = price_f * qty_f
                        if isinstance(ts, (int, float)):
                            ts_f = float(ts)
                            if ts_f > 10_000_000_000:  # ms
                                ts_f = ts_f / 1000.0
                        else:
                            ts_f = time.time()

                        liq_side = "long" if side == "buy" else "short"
                        _add_event(str(symbol), ts_f, usd, liq_side)

            except (WebSocketTimeoutException, TimeoutError):
                now = time.time()
                if now - _last_heartbeat >= 60:
                    log_info(logger, "WS heartbeat (timeout)", step="LIQ_WS", extra={"url": url})
                    _last_heartbeat = now
                continue
            except WebSocketConnectionClosedException:
                log_exception(logger, "WS disconnected; reconnecting", step="LIQ_WS")
                time.sleep(backoff)
                backoff = min(backoff * 2.0, 30.0)
                continue
            except Exception:
                log_exception(logger, "Liquidation listener error; reconnecting", step="LIQ_WS")
                time.sleep(backoff)
                backoff = min(backoff * 2.0, 30.0)

    t = threading.Thread(target=_run, name="liq_listener", daemon=True)
    t.start()
