# short_pump/liquidations.py
from __future__ import annotations

import json
import threading
import time
from collections import defaultdict, deque
from typing import Deque, Dict, Tuple

from short_pump.logging_utils import get_logger, log_exception, log_info, log_warning

logger = get_logger(__name__)

try:
    import websocket  # type: ignore
except Exception:  # pragma: no cover - runtime optional
    websocket = None


_lock = threading.Lock()
_events: Dict[str, Deque[Tuple[float, float]]] = defaultdict(deque)
_started = False
_max_age_sec = 600.0  # keep up to 10 minutes


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


def _add_event(symbol: str, ts: float, usd: float) -> None:
    if not symbol or usd <= 0:
        return
    sym = _normalize_symbol(symbol)
    with _lock:
        _events[sym].append((ts, usd))
        _purge_locked(sym, now=ts)


def _purge_locked(symbol: str, now: float) -> None:
    q = _events.get(symbol)
    if not q:
        return
    cutoff = now - _max_age_sec
    while q and q[0][0] < cutoff:
        q.popleft()


def get_liq_stats(symbol: str, window_seconds: int) -> Tuple[int, float]:
    """Return (count, usd_sum) for short liquidations over window_seconds."""
    sym = _normalize_symbol(symbol)
    now = time.time()
    with _lock:
        _purge_locked(sym, now=now)
        q = _events.get(sym)
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
        backoff = 1.0
        while True:
            try:
                ws = websocket.WebSocket()
                ws.connect(url, timeout=10)
                sub_msg = {"op": "subscribe", "args": ["all-liquidation"]}
                ws.send(json.dumps(sub_msg))

                backoff = 1.0
                while True:
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
                        # For Bybit, liquidation side "Buy" indicates short liquidation.
                        if side != "buy":
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

                        _add_event(str(symbol), ts_f, usd)

            except Exception:
                log_exception(logger, "Liquidation listener error; reconnecting", step="LIQ_WS")
                time.sleep(backoff)
                backoff = min(backoff * 2.0, 30.0)

    t = threading.Thread(target=_run, name="liq_listener", daemon=True)
    t.start()
