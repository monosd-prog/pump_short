# short_pump/server.py
from __future__ import annotations

import os
import threading
import time
from typing import Any, Dict, Optional

from fastapi import FastAPI
from pydantic import BaseModel

from short_pump.config import Config
from short_pump.logging_utils import get_logger
from short_pump.runtime import Runtime
from long_pullback.watcher import run_watch_for_symbol as run_long_watch

# Initialize logging early
logger = get_logger(__name__)

app = FastAPI(title="Pump → Short Watcher")

cfg = Config.from_env()
rt = Runtime(cfg)

_LONG_TTL_SEC = 30 * 60
_active_long_symbols: Dict[str, float] = {}


def _can_start_long(symbol: str) -> bool:
    now = time.time()
    # cleanup
    expired = [s for s, ts in _active_long_symbols.items() if now - ts > _LONG_TTL_SEC]
    for s in expired:
        _active_long_symbols.pop(s, None)
    ts = _active_long_symbols.get(symbol)
    return ts is None or (now - ts) > _LONG_TTL_SEC


class PumpEvent(BaseModel):
    symbol: str
    exchange: Optional[str] = "bybit"
    pump_pct: Optional[float] = None
    pump_ts: Optional[str] = None
    extra: Optional[Dict[str, Any]] = None


@app.get("/status")
async def status():
    return rt.status_payload()


@app.post("/pump")
async def pump(evt: PumpEvent):
    try:
        ignored = rt.apply_filters(
            symbol=evt.symbol,
            exchange=evt.exchange,
            pump_pct=evt.pump_pct,
            pump_ts=evt.pump_ts,
            extra=evt.extra,
        )
        if ignored is not None:
            return ignored

        result = await rt.start_watch(
            symbol=evt.symbol,
            exchange=evt.exchange,
            pump_pct=evt.pump_pct,
            pump_ts=evt.pump_ts,
            extra=evt.extra,
        )
        start_long = os.getenv("START_LONG_ON_PUMP", "0") == "1"
        run_id = result.get("run_id") if isinstance(result, dict) else None
        symbol = evt.symbol.strip().upper()

        logger.info("PUMP_ACCEPTED | symbol=%s | run_id=%s | start_long=%s", symbol, run_id, start_long)

        if start_long and _can_start_long(symbol):
            _active_long_symbols[symbol] = time.time()

            def _long_runner():
                try:
                    run_long_watch(symbol, mode="live")
                except Exception:
                    logger.exception("LONG_WATCH_ERROR | symbol=%s", symbol)

            t = threading.Thread(target=_long_runner, name=f"long_watch_{symbol}", daemon=True)
            t.start()

        return result
    except Exception as e:
        logger.exception(f"Error in /pump endpoint | symbol={evt.symbol}", exc_info=True)
        raise