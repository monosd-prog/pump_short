# short_pump/server.py
from __future__ import annotations

import os
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import FastAPI
from pydantic import BaseModel

from common.runtime import code_version, wall_time_utc
from common.io_dataset import ensure_dataset_files
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
_SHORT_TTL_SEC = 30 * 60
_active_short_symbols: Dict[str, Dict[str, Any]] = {}


def _can_start_long(symbol: str) -> bool:
    now = time.time()
    # cleanup
    expired = [s for s, ts in _active_long_symbols.items() if now - ts > _LONG_TTL_SEC]
    for s in expired:
        _active_long_symbols.pop(s, None)
    ts = _active_long_symbols.get(symbol)
    return ts is None or (now - ts) > _LONG_TTL_SEC


def _cleanup_short(now: float) -> None:
    expired = [s for s, v in _active_short_symbols.items() if now - v.get("started_ts", 0) > _SHORT_TTL_SEC]
    for s in expired:
        _active_short_symbols.pop(s, None)


def _to_utc_iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class PumpEvent(BaseModel):
    symbol: str
    exchange: Optional[str] = "bybit"
    pump_pct: Optional[float] = None
    pump_ts: Optional[str] = None
    extra: Optional[Dict[str, Any]] = None


@app.get("/status")
async def status():
    now = time.time()
    _cleanup_short(now)
    # cleanup long too
    expired = [s for s, ts in _active_long_symbols.items() if now - ts > _LONG_TTL_SEC]
    for s in expired:
        _active_long_symbols.pop(s, None)

    short_active = [
        {
            "symbol": s,
            "run_id": v.get("run_id", ""),
            "since_utc": _to_utc_iso(v.get("started_ts", now)),
        }
        for s, v in _active_short_symbols.items()
    ]
    long_active = [
        {
            "symbol": s,
            "since_utc": _to_utc_iso(ts),
        }
        for s, ts in _active_long_symbols.items()
    ]
    return {
        "ts_utc": _to_utc_iso(now),
        "code_version": code_version(),
        "start_long_on_pump": os.getenv("START_LONG_ON_PUMP", "0") == "1",
        "short_active": short_active,
        "long_active": long_active,
    }


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
        now = time.time()
        _cleanup_short(now)
        _active_short_symbols[symbol] = {"run_id": run_id or "", "started_ts": now}
        now_utc = wall_time_utc()
        ensure_dataset_files("short_pump", "live", now_utc, schema_version=2)
        if start_long:
            ensure_dataset_files("long_pullback", "live", now_utc, schema_version=2)

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