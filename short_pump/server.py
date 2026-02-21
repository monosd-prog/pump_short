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

# Long pullback is disabled by default; enable with ENABLE_LONG_PULLBACK=1
ENABLE_LONG_PULLBACK = os.getenv("ENABLE_LONG_PULLBACK", "0").strip().lower() in ("1", "true", "yes", "y", "on")
# FAST_FROM_PUMP: stage0 fast sampling right after pump for research/dataset only (no auto-trade)
ENABLE_FAST_FROM_PUMP = os.getenv("ENABLE_FAST_FROM_PUMP", "0").strip().lower() in ("1", "true", "yes", "y", "on")

# Initialize logging early
logger = get_logger(__name__)

app = FastAPI(title="Pump → Short Watcher")

cfg = Config.from_env()
rt = Runtime(cfg)


def _enabled_strategies() -> list[str]:
    strategies = ["short_pump"]
    if ENABLE_LONG_PULLBACK:
        strategies.append("long_pullback")
    if ENABLE_FAST_FROM_PUMP:
        strategies.append("short_pump_fast0")
    return strategies


@app.on_event("startup")
async def _log_enabled_strategies() -> None:
    logger.info("Enabled strategies: %s", ", ".join(_enabled_strategies()))

_LONG_TTL_SEC = 30 * 60
_active_long_symbols: Dict[str, float] = {}
_FAST0_TTL_SEC = int(os.getenv("FAST0_TTL_SEC", str(30 * 60)))
_active_fast0_symbols: Dict[str, float] = {}
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


def _can_start_fast0(symbol: str) -> bool:
    now = time.time()
    expired = [s for s, ts in _active_fast0_symbols.items() if now - ts > _FAST0_TTL_SEC]
    for s in expired:
        _active_fast0_symbols.pop(s, None)
    ts = _active_fast0_symbols.get(symbol)
    return ts is None or (now - ts) > _FAST0_TTL_SEC


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


# REMOVED: bootstrap_force_symbols() - auto-tracking on startup disabled.
# Tracking now starts ONLY via explicit triggers (/pump endpoint or manual scripts).
# Previously used env vars: FORCE_SYMBOLS_BOOTSTRAP_ENABLED, FORCE_SYMBOLS_BOOTSTRAP, FORCE_SYMBOLS


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
    fast0_active = [
        {"symbol": s, "since_utc": _to_utc_iso(ts)}
        for s, ts in _active_fast0_symbols.items()
    ]
    return {
        "ts_utc": _to_utc_iso(now),
        "code_version": code_version(),
        "enabled_strategies": _enabled_strategies(),
        "start_long_on_pump": ENABLE_LONG_PULLBACK and os.getenv("START_LONG_ON_PUMP", "0") == "1",
        "short_active": short_active,
        "long_active": long_active,
        "fast0_active": fast0_active,
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
        run_id = result.get("run_id") if isinstance(result, dict) else None
        symbol = evt.symbol.strip().upper()
        now = time.time()
        _cleanup_short(now)
        _active_short_symbols[symbol] = {"run_id": run_id or "", "started_ts": now}
        now_utc = wall_time_utc()
        ensure_dataset_files("short_pump", "live", now_utc, schema_version=3)
        start_long = ENABLE_LONG_PULLBACK and (os.getenv("START_LONG_ON_PUMP", "0") == "1")
        if start_long:
            ensure_dataset_files("long_pullback", "live", now_utc, schema_version=2)
        if ENABLE_FAST_FROM_PUMP:
            ensure_dataset_files("short_pump_fast0", "live", now_utc, schema_version=3)

        logger.info(
            "PUMP_ACCEPTED | symbol=%s | run_id=%s | start_long=%s | fast0=%s",
            symbol, run_id, start_long, ENABLE_FAST_FROM_PUMP,
        )

        if ENABLE_FAST_FROM_PUMP and _can_start_fast0(symbol):
            _active_fast0_symbols[symbol] = time.time()
            from short_pump.fast0_sampler import run_fast0_for_symbol

            pump_ts = (evt.pump_ts or now_utc) if evt.pump_ts else now_utc

            def _fast0_runner():
                try:
                    run_fast0_for_symbol(
                        symbol=symbol,
                        run_id=run_id or "",
                        pump_ts=pump_ts,
                        mode="live",
                    )
                except Exception:
                    logger.exception("FAST0_SAMPLER_ERROR | symbol=%s", symbol)

            t = threading.Thread(target=_fast0_runner, name=f"fast0_{symbol}", daemon=True)
            t.start()

        if start_long and _can_start_long(symbol):
            _active_long_symbols[symbol] = time.time()
            from long_pullback.watcher import run_watch_for_symbol as run_long_watch

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