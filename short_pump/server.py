# short_pump/server.py
from __future__ import annotations

import os
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from analytics.dashboard import build_dashboard_data
from trading.live_config import load_live_config, set_live_profiles

from short_pump import factor_cache
from common.io_dataset import ensure_dataset_files, get_dataset_dir
from common.runtime import code_version, wall_time_utc
from short_pump.rollout import (
    SHORT_PUMP_FILTERED_ENABLE,
    SHORT_PUMP_PREMIUM_ENABLE,
    SHORT_PUMP_WICK_ENABLE,
)
from short_pump.config import Config
from short_pump.logging_utils import get_logger
from short_pump.runtime import Runtime

# Long pullback is disabled by default; enable with ENABLE_LONG_PULLBACK=1
ENABLE_LONG_PULLBACK = os.getenv("ENABLE_LONG_PULLBACK", "0").strip().lower() in ("1", "true", "yes", "y", "on")
# FAST_FROM_PUMP: stage0 fast sampling right after pump for research/dataset only (no auto-trade)
ENABLE_FAST_FROM_PUMP = os.getenv("ENABLE_FAST_FROM_PUMP", "0").strip().lower() in ("1", "true", "yes", "y", "on")
# Datasets root for fast0 (redirects fast0 writes; short_pump unchanged).
# Env: DATASETS_ROOT (default: /root/pump_short/datasets).
DATASETS_ROOT = os.getenv("DATASETS_ROOT", "/root/pump_short/datasets").rstrip("/")

# Initialize logging early
logger = get_logger(__name__)

app = FastAPI(title="Pump → Short Watcher")

_TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"
_dashboard_templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))

cfg = Config.from_env()
rt = Runtime(cfg)


def _fast0_filtered_enabled() -> bool:
    return (os.getenv("SHORT_PUMP_FAST0_FILTERED_ENABLE", "0") or "").strip().lower() in ("1", "true", "yes", "y", "on")


def _current_exec_mode() -> str:
    mode = (os.getenv("EXECUTION_MODE") or os.getenv("AUTO_TRADING_MODE") or "paper").strip().lower()
    return mode if mode in ("paper", "live") else "paper"


def _enabled_strategies() -> list[str]:
    strategies = ["short_pump"]
    if SHORT_PUMP_PREMIUM_ENABLE:
        strategies.append("short_pump_premium")
    if SHORT_PUMP_WICK_ENABLE:
        strategies.append("short_pump_wick")
    if SHORT_PUMP_FILTERED_ENABLE:
        strategies.append("short_pump_filtered")
    if ENABLE_LONG_PULLBACK:
        strategies.append("long_pullback")
    if ENABLE_FAST_FROM_PUMP:
        strategies.append("short_pump_fast0")
        if _fast0_filtered_enabled():
            strategies.append("short_pump_fast0_filtered")
    return strategies


@app.on_event("startup")
async def _log_enabled_strategies() -> None:
    logger.info("Enabled strategies: %s", ", ".join(_enabled_strategies()))
    exec_mode = _current_exec_mode()
    base_abs = os.path.abspath(DATASETS_ROOT)
    example_dir = get_dataset_dir("short_pump_fast0", wall_time_utc(), base_dir=DATASETS_ROOT)
    logger.info(
        "DATASET_DIR | exec_mode=%s base_dir=%s example=%s",
        exec_mode, base_abs, example_dir,
    )
    if SHORT_PUMP_PREMIUM_ENABLE:
        ensure_dataset_files("short_pump_premium", "live", wall_time_utc(), schema_version=3)
    if SHORT_PUMP_WICK_ENABLE:
        ensure_dataset_files("short_pump_wick", "live", wall_time_utc(), schema_version=3)
    if SHORT_PUMP_FILTERED_ENABLE:
        ensure_dataset_files("short_pump_filtered", "live", wall_time_utc(), schema_version=3)


def _factor_default_key() -> factor_cache.CacheKey:
    days = int(os.getenv("FACTOR_DEFAULT_DAYS", "7"))
    return factor_cache.make_key(days, _enabled_strategies(), "live")


@app.on_event("startup")
async def _start_factor_refresh() -> None:
    interval = int(os.getenv("FACTOR_REFRESH_MIN", "60"))
    factor_cache.start_background(
        interval_min=interval,
        default_key_fn=_factor_default_key,
    )
    logger.info(
        "FACTOR_BG_STARTED | interval_min=%d default_key=%s",
        interval, _factor_default_key(),
    )


@app.on_event("shutdown")
async def _stop_factor_refresh() -> None:
    await factor_cache.stop_background()


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


class LiveProfilesUpdate(BaseModel):
    profiles: list[str]


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
            fast0_mode = _current_exec_mode()
            ensure_dataset_files(
                "short_pump_fast0", fast0_mode, now_utc, schema_version=3, base_dir=DATASETS_ROOT
            )

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
                    fast0_mode = _current_exec_mode()
                    run_fast0_for_symbol(
                        symbol=symbol,
                        run_id=run_id or "",
                        pump_ts=pump_ts,
                        mode=fast0_mode,
                        base_dir=DATASETS_ROOT,
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


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard_page(request: Request):
    return _dashboard_templates.TemplateResponse(
        "dashboard.html", {"request": request}
    )


@app.get("/api/live-config")
async def api_live_config():
    return JSONResponse(load_live_config())


@app.post("/api/live-config/profiles")
async def api_live_config_profiles(payload: LiveProfilesUpdate):
    profiles = list(payload.profiles or [])
    set_live_profiles(profiles)
    return JSONResponse({"ok": True, "profiles": profiles})


@app.get("/api/dashboard/data")
async def dashboard_data(days: int = 7):
    try:
        days_clamped = max(1, min(int(days), 90))
    except Exception:
        days_clamped = 7
    try:
        payload = build_dashboard_data(
            base_dir="/root/pump_short",
            days=days_clamped,
            active_short=dict(_active_short_symbols),
            active_fast0=dict(_active_fast0_symbols),
        )
    except Exception as exc:
        logger.exception("DASHBOARD_BUILD_ERROR | %s", exc)
        payload = {
            "meta": {"error": str(exc), "days": days_clamped},
            "summary": {
                "total_signals": 0,
                "total_entries": 0,
                "total_outcomes": 0,
                "win_rate_pct": 0.0,
            },
            "funnel": [],
            "outcomes_breakdown": [],
            "latency": [],
            "timeline": [],
            "skip_reasons": [],
            "active_positions": {"short": [], "fast0": []},
        }
    return JSONResponse(payload)


@app.get("/api/dashboard/factors")
async def dashboard_factors(days: int = 7, strategies: str = ""):
    try:
        days_clamped = max(1, min(int(days), 90))
    except Exception:
        days_clamped = 7
    strat_list = [s.strip() for s in (strategies or "").split(",") if s.strip()]
    if not strat_list:
        strat_list = _enabled_strategies()
    key = factor_cache.make_key(days_clamped, strat_list, "live")
    entry = factor_cache.get_entry(key)
    if entry and entry.get("status") == "ready":
        factor_cache.touch_access(key)
        return JSONResponse({
            "status": "ready",
            "computed_at": entry.get("computed_at"),
            "duration_sec": entry.get("duration_sec"),
            "days": days_clamped,
            "strategies": list(key[1]),
            "data": entry.get("data"),
        })
    if entry and entry.get("status") == "error":
        return JSONResponse({
            "status": "error",
            "error": entry.get("error"),
            "computed_at": entry.get("computed_at"),
            "days": days_clamped,
            "strategies": list(key[1]),
        })
    # Trigger on-demand compute if not already running.
    factor_cache.kick_off(key)
    return JSONResponse({
        "status": "computing",
        "eta_sec": int(factor_cache.last_duration_sec()),
        "days": days_clamped,
        "strategies": list(key[1]),
    })