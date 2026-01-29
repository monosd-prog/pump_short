import os
import asyncio
import time
from typing import Dict, Any, Optional
from dataclasses import dataclass

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from watcher import run_watch_for_symbol

# =====================
# ENV / CONFIG
# =====================

MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", "3"))
COOLDOWN_MINUTES = int(os.getenv("COOLDOWN_MINUTES", "45"))

MIN_PUMP_PCT = float(os.getenv("MIN_PUMP_PCT", "8.0"))
REQUIRE_10M_WINDOW = os.getenv("REQUIRE_10M_WINDOW", "0") == "1"

# =====================
# APP
# =====================

app = FastAPI(title="Pump → Short Watcher")

sem = asyncio.Semaphore(MAX_CONCURRENT)

# =====================
# STATE
# =====================

@dataclass
class Job:
    symbol: str
    run_id: str
    started_at_utc: str
    meta: Dict[str, Any]


active: Dict[str, Job] = {}
last_started: Dict[str, float] = {}
done_recent: Dict[str, Dict[str, Any]] = {}

# =====================
# MODELS
# =====================

class PumpEvent(BaseModel):
    symbol: str
    exchange: Optional[str] = "bybit"
    pump_pct: Optional[float] = None
    pump_ts: Optional[str] = None
    extra: Optional[Dict[str, Any]] = None

# =====================
# HELPERS
# =====================

def cooldown_ok(symbol: str) -> bool:
    ts = last_started.get(symbol)
    if ts is None:
        return True
    return (time.time() - ts) >= COOLDOWN_MINUTES * 60

# =====================
# ROUTES
# =====================

@app.get("/status")
async def status():
    return {
        "max_concurrent": MAX_CONCURRENT,
        "cooldown_minutes": COOLDOWN_MINUTES,
        "active": {
            s: {
                "run_id": j.run_id,
                "started_at_utc": j.started_at_utc,
                "meta": j.meta,
            }
            for s, j in active.items()
        },
        "done_recent": list(done_recent.values())[-20:],
    }


@app.post("/pump")
async def pump(evt: PumpEvent):
    symbol = evt.symbol.strip().upper()
    if not symbol.endswith("USDT"):
        raise HTTPException(status_code=400, detail="symbol must be XXXUSDT")

    # ---------------------
    # FILTERS
    # ---------------------

    exchange = (evt.exchange or "bybit").lower()
    if exchange != "bybit":
        return {
            "status": "ignored",
            "reason": "exchange_not_bybit",
            "symbol": symbol,
        }

    if evt.pump_pct is not None:
        try:
            pump_pct = float(evt.pump_pct)
        except Exception:
            pump_pct = None

        if pump_pct is not None and pump_pct < MIN_PUMP_PCT:
            return {
                "status": "ignored",
                "reason": "pump_pct_too_small",
                "symbol": symbol,
                "pump_pct": pump_pct,
            }

    if REQUIRE_10M_WINDOW:
        extra = evt.extra or {}
        tf = str(extra.get("tf", "")).lower()
        win = extra.get("window_minutes")

        ok_10m = False
        if tf in ("10m", "10min", "10"):
            ok_10m = True
        if isinstance(win, (int, float)) and int(win) == 10:
            ok_10m = True

        if not ok_10m:
            return {
                "status": "ignored",
                "reason": "not_10m_window",
                "symbol": symbol,
            }

    # ---------------------
    # STATE CHECKS
    # ---------------------

    if symbol in active:
        return {
            "status": "ignored",
            "reason": "already_running",
            "symbol": symbol,
            "run_id": active[symbol].run_id,
        }

    if not cooldown_ok(symbol):
        return {
            "status": "ignored",
            "reason": "cooldown",
            "symbol": symbol,
        }

    # ---------------------
    # START WATCHER
    # ---------------------

    async with sem:
        run_id = time.strftime("%Y%m%d_%H%M%S")
        started_at = time.strftime("%Y-%m-%d %H:%M:%S UTC")

        job = Job(
            symbol=symbol,
            run_id=run_id,
            started_at_utc=started_at,
            meta={
                "exchange": exchange,
                "pump_pct": evt.pump_pct,
                "pump_ts": evt.pump_ts,
                "extra": evt.extra or {},
                "source": "pump_webhook",
            },
        )

        active[symbol] = job
        last_started[symbol] = time.time()

        async def runner():
            try:
                result = await asyncio.to_thread(
                    run_watch_for_symbol,
                    symbol,
                    run_id,
                    job.meta,
                )
                done_recent[run_id] = {
                    "run_id": run_id,
                    "symbol": symbol,
                    "status": "done",
                    "result": result,
                }
            finally:
                active.pop(symbol, None)

        asyncio.create_task(runner())

        return {
            "status": "accepted",
            "symbol": symbol,
            "run_id": run_id,
        }