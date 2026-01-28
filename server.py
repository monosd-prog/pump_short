import os
import asyncio
import time
import pandas as pd
from typing import Dict, Any, Optional
from dataclasses import dataclass
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from watcher import run_watch_for_symbol, send_telegram

MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", "3"))
COOLDOWN_MINUTES = int(os.getenv("COOLDOWN_MINUTES", "45"))
TELEGRAM_ENABLED = os.getenv("TELEGRAM_ENABLED", "1") == "1"

app = FastAPI(title="Pump->Short Watcher")

sem = asyncio.Semaphore(MAX_CONCURRENT)

@dataclass
class Job:
    symbol: str
    run_id: str
    started_at_utc: str
    meta: Dict[str, Any]

active: Dict[str, Job] = {}
last_started: Dict[str, float] = {}
done: Dict[str, Dict[str, Any]] = {}


class PumpEvent(BaseModel):
    symbol: str
    exchange: Optional[str] = "bybit"
    pump_pct: Optional[float] = None
    pump_ts: Optional[str] = None
    extra: Optional[Dict[str, Any]] = None


def _cooldown_ok(symbol: str) -> bool:
    t = last_started.get(symbol)
    if t is None:
        return True
    return (time.time() - t) >= COOLDOWN_MINUTES * 60


@app.get("/status")
async def status():
    return {
        "max_concurrent": MAX_CONCURRENT,
        "cooldown_minutes": COOLDOWN_MINUTES,
        "active": {s: {"run_id": j.run_id, "started_at_utc": j.started_at_utc, "meta": j.meta} for s, j in active.items()},
        "done_recent": list(done.values())[-30:],
    }


@app.post("/pump")
async def pump(evt: PumpEvent):
    symbol = evt.symbol.strip().upper()
    if not symbol.endswith("USDT"):
        raise HTTPException(status_code=400, detail="symbol must be like XXXUSDT")

    if symbol in active:
        return {"status": "ignored", "reason": "already_running", "symbol": symbol, "run_id": active[symbol].run_id}

    if not _cooldown_ok(symbol):
        return {"status": "ignored", "reason": "cooldown", "symbol": symbol}

    meta = {
        "exchange": evt.exchange,
        "pump_pct": evt.pump_pct,
        "pump_ts": evt.pump_ts,
        "extra": evt.extra or {},
        "source": "pump_webhook",
    }

    asyncio.create_task(_run_job(symbol, meta))
    return {"status": "accepted", "symbol": symbol}


async def _run_job(symbol: str, meta: Dict[str, Any]):
    async with sem:
        run_id = pd.Timestamp.now(tz="UTC").strftime("%Y%m%d_%H%M%S")
        last_started[symbol] = time.time()
        active[symbol] = Job(symbol=symbol, run_id=run_id, started_at_utc=str(pd.Timestamp.now(tz="UTC")), meta=meta)

        try:
            # to_thread чтобы не блокировать event-loop
            result = await asyncio.to_thread(run_watch_for_symbol, symbol, run_id, meta)
            done[run_id] = {"run_id": run_id, "symbol": symbol, "status": "done", "result": result}

        except Exception as e:
            done[run_id] = {"run_id": run_id, "symbol": symbol, "status": "error", "error": repr(e)}
            if TELEGRAM_ENABLED:
                await asyncio.to_thread(send_telegram, f"❌ WATCH ERROR {symbol} | {repr(e)}")

        finally:
            active.pop(symbol, None)