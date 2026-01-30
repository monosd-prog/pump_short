# short_pump/server.py
from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import FastAPI
from pydantic import BaseModel

from short_pump.config import Config
from short_pump.logging_utils import get_logger
from short_pump.runtime import Runtime

# Initialize logging early
logger = get_logger(__name__)

app = FastAPI(title="Pump → Short Watcher")

cfg = Config.from_env()
rt = Runtime(cfg)


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

        return await rt.start_watch(
            symbol=evt.symbol,
            exchange=evt.exchange,
            pump_pct=evt.pump_pct,
            pump_ts=evt.pump_ts,
            extra=evt.extra,
        )
    except Exception as e:
        logger.exception(f"Error in /pump endpoint | symbol={evt.symbol}", exc_info=True)
        raise