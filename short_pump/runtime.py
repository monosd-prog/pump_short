# short_pump/runtime.py
from __future__ import annotations

import asyncio
import copy
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

from fastapi import HTTPException

from short_pump.config import Config
from short_pump.liquidations import start_liquidation_listener
from short_pump.logging_utils import get_logger, log_exception
from short_pump.watcher import run_watch_for_symbol

logger = get_logger(__name__)


@dataclass
class Job:
    symbol: str
    run_id: str
    started_at_utc: str
    meta: Dict[str, Any]


class Runtime:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.sem = asyncio.Semaphore(cfg.max_concurrent)

        # Start liquidation listener (non-blocking)
        try:
            start_liquidation_listener(cfg.category)
        except Exception as e:
            log_exception(logger, "Failed to start liquidation listener", step="LIQ_WS")

        self.active: Dict[str, Job] = {}
        self.last_started: Dict[str, float] = {}
        self.done_recent: Dict[str, Dict[str, Any]] = {}

    def cooldown_ok(self, symbol: str) -> bool:
        ts = self.last_started.get(symbol)
        if ts is None:
            return True
        return (time.time() - ts) >= self.cfg.cooldown_minutes * 60

    def status_payload(self) -> Dict[str, Any]:
        return {
            "max_concurrent": self.cfg.max_concurrent,
            "cooldown_minutes": self.cfg.cooldown_minutes,
            "active": {
                s: {
                    "run_id": j.run_id,
                    "started_at_utc": j.started_at_utc,
                    "meta": j.meta,
                }
                for s, j in self.active.items()
            },
            "done_recent": list(self.done_recent.values())[-20:],
        }

    def _validate_symbol(self, symbol: str) -> str:
        s = symbol.strip().upper()
        if not s.endswith("USDT"):
            raise HTTPException(status_code=400, detail="symbol must be XXXUSDT")
        return s

    def apply_filters(
        self,
        symbol: str,
        exchange: Optional[str],
        pump_pct: Optional[float],
        pump_ts: Optional[str],
        extra: Optional[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        """
        Returns:
          - dict => {"status":"ignored", ...} if should ignore
          - None => ok
        """
        symbol = self._validate_symbol(symbol)

        ex = (exchange or "bybit").lower()
        if ex != "bybit":
            return {"status": "ignored", "reason": "exchange_not_bybit", "symbol": symbol}

        # MIN_PUMP_PCT
        if pump_pct is not None:
            try:
                pp = float(pump_pct)
            except Exception as e:
                log_exception(logger, "Failed to parse pump_pct", symbol=symbol, step="FILTER", extra={"pump_pct": pump_pct})
                pp = None

            if pp is not None and pp < self.cfg.min_pump_pct:
                return {
                    "status": "ignored",
                    "reason": "pump_pct_too_small",
                    "symbol": symbol,
                    "pump_pct": pp,
                }

        # REQUIRE_10M_WINDOW
        if self.cfg.require_10m_window:
            e = extra or {}
            tf = str(e.get("tf", "")).lower()
            win = e.get("window_minutes")

            ok_10m = False
            if tf in ("10m", "10min", "10"):
                ok_10m = True
            if isinstance(win, (int, float)) and int(win) == 10:
                ok_10m = True

            if not ok_10m:
                return {"status": "ignored", "reason": "not_10m_window", "symbol": symbol}

        return None

    async def start_watch(
        self,
        symbol: str,
        exchange: Optional[str],
        pump_pct: Optional[float],
        pump_ts: Optional[str],
        extra: Optional[Dict[str, Any]],
        source: str = "pump_webhook",
    ) -> Dict[str, Any]:
        symbol = self._validate_symbol(symbol)
        ex = (exchange or "bybit").lower()

        # state checks
        if symbol in self.active:
            return {
                "status": "ignored",
                "reason": "already_running",
                "symbol": symbol,
                "run_id": self.active[symbol].run_id,
            }

        if not self.cooldown_ok(symbol):
            return {"status": "ignored", "reason": "cooldown", "symbol": symbol}

        # start
        async with self.sem:
            run_id = time.strftime("%Y%m%d_%H%M%S")
            started_at = time.strftime("%Y-%m-%d %H:%M:%S UTC")

            job = Job(
                symbol=symbol,
                run_id=run_id,
                started_at_utc=started_at,
                meta={
                    "exchange": ex,
                    "pump_pct": pump_pct,
                    "pump_ts": pump_ts,
                    "extra": extra or {},
                    "source": source,
                },
            )

            self.active[symbol] = job
            self.last_started[symbol] = time.time()

            async def runner():
                try:
                    cfg_copy = copy.deepcopy(self.cfg)
                    result = await asyncio.to_thread(
                        run_watch_for_symbol,
                        symbol,
                        run_id,
                        job.meta,
                        cfg_copy,
                    )
                    self.done_recent[run_id] = {
                        "run_id": run_id,
                        "symbol": symbol,
                        "status": "done",
                        "result": result,
                    }
                except Exception as e:
                    log_exception(logger, "Fatal error in watcher runner task", symbol=symbol, run_id=run_id, step="RUNNER_TASK")
                    self.done_recent[run_id] = {
                        "run_id": run_id,
                        "symbol": symbol,
                        "status": "error",
                        "error": str(e),
                    }
                    raise
                finally:
                    self.active.pop(symbol, None)

            def task_done_callback(task: asyncio.Task) -> None:
                if task.exception():
                    log_exception(logger, "Watcher task completed with exception", symbol=symbol, run_id=run_id, step="RUNNER_TASK")
            
            task = asyncio.create_task(runner())
            task.add_done_callback(task_done_callback)
            return {"status": "accepted", "symbol": symbol, "run_id": run_id}