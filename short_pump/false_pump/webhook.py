from __future__ import annotations

import asyncio
import logging
from aiohttp import web

from short_pump.false_pump.config import FalsePumpConfig
from short_pump.false_pump.watcher import _active_symbols, run_watcher

_CTX = {"cfg": None, "queue": None}
logger = logging.getLogger("false_pump.webhook")


async def handle_oi_signal(request) -> web.Response:
    cfg: FalsePumpConfig = _CTX["cfg"]
    queue = _CTX["queue"]
    try:
        payload = await request.json()
    except Exception:
        return web.json_response({"status": "error", "error": "invalid_json"}, status=400)

    logger.info(f"[false_pump.webhook] received signal: {payload}")
    symbol = str(payload.get("symbol", "")).strip().upper()
    direction = str(payload.get("direction", "")).strip().upper()
    try:
        oi_change_pct = float(payload.get("oi_change_pct", 0.0))
        price_change_pct = float(payload.get("price_change_pct", 0.0))
    except (TypeError, ValueError):
        return web.json_response({"status": "error", "error": "invalid_fields"}, status=400)

    signal = {
        "symbol": symbol,
        "direction": direction,
        "oi_change_pct": oi_change_pct,
        "price_change_pct": price_change_pct,
    }
    if direction != "UP":
        return web.json_response({"status": "accepted", "action": "ignored"})
    if price_change_pct <= float(cfg.trigger_min_price_pct):
        return web.json_response({"status": "accepted", "action": "ignored"})
    if oi_change_pct < float(cfg.trigger_min_oi_pct):
        return web.json_response({"status": "accepted", "action": "ignored"})
    if symbol in _active_symbols:
        return web.json_response({"status": "accepted", "action": "cooldown"})

    asyncio.create_task(run_watcher(signal, cfg, queue))
    return web.json_response({"status": "accepted", "action": "monitoring_started"})


async def start_webhook_server(cfg: FalsePumpConfig, queue) -> None:
    _CTX["cfg"] = cfg
    _CTX["queue"] = queue

    app = web.Application()
    app.router.add_post("/api/oi_signal", handle_oi_signal)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host=cfg.webhook_host, port=int(cfg.webhook_port))
    await site.start()

    while True:
        await asyncio.sleep(3600)
