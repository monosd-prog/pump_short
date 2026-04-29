from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from aiohttp import web

from short_pump.false_pump.config import FalsePumpConfig
from short_pump.false_pump.controls import get_controls
from short_pump.false_pump.watcher import _active_symbols, _cancel_flags, run_watcher

_CTX = {"cfg": None, "queue": None}
logger = logging.getLogger("false_pump.webhook")
_RECENT_OI_EVENTS = deque(maxlen=200)
_LAST_SIGNAL_TS: dict[str, float] = {}
_DUPLICATE_WINDOW_SEC = 15.0


def _push_recent_event(status: str, action: str, symbol: str, **extra) -> None:
    _RECENT_OI_EVENTS.append(
        {
            "ts": int(time.time()),
            "status": status,
            "action": action,
            "symbol": symbol,
            **extra,
        }
    )


def get_recent_oi_events(limit: int = 50) -> list[dict]:
    if limit < 1:
        limit = 1
    return list(_RECENT_OI_EVENTS)[-limit:]


def _is_duplicate_signal(
    *,
    symbol: str,
    direction: str,
    period_min: int,
    oi_change_pct: float,
    price_change_pct: float,
) -> bool:
    key = (
        symbol,
        direction,
        int(period_min),
        round(float(oi_change_pct), 3),
        round(float(price_change_pct), 3),
    )
    now = time.time()
    last_ts = _LAST_SIGNAL_TS.get(str(key))
    _LAST_SIGNAL_TS[str(key)] = now
    # Light cleanup to keep memory bounded.
    if len(_LAST_SIGNAL_TS) > 3000:
        cutoff = now - (_DUPLICATE_WINDOW_SEC * 4.0)
        for k, ts in list(_LAST_SIGNAL_TS.items()):
            if ts < cutoff:
                _LAST_SIGNAL_TS.pop(k, None)
    return bool(last_ts and (now - last_ts) <= _DUPLICATE_WINDOW_SEC)


async def handle_oi_signal(request) -> web.Response:
    cfg: FalsePumpConfig = _CTX["cfg"]
    queue = _CTX["queue"]
    try:
        payload = await request.json()
    except Exception:
        return web.json_response({"status": "error", "error": "invalid_json"}, status=400)

    logger.info(f"[false_pump.webhook] received signal: {payload}")
    exchange = str(payload.get("exchange", "")).strip().lower()
    symbol = str(payload.get("symbol", "")).strip().upper()
    if not symbol.endswith("USDT"):
        symbol = symbol + "USDT"
    direction_raw = str(payload.get("direction", "")).strip().upper()
    direction = "UP" if direction_raw in {"UP", "LONG"} else direction_raw
    try:
        period_min = int(payload.get("period_min") or 0)
    except (TypeError, ValueError):
        period_min = 0
    try:
        oi_change_pct = float(
            payload.get("oi_change_pct") or payload.get("oi_pct") or 0.0
        )
        price_change_pct = float(
            payload.get("price_change_pct") or payload.get("price_pct") or 0.0
        )
    except (TypeError, ValueError):
        return web.json_response({"status": "error", "error": "invalid_fields"}, status=400)

    signal = {
        "symbol": symbol,
        "direction": direction,
        "period_min": period_min,
        "oi_change_pct": oi_change_pct,
        "price_change_pct": price_change_pct,
    }
    controls = get_controls()
    min_oi_growth_pct = float(controls.get("min_oi_growth_pct", 2.0))
    max_growth_period_min = int(controls.get("max_growth_period_min", 90))
    if exchange != "bybit":
        _push_recent_event(
            "accepted",
            "ignored_exchange",
            symbol,
            exchange=exchange,
            direction=direction,
            period_min=period_min,
            oi_change_pct=oi_change_pct,
            price_change_pct=price_change_pct,
        )
        return web.json_response({"status": "accepted", "action": "ignored"})
    if _is_duplicate_signal(
        symbol=symbol,
        direction=direction,
        period_min=period_min,
        oi_change_pct=oi_change_pct,
        price_change_pct=price_change_pct,
    ):
        _push_recent_event(
            "accepted",
            "ignored_duplicate",
            symbol,
            direction=direction,
            period_min=period_min,
            oi_change_pct=oi_change_pct,
            price_change_pct=price_change_pct,
            dedup_window_sec=_DUPLICATE_WINDOW_SEC,
        )
        return web.json_response({"status": "accepted", "action": "ignored_duplicate"})

    if direction != "UP":
        _push_recent_event(
            "accepted",
            "ignored_direction",
            symbol,
            direction=direction_raw,
            period_min=period_min,
            oi_change_pct=oi_change_pct,
            price_change_pct=price_change_pct,
        )
        return web.json_response({"status": "accepted", "action": "ignored"})
    if period_min > 0 and period_min > max_growth_period_min:
        _push_recent_event(
            "accepted",
            "ignored_period",
            symbol,
            direction=direction,
            period_min=period_min,
            oi_change_pct=oi_change_pct,
            price_change_pct=price_change_pct,
            max_growth_period_min=max_growth_period_min,
        )
        return web.json_response({"status": "accepted", "action": "ignored"})
    if oi_change_pct < min_oi_growth_pct:
        _push_recent_event(
            "accepted",
            "ignored_oi_threshold",
            symbol,
            direction=direction,
            period_min=period_min,
            oi_change_pct=oi_change_pct,
            price_change_pct=price_change_pct,
            min_oi_growth_pct=min_oi_growth_pct,
        )
        return web.json_response({"status": "accepted", "action": "ignored"})
    if symbol in _active_symbols:
        _cancel_flags[symbol] = True
        logger.info(f"[false_pump.webhook] RESET monitoring {symbol} — новый сигнал")

    # Runtime controls influence detector behavior for the new watcher session.
    cfg.pump_price_pct = float(controls.get("pump_price_pct", cfg.pump_price_pct))
    cfg.oi_max_reaction_pct = float(
        controls.get("oi_max_reaction_pct", cfg.oi_max_reaction_pct)
    )
    cfg.near_top_pct = float(controls.get("near_top_pct", cfg.near_top_pct))
    cfg.min_flags_required = int(
        controls.get("min_flags_required", cfg.min_flags_required)
    )
    cfg.mandatory_min_hits = int(
        controls.get("mandatory_min_hits", getattr(cfg, "mandatory_min_hits", 3))
    )
    cfg.liq_min_usd = float(controls.get("liq_min_usd", cfg.liq_min_usd))

    asyncio.create_task(run_watcher(signal, cfg, queue))
    _push_recent_event(
        "accepted",
        "monitoring_started",
        symbol,
        direction=direction,
        period_min=period_min,
        oi_change_pct=oi_change_pct,
        price_change_pct=price_change_pct,
        min_oi_growth_pct=min_oi_growth_pct,
        max_growth_period_min=max_growth_period_min,
        pump_price_pct=cfg.pump_price_pct,
        oi_max_reaction_pct=cfg.oi_max_reaction_pct,
        near_top_pct=cfg.near_top_pct,
        min_flags_required=cfg.min_flags_required,
        mandatory_min_hits=cfg.mandatory_min_hits,
        liq_min_usd=cfg.liq_min_usd,
    )
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
