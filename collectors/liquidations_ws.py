#!/usr/bin/env python3
"""
Standalone Bybit linear USDT perpetual liquidations collector (WebSocket).

Does not import trading/, short_pump/, or runner. Writes append-only CSV.
"""
from __future__ import annotations

import asyncio
import csv
import json
import logging
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, List

try:
    import websockets
    from websockets.exceptions import ConnectionClosed, WebSocketException
except ImportError as e:  # pragma: no cover
    print("Missing dependency: pip install websockets", file=sys.stderr)
    raise SystemExit(1) from e

WS_URL = "wss://stream.bybit.com/v5/public/linear"
REST_URL = "https://api.bybit.com/v5/market/instruments-info"
CSV_PATH = Path(__file__).resolve().parent.parent / "datasets" / "liquidations.csv"
TOPIC_PREFIX = "allLiquidation."
MAX_SUB_ARGS = 10
STATS_INTERVAL_SEC = 600.0
BACKOFF_START = 1.0
BACKOFF_MAX = 30.0
WATCHDOG_SILENCE_SEC = 1800  # 30 minutes global silence = exit
WATCHDOG_CHECK_INTERVAL_SEC = 60
WATCHDOG_GRACE_SEC = 300  # grace period after process start
TG_ALERT_COOLDOWN_SEC = 1800

CSV_COLUMNS = ("ts_utc", "ts_ms", "symbol", "side", "qty", "price", "value_usd")

log = logging.getLogger("liquidations_ws")
_write_lock = asyncio.Lock()
_counter_lock = asyncio.Lock()
_liq_count_window = 0
_last_activity_mono: float = time.monotonic()
_activity_lock = asyncio.Lock()
_connected_workers: int = 0
_connected_lock = asyncio.Lock()
_last_tg_alert_mono: dict[str, float] = {}
_collector_start_mono: float = 0.0
_total_workers: int = 0


def _fetch_bybit_linear_usdt_trading_symbols() -> List[str]:
    """Sync fetch: USDT linear perpetuals in Trading status."""
    symbols: list[str] = []
    cursor: str | None = None
    while True:
        params: dict[str, str] = {"category": "linear", "limit": "1000"}
        if cursor:
            params["cursor"] = cursor
        url = REST_URL + "?" + urllib.parse.urlencode(params)
        try:
            with urllib.request.urlopen(url, timeout=45) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as e:
            log.error("instruments-info HTTP error: %s", e)
            raise
        result = payload.get("result") or {}
        items = result.get("list") or []
        for inst in items:
            if inst.get("quoteCoin") != "USDT":
                continue
            if inst.get("status") != "Trading":
                continue
            if inst.get("contractType") != "LinearPerpetual":
                continue
            sym = inst.get("symbol")
            if sym:
                symbols.append(str(sym))
        cursor = result.get("nextPageCursor") or None
        if not cursor:
            break
    return sorted(set(symbols))


def _ensure_csv_header(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(CSV_COLUMNS)
            f.flush()


async def _touch_global_activity() -> None:
    """Update heartbeat on any received WS frame (incl. ping/subscribe ack)."""
    global _last_activity_mono
    async with _activity_lock:
        _last_activity_mono = time.monotonic()


def _send_telegram_alert(message: str, alert_key: str) -> None:
    """Send TG alert with per-key cooldown; no-op if env missing."""
    now = time.monotonic()
    last = _last_tg_alert_mono.get(alert_key, 0.0)
    if now - last < TG_ALERT_COOLDOWN_SEC:
        return
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        log.warning("TG_ALERT skipped: no TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID in env")
        return
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        data = urllib.parse.urlencode(
            {
                "chat_id": chat_id,
                "text": f"🚨 [liquidations-collector] {message}",
                "parse_mode": "HTML",
            }
        ).encode()
        req = urllib.request.Request(url, data=data)
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp.read()
        _last_tg_alert_mono[alert_key] = now
        log.info("TG_ALERT_SENT key=%s", alert_key)
    except Exception as e:
        log.warning("TG_ALERT_FAILED key=%s err=%s", alert_key, e)


async def _global_silence_watchdog() -> None:
    """Exit if no WS frames system-wide for WATCHDOG_SILENCE_SEC (after grace)."""
    log.info(
        "GLOBAL_SILENCE_WATCHDOG started check_interval=%ds silence_limit=%ds grace=%ds",
        int(WATCHDOG_CHECK_INTERVAL_SEC),
        int(WATCHDOG_SILENCE_SEC),
        int(WATCHDOG_GRACE_SEC),
    )
    start_mono = time.monotonic()
    while True:
        await asyncio.sleep(WATCHDOG_CHECK_INTERVAL_SEC)
        now = time.monotonic()
        if now - start_mono < WATCHDOG_GRACE_SEC:
            continue
        async with _activity_lock:
            silence = now - _last_activity_mono
        if silence > WATCHDOG_SILENCE_SEC:
            msg = (
                f"WATCHDOG TRIGGERED: no WS frames for {silence:.0f}s "
                f"(threshold {WATCHDOG_SILENCE_SEC}s). Exiting (systemd will restart)."
            )
            log.error(msg)
            _send_telegram_alert(msg, alert_key="watchdog_silence")
            await asyncio.sleep(2)
            os._exit(1)


async def _inc_connected() -> None:
    global _connected_workers
    async with _connected_lock:
        _connected_workers += 1


async def _dec_connected() -> None:
    global _connected_workers
    async with _connected_lock:
        _connected_workers = max(0, _connected_workers - 1)


async def _append_liquidation_rows(rows: List[tuple[Any, ...]]) -> None:
    global _liq_count_window
    if not rows:
        return
    async with _write_lock:
        _ensure_csv_header(CSV_PATH)
        with CSV_PATH.open("a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            for row in rows:
                writer.writerow(row)
            f.flush()
    async with _counter_lock:
        _liq_count_window += len(rows)


def _row_from_event(ev: dict[str, Any]) -> tuple[Any, ...] | None:
    try:
        ts_ms = int(ev["T"])
        symbol = str(ev["s"])
        side = str(ev["S"])
        qty_s = str(ev["v"])
        price_s = str(ev["p"])
        ts_utc = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat(
            timespec="milliseconds"
        )
        value_usd = float(qty_s) * float(price_s)
    except (KeyError, TypeError, ValueError) as e:
        log.warning("skip malformed liquidation event: %s err=%s", ev, e)
        return None
    return (ts_utc, ts_ms, symbol, side, qty_s, price_s, f"{value_usd:.8f}")


async def _handle_ws_message(conn_id: int, msg: dict[str, Any]) -> None:
    op = msg.get("op")
    if op == "pong":
        return
    topic = msg.get("topic") or ""
    if not topic.startswith(TOPIC_PREFIX):
        return
    data = msg.get("data")
    if data is None:
        return
    if isinstance(data, dict):
        items = [data]
    elif isinstance(data, list):
        items = data
    else:
        return
    out_rows: list[tuple[Any, ...]] = []
    for ev in items:
        if not isinstance(ev, dict):
            continue
        row = _row_from_event(ev)
        if row:
            out_rows.append(row)
    if out_rows:
        await _append_liquidation_rows(out_rows)


async def _stats_loop() -> None:
    global _liq_count_window
    while True:
        await asyncio.sleep(STATS_INTERVAL_SEC)
        async with _counter_lock:
            n = _liq_count_window
            _liq_count_window = 0
        async with _connected_lock:
            cw = _connected_workers
        log.info(
            "STATS interval=%ds liquidations_written=%d connected_ws=%d/%d",
            int(STATS_INTERVAL_SEC),
            n,
            cw,
            _total_workers,
        )
        now_mono = time.monotonic()
        if (
            now_mono - _collector_start_mono >= WATCHDOG_GRACE_SEC
            and cw == 0
            and _total_workers > 0
        ):
            _send_telegram_alert(
                "ALERT: 0 active WebSocket connections (all workers disconnected).",
                alert_key="zero_connections",
            )


async def _connection_worker(conn_id: int, symbols: List[str]) -> None:
    args = [f"{TOPIC_PREFIX}{s}" for s in symbols]
    backoff = BACKOFF_START
    await asyncio.sleep(min(0.05 * conn_id, 3.0))
    while True:
        try:
            log.info(
                "CONNECTING conn_id=%d topics=%d sample=%s",
                conn_id,
                len(args),
                args[0] if args else "",
            )
            async with websockets.connect(
                WS_URL,
                ping_interval=20,
                ping_timeout=120,
                close_timeout=10,
                max_size=2**22,
            ) as ws:
                sub = {"op": "subscribe", "args": args}
                await ws.send(json.dumps(sub))
                log.info("CONNECTED conn_id=%d subscribed_args=%d", conn_id, len(args))
                backoff = BACKOFF_START
                await _inc_connected()
                try:
                    async for raw in ws:
                        await _touch_global_activity()
                        if isinstance(raw, bytes):
                            raw = raw.decode("utf-8")
                        try:
                            msg = json.loads(raw)
                        except json.JSONDecodeError:
                            log.warning("conn_id=%d non-json: %r", conn_id, raw[:200])
                            continue
                        if not isinstance(msg, dict):
                            continue
                        if msg.get("op") == "ping":
                            await ws.send(json.dumps({"op": "pong"}))
                            continue
                        if msg.get("success") is False:
                            log.warning("conn_id=%d WS error payload: %s", conn_id, raw[:500])
                            continue
                        await _handle_ws_message(conn_id, msg)
                finally:
                    await _dec_connected()
        except ConnectionClosed as e:
            log.warning(
                "DISCONNECT conn_id=%d code=%s reason=%r — retry in %.1fs",
                conn_id,
                e.code,
                e.reason,
                backoff,
            )
            log.info(
                "RECONNECT_SCHEDULED conn_id=%d reason=connection_closed exc_type=ConnectionClosed "
                "code=%s detail=%r backoff=%.1fs",
                conn_id,
                e.code,
                e.reason,
                backoff,
            )
        except (WebSocketException, OSError, asyncio.TimeoutError) as e:
            log.warning("DISCONNECT conn_id=%d err=%s — retry in %.1fs", conn_id, e, backoff)
            log.info(
                "RECONNECT_SCHEDULED conn_id=%d reason=ws_or_os exc_type=%s detail=%r backoff=%.1fs",
                conn_id,
                type(e).__name__,
                e,
                backoff,
            )
        except Exception as e:  # pragma: no cover
            log.exception("conn_id=%d fatal loop err=%s — retry in %.1fs", conn_id, e, backoff)
            log.info(
                "RECONNECT_SCHEDULED conn_id=%d reason=unexpected exc_type=%s detail=%r backoff=%.1fs",
                conn_id,
                type(e).__name__,
                e,
                backoff,
            )
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2.0, BACKOFF_MAX)


async def main_async() -> None:
    global _last_activity_mono, _collector_start_mono, _total_workers
    symbols = await asyncio.to_thread(_fetch_bybit_linear_usdt_trading_symbols)
    if not symbols:
        log.error("No symbols from instruments-info; exiting")
        raise SystemExit(2)
    log.info("SYMBOLS loaded count=%d (REST instruments-info)", len(symbols))
    batches: List[List[str]] = [
        symbols[i : i + MAX_SUB_ARGS] for i in range(0, len(symbols), MAX_SUB_ARGS)
    ]
    log.info("WS_PLAN connections=%d max_args_per_conn=%d", len(batches), MAX_SUB_ARGS)
    _collector_start_mono = time.monotonic()
    _total_workers = len(batches)
    async with _activity_lock:
        _last_activity_mono = time.monotonic()
    asyncio.create_task(_global_silence_watchdog())
    asyncio.create_task(_stats_loop())
    workers = [
        asyncio.create_task(_connection_worker(i, batch), name=f"liq_ws_{i}")
        for i, batch in enumerate(batches)
    ]
    await asyncio.gather(*workers)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        log.info("shutdown requested")


if __name__ == "__main__":
    main()
