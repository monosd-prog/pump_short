from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any, Dict

import aiohttp
import pandas as pd

from common.market_features import liquidation_features
from notifications.tg_format import build_short_pump_signal, format_false_pump_entry
from short_pump.bybit_api import (
    get_funding_rate,
    get_klines_1m,
    get_klines_5m,
    get_open_interest,
    get_recent_trades,
)
from short_pump.false_pump.config import FalsePumpConfig
from short_pump.false_pump.detector import detect_false_pump
from short_pump.telegram import TG_BOT_TOKEN, TG_CHAT_ID, send_telegram
from short_pump.liquidations import get_liq_stats, get_liq_stats_usd
from trading.queue import enqueue_signal

logger = logging.getLogger(__name__)

_active_symbols: dict = {}
_cancel_flags: dict = {}
FALSE_PUMP_WEBHOOK_URL = os.getenv(
    "FALSE_PUMP_WEBHOOK_URL",
    "http://localhost:8441/api/oi_signal",
)


def _funding_rate_value(payload: dict | None) -> float:
    if not payload:
        return 0.0
    try:
        return float(payload.get("fundingRate"))
    except (TypeError, ValueError):
        return 0.0


async def notify_false_pump(
    symbol: str,
    direction: str,
    oi_change_pct: float,
    price_change_pct: float,
) -> None:
    if direction != "UP":
        return
    if oi_change_pct <= 20.0 or price_change_pct <= 5.0:
        return
    payload = {
        "symbol": symbol,
        "direction": direction,
        "oi_change_pct": round(oi_change_pct, 4),
        "price_change_pct": round(price_change_pct, 4),
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                FALSE_PUMP_WEBHOOK_URL,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=3),
            ) as resp:
                logging.info(
                    f"[false_pump] notified {symbol} "
                    f"oi={oi_change_pct}% price={price_change_pct}% "
                    f"status={resp.status}"
                )
    except Exception as e:
        logging.warning(f"[false_pump] notify failed {symbol}: {e}")


def _peak_price_5m(symbol: str) -> float:
    try:
        c5 = get_klines_5m("linear", symbol, limit=12)
        if c5 is not None and not c5.empty:
            return float(pd.to_numeric(c5["close"], errors="coerce").dropna().max())
    except Exception:
        logger.exception("false_pump: failed to fetch 5m candles for %s", symbol)
    c1 = get_klines_1m("linear", symbol, limit=60)
    if c1 is None or c1.empty:
        return 0.0
    return float(pd.to_numeric(c1["close"], errors="coerce").dropna().max())


async def run_watcher(signal: dict, cfg: FalsePumpConfig, queue) -> None:
    logger = logging.getLogger("false_pump.watcher")
    symbol = str(signal.get("symbol", "")).strip().upper()
    if not symbol:
        return
    logger.info(f"[false_pump.watcher] START monitoring {symbol}")

    funding_payload_start = await asyncio.to_thread(get_funding_rate, "linear", symbol)
    funding_rate_start = _funding_rate_value(funding_payload_start)

    if TG_BOT_TOKEN and TG_CHAT_ID:
        try:
            funding_warn = ""
            if funding_rate_start < -0.005:
                funding_warn = (
                    f"\n⚠️ funding={funding_rate_start:.4f} "
                    f"(отрицательный — вход заблокирован)"
                )
            text = (
                f"👀 false_pump | МОНИТОРИНГ СТАРТ\n"
                f"sym={symbol}\n"
                f"OI-бот: oi={float(signal.get('oi_change_pct') or 0):.1f}% "
                f"price={float(signal.get('price_change_pct') or 0):.1f}% window=90m\n"
                f"Мониторинг до {int(cfg.monitor_timeout_sec / 3600)}ч"
                f"{funding_warn}\n"
                f"#false_pump #WATCH"
            )
            send_telegram(
                text,
                strategy="false_pump",
                side="SHORT",
                mode="paper",
                event_id=f"watch_{symbol}_{int(time.time())}",
                context_score=None,
                entry_ok=True,
                formatted=True,
            )
        except Exception:
            logger.exception(f"[false_pump.watcher] TG watch start failed: {symbol}")

    session_ts = time.time()
    _active_symbols[symbol] = {"started_ts": session_ts}
    try:
        peak_price_5m = await asyncio.to_thread(_peak_price_5m, symbol)
        started = time.time()
        first_tick = True

        while (time.time() - started) < int(cfg.monitor_timeout_sec):
            if not first_tick:
                cur_sym = _active_symbols.get(symbol)
                if cur_sym and cur_sym.get("started_ts") != session_ts:
                    _cancel_flags.pop(symbol, None)
                    logger.info(f"[false_pump.watcher] CANCELLED {symbol} — новый сигнал")
                    break
                if _cancel_flags.get(symbol):
                    logger.info(f"[false_pump.watcher] CANCELLED {symbol} — новый сигнал")
                    _cancel_flags.pop(symbol, None)
                    break
            first_tick = False
            try:
                candles_1m = await asyncio.to_thread(get_klines_1m, "linear", symbol, 120)
                oi_1m = await asyncio.to_thread(get_open_interest, "linear", symbol, 80)
                trades = await asyncio.to_thread(get_recent_trades, "linear", symbol, 1000)
                funding_payload = await asyncio.to_thread(get_funding_rate, "linear", symbol)
                funding_rate = _funding_rate_value(funding_payload)
                now_ts = time.time()
                liq = liquidation_features(
                    symbol=symbol,
                    now_ts=now_ts,
                    get_liq_stats=get_liq_stats,
                    get_liq_stats_usd=get_liq_stats_usd,
                )
                liq_features = {
                    "short_liq_usd": float(liq.get("liq_short_usd_30s_real") or liq.get("liq_short_usd_30s") or 0.0)
                }

                signal_ok, details = detect_false_pump(
                    candles_1m=candles_1m,
                    oi_1m=oi_1m,
                    trades=trades,
                    liq_features=liq_features,
                    funding_rate=funding_rate,
                    peak_price_5m=peak_price_5m,
                    cfg=cfg,
                    symbol=symbol,
                )

                flags_dict = details.get("flags", {}) if details else {}
                pump_pct_val = float(details.get("pump_price_pct", 0.0)) if details else 0.0
                oi_chg_val = float(details.get("oi_change_pct", 0.0)) if details else 0.0
                logger.info(
                    f"[false_pump.watcher] TICK {symbol} "
                    f"signal_ok={signal_ok} "
                    f"pump={flags_dict.get('pump_detected')} "
                    f"pump_pct={pump_pct_val:.2f}% "
                    f"oi_chg={oi_chg_val:.3f}% "
                    f"oi_thr={cfg.oi_max_reaction_pct}% "
                    f"oi_weak={flags_dict.get('oi_weak')} "
                    f"near_top={flags_dict.get('near_top')} "
                    f"funding={funding_rate:.4f} "
                    f"liq={flags_dict.get('liq_present')} "
                    f"delta={flags_dict.get('delta_ok')}"
                )

                oi_change_pct = float(details.get("oi_change_pct") or 0.0) if details else 0.0
                price_change_pct = float(details.get("pump_price_pct") or 0.0) if details else 0.0
                direction = "UP" if oi_change_pct > 0 else "DOWN"
                await notify_false_pump(symbol, direction, abs(oi_change_pct), price_change_pct)
                if signal_ok:
                    flags = details.get("flags") or {}
                    entry_price = float(details.get("price") or 0.0)
                    if entry_price <= 0:
                        break
                    sl_price = entry_price * (1.0 + float(cfg.sl_pct) / 100.0)
                    tp_price = entry_price * (1.0 - float(cfg.tp_pct) / 100.0)
                    event_id = str(int(time.time() * 1000))
                    time_utc = pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%d %H:%M:%S%z")
                    sig = build_short_pump_signal(
                        strategy="false_pump",
                        side="SHORT",
                        symbol=symbol,
                        run_id=event_id,
                        event_id=event_id,
                        time_utc=time_utc,
                        price=entry_price,
                        entry_price=entry_price,
                        tp_price=tp_price,
                        sl_price=sl_price,
                        tp_pct=float(cfg.tp_pct),
                        sl_pct=float(cfg.sl_pct),
                        entry_type="FALSE_PUMP",
                        context_score=details.get("context_score"),
                        liq_short_usd_30s=liq.get("liq_short_usd_30s"),
                        liq_long_usd_30s=liq.get("liq_long_usd_30s"),
                        funding_rate_abs=abs(float(details.get("funding_rate") or 0.0)),
                        oi_change_fast_pct=details.get("oi_change_pct"),
                        liq_short_usd_30s_real=liq.get("liq_short_usd_30s_real"),
                        liq_long_usd_30s_real=liq.get("liq_long_usd_30s_real"),
                        dist_to_peak_pct=details.get("dist_to_peak_pct"),
                        stage=4,
                        debug_payload={
                            "strategy": "false_pump",
                            "peak_price_5m": details.get("peak_price_5m"),
                            "pump_price_pct": details.get("pump_price_pct"),
                            "oi_bot_trigger": {
                                "oi_change_pct": signal.get("oi_change_pct"),
                                "price_change_pct": signal.get("price_change_pct"),
                                "direction": signal.get("direction"),
                            },
                            "flags": {
                                "oi_bot_confirmed": True,
                                "pump_detected": bool(flags.get("pump_detected")),
                                "oi_weak": bool(flags.get("oi_weak")),
                                "near_top": bool(flags.get("near_top")),
                                "delta_ok": bool(flags.get("delta_ok")),
                                "no_new_high": bool(flags.get("no_new_high")),
                                "liq_present": bool(flags.get("liq_present")),
                            },
                        },
                    )
                    await asyncio.to_thread(enqueue_signal, sig)
                    if TG_BOT_TOKEN and TG_CHAT_ID:
                        try:
                            text = format_false_pump_entry(
                                symbol=symbol,
                                event_id=event_id,
                                entry_price=entry_price,
                                tp_price=tp_price,
                                sl_price=sl_price,
                                tp_pct=float(cfg.tp_pct),
                                sl_pct=float(cfg.sl_pct),
                                funding_rate=funding_rate,
                                oi_change_pct=oi_change_pct,
                                oi_bot_oi_pct=signal.get("oi_change_pct"),
                                oi_bot_price_pct=signal.get("price_change_pct"),
                                context_score=details.get("context_score"),
                                pump_pct=details.get("pump_price_pct"),
                            )
                            send_telegram(
                                text,
                                strategy="false_pump",
                                side="SHORT",
                                mode="paper",
                                event_id=event_id,
                                context_score=details.get("context_score"),
                                entry_ok=True,
                                formatted=True,
                            )
                            logger.info(f"[false_pump.watcher] TG entry sent: {symbol}")
                        except Exception:
                            logger.exception(f"[false_pump.watcher] TG entry send failed: {symbol}")
                    break
            except Exception:
                logger.exception(f"[false_pump.watcher] TICK ERROR {symbol}")
            await asyncio.sleep(max(1, int(cfg.poll_interval_sec)))
        else:
            logging.warning("false_pump: timeout %s", symbol)
            if TG_BOT_TOKEN and TG_CHAT_ID:
                try:
                    text = (
                        f"⏱ false_pump | МОНИТОРИНГ ЗАВЕРШЁН (таймаут)\n"
                        f"sym={symbol}\n"
                        f"Лже-памп не обнаружен за {int(cfg.monitor_timeout_sec / 3600)}ч\n"
                        f"#false_pump #TIMEOUT"
                    )
                    send_telegram(
                        text,
                        strategy="false_pump",
                        side="SHORT",
                        mode="paper",
                        event_id=f"timeout_{symbol}_{int(time.time())}",
                        context_score=None,
                        entry_ok=False,
                        formatted=True,
                    )
                except Exception:
                    logger.exception(f"[false_pump.watcher] TG timeout failed: {symbol}")
    finally:
        await asyncio.sleep(max(1, int(cfg.trigger_cooldown_sec)))
        cur = _active_symbols.get(symbol)
        if cur and cur.get("started_ts") == session_ts:
            _active_symbols.pop(symbol, None)
