from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any, Dict

import aiohttp
import pandas as pd

from common.feature_contract import normalize_event_feature_row
from common.io_dataset import write_event_row, write_outcome_row, write_trade_row
from common.market_features import liquidation_features
from common.outcome_tracker import build_outcome_row
from common.runtime import wall_time_utc
from notifications.tg_format import (
    build_short_pump_signal,
    format_false_pump_entry,
    format_false_pump_timeout,
)
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
from trading.paper_outcome import close_from_outcome
from trading.queue import enqueue_signal
from trading.state import make_position_id

logger = logging.getLogger(__name__)

_active_symbols: dict = {}
_cancel_flags: dict = {}
FALSE_PUMP_WEBHOOK_URL = os.getenv(
    "FALSE_PUMP_WEBHOOK_URL",
    "http://localhost:8441/api/oi_signal",
)

_tg_start_sent: dict = {}
TG_START_COOLDOWN_SEC = 3600  # 1 час


def _fp_ds_event(
    *,
    run_id: str,
    event_id: str,
    symbol: str,
    stage: int,
    entry_ok: int,
    skip_reasons: str,
    context_score: float | None,
    payload: Dict[str, Any] | None,
    extra: Dict[str, Any] | None = None,
) -> None:
    """Write a canonical false_pump event row to datasets/strategy=false_pump/mode=paper/events_v3.csv."""
    pl = payload or {}
    wall_ts = wall_time_utc()
    row = normalize_event_feature_row(
        base={
            "run_id": run_id,
            "event_id": event_id,
            "symbol": symbol,
            "strategy": "false_pump",
            "mode": "paper",
            "source_mode": "paper",
            "side": "SHORT",
            "wall_time_utc": wall_ts,
            "time_utc": pl.get("time_utc", ""),
            "stage": stage,
            "entry_ok": int(bool(entry_ok)),
            "skip_reasons": skip_reasons,
            "context_score": context_score if context_score is not None else "",
            "price": pl.get("price", ""),
            "dist_to_peak_pct": float(pl.get("dist_to_peak_pct") or 0.0),
            "liq_short_count_30s": pl.get("liq_short_count_30s", 0),
            "liq_short_usd_30s": pl.get("liq_short_usd_30s", 0),
            "liq_long_count_30s": pl.get("liq_long_count_30s", 0),
            "liq_long_usd_30s": pl.get("liq_long_usd_30s", 0),
            "liq_short_count_1m": pl.get("liq_short_count_1m", 0),
            "liq_short_usd_1m": pl.get("liq_short_usd_1m", 0),
            "liq_long_count_1m": pl.get("liq_long_count_1m", 0),
            "liq_long_usd_1m": pl.get("liq_long_usd_1m", 0),
            "outcome_label": pl.get("outcome_label", ""),
        },
        payload=pl,
        extra=extra or None,
    )
    try:
        write_event_row(
            row,
            strategy="false_pump",
            mode="paper",
            wall_time_utc=wall_ts,
            schema_version=3,
            path_mode="paper",
        )
    except Exception:
        logger.exception(
            "[false_pump.watcher] EVENT_WRITE_FAILED symbol=%s event_id=%s",
            symbol,
            event_id,
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


async def _track_false_pump_outcome(
    *,
    symbol: str,
    run_id_fp: str,
    event_id: str,
    trade_id: str,
    entry_ts_utc: pd.Timestamp,
    entry_price: float,
    tp_price: float,
    sl_price: float,
    deadline_ts: float,
    poll_interval_sec: int,
    context_score: float | None,
) -> None:
    """
    Inline async outcome poller for false_pump (mirrors track_outcome_short touch-model).
    Polls 1m klines until TP/SL hit or monitor_timeout_sec deadline.
    On outcome: close_from_outcome (→ trading_closes.csv + TG) + write_outcome_row (→ outcomes_v3.csv).
    SHORT convention: SL_hit when high >= sl_price; TP_hit when low <= tp_price.
    Same-candle conflict: SL first (conservative).
    """
    end_reason = "TIMEOUT"
    exit_price = entry_price
    hit_ts_utc: str | None = None
    mfe_pct = 0.0
    mae_pct = 0.0
    last_close: float | None = None
    entry_ms = int(entry_ts_utc.timestamp() * 1000)

    try:
        while time.time() < deadline_ts:
            try:
                kl = await asyncio.to_thread(get_klines_1m, "linear", symbol, 10)
            except Exception:
                logger.exception("[false_pump.watcher] OUTCOME_POLL_FETCH_FAIL %s", symbol)
                await asyncio.sleep(poll_interval_sec)
                continue
            if kl is None or kl.empty:
                await asyncio.sleep(poll_interval_sec)
                continue
            try:
                kdf = kl[kl["ts"] >= pd.Timestamp(entry_ms, unit="ms", tz="UTC")]
            except Exception:
                kdf = kl
            if kdf is None or kdf.empty:
                await asyncio.sleep(poll_interval_sec)
                continue
            hit = False
            for _, candle in kdf.iterrows():
                try:
                    c_high = float(candle["high"])
                    c_low = float(candle["low"])
                    c_close = float(candle["close"])
                    c_ts = candle["ts"]
                except Exception:
                    continue
                last_close = c_close
                if entry_price > 0:
                    run_mae = (c_high - entry_price) / entry_price * 100.0
                    run_mfe = (entry_price - c_low) / entry_price * 100.0
                    if run_mae > mae_pct:
                        mae_pct = run_mae
                    if run_mfe > mfe_pct:
                        mfe_pct = run_mfe
                sl_hit = c_high >= sl_price
                tp_hit = c_low <= tp_price
                if sl_hit and tp_hit:
                    end_reason = "SL_hit"
                    exit_price = sl_price
                    try:
                        hit_ts_utc = c_ts.isoformat() if hasattr(c_ts, "isoformat") else str(c_ts)
                    except Exception:
                        hit_ts_utc = pd.Timestamp.now(tz="UTC").isoformat()
                    hit = True
                    break
                if sl_hit:
                    end_reason = "SL_hit"
                    exit_price = sl_price
                    try:
                        hit_ts_utc = c_ts.isoformat() if hasattr(c_ts, "isoformat") else str(c_ts)
                    except Exception:
                        hit_ts_utc = pd.Timestamp.now(tz="UTC").isoformat()
                    hit = True
                    break
                if tp_hit:
                    end_reason = "TP_hit"
                    exit_price = tp_price
                    try:
                        hit_ts_utc = c_ts.isoformat() if hasattr(c_ts, "isoformat") else str(c_ts)
                    except Exception:
                        hit_ts_utc = pd.Timestamp.now(tz="UTC").isoformat()
                    hit = True
                    break
            if hit:
                break
            await asyncio.sleep(poll_interval_sec)
        if end_reason == "TIMEOUT":
            exit_price = last_close if last_close is not None else entry_price
            hit_ts_utc = pd.Timestamp.now(tz="UTC").isoformat()
    except Exception:
        logger.exception("[false_pump.watcher] OUTCOME_POLL_LOOP_ERROR %s", symbol)
        return

    if entry_price > 0:
        pnl_pct = (entry_price - float(exit_price)) / entry_price * 100.0
    else:
        pnl_pct = 0.0

    outcome_time_utc = hit_ts_utc or pd.Timestamp.now(tz="UTC").isoformat()
    try:
        hold_seconds = max(1.0, float((pd.Timestamp.now(tz="UTC") - entry_ts_utc).total_seconds()))
    except Exception:
        hold_seconds = 1.0

    try:
        close_from_outcome(
            strategy="false_pump",
            symbol=symbol,
            run_id=run_id_fp,
            event_id=str(event_id),
            res=end_reason,
            pnl_pct=pnl_pct,
            ts_utc=outcome_time_utc,
            outcome_meta={
                "mfe_pct": float(mfe_pct),
                "mae_pct": float(mae_pct),
            },
        )
    except Exception:
        logger.exception(
            "[false_pump.watcher] CLOSE_FROM_OUTCOME_FAILED symbol=%s event_id=%s",
            symbol,
            event_id,
        )

    summary: Dict[str, Any] = {
        "end_reason": end_reason,
        "outcome": end_reason,
        "pnl_pct": float(pnl_pct),
        "entry_price": float(entry_price),
        "tp_price": float(tp_price),
        "sl_price": float(sl_price),
        "exit_price": float(exit_price),
        "mfe_pct": float(mfe_pct),
        "mae_pct": float(mae_pct),
        "hold_seconds": float(hold_seconds),
        "entry_time_utc": entry_ts_utc.isoformat(),
        "trade_type": "PAPER",
        "risk_profile": "",
    }
    try:
        outcome_row = build_outcome_row(
            summary,
            trade_id=str(trade_id),
            event_id=str(event_id),
            run_id=run_id_fp,
            symbol=symbol,
            strategy="false_pump",
            mode="paper",
            side="SHORT",
            outcome_time_utc=outcome_time_utc,
            entry_snapshot={
                "context_score": context_score,
                "entry_price": entry_price,
                "tp_price": tp_price,
                "sl_price": sl_price,
            },
            extra_details={
                "entry_time_utc": entry_ts_utc.isoformat(),
                "outcome_time_utc": outcome_time_utc,
                "hold_seconds": hold_seconds,
            },
        )
        if outcome_row is not None:
            outcome_row["outcome_source"] = "false_pump_watcher"
            write_outcome_row(
                outcome_row,
                strategy="false_pump",
                mode="paper",
                wall_time_utc=outcome_time_utc,
                schema_version=3,
                path_mode="paper",
            )
    except Exception:
        logger.exception(
            "[false_pump.watcher] OUTCOME_WRITE_FAILED symbol=%s event_id=%s",
            symbol,
            event_id,
        )

    logger.info(
        "[false_pump.watcher] OUTCOME_FINALIZED symbol=%s event_id=%s outcome=%s pnl_pct=%.4f exit=%s",
        symbol,
        event_id,
        end_reason,
        pnl_pct,
        exit_price,
    )


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
            now_ts = time.time()
            last_tg_start = _tg_start_sent.get(symbol, 0)
            if now_ts - last_tg_start >= TG_START_COOLDOWN_SEC:
                _tg_start_sent[symbol] = now_ts
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
            else:
                logger.info(
                    f"[false_pump.watcher] TG start cooldown {symbol} "
                    f"last={int(now_ts - last_tg_start)}s ago, skip"
                )
        except Exception:
            logger.exception(f"[false_pump.watcher] TG watch start failed: {symbol}")

    session_ts = time.time()
    _active_symbols[symbol] = {"started_ts": session_ts}
    run_id_fp = f"fp_{symbol}_{int(session_ts)}"
    try:
        _fp_ds_event(
            run_id=run_id_fp,
            event_id=f"{run_id_fp}_watch_start",
            symbol=symbol,
            stage=0,
            entry_ok=0,
            skip_reasons="watch_start",
            context_score=0.0,
            payload={
                "time_utc": pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%d %H:%M:%S%z"),
                "price": 0.0,
                "dist_to_peak_pct": 0.0,
                "source": "false_pump_webhook",
                "oi_change_pct": signal.get("oi_change_pct"),
                "price_change_pct": signal.get("price_change_pct"),
            },
            extra=None,
        )
    except Exception:
        logger.exception("[false_pump.watcher] WATCH_START event write failed symbol=%s", symbol)
    try:
        peak_price_5m = await asyncio.to_thread(_peak_price_5m, symbol)
        started = time.time()
        first_tick = True
        funding_at_signal: float | None = None
        monitor_entry_price: float | None = None
        monitor_last_price: float | None = None
        last_funding_rate: float | None = None
        last_dist_to_peak_pct: float | None = None
        last_context_score: float | None = None
        last_oi_change_pct: float | None = None
        last_pump_price_pct: float | None = None
        last_stage: int | None = None
        last_pump_detected: bool | None = None
        last_oi_weak: bool | None = None
        last_near_top: bool | None = None
        last_delta_ok: bool | None = None
        last_no_new_high: bool | None = None
        last_liq_present: bool | None = None

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
                if funding_at_signal is None:
                    funding_at_signal = funding_rate
                    logger.info(
                        f"[false_pump.watcher] FUNDING_BASELINE {symbol} "
                        f"funding_at_signal={funding_at_signal:.4f}"
                    )
                    if TG_BOT_TOKEN and TG_CHAT_ID:
                        try:
                            now_ts = time.time()
                            last_tg_start = _tg_start_sent.get(symbol, 0)
                            if now_ts - last_tg_start >= TG_START_COOLDOWN_SEC:
                                _tg_start_sent[symbol] = now_ts
                                text = (
                                    f"👀 false_pump | МОНИТОРИНГ СТАРТ\n"
                                    f"sym={symbol}\n"
                                    f"funding_baseline={funding_at_signal:.4f} (зафиксирован на старте)\n"
                                    f"#false_pump #WATCH"
                                )
                                send_telegram(
                                    text,
                                    strategy="false_pump",
                                    side="SHORT",
                                    mode="paper",
                                    event_id=f"watch_baseline_{symbol}_{int(time.time())}",
                                    context_score=None,
                                    entry_ok=True,
                                    formatted=True,
                                )
                            else:
                                logger.info(
                                    f"[false_pump.watcher] TG start cooldown {symbol} "
                                    f"last={int(now_ts - last_tg_start)}s ago, skip"
                                )
                        except Exception:
                            logger.exception(
                                f"[false_pump.watcher] TG watch baseline failed: {symbol}"
                            )
                funding_delta = (
                    funding_rate - funding_at_signal if funding_at_signal is not None else 0.0
                )
                funding_improving = (
                    funding_at_signal is not None
                    and funding_at_signal < -0.005
                    and funding_delta > 0.002
                )
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
                    funding_improving=funding_improving,
                    funding_at_signal=funding_at_signal,
                    funding_delta=funding_delta,
                )

                flags_dict = details.get("flags", {}) if details else {}
                observed_price = float(details.get("price") or 0.0) if details else 0.0
                last_funding_rate = funding_rate
                last_dist_to_peak_pct = (
                    float(details.get("dist_to_peak_pct"))
                    if details and details.get("dist_to_peak_pct") is not None
                    else None
                )
                last_context_score = (
                    float(details.get("context_score"))
                    if details and details.get("context_score") is not None
                    else None
                )
                last_oi_change_pct = float(details.get("oi_change_pct") or 0.0) if details else None
                last_pump_price_pct = float(details.get("pump_price_pct") or 0.0) if details else None
                last_stage = int(details.get("stage")) if details and details.get("stage") is not None else None
                last_pump_detected = bool(flags_dict.get("pump_detected")) if flags_dict else None
                last_oi_weak = bool(flags_dict.get("oi_weak")) if flags_dict else None
                last_near_top = bool(flags_dict.get("near_top")) if flags_dict else None
                last_delta_ok = bool(flags_dict.get("delta_ok")) if flags_dict else None
                if observed_price > 0:
                    if monitor_entry_price is None:
                        monitor_entry_price = observed_price
                    monitor_last_price = observed_price
                pump_pct_val = float(details.get("pump_price_pct", 0.0)) if details else 0.0
                oi_chg_val = float(details.get("oi_change_pct", 0.0)) if details else 0.0
                last_no_new_high = bool(flags_dict.get("no_new_high")) if flags_dict else None
                last_liq_present = bool(flags_dict.get("liq_present")) if flags_dict else None
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
                    f"f_base={funding_at_signal:.4f} "
                    f"f_delta={funding_delta:+.4f} "
                    f"f_impr={funding_improving} "
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
                    logger.info(
                        f"[false_pump.watcher] SIGNAL_OK {symbol} entry_price={entry_price}"
                    )
                    sl_price = entry_price * (1.0 + float(cfg.sl_pct) / 100.0)
                    tp_price = entry_price * (1.0 - float(cfg.tp_pct) / 100.0)
                    event_id = str(int(time.time() * 1000))
                    time_utc = pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%d %H:%M:%S%z")
                    entry_ts_utc = pd.Timestamp.now(tz="UTC")
                    trade_id = make_position_id("false_pump", run_id_fp, str(event_id), symbol)
                    entry_payload_row = {
                        "time_utc": time_utc,
                        "price": entry_price,
                        "dist_to_peak_pct": details.get("dist_to_peak_pct") or 0.0,
                        "liq_short_usd_30s": liq.get("liq_short_usd_30s") or 0.0,
                        "liq_long_usd_30s": liq.get("liq_long_usd_30s") or 0.0,
                        "liq_short_count_30s": liq.get("liq_short_count_30s") or 0,
                        "liq_long_count_30s": liq.get("liq_long_count_30s") or 0,
                        "liq_short_usd_1m": liq.get("liq_short_usd_1m") or 0.0,
                        "liq_long_usd_1m": liq.get("liq_long_usd_1m") or 0.0,
                        "liq_short_count_1m": liq.get("liq_short_count_1m") or 0,
                        "liq_long_count_1m": liq.get("liq_long_count_1m") or 0,
                        "funding_rate": funding_rate,
                        "funding_rate_abs": abs(float(details.get("funding_rate") or 0.0)),
                        "oi_change_pct": details.get("oi_change_pct"),
                        "pump_price_pct": details.get("pump_price_pct"),
                        "flags": flags,
                        "tp_price": tp_price,
                        "sl_price": sl_price,
                        "entry_type": "FALSE_PUMP",
                    }
                    _fp_ds_event(
                        run_id=run_id_fp,
                        event_id=str(event_id),
                        symbol=symbol,
                        stage=4,
                        entry_ok=1,
                        skip_reasons="entry_ok_false_pump",
                        context_score=details.get("context_score"),
                        payload=entry_payload_row,
                        extra=None,
                    )
                    try:
                        write_trade_row(
                            {
                                "trade_id": trade_id,
                                "event_id": str(event_id),
                                "run_id": run_id_fp,
                                "symbol": symbol,
                                "strategy": "false_pump",
                                "mode": "paper",
                                "side": "SHORT",
                                "entry_time_utc": entry_ts_utc.isoformat(),
                                "entry_price": entry_price,
                                "tp_price": tp_price,
                                "sl_price": sl_price,
                                "trade_type": "PAPER",
                            },
                            strategy="false_pump",
                            mode="paper",
                            wall_time_utc=entry_ts_utc.isoformat(),
                            schema_version=3,
                            path_mode="paper",
                        )
                    except Exception:
                        logger.exception(
                            "[false_pump.watcher] TRADE_WRITE_FAILED symbol=%s event_id=%s",
                            symbol,
                            event_id,
                        )
                    sig = build_short_pump_signal(
                        strategy="false_pump",
                        side="SHORT",
                        symbol=symbol,
                        run_id=run_id_fp,
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
                            "funding_improving": details.get("funding_improving"),
                            "funding_at_signal": details.get("funding_at_signal"),
                            "funding_delta": details.get("funding_delta"),
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
                    logger.info(
                        f"[false_pump.watcher] ENQUEUE_START {symbol} event_id={event_id}"
                    )
                    await asyncio.to_thread(enqueue_signal, sig)
                    logger.info(
                        f"[false_pump.watcher] ENQUEUE_DONE {symbol} event_id={event_id}"
                    )
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
                    await _track_false_pump_outcome(
                        symbol=symbol,
                        run_id_fp=run_id_fp,
                        event_id=str(event_id),
                        trade_id=trade_id,
                        entry_ts_utc=entry_ts_utc,
                        entry_price=entry_price,
                        tp_price=tp_price,
                        sl_price=sl_price,
                        deadline_ts=started + int(cfg.monitor_timeout_sec),
                        poll_interval_sec=max(1, int(cfg.poll_interval_sec)),
                        context_score=details.get("context_score"),
                    )
                    return
            except Exception as e:
                err_str = str(e)
                if "10006" in err_str or "Rate Limit" in err_str or "Too many visits" in err_str:
                    logger.warning(f"[false_pump.watcher] RATE LIMIT {symbol} — sleep 30s")
                    await asyncio.sleep(30)
                else:
                    logger.exception(f"[false_pump.watcher] TICK ERROR {symbol}")
                await asyncio.sleep(max(1, int(cfg.poll_interval_sec)))
                continue
            await asyncio.sleep(max(1, int(cfg.poll_interval_sec)))
        else:
            logging.warning("false_pump: timeout %s", symbol)
            timeout_event_id = f"timeout_{symbol}_{int(time.time())}"
            if TG_BOT_TOKEN and TG_CHAT_ID:
                try:
                    text = format_false_pump_timeout(
                        symbol=symbol,
                        hours=float(cfg.monitor_timeout_sec) / 3600.0,
                        funding_rate=last_funding_rate,
                        dist_to_peak_pct=last_dist_to_peak_pct,
                        context_score=last_context_score,
                        oi_change_pct=last_oi_change_pct,
                        pump_price_pct=last_pump_price_pct,
                        stage=last_stage,
                        pump_detected=last_pump_detected,
                        oi_weak=last_oi_weak,
                        near_top=last_near_top,
                        delta_ok=last_delta_ok,
                        no_new_high=last_no_new_high,
                        liq_present=last_liq_present,
                    )
                    send_telegram(
                        text,
                        strategy="false_pump",
                        side="SHORT",
                        mode="paper",
                        event_id=timeout_event_id,
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
