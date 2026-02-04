# short_pump/watcher.py
from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, Optional

import pandas as pd

from short_pump.bybit_api import (
    get_klines_5m,
    get_klines_1m,
    get_open_interest,
    get_recent_trades,
    get_funding_rate,
)
from short_pump.config import Config
from short_pump.context5m import (
    StructureState,
    build_dbg5,
    compute_context_score_5m,
    update_structure,
)
from short_pump.entry import decide_entry_fast, decide_entry_1m
from short_pump.features import normalize_funding, oi_change_pct
from short_pump.io_csv import append_csv
from short_pump.liquidations import get_liq_health, get_liq_stats, register_symbol
from short_pump.logging_utils import get_logger, log_exception, log_info, log_warning
from common.outcome_tracker import build_outcome_row
from short_pump.outcome import track_outcome_short
from short_pump.telegram import TG_SEND_OUTCOME, send_telegram
from common.io_dataset import write_event_row, write_outcome_row, write_trade_row
from common.runtime import wall_time_utc

logger = get_logger(__name__)


def _utc_now_str() -> str:
    return pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%d %H:%M:%S%z")


def _ds_event(
    *,
    run_id: str,
    symbol: str,
    event_id: str,
    stage: int,
    entry_ok: bool,
    skip_reasons: str,
    context_score: float | None,
    payload: Dict[str, Any] | None,
    extra: Dict[str, Any] | None = None,
) -> None:
    row = {
        "run_id": run_id,
        "event_id": event_id,
        "symbol": symbol,
        "strategy": "short_pump",
        "mode": "live",
        "side": "SHORT",
        "wall_time_utc": wall_time_utc(),
        "time_utc": (payload or {}).get("time_utc", ""),
        "stage": stage,
        "entry_ok": int(bool(entry_ok)),
        "skip_reasons": skip_reasons,
        "context_score": context_score if context_score is not None else "",
        "price": (payload or {}).get("price", ""),
        "dist_to_peak_pct": (payload or {}).get("dist_to_peak_pct", ""),
        "cvd_delta_ratio_30s": (payload or {}).get("cvd_delta_ratio_30s", ""),
        "cvd_delta_ratio_1m": (payload or {}).get("cvd_delta_ratio_1m", ""),
        "oi_change_5m_pct": (extra or {}).get("oi_change_5m_pct", ""),
        "oi_change_1m_pct": (payload or {}).get("oi_change_1m_pct", ""),
        "oi_change_fast_pct": (payload or {}).get("oi_change_fast_pct", ""),
        "funding_rate": (payload or {}).get("funding_rate", ""),
        "funding_rate_abs": (payload or {}).get("funding_rate_abs", ""),
        "liq_short_count_30s": (payload or {}).get("liq_short_count_30s", ""),
        "liq_short_usd_30s": (payload or {}).get("liq_short_usd_30s", ""),
        "liq_long_count_30s": (payload or {}).get("liq_long_count_30s", ""),
        "liq_long_usd_30s": (payload or {}).get("liq_long_usd_30s", ""),
        "payload_json": json.dumps(payload or {}, ensure_ascii=False),
    }
    if extra:
        row.update(extra)
    try:
        write_event_row(row, strategy="short_pump", mode="live", wall_time_utc=row["wall_time_utc"], schema_version=2)
    except Exception as e:
        log_exception(logger, "SHORT_DATASET_WRITE_ERROR | event", symbol=symbol, run_id=run_id, step="DATASET", extra={"event_id": event_id, "error": str(e)})


def _ds_trade(
    *,
    run_id: str,
    symbol: str,
    trade_id: str,
    event_id: str,
    entry_time_utc: str,
    entry_price: float,
    tp_price: float,
    sl_price: float,
    trade_type: str,
) -> None:
    row = {
        "trade_id": trade_id,
        "event_id": event_id,
        "run_id": run_id,
        "symbol": symbol,
        "strategy": "short_pump",
        "mode": "live",
        "side": "SHORT",
        "entry_time_utc": entry_time_utc,
        "entry_price": entry_price,
        "tp_price": tp_price,
        "sl_price": sl_price,
        "trade_type": trade_type,
    }
    try:
        write_trade_row(row, strategy="short_pump", mode="live", wall_time_utc=entry_time_utc, schema_version=2)
    except Exception as e:
        log_exception(logger, "SHORT_DATASET_WRITE_ERROR | trade", symbol=symbol, run_id=run_id, step="DATASET", extra={"trade_id": trade_id, "error": str(e)})


def _ds_outcome(
    *,
    run_id: str,
    symbol: str,
    trade_id: str,
    event_id: str,
    outcome_time_utc: str,
    outcome: str,
    pnl_pct: float,
    details_json: str,
) -> None:
    row = {
        "trade_id": trade_id,
        "event_id": event_id,
        "run_id": run_id,
        "symbol": symbol,
        "strategy": "short_pump",
        "mode": "live",
        "side": "SHORT",
        "outcome_time_utc": outcome_time_utc,
        "outcome": outcome,
        "pnl_pct": pnl_pct,
        "details_json": details_json,
    }
    try:
        write_outcome_row(row, strategy="short_pump", mode="live", wall_time_utc=outcome_time_utc, schema_version=2)
    except Exception as e:
        log_exception(logger, "SHORT_DATASET_WRITE_ERROR | outcome", symbol=symbol, run_id=run_id, step="DATASET", extra={"trade_id": trade_id, "error": str(e)})


def run_watch_for_symbol(
    symbol: str,
    run_id: Optional[str] = None,
    meta: Optional[Dict[str, Any]] = None,
    cfg: Optional[Config] = None,
) -> Dict[str, Any]:
    cfg = cfg or Config.from_env()
    cfg.symbol = symbol.strip().upper()
    logger = get_logger(__name__, strategy_name="short_pump", symbol=cfg.symbol)
    meta = meta or {}
    run_id = run_id or time.strftime("%Y%m%d_%H%M%S")

    log_info(
        logger,
        "WATCH_START",
        symbol=cfg.symbol,
        run_id=run_id,
        step="WATCH_START",
        extra={
            "meta": meta,
            "entry_mode": cfg.entry_mode,
            "entry_1m_enabled": cfg.entry_mode != "FAST_ONLY",
            "entry_fast_enabled": True,
            "env_entry_mode": os.environ.get("ENTRY_MODE"),
            "cfg_id": id(cfg),
        },
    )
    register_symbol(cfg.symbol)

    log_5m = f"logs/{run_id}_{cfg.symbol}_5m.csv"
    log_1m = f"logs/{run_id}_{cfg.symbol}_1m.csv"
    log_fast = f"logs/{run_id}_{cfg.symbol}_fast.csv"
    log_summary = f"logs/{run_id}_{cfg.symbol}_summary.csv"

    st = StructureState()
    watch_time_utc = wall_time_utc()
    _ds_event(
        run_id=run_id,
        symbol=cfg.symbol,
        event_id=f"{run_id}_watch_start",
        stage=st.stage,
        entry_ok=False,
        skip_reasons="watch_start",
        context_score=None,
        payload={
            "time_utc": meta.get("pump_ts") or watch_time_utc,
            "price": "",
            "pump_pct": meta.get("pump_pct", ""),
            "source": meta.get("source", ""),
        },
        extra=None,
    )
    start_ts = pd.Timestamp.now(tz="UTC")
    end_ts = start_ts + pd.Timedelta(minutes=cfg.watch_minutes)

    pump_start_ts = None
    if meta.get("pump_ts"):
        try:
            pump_start_ts = pd.Timestamp(meta["pump_ts"])
            if pump_start_ts.tzinfo is None:
                pump_start_ts = pump_start_ts.tz_localize("UTC")
            else:
                pump_start_ts = pump_start_ts.tz_convert("UTC")
        except Exception as e:
            log_exception(logger, "Failed to parse pump_ts", symbol=cfg.symbol, run_id=run_id, step="WATCH_START", extra={"pump_ts": meta.get("pump_ts")})
            pump_start_ts = None

    last_5m_wall_write = 0.0
    last_1m_wall_write = 0.0
    last_heartbeat_wall = 0.0

    entry_ok = False
    entry_payload: Dict[str, Any] = {}

    try:
        while pd.Timestamp.now(tz="UTC") < end_ts:
            # =====================
            # 5m CONTEXT LOOP
            # =====================
            try:
                # heartbeat log (once per 60s per run)
                now_wall = time.time()
                if now_wall - last_heartbeat_wall >= 60:
                    cs_val = None
                    try:
                        cs_val = context_score  # may be undefined on first tick
                    except Exception:
                        cs_val = None
                    logger.info(
                        "SHORT_TICK | symbol=%s | run_id=%s | stage=%s | step=HEARTBEAT | context_score=%s",
                        cfg.symbol,
                        run_id,
                        st.stage,
                        cs_val,
                    )
                    last_heartbeat_wall = now_wall

                candles_5m = get_klines_5m(cfg.category, cfg.symbol, limit=250)
                if candles_5m is None or candles_5m.empty:
                    time.sleep(10)
                    continue

                if pump_start_ts is not None:
                    after = candles_5m[candles_5m["ts"] >= pump_start_ts].copy()
                    if after is not None and not after.empty:
                        peak_price = float(after["high"].max())
                    else:
                        peak_price = float(candles_5m["high"].tail(20).max())
                else:
                    peak_price = float(candles_5m["high"].tail(20).max())

                last_price = float(candles_5m.iloc[-1]["close"])
                prev_stage = st.stage
                st = update_structure(cfg, st, last_price, peak_price)
                if st.stage != prev_stage:
                    log_info(logger, f"Stage transition {prev_stage}→{st.stage}", symbol=cfg.symbol, run_id=run_id, stage=st.stage, step="CONTEXT_5M")

                oi = get_open_interest(cfg.category, cfg.symbol, limit=80)
                trades = get_recent_trades(cfg.category, cfg.symbol, limit=1000)
                funding_payload = get_funding_rate(cfg.category, cfg.symbol)
                funding_rate, funding_rate_ts_utc = normalize_funding(funding_payload)
                funding_rate_abs = abs(funding_rate) if funding_rate is not None else None

                # Convert DataFrame to list of dicts for build_dbg5
                candles_5m_list = candles_5m.to_dict("records") if candles_5m is not None and not candles_5m.empty else []
                oi_dict = None
                if oi is not None and not oi.empty:
                    # Pass OI DataFrame for calculation
                    oi_dict = {"oi_df": oi}
                    # Log OI missing warning once per run
                    if not hasattr(run_watch_for_symbol, "_oi_warned"):
                        run_watch_for_symbol._oi_warned = set()
                    if run_id not in run_watch_for_symbol._oi_warned:
                        log_info(logger, "OI data available", symbol=cfg.symbol, run_id=run_id, step="FETCH_5M", extra={"oi_rows": len(oi)})
                        run_watch_for_symbol._oi_warned.add(run_id)
                else:
                    # Log OI missing warning once per run
                    if not hasattr(run_watch_for_symbol, "_oi_warned"):
                        run_watch_for_symbol._oi_warned = set()
                    if run_id not in run_watch_for_symbol._oi_warned:
                        log_warning(logger, "OI missing", symbol=cfg.symbol, run_id=run_id, step="FETCH_5M")
                        run_watch_for_symbol._oi_warned.add(run_id)
                trades_list = trades.to_dict("records") if trades is not None and not trades.empty else []

                dbg5 = build_dbg5(cfg, candles_5m_list, oi_dict, trades_list, st)
                context_score, ctx_parts = compute_context_score_5m(dbg5)

                candle_ts = pd.Timestamp(dbg5["time_utc"])
                if candle_ts.tzinfo is None:
                    candle_ts = candle_ts.tz_localize("UTC")
                lag_sec = (pd.Timestamp.now(tz="UTC") - candle_ts).total_seconds()

                # write 5m log ~ once per 5m candle
                if time.time() - last_5m_wall_write >= 5:
                    append_csv(
                        log_5m,
                        {
                            "run_id": run_id,
                            "symbol": cfg.symbol,
                            "time_utc": dbg5["time_utc"],
                            "stage": dbg5["stage"],
                            "price": dbg5["price"],
                            "peak_price": dbg5["peak_price"],
                            "dist_to_peak_pct": dbg5["dist_to_peak_pct"],
                            "oi_change_5m_pct": dbg5.get("oi_change_5m_pct"),
                            "oi_divergence_5m": dbg5.get("oi_divergence_5m"),
                            "vol_z": dbg5.get("vol_z"),
                            "atr_14_5m_pct": dbg5.get("atr_14_5m_pct"),
                            "funding_rate": funding_rate,
                            "funding_rate_ts_utc": funding_rate_ts_utc,
                            "funding_rate_abs": funding_rate_abs,
                            "context_score": context_score,
                            "wall_time_utc": _utc_now_str(),
                            "candle_lag_sec": lag_sec,
                            "context_parts": json.dumps(ctx_parts, ensure_ascii=False),
                        },
                    )
                    last_5m_wall_write = time.time()

            except Exception as e:
                log_exception(logger, "Error in 5m context loop", symbol=cfg.symbol, run_id=run_id, stage=st.stage, step="FETCH_5M")
                time.sleep(5)
                continue

            # =====================
            # ARM / 1m / FAST
            # =====================
            if st.stage >= 3 and not entry_ok:
                # ARM notify once
                if not st.armed_notified:
                    st.armed_notified = True
                    log_info(logger, "ARMED", symbol=cfg.symbol, run_id=run_id, stage=st.stage, step="ARMED", extra={"context_score": context_score, "dist_to_peak_pct": dbg5.get('dist_to_peak_pct')})
                    _ds_event(
                        run_id=run_id,
                        symbol=cfg.symbol,
                        event_id=f"{run_id}_armed",
                        stage=st.stage,
                        entry_ok=True,
                        skip_reasons="armed",
                        context_score=context_score,
                        payload={
                            "time_utc": dbg5.get("time_utc") or wall_time_utc(),
                            "price": dbg5.get("price") or last_price,
                            "dist_to_peak_pct": dbg5.get("dist_to_peak_pct"),
                            "pump_pct": meta.get("pump_pct", ""),
                            "source": meta.get("source", ""),
                        },
                        extra={"oi_change_5m_pct": dbg5.get("oi_change_5m_pct")},
                    )
                    send_telegram(
                        f"ARMED: {cfg.symbol}\n"
                        f"run_id={run_id}\n"
                        f"stage={st.stage}\n"
                        f"context_score={context_score:.2f}\n"
                        f"dist_to_peak={dbg5.get('dist_to_peak_pct'):.2f}%",
                        strategy="short_pump",
                        side="SHORT",
                        mode="ARMED",
                        event_id=run_id,
                        context_score=context_score,
                        entry_ok=True,
                        skip_reasons=None,
                    )

                # 1m polling (skip in FAST_ONLY)
                if cfg.entry_mode != "FAST_ONLY":
                    candles_1m = get_klines_1m(cfg.category, cfg.symbol, limit=250)
                    if candles_1m is not None and not candles_1m.empty and (time.time() - last_1m_wall_write >= 3):
                        # Get trades and OI for decide_entry_1m
                        trades_1m = get_recent_trades(cfg.category, cfg.symbol, limit=1000)
                        oi_1m = get_open_interest(cfg.category, cfg.symbol, limit=20)  # 1m needs shorter lookback
                        funding_payload = get_funding_rate(cfg.category, cfg.symbol)
                        funding_rate, funding_rate_ts_utc = normalize_funding(funding_payload)
                        funding_rate_abs = abs(funding_rate) if funding_rate is not None else None

                        entry_ok, entry_payload = decide_entry_1m(
                            cfg, candles_1m, trades_1m, oi_1m, context_score, ctx_parts, dbg5.get("peak_price", 0.0)
                        )
                        # Update context_score with CVD if available
                        context_score_with_cvd = entry_payload.get("context_score", context_score)
                        ctx_parts = entry_payload.get("context_parts", ctx_parts)

                        # Liquidation stats
                        now_ts = time.time()
                        liq_short_count_30s, liq_short_usd_30s = get_liq_stats(cfg.symbol, now_ts, 30, side="short")
                        liq_short_count_1m, liq_short_usd_1m = get_liq_stats(cfg.symbol, now_ts, 60, side="short")
                        liq_long_count_30s, liq_long_usd_30s = get_liq_stats(cfg.symbol, now_ts, 30, side="long")
                        liq_long_count_1m, liq_long_usd_1m = get_liq_stats(cfg.symbol, now_ts, 60, side="long")
                        if not hasattr(run_watch_for_symbol, "_liq_sample_warned"):
                            run_watch_for_symbol._liq_sample_warned = set()
                        if run_id not in run_watch_for_symbol._liq_sample_warned:
                            health = get_liq_health()
                            log_info(
                                logger,
                                "LIQ_STATS_SAMPLE",
                                symbol=cfg.symbol,
                                run_id=run_id,
                                stage=st.stage,
                                step="LIQ_STATS",
                                extra={
                                    "liq_short_count_30s": liq_short_count_30s,
                                    "liq_short_usd_30s": liq_short_usd_30s,
                                    "liq_long_count_30s": liq_long_count_30s,
                                    "liq_long_usd_30s": liq_long_usd_30s,
                                    **health,
                                },
                            )
                            run_watch_for_symbol._liq_sample_warned.add(run_id)
                        entry_payload.update({
                            "liq_short_count_30s": liq_short_count_30s,
                            "liq_short_usd_30s": liq_short_usd_30s,
                            "liq_short_count_1m": liq_short_count_1m,
                            "liq_short_usd_1m": liq_short_usd_1m,
                            "liq_long_count_30s": liq_long_count_30s,
                            "liq_long_usd_30s": liq_long_usd_30s,
                            "liq_long_count_1m": liq_long_count_1m,
                            "liq_long_usd_1m": liq_long_usd_1m,
                            "funding_rate": funding_rate,
                            "funding_rate_ts_utc": funding_rate_ts_utc,
                            "funding_rate_abs": funding_rate_abs,
                        })

                        append_csv(
                            log_1m,
                            {
                                "run_id": run_id,
                                "symbol": cfg.symbol,
                                "time_utc": str(candles_1m.iloc[-1]["ts"]),
                                "price": float(candles_1m.iloc[-1]["close"]),
                                "entry_ok": bool(entry_ok),
                                "oi_change_1m_pct": entry_payload.get("oi_change_1m_pct"),
                                "cvd_delta_ratio_30s": entry_payload.get("cvd_delta_ratio_30s"),
                                "cvd_delta_ratio_1m": entry_payload.get("cvd_delta_ratio_1m"),
                                "cvd_part": entry_payload.get("cvd_part"),
                                "funding_rate": funding_rate,
                                "funding_rate_ts_utc": funding_rate_ts_utc,
                                "funding_rate_abs": funding_rate_abs,
                                "liq_short_count_30s": liq_short_count_30s,
                                "liq_short_usd_30s": liq_short_usd_30s,
                                "liq_short_count_1m": liq_short_count_1m,
                                "liq_short_usd_1m": liq_short_usd_1m,
                                "liq_long_count_30s": liq_long_count_30s,
                                "liq_long_usd_30s": liq_long_usd_30s,
                                "liq_long_count_1m": liq_long_count_1m,
                                "liq_long_usd_1m": liq_long_usd_1m,
                                "entry_payload": json.dumps(entry_payload, ensure_ascii=False),
                            },
                        )
                        last_1m_wall_write = time.time()

                # fast polling inside ARMED
                try:
                    trades_fast = get_recent_trades(cfg.category, cfg.symbol, limit=1000)
                    oi_fast = get_open_interest(cfg.category, cfg.symbol, limit=20)  # Fast needs shorter lookback
                    funding_payload = get_funding_rate(cfg.category, cfg.symbol)
                    funding_rate, funding_rate_ts_utc = normalize_funding(funding_payload)
                    funding_rate_abs = abs(funding_rate) if funding_rate is not None else None
                    ok_fast, payload_fast = decide_entry_fast(
                        cfg, trades_fast, oi_fast, context_score, ctx_parts, dbg5.get("peak_price", 0.0)
                    )
                    # Update context_score with CVD if available
                    context_score_with_cvd = payload_fast.get("context_score", context_score)
                    ctx_parts = payload_fast.get("context_parts", ctx_parts)

                    # Liquidation stats
                    now_ts = time.time()
                    liq_short_count_30s, liq_short_usd_30s = get_liq_stats(cfg.symbol, now_ts, 30, side="short")
                    liq_short_count_1m, liq_short_usd_1m = get_liq_stats(cfg.symbol, now_ts, 60, side="short")
                    liq_long_count_30s, liq_long_usd_30s = get_liq_stats(cfg.symbol, now_ts, 30, side="long")
                    liq_long_count_1m, liq_long_usd_1m = get_liq_stats(cfg.symbol, now_ts, 60, side="long")
                    if not hasattr(run_watch_for_symbol, "_liq_sample_warned"):
                        run_watch_for_symbol._liq_sample_warned = set()
                    if run_id not in run_watch_for_symbol._liq_sample_warned:
                        health = get_liq_health()
                        log_info(
                            logger,
                            "LIQ_STATS_SAMPLE",
                            symbol=cfg.symbol,
                            run_id=run_id,
                            stage=st.stage,
                            step="LIQ_STATS",
                            extra={
                                "liq_short_count_30s": liq_short_count_30s,
                                "liq_short_usd_30s": liq_short_usd_30s,
                                "liq_long_count_30s": liq_long_count_30s,
                                "liq_long_usd_30s": liq_long_usd_30s,
                                **health,
                            },
                        )
                        run_watch_for_symbol._liq_sample_warned.add(run_id)
                    payload_fast.update({
                        "liq_short_count_30s": liq_short_count_30s,
                        "liq_short_usd_30s": liq_short_usd_30s,
                        "liq_short_count_1m": liq_short_count_1m,
                        "liq_short_usd_1m": liq_short_usd_1m,
                        "liq_long_count_30s": liq_long_count_30s,
                        "liq_long_usd_30s": liq_long_usd_30s,
                        "liq_long_count_1m": liq_long_count_1m,
                        "liq_long_usd_1m": liq_long_usd_1m,
                        "funding_rate": funding_rate,
                        "funding_rate_ts_utc": funding_rate_ts_utc,
                        "funding_rate_abs": funding_rate_abs,
                    })
                    append_csv(
                        log_fast,
                        {
                            "run_id": run_id,
                            "symbol": cfg.symbol,
                            "wall_time_utc": _utc_now_str(),
                            "entry_ok": bool(ok_fast),
                            "oi_change_fast_pct": payload_fast.get("oi_change_fast_pct"),
                            "cvd_delta_ratio_30s": payload_fast.get("cvd_delta_ratio_30s"),
                            "cvd_delta_ratio_1m": payload_fast.get("cvd_delta_ratio_1m"),
                            "cvd_part": payload_fast.get("cvd_part"),
                            "funding_rate": funding_rate,
                            "funding_rate_ts_utc": funding_rate_ts_utc,
                            "funding_rate_abs": funding_rate_abs,
                            "liq_short_count_30s": liq_short_count_30s,
                            "liq_short_usd_30s": liq_short_usd_30s,
                            "liq_short_count_1m": liq_short_count_1m,
                            "liq_short_usd_1m": liq_short_usd_1m,
                            "liq_long_count_30s": liq_long_count_30s,
                            "liq_long_usd_30s": liq_long_usd_30s,
                            "liq_long_count_1m": liq_long_count_1m,
                            "liq_long_usd_1m": liq_long_usd_1m,
                            "entry_payload": json.dumps(payload_fast, ensure_ascii=False),
                        },
                    )
                    if ok_fast:
                        if cfg.entry_mode == "FAST_ONLY" and payload_fast.get("entry_source") != "fast":
                            log_warning(
                                logger,
                                "FAST_ONLY: non-fast entry suppressed",
                                symbol=cfg.symbol,
                                run_id=run_id,
                                stage=st.stage,
                                step="FAST",
                                extra={"entry_source": payload_fast.get("entry_source")},
                            )
                        else:
                            entry_ok = True
                            entry_payload = payload_fast
                except Exception as e:
                    log_exception(logger, "Error in fast polling", symbol=cfg.symbol, run_id=run_id, stage=st.stage, step="FAST")

            # =====================
            # ENTRY OK → OUTCOME
            # =====================
            if entry_ok:
                entry_type = entry_payload.get("entry_type", "UNKNOWN")
                if cfg.entry_mode == "FAST_ONLY" and entry_type != "FAST":
                    log_warning(
                        logger,
                        "FAST_ONLY: ENTRY_OK suppressed due to non-FAST entry_type",
                        symbol=cfg.symbol,
                        run_id=run_id,
                        stage=st.stage,
                        step="ENTRY_DECISION",
                        extra={"entry_type": entry_type, "entry_source": entry_payload.get("entry_source")},
                    )
                    entry_ok = False
                    entry_payload = {}
                    continue
                log_info(logger, "ENTRY_OK", symbol=cfg.symbol, run_id=run_id, stage=st.stage, step="ENTRY_DECISION", extra={"entry_type": entry_type, "entry_payload": entry_payload})
                # Extract entry parameters from entry_payload
                entry_price = float(entry_payload.get("price", 0.0))
                entry_source = entry_payload.get("entry_source", "unknown")
                entry_type = entry_payload.get("entry_type", "unknown")
                entry_ts_str = entry_payload.get("time_utc", "")
                try:
                    entry_ts_utc = pd.Timestamp(entry_ts_str)
                    if entry_ts_utc.tzinfo is None:
                        entry_ts_utc = entry_ts_utc.tz_localize("UTC")
                    else:
                        entry_ts_utc = entry_ts_utc.tz_convert("UTC")
                except Exception as e:
                    log_exception(logger, "Failed to parse entry_ts_utc, using now()", symbol=cfg.symbol, run_id=run_id, stage=st.stage, step="ENTRY_DECISION", extra={"time_utc": entry_ts_str})
                    entry_ts_utc = pd.Timestamp.now(tz="UTC")
                
                # Calculate TP/SL from config based on entry_type
                if entry_type == "CONFIRM":
                    tp_price = entry_price * (1.0 - cfg.tp_pct_confirm)
                    sl_price = entry_price * (1.0 + cfg.sl_pct_confirm)
                elif entry_type == "EARLY":
                    tp_price = entry_price * (1.0 - cfg.tp_pct_early)
                    sl_price = entry_price * (1.0 + cfg.sl_pct_early)
                else:  # FAST or unknown
                    tp_price = entry_price * (1.0 - cfg.tp_pct_confirm)
                    sl_price = entry_price * (1.0 + cfg.sl_pct_confirm)

                entry_payload.update(
                    {
                        "paper_entry_time_utc": entry_ts_utc.isoformat(),
                        "paper_entry_price": entry_price,
                        "paper_tp_price": tp_price,
                        "paper_sl_price": sl_price,
                        "trade_type": "PAPER" if os.getenv("PAPER_MODE", "1") == "1" else "LIVE",
                    }
                )
                entry_snapshot = {
                    "entry_ts_utc": entry_ts_utc.isoformat(),
                    "entry_price": entry_price,
                    "tp_pct": float((entry_price - tp_price) / entry_price * 100.0) if entry_price > 0 else 0.0,
                    "sl_pct": float((sl_price - entry_price) / entry_price * 100.0) if entry_price > 0 else 0.0,
                    "tp_price": tp_price,
                    "sl_price": sl_price,
                    "context_score": entry_payload.get("context_score", context_score),
                    "context_parts": entry_payload.get("context_parts", ctx_parts),
                    "stage": st.stage,
                    "entry_mode": cfg.entry_mode,
                }
                try:
                    now_ts = time.time()
                    entry_snapshot.update(
                        {
                            "liq_short_count_30s": get_liq_stats(cfg.symbol, now_ts, 30, side="short")[0],
                            "liq_short_usd_30s": get_liq_stats(cfg.symbol, now_ts, 30, side="short")[1],
                            "liq_short_count_60s": get_liq_stats(cfg.symbol, now_ts, 60, side="short")[0],
                            "liq_short_usd_60s": get_liq_stats(cfg.symbol, now_ts, 60, side="short")[1],
                            "liq_short_count_5m": get_liq_stats(cfg.symbol, now_ts, 300, side="short")[0],
                            "liq_short_usd_5m": get_liq_stats(cfg.symbol, now_ts, 300, side="short")[1],
                            "liq_long_count_30s": get_liq_stats(cfg.symbol, now_ts, 30, side="long")[0],
                            "liq_long_usd_30s": get_liq_stats(cfg.symbol, now_ts, 30, side="long")[1],
                            "liq_long_count_60s": get_liq_stats(cfg.symbol, now_ts, 60, side="long")[0],
                            "liq_long_usd_60s": get_liq_stats(cfg.symbol, now_ts, 60, side="long")[1],
                            "liq_long_count_5m": get_liq_stats(cfg.symbol, now_ts, 300, side="long")[0],
                            "liq_long_usd_5m": get_liq_stats(cfg.symbol, now_ts, 300, side="long")[1],
                        }
                    )
                except Exception:
                    pass

                log_info(
                    logger,
                    "OUTCOME_WATCH_START",
                    symbol=cfg.symbol,
                    run_id=run_id,
                    stage=st.stage,
                    step="OUTCOME",
                    extra={
                        "entry_price": entry_price,
                        "tp_price": tp_price,
                        "sl_price": sl_price,
                        "watch_minutes": cfg.outcome_watch_minutes,
                        "poll_seconds": cfg.outcome_poll_seconds,
                    },
                )
                event_id = entry_payload.get("event_id") or f"{run_id}_entry_{entry_source}"
                trade_id = f"{event_id}_trade"
                _ds_event(
                    run_id=run_id,
                    symbol=cfg.symbol,
                    event_id=str(event_id),
                    stage=st.stage,
                    entry_ok=True,
                    skip_reasons=f"entry_ok_{entry_source}",
                    context_score=entry_payload.get("context_score", context_score),
                    payload=entry_payload,
                    extra=None,
                )
                _ds_trade(
                    run_id=run_id,
                    symbol=cfg.symbol,
                    trade_id=str(trade_id),
                    event_id=str(event_id),
                    entry_time_utc=entry_ts_utc.isoformat(),
                    entry_price=entry_price,
                    tp_price=tp_price,
                    sl_price=sl_price,
                    trade_type=entry_payload.get("trade_type", ""),
                )

                try:
                    entry_source = entry_payload.get("entry_source", "unknown")
                    mode = "FAST" if entry_source == "fast" else "ARMED"
                    context_score_msg = entry_payload.get("context_score", context_score)
                    send_telegram(
                        f"ENTRY OK ({entry_type}): {cfg.symbol}\n"
                        f"run_id={run_id}\n"
                        f"paper_entry={entry_price} tp={tp_price} sl={sl_price}\n"
                        f"{json.dumps(entry_payload, ensure_ascii=False)}",
                        strategy="short_pump",
                        side="SHORT",
                        mode=mode,
                        event_id=str(event_id),
                        context_score=float(context_score_msg) if context_score_msg is not None else None,
                        entry_ok=True,
                        skip_reasons=None,
                    )
                except Exception as e:
                    log_exception(logger, "TELEGRAM_SEND failed for ENTRY_OK", symbol=cfg.symbol, run_id=run_id, stage=st.stage, step="TELEGRAM_SEND")
                
                try:
                    summary = track_outcome_short(
                        cfg=cfg,
                        entry_ts_utc=entry_ts_utc,
                        entry_price=entry_price,
                        tp_price=tp_price,
                        sl_price=sl_price,
                        entry_source=entry_source,
                        entry_type=entry_type,
                        run_id=run_id,
                        symbol=cfg.symbol,
                    )
                    summary["funding_rate"] = entry_payload.get("funding_rate")
                    summary["funding_rate_ts_utc"] = entry_payload.get("funding_rate_ts_utc")
                    summary["funding_rate_abs"] = entry_payload.get("funding_rate_abs")
                    summary["entry_mode"] = cfg.entry_mode
                    summary["context_score"] = entry_snapshot.get("context_score")
                    summary["context_parts"] = json.dumps(entry_snapshot.get("context_parts"), ensure_ascii=False)
                    summary["entry_snapshot"] = json.dumps(entry_snapshot, ensure_ascii=False)
                    try:
                        exit_ts = pd.Timestamp(summary.get("exit_time_utc") or summary.get("hit_time_utc") or entry_ts_utc)
                        if exit_ts.tzinfo is None:
                            exit_ts = exit_ts.tz_localize("UTC")
                        hold_seconds = (exit_ts - entry_ts_utc).total_seconds()
                    except Exception:
                        hold_seconds = 0.0
                    summary["hold_seconds"] = hold_seconds
                    log_info(logger, "OUTCOME", symbol=cfg.symbol, run_id=run_id, stage=st.stage, step="OUTCOME", extra={"outcome": summary.get("outcome"), "end_reason": summary.get("end_reason")})
                    log_info(
                        logger,
                        "OUTCOME_DONE",
                        symbol=cfg.symbol,
                        run_id=run_id,
                        stage=st.stage,
                        step="OUTCOME",
                        extra={
                            "outcome_type": summary.get("end_reason"),
                            "hold_seconds": hold_seconds,
                            "pnl_pct": summary.get("pnl_pct"),
                            "mae_pct": summary.get("mae_pct"),
                            "mfe_pct": summary.get("mfe_pct"),
                        },
                    )
                    outcome_time_utc = summary.get("exit_time_utc") or summary.get("hit_time_utc") or wall_time_utc()
                    outcome_row = build_outcome_row(
                        summary,
                        trade_id=str(trade_id),
                        event_id=str(event_id),
                        run_id=run_id,
                        symbol=cfg.symbol,
                        strategy="short_pump",
                        mode="live",
                        side="SHORT",
                        outcome_time_utc=outcome_time_utc,
                        entry_snapshot=entry_snapshot,
                        extra_details={
                            "entry_mode": cfg.entry_mode,
                            "context_score": entry_snapshot.get("context_score"),
                            "outcome_time_utc": summary.get("exit_time_utc") or summary.get("hit_time_utc"),
                        },
                    )
                    if outcome_row is not None:
                        write_outcome_row(
                            outcome_row,
                            strategy="short_pump",
                            mode="live",
                            wall_time_utc=outcome_time_utc,
                            schema_version=2,
                        )
                    try:
                        append_csv(log_summary, summary)
                    except Exception as e:
                        log_exception(logger, "CSV_WRITE failed for summary", symbol=cfg.symbol, run_id=run_id, stage=st.stage, step="CSV_WRITE", extra={"log_file": log_summary})

                    if TG_SEND_OUTCOME:
                        try:
                            event_id = entry_payload.get("event_id") or run_id
                            context_score_msg = entry_payload.get("context_score", context_score)
                            send_telegram(
                                f"OUTCOME: {cfg.symbol}\n"
                                f"run_id={run_id}\n"
                                f"{summary.get('outcome')} | {summary.get('end_reason')}\n"
                                f"pnl_pct={summary.get('pnl_pct')} entry={summary.get('entry_price')} exit={summary.get('exit_price')}",
                                strategy="short_pump",
                                side="SHORT",
                                mode="FAST" if entry_payload.get("entry_source") == "fast" else "ARMED",
                                event_id=str(event_id),
                                context_score=float(context_score_msg) if context_score_msg is not None else None,
                                entry_ok=True,
                                skip_reasons=None,
                            )
                        except Exception as e:
                            log_exception(logger, "TELEGRAM_SEND failed for OUTCOME", symbol=cfg.symbol, run_id=run_id, stage=st.stage, step="TELEGRAM_SEND")

                    return summary
                except Exception as e:
                    log_exception(logger, "Error in track_outcome_short", symbol=cfg.symbol, run_id=run_id, stage=st.stage, step="OUTCOME")
                    raise

            time.sleep(cfg.poll_seconds)

        # timeout
        summary = {
            "run_id": run_id,
            "symbol": cfg.symbol,
            "end_reason": f"TIMEOUT_STAGE_{st.stage}",
        }
        log_info(logger, "TIMEOUT", symbol=cfg.symbol, run_id=run_id, stage=st.stage, step="TIMEOUT")
        append_csv(log_summary, summary)
        return summary

    except KeyboardInterrupt:
        summary = {
            "run_id": run_id,
            "symbol": cfg.symbol,
            "end_reason": "INTERRUPTED",
        }
        log_info(logger, "INTERRUPTED", symbol=cfg.symbol, run_id=run_id, stage=st.stage, step="INTERRUPTED")
        append_csv(log_summary, summary)
        return summary
    except Exception as e:
        log_exception(logger, "Fatal error in run_watch_for_symbol", symbol=cfg.symbol, run_id=run_id, stage=st.stage, step="FATAL")
        raise