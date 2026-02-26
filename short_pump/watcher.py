# short_pump/watcher.py
from __future__ import annotations

import json
import math
import os
import time
from datetime import datetime, timezone
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
from short_pump.liquidations import (
    get_liq_debug_state,
    get_liq_health,
    get_liq_stats,
    register_symbol,
    unregister_symbol,
)
from short_pump.logging_utils import get_logger, log_exception, log_info, log_warning
from common.outcome_tracker import build_outcome_row
from short_pump.outcome import track_outcome_short
from short_pump.telegram import TG_ARMED_ENABLE, TG_SEND_OUTCOME, is_tradeable_short_pump, send_telegram, tg_entry_filter
from notifications.tg_format import build_short_pump_signal, format_armed_short, format_entry_ok, format_outcome, format_tg
from common.io_dataset import write_event_row, write_outcome_row, write_trade_row
from common.runtime import wall_time_utc

logger = get_logger(__name__)


def _utc_now_str() -> str:
    return pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%d %H:%M:%S%z")


def _dist_to_peak_pct(peak_price: float | None, current_price: float | None) -> float:
    try:
        peak = float(peak_price or 0.0)
        price = float(current_price or 0.0)
    except (TypeError, ValueError):
        return 0.0
    if peak <= 0:
        return 0.0
    return max(0.0, (peak - price) / peak * 100.0)


def _sanitize_dist_to_peak(val: object) -> float:
    try:
        v = float(val)
    except (TypeError, ValueError):
        return 0.0
    if not math.isfinite(v):
        return 0.0
    return v


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
    pl = payload or {}
    snap = run_watch_for_symbol._last_liq_snapshot.get(symbol, {}) if hasattr(run_watch_for_symbol, "_last_liq_snapshot") else {}

    def _liq_val(key: str):  # use payload if present and non-empty, else snapshot, else 0
        v = pl.get(key)
        if v is not None and v != "":
            return v
        return snap.get(key, 0)

    row = {
        "run_id": run_id,
        "event_id": event_id,
        "symbol": symbol,
        "strategy": "short_pump",
        "mode": "live",
        "side": "SHORT",
        "wall_time_utc": wall_time_utc(),
        "time_utc": pl.get("time_utc", ""),
        "stage": stage,
        "entry_ok": int(bool(entry_ok)),
        "skip_reasons": skip_reasons,
        "context_score": context_score if context_score is not None else "",
        "price": pl.get("price", ""),
        "dist_to_peak_pct": float(pl.get("dist_to_peak_pct") or 0.0),
        "cvd_delta_ratio_30s": pl.get("cvd_delta_ratio_30s", ""),
        "cvd_delta_ratio_1m": pl.get("cvd_delta_ratio_1m", ""),
        "oi_change_5m_pct": (extra or {}).get("oi_change_5m_pct", ""),
        "oi_change_1m_pct": pl.get("oi_change_1m_pct", ""),
        "oi_change_fast_pct": pl.get("oi_change_fast_pct", ""),
        "funding_rate": pl.get("funding_rate", ""),
        "funding_rate_abs": pl.get("funding_rate_abs", ""),
        "liq_short_count_30s": _liq_val("liq_short_count_30s"),
        "liq_short_usd_30s": _liq_val("liq_short_usd_30s"),
        "liq_long_count_30s": _liq_val("liq_long_count_30s"),
        "liq_long_usd_30s": _liq_val("liq_long_usd_30s"),
        "liq_short_count_1m": _liq_val("liq_short_count_1m"),
        "liq_short_usd_1m": _liq_val("liq_short_usd_1m"),
        "liq_long_count_1m": _liq_val("liq_long_count_1m"),
        "liq_long_usd_1m": _liq_val("liq_long_usd_1m"),
        "outcome_label": (payload or {}).get("outcome_label", ""),
        "payload_json": json.dumps(payload or {}, ensure_ascii=False),
    }
    if extra:
        row.update(extra)
    # DIAG: values we are about to write to events CSV (why liq_* often 0)
    logger.info(
        "LIQ_STATS_BEFORE_CSV",
        extra={
            "event_id": event_id,
            "run_id": run_id,
            "symbol": symbol,
            "liq_short_count_30s": row.get("liq_short_count_30s"),
            "liq_short_usd_30s": row.get("liq_short_usd_30s"),
            "liq_long_count_30s": row.get("liq_long_count_30s"),
            "liq_long_usd_30s": row.get("liq_long_usd_30s"),
            "liq_short_count_1m": row.get("liq_short_count_1m"),
            "liq_short_usd_1m": row.get("liq_short_usd_1m"),
            "liq_long_count_1m": row.get("liq_long_count_1m"),
            "liq_long_usd_1m": row.get("liq_long_usd_1m"),
        },
    )
    try:
        write_event_row(row, strategy="short_pump", mode="live", wall_time_utc=row["wall_time_utc"], schema_version=3)
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
        write_trade_row(row, strategy="short_pump", mode="live", wall_time_utc=entry_time_utc, schema_version=3)
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
        write_outcome_row(row, strategy="short_pump", mode="live", wall_time_utc=outcome_time_utc, schema_version=3)
    except Exception as e:
        log_exception(logger, "SHORT_DATASET_WRITE_ERROR | outcome", symbol=symbol, run_id=run_id, step="DATASET", extra={"trade_id": trade_id, "error": str(e)})


def run_watch_for_symbol(
    symbol: str,
    run_id: Optional[str] = None,
    meta: Optional[Dict[str, Any]] = None,
    cfg: Optional[Config] = None,
) -> Dict[str, Any]:
    import sys

    import short_pump.liquidations as liq_mod

    cfg = cfg or Config.from_env()
    cfg.symbol = symbol.strip().upper()
    logger = get_logger(__name__, strategy_name="short_pump", symbol=cfg.symbol)
    meta = meta or {}
    run_id = run_id or time.strftime("%Y%m%d_%H%M%S")
    diag_key = (run_id, cfg.symbol)
    if not hasattr(run_watch_for_symbol, "_diag_liq_done"):
        run_watch_for_symbol._diag_liq_done = set()
    if diag_key not in run_watch_for_symbol._diag_liq_done:
        run_watch_for_symbol._diag_liq_done.add(diag_key)
        sys_keys = [k for k in sys.modules.keys() if "liquidations" in k]
        logger.info(
            "WATCHER_SEES_LIQ_MODULE_LOG | pid=%s | liq_file=%s | liq_name=%s | liq_obj_id=%s | sys_keys=%s",
            os.getpid(),
            liq_mod.__file__,
            liq_mod.__name__,
            id(liq_mod),
            sys_keys,
        )

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
    log_info(logger, "REGISTER_SYMBOL_WILL_CALL", symbol=cfg.symbol, run_id=run_id, step="LIQ_WS")
    register_symbol(cfg.symbol)
    log_info(logger, "REGISTER_SYMBOL_DID_CALL", symbol=cfg.symbol, run_id=run_id, step="LIQ_WS")
    def _cleanup_symbol() -> None:
        try:
            unregister_symbol(cfg.symbol)
        except Exception:
            log_exception(logger, "Failed to unregister symbol", symbol=cfg.symbol, run_id=run_id, step="LIQ_WS")

    st = None
    log_5m = f"logs/{run_id}_{cfg.symbol}_5m.csv"
    log_1m = f"logs/{run_id}_{cfg.symbol}_1m.csv"
    log_fast = f"logs/{run_id}_{cfg.symbol}_fast.csv"
    log_summary = f"logs/{run_id}_{cfg.symbol}_summary.csv"

    st = StructureState()
    if os.getenv("FORCE_ARMED") == "1":
        st.stage = 4
        log_info(logger, "TEST_FORCE_ARMED", symbol=cfg.symbol, run_id=run_id, step="WATCH_START")
    watch_time_utc = wall_time_utc()
    _ds_event(
        run_id=run_id,
        symbol=cfg.symbol,
        event_id=f"{run_id}_watch_start",
        stage=st.stage,
        entry_ok=False,
        skip_reasons="watch_start",
        context_score=0.0,
        payload={
            "time_utc": meta.get("pump_ts") or watch_time_utc,
            "price": 0.0,
            "dist_to_peak_pct": _sanitize_dist_to_peak(meta.get("dist_to_peak_pct")),
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
    if not hasattr(run_watch_for_symbol, "_last_liq_log_ts"):
        run_watch_for_symbol._last_liq_log_ts = {}
    if not hasattr(run_watch_for_symbol, "_last_liq_state_log_ts"):
        run_watch_for_symbol._last_liq_state_log_ts = {}
    if not hasattr(run_watch_for_symbol, "_last_liq_nonzero_log_ts"):
        run_watch_for_symbol._last_liq_nonzero_log_ts = {}
    if not hasattr(run_watch_for_symbol, "_diag_ping_done"):
        run_watch_for_symbol._diag_ping_done = set()
    if not hasattr(run_watch_for_symbol, "_last_liq_snapshot"):
        run_watch_for_symbol._last_liq_snapshot = {}
    if not hasattr(run_watch_for_symbol, "_last_liq_snap_ts"):
        run_watch_for_symbol._last_liq_snap_ts = {}
    if not hasattr(run_watch_for_symbol, "_outcome_emitted"):
        run_watch_for_symbol._outcome_emitted = set()

    entry_ok = False
    entry_payload: Dict[str, Any] = {}
    entry_candidate_active = False
    entry_candidate_snapshot: Dict[str, Any] = {}
    entry_candidate_set_ts = 0.0
    entry_candidate_expires_ts = 0.0
    entry_candidate_source = ""

    def _context_filter_pass(payload: Dict[str, Any], current_funding_abs: float | None, base_context_score: float) -> tuple[bool, Dict[str, Any]]:
        flags = payload.get("flags") if isinstance(payload.get("flags"), dict) else {}
        dist_pct = float(payload.get("dist_to_peak_pct") or 0.0)
        near_top = bool(flags.get("near_top")) if "near_top" in flags else (dist_pct <= cfg.dist_to_peak_max_pct * 100.0)
        oi_val = payload.get("oi_change_fast_pct")
        if oi_val is None:
            oi_val = payload.get("oi_change_1m_pct")
        context_score_val = float(payload.get("context_score", base_context_score) or 0.0)
        oi_ok = True
        if cfg.entry_context_oi_change_max_pct is not None and oi_val is not None:
            oi_ok = float(oi_val) <= float(cfg.entry_context_oi_change_max_pct)
        funding_ok = True
        if cfg.entry_context_funding_abs_max is not None and current_funding_abs is not None:
            funding_ok = float(current_funding_abs) <= float(cfg.entry_context_funding_abs_max)
        context_ok = (
            st.stage >= 3
            and near_top
            and oi_ok
            and funding_ok
            and context_score_val >= float(cfg.entry_context_min_score)
        )
        return context_ok, {
            "near_top": near_top,
            "dist_to_peak_pct": dist_pct,
            "context_score": context_score_val,
            "oi_ok": oi_ok,
            "funding_ok": funding_ok,
        }

    def _trigger_pass(payload: Dict[str, Any]) -> tuple[bool, Dict[str, Any]]:
        flags = payload.get("flags") if isinstance(payload.get("flags"), dict) else {}
        entry_source = str(payload.get("entry_source") or "")
        delta_ok = bool(flags.get("delta_ok"))
        break_low = bool(flags.get("break_low"))
        if entry_source == "1m":
            trigger_ok = break_low and delta_ok
        else:
            trigger_ok = delta_ok
        return trigger_ok, {"entry_source": entry_source, "delta_ok": delta_ok, "break_low": break_low}

    def _set_candidate(payload: Dict[str, Any], now_ts: float, source: str, context_meta: Dict[str, Any]) -> None:
        nonlocal entry_candidate_active, entry_candidate_snapshot, entry_candidate_set_ts, entry_candidate_expires_ts, entry_candidate_source
        ttl = max(1.0, float(cfg.entry_candidate_ttl_seconds))
        entry_candidate_active = True
        entry_candidate_set_ts = now_ts
        entry_candidate_expires_ts = now_ts + ttl
        entry_candidate_source = source
        entry_candidate_snapshot = {
            "candidate_state": "ENTRY_CANDIDATE",
            "candidate_set_ts": now_ts,
            "candidate_ttl_seconds": ttl,
            "candidate_expires_ts": entry_candidate_expires_ts,
            "source": source,
            "stage": st.stage,
            "time_utc": payload.get("time_utc"),
            "price": payload.get("price"),
            "dist_to_peak_pct": payload.get("dist_to_peak_pct"),
            "context_score": payload.get("context_score", context_score),
            "oi_change_1m_pct": payload.get("oi_change_1m_pct"),
            "oi_change_fast_pct": payload.get("oi_change_fast_pct"),
            "funding_rate_abs": payload.get("funding_rate_abs"),
            "liq_short_count_30s": payload.get("liq_short_count_30s", 0),
            "liq_short_usd_30s": payload.get("liq_short_usd_30s", 0.0),
            "liq_long_count_30s": payload.get("liq_long_count_30s", 0),
            "liq_long_usd_30s": payload.get("liq_long_usd_30s", 0.0),
            "liq_short_count_1m": payload.get("liq_short_count_1m", 0),
            "liq_short_usd_1m": payload.get("liq_short_usd_1m", 0.0),
            "liq_long_count_1m": payload.get("liq_long_count_1m", 0),
            "liq_long_usd_1m": payload.get("liq_long_usd_1m", 0.0),
            "flags": payload.get("flags"),
            "context_meta": context_meta,
        }
        log_info(
            logger,
            "ENTRY_CANDIDATE_SET",
            symbol=cfg.symbol,
            run_id=run_id,
            stage=st.stage,
            step="ENTRY_CANDIDATE",
            extra={
                "ttl_seconds": ttl,
                "source": source,
                "context_score": entry_candidate_snapshot.get("context_score"),
                "dist_to_peak_pct": entry_candidate_snapshot.get("dist_to_peak_pct"),
                "liq_long_usd_30s": entry_candidate_snapshot.get("liq_long_usd_30s"),
                "liq_short_usd_30s": entry_candidate_snapshot.get("liq_short_usd_30s"),
            },
        )

    def _apply_liq_adjustments(payload: Dict[str, Any], base_score: float) -> tuple[float, bool, str]:
        long_usd_30s = float(payload.get("liq_long_usd_30s") or 0.0)
        short_usd_30s = float(payload.get("liq_short_usd_30s") or 0.0)
        adj_score = float(base_score)
        veto = False
        veto_reason = ""
        if cfg.entry_liq_long_bonus_usd_30s is not None and long_usd_30s >= float(cfg.entry_liq_long_bonus_usd_30s):
            adj_score += float(cfg.entry_liq_long_bonus_score)
        if cfg.entry_liq_short_penalty_usd_30s is not None and short_usd_30s >= float(cfg.entry_liq_short_penalty_usd_30s):
            adj_score -= float(cfg.entry_liq_short_penalty_score)
        if cfg.entry_liq_short_veto_usd_30s is not None and short_usd_30s >= float(cfg.entry_liq_short_veto_usd_30s):
            veto = True
            veto_reason = "liq_short_veto"
        return max(0.0, min(1.0, adj_score)), veto, veto_reason

    try:
        while pd.Timestamp.now(tz="UTC") < end_ts:
            log_info(
                logger,
                "WATCHER_LOOP_ALIVE",
                symbol=cfg.symbol,
                run_id=run_id,
                step="WATCHER",
            )
            diag_key = (run_id, cfg.symbol)
            if diag_key not in run_watch_for_symbol._diag_ping_done:
                run_watch_for_symbol._diag_ping_done.add(diag_key)
                logger.info(
                    "LIQ_STATE_PING | symbol=%s | run_id=%s | step=DIAG | pid=%s | watcher_file=%s",
                    cfg.symbol,
                    run_id,
                    os.getpid(),
                    __file__,
                )
                dbg = get_liq_debug_state(cfg.symbol)
                logger.info(
                    "LIQ_STATE_DUMP | symbol=%s | run_id=%s | step=DIAG | pid=%s | rx_events_total=%s | last_event_ts_ms=%s | last_symbol=%s | buffer_short_len=%s | buffer_long_len=%s",
                    cfg.symbol,
                    run_id,
                    os.getpid(),
                    dbg.get("rx_events_total"),
                    dbg.get("last_event_ts_ms"),
                    dbg.get("last_symbol"),
                    dbg.get("buffer_short_len"),
                    dbg.get("buffer_long_len"),
                )
            now_wall = time.time()
            last_state_log_ts = run_watch_for_symbol._last_liq_state_log_ts.get(cfg.symbol, 0.0)
            if now_wall - last_state_log_ts >= 60:
                stage = st.stage if st is not None else None
                dbg = get_liq_debug_state(cfg.symbol)
                extra_safe = {k: v for k, v in dbg.items() if k != "symbol"}
                log_info(
                    logger,
                    "LIQ_STATE_FROM_WATCHER",
                    symbol=cfg.symbol,
                    run_id=run_id,
                    stage=stage,
                    step="LIQ_STATS",
                    extra=extra_safe,
                )
                run_watch_for_symbol._last_liq_state_log_ts[cfg.symbol] = now_wall
            # Liq snapshot for CSV (every 15s per symbol)
            if now_wall - run_watch_for_symbol._last_liq_snap_ts.get(cfg.symbol, 0) >= 15:
                run_watch_for_symbol._last_liq_snap_ts[cfg.symbol] = now_wall
                sc, su30 = get_liq_stats(cfg.symbol, now_wall, 30, side="short")
                sc60, su60 = get_liq_stats(cfg.symbol, now_wall, 60, side="short")
                lc, lu30 = get_liq_stats(cfg.symbol, now_wall, 30, side="long")
                lc60, lu60 = get_liq_stats(cfg.symbol, now_wall, 60, side="long")
                run_watch_for_symbol._last_liq_snapshot[cfg.symbol] = {
                    "liq_short_count_30s": sc,
                    "liq_short_usd_30s": su30,
                    "liq_long_count_30s": lc,
                    "liq_long_usd_30s": lu30,
                    "liq_short_count_1m": sc60,
                    "liq_short_usd_1m": su60,
                    "liq_long_count_1m": lc60,
                    "liq_long_usd_1m": lu60,
                    "liq_snap_ts": now_wall,
                }
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
                    armed_dist = _dist_to_peak_pct(dbg5.get("peak_price"), dbg5.get("price") or last_price)
                    log_info(
                        logger,
                        "EVENT_DIST_TO_PEAK",
                        symbol=cfg.symbol,
                        run_id=run_id,
                        stage=st.stage,
                        step="ARMED",
                        extra={
                            "event_id": f"{run_id}_armed",
                            "price": dbg5.get("price") or last_price,
                            "peak": dbg5.get("peak_price"),
                            "dist_to_peak_pct": armed_dist,
                        },
                    )
                    log_info(logger, "ARMED", symbol=cfg.symbol, run_id=run_id, stage=st.stage, step="ARMED", extra={"context_score": context_score, "dist_to_peak_pct": dbg5.get('dist_to_peak_pct')})
                    _ds_event(
                        run_id=run_id,
                        symbol=cfg.symbol,
                        event_id=f"{run_id}_armed",
                        stage=st.stage,
                        # ARMED is readiness only, not an entry signal
                        entry_ok=False,
                        skip_reasons="armed",
                        context_score=context_score,
                        payload={
                            "time_utc": dbg5.get("time_utc") or wall_time_utc(),
                            "price": dbg5.get("price") or last_price,
                            "dist_to_peak_pct": armed_dist,
                            "pump_pct": meta.get("pump_pct", ""),
                            "source": meta.get("source", ""),
                        },
                        extra={"oi_change_5m_pct": dbg5.get("oi_change_5m_pct")},
                    )
                    if TG_ARMED_ENABLE:
                        send_telegram(
                            format_armed_short(
                                symbol=cfg.symbol,
                                run_id=run_id,
                                event_id=f"{run_id}_armed",
                                time_utc=dbg5.get("time_utc") or wall_time_utc(),
                                price=dbg5.get("price") or last_price,
                                dist_to_peak_pct=armed_dist,
                                context_score=context_score,
                            ),
                            strategy="short_pump",
                            side="SHORT",
                            mode="ARMED",
                            event_id=run_id,
                            context_score=context_score,
                            entry_ok=True,
                            skip_reasons=None,
                            formatted=True,
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

                        decision_1m_ok, decision_1m_payload = decide_entry_1m(
                            cfg, candles_1m, trades_1m, oi_1m, context_score, ctx_parts, dbg5.get("peak_price", 0.0)
                        )
                        # Update context_score with CVD if available
                        context_score_with_cvd = decision_1m_payload.get("context_score", context_score)
                        ctx_parts = decision_1m_payload.get("context_parts", ctx_parts)

                        # Liquidation stats
                        now_ts = time.time()
                        liq_short_count_30s, liq_short_usd_30s = get_liq_stats(cfg.symbol, now_ts, 30, side="short")
                        liq_short_count_1m, liq_short_usd_1m = get_liq_stats(cfg.symbol, now_ts, 60, side="short")
                        liq_long_count_30s, liq_long_usd_30s = get_liq_stats(cfg.symbol, now_ts, 30, side="long")
                        liq_long_count_1m, liq_long_usd_1m = get_liq_stats(cfg.symbol, now_ts, 60, side="long")
                        dbg = get_liq_debug_state(cfg.symbol)
                        log_info(
                            logger,
                            "LIQ_STATS_AFTER_GET_1M",
                            symbol=cfg.symbol,
                            run_id=run_id,
                            stage=st.stage,
                            step="LIQ_STATS",
                            extra={
                                "now_ts": now_ts,
                                "liq_short_count_30s": liq_short_count_30s,
                                "liq_short_usd_30s": liq_short_usd_30s,
                                "liq_long_count_30s": liq_long_count_30s,
                                "liq_long_usd_30s": liq_long_usd_30s,
                                "liq_short_count_1m": liq_short_count_1m,
                                "liq_short_usd_1m": liq_short_usd_1m,
                                "liq_long_count_1m": liq_long_count_1m,
                                "liq_long_usd_1m": liq_long_usd_1m,
                                "buffer_short_len": dbg.get("buffer_short_len"),
                                "buffer_long_len": dbg.get("buffer_long_len"),
                                "buffer_short_last_ts_ms": dbg.get("buffer_short_last_ts_ms"),
                                "buffer_long_last_ts_ms": dbg.get("buffer_long_last_ts_ms"),
                                "last_event_ts_ms": dbg.get("last_event_ts_ms"),
                                "last_symbol": dbg.get("last_symbol"),
                                "rx_events_total": dbg.get("rx_events_total"),
                            },
                        )
                        if (
                            liq_short_count_30s > 0
                            or liq_short_usd_30s > 0
                            or liq_long_count_30s > 0
                            or liq_long_usd_30s > 0
                        ):
                            last_nonzero_ts = run_watch_for_symbol._last_liq_nonzero_log_ts.get(cfg.symbol, 0.0)
                            if now_ts - last_nonzero_ts >= 5:
                                log_info(
                                    logger,
                                    "LIQ_STATS_NONZERO",
                                    symbol=cfg.symbol,
                                    run_id=run_id,
                                    stage=st.stage,
                                    step="LIQ_STATS",
                                    now_ts=now_ts,
                                    liq_short_count_30s=liq_short_count_30s,
                                    liq_short_usd_30s=liq_short_usd_30s,
                                    liq_long_count_30s=liq_long_count_30s,
                                    liq_long_usd_30s=liq_long_usd_30s,
                                )
                                run_watch_for_symbol._last_liq_nonzero_log_ts[cfg.symbol] = now_ts
                        last_liq_ts = run_watch_for_symbol._last_liq_log_ts.get(cfg.symbol, 0.0)
                        if now_ts - last_liq_ts >= 60:
                            log_info(
                                logger,
                                "LIQ_STATS_SAMPLE_DEBUG",
                                symbol=cfg.symbol,
                                run_id=run_id,
                                stage=st.stage,
                                step="LIQ_STATS",
                                extra={
                                    "liq_short_count_30s": liq_short_count_30s,
                                    "liq_short_usd_30s": liq_short_usd_30s,
                                    "liq_long_count_30s": liq_long_count_30s,
                                    "liq_long_usd_30s": liq_long_usd_30s,
                                    "liq_short_count_1m": liq_short_count_1m,
                                    "liq_short_usd_1m": liq_short_usd_1m,
                                    "liq_long_count_1m": liq_long_count_1m,
                                    "liq_long_usd_1m": liq_long_usd_1m,
                                },
                            )
                            run_watch_for_symbol._last_liq_log_ts[cfg.symbol] = now_ts
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
                        decision_1m_payload.update({
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
                        now_decision_ts = time.time()
                        if entry_candidate_active and now_decision_ts > entry_candidate_expires_ts:
                            entry_candidate_active = False
                            entry_candidate_snapshot = {}
                            entry_candidate_source = ""

                        context_pass, context_meta = _context_filter_pass(decision_1m_payload, funding_rate_abs, context_score)
                        if context_pass and not entry_candidate_active:
                            _set_candidate(decision_1m_payload, now_decision_ts, "1m", context_meta)

                        trigger_pass, trigger_meta = _trigger_pass(decision_1m_payload)
                        if (
                            entry_candidate_active
                            and now_decision_ts <= entry_candidate_expires_ts
                            and trigger_pass
                            and cfg.entry_mode != "FAST_ONLY"
                        ):
                            base_score = float(decision_1m_payload.get("context_score", context_score) or 0.0)
                            adjusted_score, vetoed, veto_reason = _apply_liq_adjustments(decision_1m_payload, base_score)
                            if (not vetoed) and adjusted_score >= float(cfg.entry_context_min_score):
                                confirmed_payload = dict(decision_1m_payload)
                                confirmed_payload["context_score"] = adjusted_score
                                confirmed_payload["candidate_snapshot"] = dict(entry_candidate_snapshot)
                                confirmed_payload["entry_snapshot"] = {
                                    "state": "ENTRY_OK_CONFIRMED",
                                    "confirmed_ts": now_decision_ts,
                                    "source": trigger_meta.get("entry_source") or "1m",
                                    "trigger_meta": trigger_meta,
                                    "context_meta": context_meta,
                                    "base_context_score": base_score,
                                    "adjusted_context_score": adjusted_score,
                                    "candidate_set_ts": entry_candidate_set_ts,
                                    "candidate_expires_ts": entry_candidate_expires_ts,
                                    "liq_long_usd_30s": confirmed_payload.get("liq_long_usd_30s"),
                                    "liq_short_usd_30s": confirmed_payload.get("liq_short_usd_30s"),
                                }
                                log_info(
                                    logger,
                                    "ENTRY_OK_CONFIRMED",
                                    symbol=cfg.symbol,
                                    run_id=run_id,
                                    stage=st.stage,
                                    step="ENTRY_DECISION",
                                    extra={
                                        "source": confirmed_payload.get("entry_source"),
                                        "trigger_meta": trigger_meta,
                                        "base_context_score": base_score,
                                        "adjusted_context_score": adjusted_score,
                                        "candidate_set_ts": entry_candidate_set_ts,
                                        "candidate_expires_ts": entry_candidate_expires_ts,
                                        "cand_liq_long_usd_30s": entry_candidate_snapshot.get("liq_long_usd_30s"),
                                        "cand_liq_short_usd_30s": entry_candidate_snapshot.get("liq_short_usd_30s"),
                                    },
                                )
                                entry_ok = True
                                entry_payload = confirmed_payload
                                entry_candidate_active = False
                                entry_candidate_snapshot = {}
                                entry_candidate_source = ""
                            elif vetoed:
                                log_info(
                                    logger,
                                    "ENTRY_TRIGGER_VETOED",
                                    symbol=cfg.symbol,
                                    run_id=run_id,
                                    stage=st.stage,
                                    step="ENTRY_DECISION",
                                    extra={"reason": veto_reason, "source": trigger_meta.get("entry_source")},
                                )

                        append_csv(
                            log_1m,
                            {
                                "run_id": run_id,
                                "symbol": cfg.symbol,
                                "time_utc": str(candles_1m.iloc[-1]["ts"]),
                                "price": float(candles_1m.iloc[-1]["close"]),
                                "entry_ok": bool(decision_1m_ok),
                                "oi_change_1m_pct": decision_1m_payload.get("oi_change_1m_pct"),
                                "cvd_delta_ratio_30s": decision_1m_payload.get("cvd_delta_ratio_30s"),
                                "cvd_delta_ratio_1m": decision_1m_payload.get("cvd_delta_ratio_1m"),
                                "cvd_part": decision_1m_payload.get("cvd_part"),
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
                                "entry_payload": json.dumps(decision_1m_payload, ensure_ascii=False),
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
                    dbg = get_liq_debug_state(cfg.symbol)
                    log_info(
                        logger,
                        "LIQ_STATS_AFTER_GET_FAST",
                        symbol=cfg.symbol,
                        run_id=run_id,
                        stage=st.stage,
                        step="LIQ_STATS",
                        extra={
                            "now_ts": now_ts,
                            "liq_short_count_30s": liq_short_count_30s,
                            "liq_short_usd_30s": liq_short_usd_30s,
                            "liq_long_count_30s": liq_long_count_30s,
                            "liq_long_usd_30s": liq_long_usd_30s,
                            "liq_short_count_1m": liq_short_count_1m,
                            "liq_short_usd_1m": liq_short_usd_1m,
                            "liq_long_count_1m": liq_long_count_1m,
                            "liq_long_usd_1m": liq_long_usd_1m,
                            "buffer_short_len": dbg.get("buffer_short_len"),
                            "buffer_long_len": dbg.get("buffer_long_len"),
                            "buffer_short_last_ts_ms": dbg.get("buffer_short_last_ts_ms"),
                            "buffer_long_last_ts_ms": dbg.get("buffer_long_last_ts_ms"),
                            "last_event_ts_ms": dbg.get("last_event_ts_ms"),
                            "last_symbol": dbg.get("last_symbol"),
                            "rx_events_total": dbg.get("rx_events_total"),
                        },
                    )
                    if (
                        liq_short_count_30s > 0
                        or liq_short_usd_30s > 0
                        or liq_long_count_30s > 0
                        or liq_long_usd_30s > 0
                    ):
                        last_nonzero_ts = run_watch_for_symbol._last_liq_nonzero_log_ts.get(cfg.symbol, 0.0)
                        if now_ts - last_nonzero_ts >= 5:
                            log_info(
                                logger,
                                "LIQ_STATS_NONZERO",
                                symbol=cfg.symbol,
                                run_id=run_id,
                                stage=st.stage,
                                step="LIQ_STATS",
                                now_ts=now_ts,
                                liq_short_count_30s=liq_short_count_30s,
                                liq_short_usd_30s=liq_short_usd_30s,
                                liq_long_count_30s=liq_long_count_30s,
                                liq_long_usd_30s=liq_long_usd_30s,
                            )
                            run_watch_for_symbol._last_liq_nonzero_log_ts[cfg.symbol] = now_ts
                    last_liq_ts = run_watch_for_symbol._last_liq_log_ts.get(cfg.symbol, 0.0)
                    if now_ts - last_liq_ts >= 60:
                        log_info(
                            logger,
                            "LIQ_STATS_SAMPLE_DEBUG",
                            symbol=cfg.symbol,
                            run_id=run_id,
                            stage=st.stage,
                            step="LIQ_STATS",
                            extra={
                                "liq_short_count_30s": liq_short_count_30s,
                                "liq_short_usd_30s": liq_short_usd_30s,
                                "liq_long_count_30s": liq_long_count_30s,
                                "liq_long_usd_30s": liq_long_usd_30s,
                                "liq_short_count_1m": liq_short_count_1m,
                                "liq_short_usd_1m": liq_short_usd_1m,
                                "liq_long_count_1m": liq_long_count_1m,
                                "liq_long_usd_1m": liq_long_usd_1m,
                            },
                        )
                        run_watch_for_symbol._last_liq_log_ts[cfg.symbol] = now_ts
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
                    now_decision_ts = time.time()
                    if entry_candidate_active and now_decision_ts > entry_candidate_expires_ts:
                        entry_candidate_active = False
                        entry_candidate_snapshot = {}
                        entry_candidate_source = ""

                    context_pass, context_meta = _context_filter_pass(payload_fast, funding_rate_abs, context_score)
                    if context_pass and not entry_candidate_active:
                        _set_candidate(payload_fast, now_decision_ts, "fast", context_meta)

                    trigger_pass, trigger_meta = _trigger_pass(payload_fast)
                    if entry_candidate_active and now_decision_ts <= entry_candidate_expires_ts and trigger_pass:
                        base_score = float(payload_fast.get("context_score", context_score) or 0.0)
                        adjusted_score, vetoed, veto_reason = _apply_liq_adjustments(payload_fast, base_score)
                        if (not vetoed) and adjusted_score >= float(cfg.entry_context_min_score):
                            confirmed_payload = dict(payload_fast)
                            confirmed_payload["context_score"] = adjusted_score
                            confirmed_payload["candidate_snapshot"] = dict(entry_candidate_snapshot)
                            confirmed_payload["entry_snapshot"] = {
                                "state": "ENTRY_OK_CONFIRMED",
                                "confirmed_ts": now_decision_ts,
                                "source": trigger_meta.get("entry_source") or "fast",
                                "trigger_meta": trigger_meta,
                                "context_meta": context_meta,
                                "base_context_score": base_score,
                                "adjusted_context_score": adjusted_score,
                                "candidate_set_ts": entry_candidate_set_ts,
                                "candidate_expires_ts": entry_candidate_expires_ts,
                                "liq_long_usd_30s": confirmed_payload.get("liq_long_usd_30s"),
                                "liq_short_usd_30s": confirmed_payload.get("liq_short_usd_30s"),
                            }
                            log_info(
                                logger,
                                "ENTRY_OK_CONFIRMED",
                                symbol=cfg.symbol,
                                run_id=run_id,
                                stage=st.stage,
                                step="ENTRY_DECISION",
                                extra={
                                    "source": confirmed_payload.get("entry_source"),
                                    "trigger_meta": trigger_meta,
                                    "base_context_score": base_score,
                                    "adjusted_context_score": adjusted_score,
                                    "candidate_set_ts": entry_candidate_set_ts,
                                    "candidate_expires_ts": entry_candidate_expires_ts,
                                    "cand_liq_long_usd_30s": entry_candidate_snapshot.get("liq_long_usd_30s"),
                                    "cand_liq_short_usd_30s": entry_candidate_snapshot.get("liq_short_usd_30s"),
                                },
                            )
                            entry_ok = True
                            entry_payload = confirmed_payload
                            entry_candidate_active = False
                            entry_candidate_snapshot = {}
                            entry_candidate_source = ""
                        elif vetoed:
                            log_info(
                                logger,
                                "ENTRY_TRIGGER_VETOED",
                                symbol=cfg.symbol,
                                run_id=run_id,
                                stage=st.stage,
                                step="ENTRY_DECISION",
                                extra={"reason": veto_reason, "source": trigger_meta.get("entry_source")},
                            )
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
                except Exception as e:
                    log_exception(logger, "Error in fast polling", symbol=cfg.symbol, run_id=run_id, stage=st.stage, step="FAST")
            elif entry_candidate_active:
                entry_candidate_active = False
                entry_candidate_snapshot = {}
                entry_candidate_source = ""

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
                entry_wall_ts_ms = int(time.time() * 1000)
                entry_time_wall_utc = (
                    datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
                )
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
                entry_payload["entry_snapshot"] = entry_snapshot
                try:
                    now_ts = time.time()
                    liq_short_30s = get_liq_stats(cfg.symbol, now_ts, 30, side="short")
                    liq_long_30s = get_liq_stats(cfg.symbol, now_ts, 30, side="long")
                    liq_short_60s = get_liq_stats(cfg.symbol, now_ts, 60, side="short")
                    liq_long_60s = get_liq_stats(cfg.symbol, now_ts, 60, side="long")
                    liq_short_5m = get_liq_stats(cfg.symbol, now_ts, 300, side="short")
                    liq_long_5m = get_liq_stats(cfg.symbol, now_ts, 300, side="long")
                    entry_payload.update(
                        {
                            "liq_short_count_30s": liq_short_30s[0],
                            "liq_short_usd_30s": liq_short_30s[1],
                            "liq_long_count_30s": liq_long_30s[0],
                            "liq_long_usd_30s": liq_long_30s[1],
                            "liq_short_count_1m": liq_short_60s[0],
                            "liq_short_usd_1m": liq_short_60s[1],
                            "liq_long_count_1m": liq_long_60s[0],
                            "liq_long_usd_1m": liq_long_60s[1],
                        }
                    )
                    entry_snapshot.update(
                        {
                            "liq_short_count_30s": liq_short_30s[0],
                            "liq_short_usd_30s": liq_short_30s[1],
                            "liq_long_count_30s": liq_long_30s[0],
                            "liq_long_usd_30s": liq_long_30s[1],
                        }
                    )
                    log_info(
                        logger,
                        "LIQ_SNAPSHOT_AT_ENTRY",
                        symbol=cfg.symbol,
                        run_id=run_id,
                        stage=st.stage,
                        step="ENTRY_DECISION",
                        extra={
                            "now_ts": now_ts,
                            "count_short_30s": liq_short_30s[0],
                            "usd_short_30s": liq_short_30s[1],
                            "count_long_30s": liq_long_30s[0],
                            "usd_long_30s": liq_long_30s[1],
                            "count_short_60s": liq_short_60s[0],
                            "usd_short_60s": liq_short_60s[1],
                            "count_long_60s": liq_long_60s[0],
                            "usd_long_60s": liq_long_60s[1],
                            "count_short_5m": liq_short_5m[0],
                            "usd_short_5m": liq_short_5m[1],
                            "count_long_5m": liq_long_5m[0],
                            "usd_long_5m": liq_long_5m[1],
                        },
                    )
                except Exception as e:
                    log_warning(
                        logger,
                        "LIQ_SNAPSHOT_FAILED",
                        symbol=cfg.symbol,
                        run_id=run_id,
                        stage=st.stage,
                        step="ENTRY_DECISION",
                        extra={"exc_type": type(e).__name__, "exc_repr": repr(e)},
                    )

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
                if not entry_payload.get("dist_to_peak_pct"):
                    entry_payload["dist_to_peak_pct"] = _dist_to_peak_pct(dbg5.get("peak_price"), entry_price)
                log_info(
                    logger,
                    "EVENT_DIST_TO_PEAK",
                    symbol=cfg.symbol,
                    run_id=run_id,
                    stage=st.stage,
                    step="ENTRY_DECISION",
                    extra={
                        "event_id": str(event_id),
                        "price": entry_price,
                        "peak": dbg5.get("peak_price"),
                        "dist_to_peak_pct": entry_payload.get("dist_to_peak_pct"),
                    },
                )
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

                tradeable = is_tradeable_short_pump(st.stage, entry_payload.get("dist_to_peak_pct"))
                entry_payload["tradeable"] = tradeable
                try:
                    entry_source = entry_payload.get("entry_source", "unknown")
                    mode = "FAST" if entry_source == "fast" else "ARMED"
                    context_score_msg = entry_payload.get("context_score", context_score)
                    dist_to_peak = entry_payload.get("dist_to_peak_pct")
                    if not tradeable:
                        log_info(
                            logger,
                            "TG_ENTRY_OK_FILTERED",
                            symbol=cfg.symbol,
                            run_id=run_id,
                            stage=st.stage,
                            step="TELEGRAM",
                            extra={
                                "event_id": str(event_id),
                                "dist_to_peak_pct": dist_to_peak,
                                "reason": "not tradeable (stage!=4 or dist<TG_ENTRY_DIST_MIN)",
                            },
                        )
                    else:
                        sig = build_short_pump_signal(
                            strategy="short_pump",
                            side="SHORT",
                            symbol=cfg.symbol,
                            run_id=run_id,
                            event_id=str(event_id),
                            time_utc=entry_payload.get("time_utc", ""),
                            price=entry_payload.get("price", entry_price),
                            entry_price=entry_price,
                            tp_price=tp_price,
                            sl_price=sl_price,
                            tp_pct=entry_snapshot.get("tp_pct"),
                            sl_pct=entry_snapshot.get("sl_pct"),
                            entry_type=entry_type,
                            context_score=context_score_msg,
                            ctx_parts=entry_payload.get("context_parts"),
                            liq_short_usd_30s=entry_payload.get("liq_short_usd_30s"),
                            liq_long_usd_30s=entry_payload.get("liq_long_usd_30s"),
                            oi_change_fast_pct=entry_payload.get("oi_change_fast_pct"),
                            cvd_delta_ratio_30s=entry_payload.get("cvd_delta_ratio_30s"),
                            cvd_delta_ratio_1m=entry_payload.get("cvd_delta_ratio_1m"),
                            dist_to_peak_pct=dist_to_peak,
                            stage=st.stage,
                            debug_payload=entry_payload,
                        )
                        send_telegram(
                            format_tg(sig),
                            strategy="short_pump",
                            side="SHORT",
                            mode=mode,
                            event_id=str(event_id),
                            context_score=float(context_score_msg) if context_score_msg is not None else None,
                            entry_ok=True,
                            skip_reasons=None,
                            formatted=True,
                        )
                        # Option A: mirror ENTRY_OK Signal to trading queue (behind feature flag)
                        try:
                            from trading.config import AUTO_TRADING_ENABLE
                            if AUTO_TRADING_ENABLE:
                                from trading.queue import enqueue_signal
                                enqueue_signal(sig)
                        except Exception:
                            log_exception(logger, "TRADING_ENQUEUE failed for ENTRY_OK", symbol=cfg.symbol, run_id=run_id, stage=st.stage, step="TRADING_ENQUEUE")
                except Exception as e:
                    log_exception(logger, "TELEGRAM_SEND failed for ENTRY_OK", symbol=cfg.symbol, run_id=run_id, stage=st.stage, step="TELEGRAM_SEND")

                if not tradeable:
                    log_info(
                        logger,
                        "OUTCOME_SKIPPED_NOT_TRADEABLE",
                        symbol=cfg.symbol,
                        run_id=run_id,
                        stage=st.stage,
                        step="OUTCOME",
                        extra={"dist_to_peak_pct": dist_to_peak, "event_id": str(event_id)},
                    )
                    _cleanup_symbol()
                    return {"run_id": run_id, "symbol": cfg.symbol, "end_reason": "SKIPPED_NOT_TRADEABLE"}

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
                    outcome_wall_ts_ms = int(time.time() * 1000)
                    outcome_time_wall_utc = (
                        datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
                    )
                    if entry_wall_ts_ms:
                        hold_seconds = max(0.0, (outcome_wall_ts_ms - entry_wall_ts_ms) / 1000.0)
                    else:
                        hold_seconds = 0.0
                    summary["hold_seconds"] = hold_seconds
                    path_1m = summary.get("path_1m") or []
                    if path_1m:
                        log_info(
                            logger,
                            "OUTCOME_PATH",
                            symbol=cfg.symbol,
                            run_id=run_id,
                            stage=st.stage,
                            step="OUTCOME",
                            extra={
                                "len_path_1m": len(path_1m),
                                "first_t": path_1m[0].get("t") if path_1m else None,
                                "last_t": path_1m[-1].get("t") if path_1m else None,
                            },
                        )
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
                            "tp_sl_same_candle": summary.get("tp_sl_same_candle"),
                            "conflict_policy": summary.get("conflict_policy"),
                            "alt_outcome_tp_first": summary.get("alt_outcome_tp_first"),
                            "alt_outcome_sl_first": summary.get("alt_outcome_sl_first"),
                        },
                    )
                    # Map end_reason to outcome_label: TP_hit -> TP, SL_hit -> SL, TIMEOUT -> TIMEOUT
                    end_reason = summary.get("end_reason") or summary.get("outcome") or "TIMEOUT"
                    if end_reason == "TP_hit":
                        outcome_label = "TP"
                        skip_reason_outcome = "outcome_tp"
                    elif end_reason == "SL_hit":
                        outcome_label = "SL"
                        skip_reason_outcome = "outcome_sl"
                    else:
                        outcome_label = "TIMEOUT"
                        skip_reason_outcome = "outcome_timeout"
                    outcome_time_utc = summary.get("exit_time_utc") or summary.get("hit_time_utc") or wall_time_utc()
                    outcome_event_id = f"{run_id}_outcome"
                    # Idempotency: emit outcome event only once per run_id
                    if outcome_event_id not in run_watch_for_symbol._outcome_emitted:
                        run_watch_for_symbol._outcome_emitted.add(outcome_event_id)
                        exit_price = summary.get("exit_price", 0.0)
                        log_info(
                            logger,
                            "OUTCOME_FINAL",
                            symbol=cfg.symbol,
                            run_id=run_id,
                            stage=st.stage,
                            step="OUTCOME",
                            extra={
                                "outcome_label": outcome_label,
                                "exit_price": exit_price,
                                "exit_time_utc": outcome_time_utc,
                                "reason": skip_reason_outcome,
                            },
                        )
                        _ds_event(
                            run_id=run_id,
                            symbol=cfg.symbol,
                            event_id=outcome_event_id,
                            stage=st.stage,
                            entry_ok=1,
                            skip_reasons=skip_reason_outcome,
                            context_score=entry_snapshot.get("context_score"),
                            payload={
                                "time_utc": outcome_time_utc,
                                "price": exit_price,
                                "outcome_label": outcome_label,
                                "exit_time_utc": outcome_time_utc,
                                "exit_price": exit_price,
                                "tp_price": tp_price,
                                "sl_price": sl_price,
                                "watch_minutes": cfg.outcome_watch_minutes,
                                "end_reason": end_reason,
                                "pnl_pct": summary.get("pnl_pct"),
                                "hold_seconds": hold_seconds,
                            },
                            extra=None,
                        )
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
                            "entry_wall_ts_ms": entry_wall_ts_ms if entry_wall_ts_ms else None,
                            "entry_time_wall_utc": entry_time_wall_utc if entry_wall_ts_ms else None,
                            "outcome_wall_ts_ms": outcome_wall_ts_ms,
                            "outcome_time_wall_utc": outcome_time_wall_utc,
                            "hold_seconds": hold_seconds,
                        },
                    )
                    if outcome_row is not None:
                        try:
                            details_obj = json.loads(outcome_row.get("details_json") or "{}")
                            if "entry_time_wall_utc" not in details_obj or "outcome_time_wall_utc" not in details_obj:
                                log_warning(
                                    logger,
                                    "OUTCOME_DETAILS_MISSING_WALL_TIME",
                                    symbol=cfg.symbol,
                                    run_id=run_id,
                                    stage=st.stage,
                                    step="OUTCOME",
                                )
                        except Exception:
                            log_warning(
                                logger,
                                "OUTCOME_DETAILS_PARSE_ERROR",
                                symbol=cfg.symbol,
                                run_id=run_id,
                                stage=st.stage,
                                step="OUTCOME",
                            )
                        write_outcome_row(
                            outcome_row,
                            strategy="short_pump",
                            mode="live",
                            wall_time_utc=outcome_time_utc,
                            schema_version=3,
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
                                format_outcome(
                                    strategy="short_pump",
                                    side="SHORT",
                                    symbol=cfg.symbol,
                                    run_id=run_id,
                                    event_id=str(event_id),
                                    outcome=summary.get("end_reason") or summary.get("outcome") or "UNKNOWN",
                                    entry_price=summary.get("entry_price") or entry_snapshot.get("entry_price"),
                                    tp_price=entry_snapshot.get("tp_price"),
                                    sl_price=entry_snapshot.get("sl_price"),
                                    tp_pct=entry_snapshot.get("tp_pct"),
                                    sl_pct=entry_snapshot.get("sl_pct"),
                                    pnl_pct=summary.get("pnl_pct"),
                                    hold_seconds=summary.get("hold_seconds"),
                                    mae_pct=summary.get("mae_pct"),
                                    mfe_pct=summary.get("mfe_pct"),
                                    debug_payload=summary,
                                ),
                                strategy="short_pump",
                                side="SHORT",
                                mode="FAST" if entry_payload.get("entry_source") == "fast" else "ARMED",
                                event_id=str(event_id),
                                context_score=float(context_score_msg) if context_score_msg is not None else None,
                                entry_ok=True,
                                skip_reasons=None,
                                formatted=True,
                            )
                        except Exception as e:
                            log_exception(logger, "TELEGRAM_SEND failed for OUTCOME", symbol=cfg.symbol, run_id=run_id, stage=st.stage, step="TELEGRAM_SEND")

                    # Paper: close position on OUTCOME (TP_hit/SL_hit)
                    try:
                        from trading.config import AUTO_TRADING_ENABLE, MODE
                        if AUTO_TRADING_ENABLE and MODE == "paper":
                            from trading.paper_outcome import close_from_outcome
                            event_id_outcome = entry_payload.get("event_id") or run_id
                            end_reason = summary.get("end_reason") or summary.get("outcome") or ""
                            outcome_time_utc_val = summary.get("exit_time_utc") or summary.get("hit_time_utc") or wall_time_utc()
                            close_from_outcome(
                                strategy="short_pump",
                                symbol=cfg.symbol,
                                run_id=run_id,
                                event_id=str(event_id_outcome),
                                res=end_reason,
                                pnl_pct=summary.get("pnl_pct"),
                                ts_utc=outcome_time_utc_val,
                                outcome_meta={
                                    "mfe_pct": summary.get("mfe_pct"),
                                    "mae_pct": summary.get("mae_pct"),
                                    "mfe_r": summary.get("mfe_r"),
                                    "mae_r": summary.get("mae_r"),
                                },
                            )
                    except Exception:
                        log_exception(logger, "TRADING_CLOSE_FROM_OUTCOME failed", symbol=cfg.symbol, run_id=run_id, stage=st.stage, step="OUTCOME")

                    _cleanup_symbol()
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
        _cleanup_symbol()
        return summary

    except KeyboardInterrupt:
        summary = {
            "run_id": run_id,
            "symbol": cfg.symbol,
            "end_reason": "INTERRUPTED",
        }
        log_info(logger, "INTERRUPTED", symbol=cfg.symbol, run_id=run_id, stage=st.stage, step="INTERRUPTED")
        append_csv(log_summary, summary)
        _cleanup_symbol()
        return summary
    except Exception as e:
        _cleanup_symbol()
        log_exception(logger, "Fatal error in run_watch_for_symbol", symbol=cfg.symbol, run_id=run_id, stage=st.stage, step="FATAL")
        raise