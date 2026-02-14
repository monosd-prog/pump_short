from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import pandas as pd

from common.bybit_api import get_klines_1m, get_klines_5m, get_open_interest, get_recent_trades
from common.io_dataset import write_event_row, write_outcome_row
from common.logging_utils import get_logger
from common.outcome_tracker import build_outcome_row, track_outcome
from common.runtime import run_id as gen_run_id, wall_time_utc
from long_pullback.config import Config
from long_pullback.context5m import LongPullbackState, update_context_5m
from long_pullback.entry import decide_entry_long
from long_pullback.telegram import TG_SEND_OUTCOME, send_telegram
from notifications.tg_format import format_entry_ok, format_outcome
from short_pump.io_csv import append_csv
from short_pump.liquidations import get_liq_stats, register_symbol

logger = get_logger(__name__, strategy="long_pullback")


def is_strategy_enabled_for_tg(strategy: str) -> bool:
    raw = os.getenv("TG_STRATEGIES", "short_pump")
    if raw.strip().upper() == "ALL":
        return True
    allowed = {s.strip().lower() for s in raw.split(",") if s.strip()}
    return (strategy or "").strip().lower() in allowed


def _resolve_conflict_policy(default: str) -> str:
    raw = os.getenv("CONFLICT_POLICY", default).strip().upper()
    if raw in ("SL_FIRST", "TP_FIRST", "NEUTRAL"):
        return raw
    return "SL_FIRST"


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
    trade_type: str,
    mode: str,
    strategy: str,
) -> None:
    row = {
        "trade_id": trade_id,
        "event_id": event_id,
        "run_id": run_id,
        "symbol": symbol,
        "strategy": strategy,
        "mode": mode,
        "side": "LONG",
        "outcome_time_utc": outcome_time_utc,
        "outcome": outcome,
        "pnl_pct": pnl_pct,
        "details_json": details_json,
        "trade_type": trade_type,
    }
    write_outcome_row(row, strategy=strategy, mode=mode, wall_time_utc=outcome_time_utc, schema_version=2)


def run_watch_for_symbol(
    symbol: str,
    run_id: Optional[str] = None,
    mode: str = "live",
    cfg: Optional[Config] = None,
) -> Dict[str, Any]:
    cfg = cfg or Config()
    symbol = str(symbol).strip().upper()
    run_id = run_id or gen_run_id()
    logger = get_logger(__name__, strategy="long_pullback", symbol=symbol)
    register_symbol(symbol)
    tg_enabled = is_strategy_enabled_for_tg("long_pullback")
    logger.info(
        "TG_STRATEGIES_CHECK | strategy=%s | enabled=%s | tg_strategies=%s",
        "long_pullback",
        tg_enabled,
        os.getenv("TG_STRATEGIES", "short_pump"),
    )

    log_5m = f"{cfg.logs_root}/{run_id}_{symbol}_5m.csv"
    log_1m = f"{cfg.logs_root}/{run_id}_{symbol}_1m.csv"

    logger.info("LONG_WATCH_START | symbol_repr=%r | symbol=%s", symbol, symbol)

    while True:
        try:
            st = LongPullbackState()
            entry_ok = False
            entry_payload: Dict[str, Any] = {}

            start_ts = pd.Timestamp.now(tz="UTC")
            end_ts = start_ts + pd.Timedelta(minutes=90)

            while pd.Timestamp.now(tz="UTC") < end_ts:
                candles_5m = get_klines_5m("linear", symbol, limit=250)
                logger.info(
                    "LONG_TICK | symbol=%s | step=FETCH_5M | candles_5m_rows=%s",
                    symbol,
                    candles_5m.shape[0] if candles_5m is not None else 0,
                )
                st, ctx_parts = update_context_5m(cfg, st, candles_5m)
                context_score = sum(ctx_parts.values())

                # Write tick-event for every new 5m candle
                last_ts = None
                if candles_5m is not None and not candles_5m.empty:
                    last_ts = candles_5m.iloc[-1]["ts"]
                if last_ts is not None and last_ts != st.last_5m_ts:
                    st.last_5m_ts = last_ts
                    last_close = None
                    try:
                        last_close = float(candles_5m.iloc[-1]["close"])
                    except Exception:
                        last_close = None
                    peak_price = st.peak_price
                    dist_to_peak_pct = (
                        max(0.0, (peak_price - last_close) / peak_price * 100.0)
                        if peak_price and last_close
                        else 0.0
                    )
                    event_row = {
                        "run_id": run_id,
                        "event_id": str(last_ts),
                        "symbol": symbol,
                        "wall_time_utc": wall_time_utc(),
                        "strategy": cfg.strategy_name,
                        "mode": mode,
                        "side": "LONG",
                        "stage": st.stage,
                        "context_score": context_score,
                        "context_parts": json.dumps(ctx_parts, ensure_ascii=False),
                        "entry_ok": False,
                        "skip_reasons": "stage_lt_2" if st.stage < 2 else "",
                        "price": last_close if last_close is not None else "",
                        "dist_to_peak_pct": float(dist_to_peak_pct),
                        "entry_payload": "",
                        "time_utc": str(last_ts),
                    }
                    write_event_row(event_row, strategy=cfg.strategy_name, mode=mode, wall_time_utc=event_row["wall_time_utc"], schema_version=2)

                # log 5m
                try:
                    append_csv(
                        log_5m,
                        {
                            "run_id": run_id,
                            "symbol": symbol,
                            "time_utc": str(candles_5m.iloc[-1]["ts"]) if candles_5m is not None and not candles_5m.empty else "",
                            "stage": st.stage,
                            "context_score": context_score,
                            "context_parts": json.dumps(ctx_parts, ensure_ascii=False),
                        },
                    )
                except Exception:
                    logger.exception("LONG_APPEND_5M_ERROR | symbol=%s | path=%s", symbol, log_5m)

                if st.stage >= 2 and not entry_ok:
                    candles_1m = get_klines_1m("linear", symbol, limit=250)
                    trades = get_recent_trades("linear", symbol, limit=1000)
                    oi = get_open_interest("linear", symbol, limit=80)

                    entry_ok, entry_payload = decide_entry_long(
                        cfg,
                        run_id=run_id,
                        symbol=symbol,
                        candles_1m=candles_1m,
                        trades=trades,
                        oi_1m=oi,
                        ctx_parts=ctx_parts,
                        peak_price=st.peak_price,
                        mode=mode,
                    )
                    logger.info(
                        "LONG_ENTRY_CHECK | symbol=%s | entry_ok=%s | payload_keys=%s",
                        symbol,
                        entry_ok,
                        list(entry_payload.keys()) if isinstance(entry_payload, dict) else type(entry_payload),
                    )

                    try:
                        append_csv(
                            log_1m,
                            {
                                "run_id": run_id,
                                "symbol": symbol,
                                "time_utc": str(candles_1m.iloc[-1]["ts"]) if candles_1m is not None and not candles_1m.empty else "",
                                "entry_ok": bool(entry_ok),
                                "entry_payload": json.dumps(entry_payload, ensure_ascii=False),
                            },
                        )
                    except Exception:
                        logger.exception("LONG_APPEND_1M_ERROR | symbol=%s | path=%s", symbol, log_1m)

                    if entry_ok:
                        event_id = str(entry_payload.get("event_id", run_id))
                        time_utc = entry_payload.get("time_utc")
                        price = entry_payload.get("price")
                        ctx_score = entry_payload.get("context_score")
                        entry_wall_ts_ms = int(time.time() * 1000)
                        entry_time_wall_utc = (
                            datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
                        )
                        if not entry_payload.get("dist_to_peak_pct"):
                            peak_val = st.peak_price
                            entry_price = float(price) if price is not None else 0.0
                            entry_payload["dist_to_peak_pct"] = (
                                max(0.0, (peak_val - entry_price) / peak_val * 100.0) if peak_val > 0 else 0.0
                            )
                        logger.info(
                            "EVENT_DIST_TO_PEAK | symbol=%s | run_id=%s | event_id=%s | price=%s | peak=%s | dist_to_peak_pct=%s",
                            symbol,
                            run_id,
                            event_id,
                            price,
                            st.peak_price,
                            entry_payload.get("dist_to_peak_pct"),
                        )
                        logger.info("LONG_ENTRY_OK_TG | symbol=%s | run_id=%s | event_id=%s", symbol, run_id, event_id)
                        if tg_enabled:
                            try:
                                send_telegram(
                                    format_entry_ok(
                                        strategy=cfg.strategy_name,
                                        side="LONG",
                                        symbol=symbol,
                                        run_id=run_id,
                                        event_id=str(event_id),
                                        time_utc=str(time_utc or ""),
                                        price=price,
                                        entry_price=float(price) if price is not None else 0.0,
                                        tp_price=tp_price,
                                        sl_price=sl_price,
                                        tp_pct=entry_payload.get("tp_pct") or (cfg.tp_pct * 100.0),
                                        sl_pct=entry_payload.get("sl_pct") or (cfg.sl_pct * 100.0),
                                        entry_type=entry_payload.get("entry_type", "PULLBACK"),
                                        context_score=ctx_score,
                                        ctx_parts=entry_payload.get("context_parts"),
                                        liq_short_usd_30s=entry_snapshot.get("liq_short_usd_30s"),
                                        liq_long_usd_30s=entry_snapshot.get("liq_long_usd_30s"),
                                        oi_change_fast_pct=None,
                                        cvd_delta_ratio_30s=entry_payload.get("cvd_delta_ratio_30s"),
                                        debug_payload=entry_payload,
                                    ),
                                    strategy="long_pullback",
                                    side="LONG",
                                    mode="LIVE",
                                    event_id=event_id,
                                    context_score=float(ctx_score) if ctx_score is not None else None,
                                    entry_ok=True,
                                    skip_reasons=None,
                                    formatted=True,
                                )
                            except Exception:
                                logger.exception("LONG_TG_SEND_ERROR | symbol=%s", symbol)
                        else:
                            logger.info(
                                "TG_SUPPRESSED | strategy=long_pullback | kind=ENTRY_OK | symbol=%s | run_id=%s | event_id=%s",
                                symbol,
                                run_id,
                                event_id,
                            )

                        try:
                            entry_ts_utc = pd.Timestamp(time_utc) if time_utc else pd.Timestamp.now(tz="UTC")
                            if entry_ts_utc.tzinfo is None:
                                entry_ts_utc = entry_ts_utc.tz_localize("UTC")
                        except Exception:
                            entry_ts_utc = pd.Timestamp.now(tz="UTC")
                        entry_price = float(price) if price is not None else 0.0
                        tp_price = float(entry_payload.get("tp_price") or (entry_price * (1.0 + cfg.tp_pct)))
                        sl_price = float(entry_payload.get("sl_price") or (entry_price * (1.0 - cfg.sl_pct)))
                        entry_snapshot = {
                            "entry_ts_utc": entry_ts_utc.isoformat(),
                            "entry_price": entry_price,
                            "tp_pct": float(entry_payload.get("tp_pct") or (cfg.tp_pct * 100.0)),
                            "sl_pct": float(entry_payload.get("sl_pct") or (cfg.sl_pct * 100.0)),
                            "tp_price": tp_price,
                            "sl_price": sl_price,
                            "context_score": entry_payload.get("context_score"),
                            "context_parts": entry_payload.get("context_parts"),
                            "stage": st.stage,
                            "entry_mode": mode,
                            "entry_source": entry_payload.get("entry_source", "1m"),
                            "entry_type": entry_payload.get("entry_type", "PULLBACK"),
                        }
                        entry_snapshot.update(
                            {
                                "cvd_delta_ratio_30s": entry_payload.get("cvd_delta_ratio_30s"),
                                "cvd_delta_ratio_1m": entry_payload.get("cvd_delta_ratio_1m"),
                                "oi_change_1m_pct": entry_payload.get("oi_change_1m_pct"),
                                "entry_source": entry_payload.get("entry_source"),
                                "entry_type": entry_payload.get("entry_type"),
                            }
                        )
                        if "dist_to_peak_pct" in entry_payload:
                            entry_snapshot["dist_to_peak_pct"] = entry_payload.get("dist_to_peak_pct")
                        try:
                            now_ts = time.time()
                            entry_snapshot.update(
                                {
                                    "liq_short_count_30s": get_liq_stats(symbol, now_ts, 30, side="short")[0],
                                    "liq_short_usd_30s": get_liq_stats(symbol, now_ts, 30, side="short")[1],
                                    "liq_short_count_60s": get_liq_stats(symbol, now_ts, 60, side="short")[0],
                                    "liq_short_usd_60s": get_liq_stats(symbol, now_ts, 60, side="short")[1],
                                    "liq_short_count_5m": get_liq_stats(symbol, now_ts, 300, side="short")[0],
                                    "liq_short_usd_5m": get_liq_stats(symbol, now_ts, 300, side="short")[1],
                                    "liq_long_count_30s": get_liq_stats(symbol, now_ts, 30, side="long")[0],
                                    "liq_long_usd_30s": get_liq_stats(symbol, now_ts, 30, side="long")[1],
                                    "liq_long_count_60s": get_liq_stats(symbol, now_ts, 60, side="long")[0],
                                    "liq_long_usd_60s": get_liq_stats(symbol, now_ts, 60, side="long")[1],
                                    "liq_long_count_5m": get_liq_stats(symbol, now_ts, 300, side="long")[0],
                                    "liq_long_usd_5m": get_liq_stats(symbol, now_ts, 300, side="long")[1],
                                }
                            )
                        except Exception:
                            pass

                        logger.info(
                            "OUTCOME_WATCH_START | symbol=%s | run_id=%s | entry_price=%s | tp_price=%s | sl_price=%s | watch_minutes=%s | poll_seconds=%s",
                            symbol,
                            run_id,
                            entry_price,
                            tp_price,
                            sl_price,
                            cfg.outcome_watch_minutes,
                            cfg.outcome_poll_seconds,
                        )
                        try:
                            summary = track_outcome(
                                cfg,
                                side="long",
                                entry_ts_utc=entry_ts_utc,
                                entry_price=entry_price,
                                tp_price=tp_price,
                                sl_price=sl_price,
                                entry_source=entry_payload.get("entry_source", "1m"),
                                entry_type=entry_payload.get("entry_type", "PULLBACK"),
                                conflict_policy=_resolve_conflict_policy(cfg.conflict_policy),
                                run_id=run_id,
                                symbol=symbol,
                                category=cfg.category,
                                strategy_name=cfg.strategy_name,
                            )
                            outcome_wall_ts_ms = int(time.time() * 1000)
                            outcome_time_wall_utc = (
                                datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
                            )
                            if entry_wall_ts_ms:
                                hold_seconds = max(0.0, (outcome_wall_ts_ms - entry_wall_ts_ms) / 1000.0)
                            else:
                                hold_seconds = 0.0
                            if hold_seconds == 0.0 and summary.get("end_reason") in ("TP_hit", "SL_hit"):
                                min_hold = float(getattr(cfg, "outcome_poll_seconds", 60) or 60)
                                hold_seconds = max(hold_seconds, min_hold)
                            summary["hold_seconds"] = hold_seconds
                            path_1m = summary.get("path_1m") or []
                            if path_1m:
                                logger.info(
                                    "OUTCOME_PATH | symbol=%s | run_id=%s | len_path_1m=%s | first_t=%s | last_t=%s",
                                    symbol,
                                    run_id,
                                    len(path_1m),
                                    path_1m[0].get("t") if path_1m else None,
                                    path_1m[-1].get("t") if path_1m else None,
                                )
                            logger.info(
                                "OUTCOME_DONE | symbol=%s | run_id=%s | outcome_type=%s | hold_seconds=%s | pnl_pct=%s | mae_pct=%s | mfe_pct=%s",
                                symbol,
                                run_id,
                                summary.get("end_reason"),
                                hold_seconds,
                                summary.get("pnl_pct"),
                                summary.get("mae_pct"),
                                summary.get("mfe_pct"),
                            )
                            logger.info(
                                "OUTCOME_CONFLICT | symbol=%s | run_id=%s | tp_sl_same_candle=%s | conflict_policy=%s | alt_tp=%s | alt_sl=%s",
                                symbol,
                                run_id,
                                summary.get("tp_sl_same_candle"),
                                summary.get("conflict_policy"),
                                summary.get("alt_outcome_tp_first"),
                                summary.get("alt_outcome_sl_first"),
                            )
                            trade_id = entry_payload.get("trade_id") or f"{event_id}_trade"
                            outcome_time_utc = outcome_time_wall_utc
                            outcome_row = build_outcome_row(
                                summary,
                                trade_id=str(trade_id),
                                event_id=str(event_id),
                                run_id=run_id,
                                symbol=symbol,
                                strategy=cfg.strategy_name,
                                mode=mode,
                                side="LONG",
                                outcome_time_utc=outcome_time_utc,
                                entry_snapshot=entry_snapshot,
                                extra_details={
                                    "entry_mode": mode,
                                    "context_score": entry_snapshot.get("context_score"),
                                    "outcome_time_utc": summary.get("exit_time_utc") or summary.get("hit_time_utc"),
                                    "entry_time_wall_utc": entry_time_wall_utc if entry_wall_ts_ms else None,
                                    "outcome_time_wall_utc": outcome_time_wall_utc,
                                    "hold_seconds": hold_seconds,
                                },
                            )
                            if outcome_row is not None:
                                try:
                                    details_obj = json.loads(outcome_row.get("details_json") or "{}")
                                    if "entry_time_wall_utc" not in details_obj or "outcome_time_wall_utc" not in details_obj:
                                        logger.warning(
                                            "OUTCOME_DETAILS_MISSING_WALL_TIME | symbol=%s | event_id=%s",
                                            symbol,
                                            event_id,
                                        )
                                except Exception:
                                    logger.warning(
                                        "OUTCOME_DETAILS_PARSE_ERROR | symbol=%s | event_id=%s",
                                        symbol,
                                        event_id,
                                    )
                                write_outcome_row(
                                    outcome_row,
                                    strategy=cfg.strategy_name,
                                    mode=mode,
                                    wall_time_utc=outcome_time_utc,
                                    schema_version=2,
                                )
                            if TG_SEND_OUTCOME:
                                if tg_enabled:
                                    try:
                                        send_telegram(
                                            format_outcome(
                                                strategy=cfg.strategy_name,
                                                side="LONG",
                                                symbol=symbol,
                                                run_id=run_id,
                                                event_id=str(event_id),
                                                outcome=summary.get("end_reason") or summary.get("outcome") or "UNKNOWN",
                                                entry_price=entry_snapshot.get("entry_price"),
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
                                            strategy=cfg.strategy_name,
                                            side="LONG",
                                            mode="LIVE",
                                            event_id=str(event_id),
                                            context_score=float(entry_snapshot.get("context_score"))
                                            if entry_snapshot.get("context_score") is not None
                                            else None,
                                            entry_ok=True,
                                            skip_reasons=None,
                                            formatted=True,
                                        )
                                    except Exception:
                                        logger.exception("LONG_TG_SEND_ERROR | symbol=%s", symbol)
                                else:
                                    logger.info(
                                        "TG_SUPPRESSED | strategy=long_pullback | kind=OUTCOME | symbol=%s | run_id=%s | event_id=%s",
                                        symbol,
                                        run_id,
                                        event_id,
                                    )
                        except Exception:
                            logger.exception("LONG_OUTCOME_ERROR | symbol=%s", symbol)
                        entry_ok = False
                        entry_payload = {}

                time.sleep(cfg.poll_seconds_5m)
        except Exception:
            logger.exception("LONG_LOOP_ERROR | symbol=%s", symbol)
        time.sleep(cfg.poll_seconds_5m)
