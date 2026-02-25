# short_pump/fast0_sampler.py
"""Stage-0 fast sampling right after pump signal. ENTRY_OK, trades, outcomes for ML."""

from __future__ import annotations

import json
import os
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Union

import pandas as pd

from short_pump.bybit_api import (
    get_funding_rate,
    get_klines_1m,
    get_klines_5m,
    get_open_interest,
    get_orderbook,
    get_recent_trades,
)
from short_pump.config import Config
from short_pump.context5m import StructureState, build_dbg5, compute_context_score_5m
from short_pump.features import cvd_delta_ratio, normalize_funding, oi_change_pct
from short_pump.liquidations import get_liq_stats, register_symbol, unregister_symbol
from short_pump.logging_utils import get_logger, log_exception, log_info
from common.io_dataset import ensure_dataset_files, write_event_row, write_outcome_row, write_trade_row
from common.outcome_tracker import build_outcome_row, track_outcome
from common.runtime import wall_time_utc
from notifications.tg_format import format_fast0_entry_ok
from short_pump.telegram import send_telegram

logger = get_logger(__name__)

STRATEGY = "short_pump_fast0"
ENABLE_ORDERBOOK = os.getenv("ENABLE_ORDERBOOK", "0").strip().lower() in ("1", "true", "yes", "y", "on")
FAST0_WINDOW_SEC = int(os.getenv("FAST0_WINDOW_SEC", "180"))
FAST0_POLL_SECONDS = int(os.getenv("FAST0_POLL_SECONDS", "10"))
FAST0_TG_ENTRY_ENABLE = os.getenv("FAST0_TG_ENTRY_ENABLE", "0").strip().lower() in ("1", "true", "yes", "y", "on")
FAST0_ENTRY_CONTEXT_MIN = float(os.getenv("FAST0_ENTRY_CONTEXT_MIN", "0.60").replace(",", "."))
FAST0_ENTRY_DIST_MIN = float(os.getenv("FAST0_ENTRY_DIST_MIN", "0.50").replace(",", "."))
FAST0_ENTRY_CVD30S_MAX = float(os.getenv("FAST0_ENTRY_CVD30S_MAX", "-0.10").replace(",", "."))
FAST0_ENTRY_CVD1M_MAX = float(os.getenv("FAST0_ENTRY_CVD1M_MAX", "-0.05").replace(",", "."))
FAST0_ENTRY_MIN_TICK = int(os.getenv("FAST0_ENTRY_MIN_TICK", "2"))
FAST0_TP_PCT = float(os.getenv("FAST0_TP_PCT", "0.012").replace(",", "."))
FAST0_SL_PCT = float(os.getenv("FAST0_SL_PCT", "0.010").replace(",", "."))
FAST0_OUTCOME_WATCH_SEC = int(os.getenv("FAST0_OUTCOME_WATCH_SEC", "1800"))
FAST0_OUTCOME_POLL_SEC = int(os.getenv("FAST0_OUTCOME_POLL_SEC", "5"))
FAST0_MAX_WATCHERS = int(os.getenv("FAST0_MAX_WATCHERS", "20"))

_active_fast0_watchers = 0
_fast0_watchers_lock = threading.Lock()


def _orderbook_imbalance_and_spread(
    category: str, symbol: str, levels: int = 10
) -> tuple[Optional[float], Optional[float]]:
    """Compute (imbalance, spread_bps). Imbalance = (bid-sum - ask-sum)/(bid-sum + ask-sum)."""
    if not ENABLE_ORDERBOOK:
        return None, None
    ob = get_orderbook(category, symbol, limit=levels)
    if not ob or not ob.get("bids") or not ob.get("asks"):
        return None, None
    bids = ob["bids"][:levels]
    asks = ob["asks"][:levels]
    sum_bid = sum(float(p) * float(s) for p, s in bids)
    sum_ask = sum(float(p) * float(s) for p, s in asks)
    total = sum_bid + sum_ask
    if total <= 0:
        return None, None
    imbalance = (sum_bid - sum_ask) / total
    best_bid = float(bids[0][0]) if bids else 0.0
    best_ask = float(asks[0][0]) if asks else 0.0
    mid = (best_bid + best_ask) / 2.0 if (best_bid and best_ask) else 0.0
    spread_bps = (best_ask - best_bid) / mid * 10000.0 if mid else None
    return imbalance, spread_bps


def _volume_from_candles(candles: pd.DataFrame, last_n: int) -> float:
    if candles is None or candles.empty or len(candles) < last_n:
        return 0.0
    return float(candles["volume"].tail(last_n).sum())


def should_fast0_entry_ok(payload: Dict[str, Any], tick: int) -> tuple[bool, str]:
    """
    Returns (pass, reason). pass=True if thresholds met, else (False, reason).
    """
    if tick < FAST0_ENTRY_MIN_TICK:
        return False, f"tick<{FAST0_ENTRY_MIN_TICK}"
    cs = payload.get("context_score")
    try:
        cs_val = float(cs) if cs is not None else -999.0
    except (TypeError, ValueError):
        return False, "context_score_invalid"
    if cs_val < FAST0_ENTRY_CONTEXT_MIN:
        return False, f"context_score={cs_val}<{FAST0_ENTRY_CONTEXT_MIN}"
    dist = payload.get("dist_to_peak_pct")
    try:
        dist_val = float(dist) if dist is not None else -999.0
    except (TypeError, ValueError):
        return False, "dist_invalid"
    if dist_val < FAST0_ENTRY_DIST_MIN:
        return False, f"dist_to_peak={dist_val}<{FAST0_ENTRY_DIST_MIN}"
    cvd30 = payload.get("cvd_delta_ratio_30s")
    try:
        cvd30_val = float(cvd30) if cvd30 is not None else 999.0
    except (TypeError, ValueError):
        cvd30_val = 999.0
    if cvd30_val > FAST0_ENTRY_CVD30S_MAX:
        return False, f"cvd30s={cvd30_val}>{FAST0_ENTRY_CVD30S_MAX}"
    cvd1m = payload.get("cvd_delta_ratio_1m")
    try:
        cvd1m_val = float(cvd1m) if cvd1m is not None else 999.0
    except (TypeError, ValueError):
        cvd1m_val = 999.0
    if cvd1m_val > FAST0_ENTRY_CVD1M_MAX:
        return False, f"cvd1m={cvd1m_val}>{FAST0_ENTRY_CVD1M_MAX}"
    return True, "ok"


@dataclass
class _Fast0OutcomeCfg:
    category: str = "linear"
    outcome_watch_minutes: int = 30
    outcome_poll_seconds: int = 5


def _run_fast0_outcome_watcher(
    *,
    symbol: str,
    run_id: str,
    event_id: str,
    trade_id: str,
    entry_price: float,
    tp_price: float,
    sl_price: float,
    entry_time_utc: str,
    base_dir: Optional[str],
    mode: str,
) -> None:
    """Daemon thread: track TP/SL, write outcome row."""
    global _active_fast0_watchers
    with _fast0_watchers_lock:
        if _active_fast0_watchers >= FAST0_MAX_WATCHERS:
            logger.warning(
                "FAST0_MAX_WATCHERS reached, skipping outcome watch",
                extra={"symbol": symbol, "trade_id": trade_id},
            )
            return
        _active_fast0_watchers += 1
    try:
        cfg = _Fast0OutcomeCfg(
            category=Config.from_env().category,
            outcome_watch_minutes=FAST0_OUTCOME_WATCH_SEC // 60,
            outcome_poll_seconds=FAST0_OUTCOME_POLL_SEC,
        )
        try:
            entry_ts = pd.Timestamp(entry_time_utc)
            if entry_ts.tzinfo is None:
                entry_ts = entry_ts.tz_localize("UTC")
        except Exception:
            entry_ts = pd.Timestamp.now(tz="UTC")
        summary = track_outcome(
            cfg,
            side="short",
            entry_ts_utc=entry_ts,
            entry_price=entry_price,
            tp_price=tp_price,
            sl_price=sl_price,
            entry_source="fast0",
            entry_type="fast0",
            run_id=run_id,
            symbol=symbol,
            category=cfg.category,
            fetch_klines_1m=get_klines_1m,
            strategy_name=STRATEGY,
        )
        end_reason = summary.get("end_reason") or summary.get("outcome") or "TIMEOUT"
        if end_reason in ("TP_hit", "SL_hit", "CONFLICT") and summary.get("hit_time_utc"):
            outcome_time_utc = summary["hit_time_utc"]
            minutes_to_hit = summary.get("minutes_to_hit")
            hold_sec = (minutes_to_hit * 60.0) if minutes_to_hit is not None else 1.0
        else:
            outcome_ts = datetime.now(timezone.utc)
            outcome_time_utc = outcome_ts.isoformat()
            hold_sec = (outcome_ts - entry_ts.to_pydatetime()).total_seconds()
            summary["exit_time_utc"] = outcome_time_utc
        summary["hold_seconds"] = max(1.0, hold_sec)
        pnl = summary.get("pnl_pct")
        summary["pnl_pct"] = float(pnl) if pnl is not None else 0.0
        orow = build_outcome_row(
            summary,
            trade_id=trade_id,
            event_id=event_id,
            run_id=run_id,
            symbol=symbol,
            strategy=STRATEGY,
            mode=mode,
            side="SHORT",
            outcome_time_utc=outcome_time_utc,
        )
        if orow:
            write_outcome_row(
                orow,
                strategy=STRATEGY,
                mode=mode,
                wall_time_utc=outcome_time_utc,
                schema_version=3,
                base_dir=base_dir,
            )
            logger.info(
                "FAST0_OUTCOME",
                extra={"symbol": symbol, "trade_id": trade_id, "outcome": summary.get("outcome")},
            )
    except Exception as e:
        log_exception(logger, "FAST0_OUTCOME_ERROR", step="FAST0_OUTCOME", extra={"trade_id": trade_id})
    finally:
        with _fast0_watchers_lock:
            _active_fast0_watchers -= 1


def run_fast0_for_symbol(
    symbol: str,
    run_id: str,
    pump_ts: str,
    mode: str = "live",
    max_ticks: Optional[int] = None,
    base_dir: Union[str, Path, None] = None,
) -> None:
    """
    Run fast stage-0 sampling loop for symbol. Writes events to short_pump_fast0 strategy.
    No entry decisions, no trading. Data for later EV/WR analysis.
    base_dir: datasets root (e.g. /root/pump_short/datasets). If None, uses CWD/datasets.
    """
    cfg = Config.from_env()
    cfg.symbol = symbol.strip().upper()
    logger = get_logger(__name__, strategy_name=STRATEGY, symbol=cfg.symbol)
    run_id = run_id or time.strftime("%Y%m%d_%H%M%S")
    base_dir_str = str(base_dir) if base_dir else None

    register_symbol(cfg.symbol)
    entry_ok_fired = False
    try:
        start_ts = time.time()
        tick = 0
        st = StructureState(stage=0)

        now_utc_start = wall_time_utc()
        ensure_dataset_files(STRATEGY, mode, now_utc_start, schema_version=3, base_dir=base_dir_str)

        log_info(
            logger,
            "FAST0_START",
            symbol=cfg.symbol,
            run_id=run_id,
            step="FAST0",
            extra={
                "pump_ts": pump_ts,
                "window_sec": FAST0_WINDOW_SEC,
                "poll_sec": FAST0_POLL_SECONDS,
                "FAST0_DATASET_ROOT": base_dir_str or "(cwd)/datasets",
            },
        )
        logger.info("FAST0_DATASET_ROOT=%s", base_dir_str or os.path.join(os.getcwd(), "datasets"))

        while (time.time() - start_ts) < FAST0_WINDOW_SEC and (max_ticks is None or tick < max_ticks):
            try:
                time.sleep(FAST0_POLL_SECONDS)
                tick += 1
                now_ts = time.time()
                now_utc = wall_time_utc()

                # Fetch data
                candles_5m = get_klines_5m(cfg.category, cfg.symbol, limit=250)
                candles_1m = get_klines_1m(cfg.category, cfg.symbol, limit=60)
                oi = get_open_interest(cfg.category, cfg.symbol, limit=80)
                trades = get_recent_trades(cfg.category, cfg.symbol, limit=1000)
                funding_payload = get_funding_rate(cfg.category, cfg.symbol)
                funding_rate, funding_rate_ts_utc = normalize_funding(funding_payload)
                funding_rate_abs = abs(funding_rate) if funding_rate is not None else None

                if candles_5m is None or candles_5m.empty:
                    continue
                peak_price = float(candles_5m["high"].tail(20).max())
                last_price = float(candles_5m.iloc[-1]["close"])
                st.peak_price = peak_price

                candles_5m_list = candles_5m.to_dict("records")
                oi_dict = {"oi_df": oi} if oi is not None and not oi.empty else None
                trades_list = trades.to_dict("records") if trades is not None and not trades.empty else []
                dbg5 = build_dbg5(cfg, candles_5m_list, oi_dict, trades_list, st)
                context_score, _ = compute_context_score_5m(dbg5)

                # Liquidation stats
                liq_short_count_30s, liq_short_usd_30s = get_liq_stats(cfg.symbol, now_ts, 30, side="short")
                liq_short_count_1m, liq_short_usd_1m = get_liq_stats(cfg.symbol, now_ts, 60, side="short")
                liq_long_count_30s, liq_long_usd_30s = get_liq_stats(cfg.symbol, now_ts, 30, side="long")
                liq_long_count_1m, liq_long_usd_1m = get_liq_stats(cfg.symbol, now_ts, 60, side="long")

                # CVD ratios
                since_30s = pd.Timestamp.now(tz="UTC") - pd.Timedelta(seconds=30)
                since_1m = pd.Timestamp.now(tz="UTC") - pd.Timedelta(seconds=60)
                trades_df = trades if trades is not None and not trades.empty else pd.DataFrame()
                cvd_30s = cvd_delta_ratio(trades_df, since_30s) if not trades_df.empty else None
                cvd_1m = cvd_delta_ratio(trades_df, since_1m) if not trades_df.empty else None

                # OI changes
                oi_change_5m = oi_change_pct(oi, lookback_minutes=5) if oi is not None and not oi.empty else None
                oi_change_1m = oi_change_pct(oi, lookback_minutes=1) if oi is not None and not oi.empty else None

                # Volumes
                volume_5m = _volume_from_candles(candles_5m, 1) if candles_5m is not None else 0.0
                volume_1m = _volume_from_candles(candles_1m, 1) if candles_1m is not None else 0.0
                vol_z = dbg5.get("vol_z")

                # Orderbook
                ob_imbalance, spread_bps = _orderbook_imbalance_and_spread(cfg.category, cfg.symbol, 10)

                dist_to_peak = (peak_price - last_price) / peak_price * 100.0 if peak_price > 0 else 0.0

                payload: Dict[str, Any] = {
                    "time_utc": dbg5.get("time_utc", now_utc),
                    "price": last_price,
                    "dist_to_peak_pct": dist_to_peak,
                    "context_score": context_score,
                    "cvd_delta_ratio_30s": cvd_30s,
                    "cvd_delta_ratio_1m": cvd_1m,
                    "oi_change_5m_pct": oi_change_5m,
                    "oi_change_1m_pct": oi_change_1m,
                    "oi_change_fast_pct": None,
                    "funding_rate": funding_rate,
                    "funding_rate_abs": funding_rate_abs,
                    "funding_rate_ts_utc": funding_rate_ts_utc,
                    "liq_short_count_30s": liq_short_count_30s,
                    "liq_short_usd_30s": liq_short_usd_30s,
                    "liq_long_count_30s": liq_long_count_30s,
                    "liq_long_usd_30s": liq_long_usd_30s,
                    "liq_short_count_1m": liq_short_count_1m,
                    "liq_short_usd_1m": liq_short_usd_1m,
                    "liq_long_count_1m": liq_long_count_1m,
                    "liq_long_usd_1m": liq_long_usd_1m,
                    "volume_1m": volume_1m,
                    "volume_5m": volume_5m,
                    "volume_zscore": vol_z,
                    "orderbook_imbalance_10": ob_imbalance,
                    "spread_bps": spread_bps,
                    "fast0": True,
                    "pump_ts": pump_ts,
                    "tick": tick,
                }

                event_id = f"{run_id}_fast0_{tick}_{uuid.uuid4().hex[:8]}"

                entry_ok_pass, entry_reason = should_fast0_entry_ok(payload, tick)
                if not entry_ok_pass:
                    log_info(
                        logger,
                        "FAST0_ENTRY_FILTERED",
                        symbol=cfg.symbol,
                        run_id=run_id,
                        step="FAST0",
                        extra={"reason": entry_reason, "tick": tick, "context_score": context_score, "dist_to_peak_pct": dist_to_peak},
                    )
                elif entry_ok_fired:
                    log_info(
                        logger,
                        "FAST0_ENTRY_SUPPRESSED",
                        symbol=cfg.symbol,
                        run_id=run_id,
                        step="FAST0",
                        extra={"tick": tick, "reason": "already_fired"},
                    )
                entry_ok = entry_ok_pass and not entry_ok_fired
                if entry_ok:
                    entry_ok_fired = True
                payload["entry_ok"] = entry_ok

                row = {
                    "run_id": run_id,
                    "event_id": event_id,
                    "symbol": cfg.symbol,
                    "strategy": STRATEGY,
                    "mode": mode,
                    "side": "SHORT",
                    "wall_time_utc": now_utc,
                    "time_utc": payload.get("time_utc", ""),
                    "stage": 0,
                    "entry_ok": 1 if entry_ok else 0,
                    "skip_reasons": "" if entry_ok else "fast0_sample",
                    "context_score": context_score if context_score is not None else "",
                    "price": last_price,
                    "dist_to_peak_pct": dist_to_peak,
                    "cvd_delta_ratio_30s": payload.get("cvd_delta_ratio_30s", ""),
                    "cvd_delta_ratio_1m": payload.get("cvd_delta_ratio_1m", ""),
                    "oi_change_5m_pct": payload.get("oi_change_5m_pct", ""),
                    "oi_change_1m_pct": payload.get("oi_change_1m_pct", ""),
                    "oi_change_fast_pct": "",
                    "funding_rate": payload.get("funding_rate", ""),
                    "funding_rate_abs": payload.get("funding_rate_abs", ""),
                    "liq_short_count_30s": liq_short_count_30s,
                    "liq_short_usd_30s": liq_short_usd_30s,
                    "liq_long_count_30s": liq_long_count_30s,
                    "liq_long_usd_30s": liq_long_usd_30s,
                    "liq_short_count_1m": liq_short_count_1m,
                    "liq_short_usd_1m": liq_short_usd_1m,
                    "liq_long_count_1m": liq_long_count_1m,
                    "liq_long_usd_1m": liq_long_usd_1m,
                    "outcome_label": "",
                    "payload_json": json.dumps(payload, ensure_ascii=False),
                }

                write_event_row(
                    row,
                    strategy=STRATEGY,
                    mode=mode,
                    wall_time_utc=now_utc,
                    schema_version=3,
                    base_dir=base_dir_str,
                )

                if entry_ok:
                    entry_price = last_price
                    tp_price = entry_price * (1.0 - FAST0_TP_PCT)
                    sl_price = entry_price * (1.0 + FAST0_SL_PCT)
                    trade_id = f"{event_id}_trade"
                    trade_row = {
                        "trade_id": trade_id,
                        "event_id": event_id,
                        "run_id": run_id,
                        "symbol": cfg.symbol,
                        "strategy": STRATEGY,
                        "mode": mode,
                        "side": "SHORT",
                        "entry_time_utc": payload.get("time_utc", now_utc),
                        "entry_price": entry_price,
                        "tp_price": tp_price,
                        "sl_price": sl_price,
                        "trade_type": "FAST0_PAPER",
                    }
                    entry_time_str = payload.get("time_utc", now_utc)
                    write_trade_row(
                        trade_row,
                        strategy=STRATEGY,
                        mode=mode,
                        wall_time_utc=now_utc,
                        schema_version=3,
                        base_dir=base_dir_str,
                    )
                    if FAST0_TG_ENTRY_ENABLE:
                        try:
                            send_telegram(
                                format_fast0_entry_ok(
                                    symbol=cfg.symbol,
                                    run_id=run_id,
                                    dist_to_peak_pct=dist_to_peak,
                                    context_score=context_score,
                                    cvd_30s=cvd_30s,
                                    cvd_1m=cvd_1m,
                                    liq_short_usd_30s=liq_short_usd_30s,
                                    liq_long_usd_30s=liq_long_usd_30s,
                                    ts_utc=payload.get("time_utc", now_utc),
                                    event_id=event_id,
                                ),
                                strategy=STRATEGY,
                                side="SHORT",
                                mode="FAST0",
                                event_id=event_id,
                                context_score=context_score,
                                entry_ok=True,
                                formatted=True,
                            )
                        except Exception:
                            log_exception(logger, "FAST0_TG_SEND_ERROR", symbol=cfg.symbol, run_id=run_id, step="FAST0")
                    t = threading.Thread(
                        target=_run_fast0_outcome_watcher,
                        kwargs={
                            "symbol": cfg.symbol,
                            "run_id": run_id,
                            "event_id": event_id,
                            "trade_id": trade_id,
                            "entry_price": entry_price,
                            "tp_price": tp_price,
                            "sl_price": sl_price,
                            "entry_time_utc": entry_time_str,
                            "base_dir": base_dir_str,
                            "mode": mode,
                        },
                        name=f"fast0_outcome_{cfg.symbol}_{trade_id[:16]}",
                        daemon=True,
                    )
                    t.start()
                    log_info(
                        logger,
                        "FAST0_ENTRY_OK",
                        symbol=cfg.symbol,
                        run_id=run_id,
                        step="FAST0",
                        extra={"event_id": event_id, "trade_id": trade_id},
                    )

                log_info(
                    logger,
                    "FAST0_TICK",
                    symbol=cfg.symbol,
                    run_id=run_id,
                    step="FAST0",
                    extra={"tick": tick, "event_id": event_id, "price": last_price, "dist_to_peak_pct": dist_to_peak},
                )
            except Exception as e:
                log_exception(logger, "FAST0_TICK_ERROR", symbol=cfg.symbol, run_id=run_id, step="FAST0")
                time.sleep(5)

        log_info(logger, "FAST0_DONE", symbol=cfg.symbol, run_id=run_id, step="FAST0", extra={"ticks": tick})
    finally:
        unregister_symbol(cfg.symbol)
