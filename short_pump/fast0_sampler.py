# short_pump/fast0_sampler.py
"""Stage-0 fast sampling right after pump signal. Research/dataset only, no auto-trade."""

from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

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
from common.io_dataset import write_event_row
from common.runtime import wall_time_utc

logger = get_logger(__name__)

STRATEGY = "short_pump_fast0"
ENABLE_ORDERBOOK = os.getenv("ENABLE_ORDERBOOK", "0").strip().lower() in ("1", "true", "yes", "y", "on")
FAST0_WINDOW_SEC = int(os.getenv("FAST0_WINDOW_SEC", "180"))
FAST0_POLL_SECONDS = int(os.getenv("FAST0_POLL_SECONDS", "10"))


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


def run_fast0_for_symbol(
    symbol: str,
    run_id: str,
    pump_ts: str,
    mode: str = "live",
    max_ticks: Optional[int] = None,
) -> None:
    """
    Run fast stage-0 sampling loop for symbol. Writes events to short_pump_fast0 strategy.
    No entry decisions, no trading. Data for later EV/WR analysis.
    """
    cfg = Config.from_env()
    cfg.symbol = symbol.strip().upper()
    logger = get_logger(__name__, strategy_name=STRATEGY, symbol=cfg.symbol)
    run_id = run_id or time.strftime("%Y%m%d_%H%M%S")

    register_symbol(cfg.symbol)
    try:
        start_ts = time.time()
        tick = 0
        st = StructureState(stage=0)

        log_info(
            logger,
            "FAST0_START",
            symbol=cfg.symbol,
            run_id=run_id,
            step="FAST0",
            extra={"pump_ts": pump_ts, "window_sec": FAST0_WINDOW_SEC, "poll_sec": FAST0_POLL_SECONDS},
        )

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
                    "entry_ok": 0,
                    "skip_reasons": "fast0_sample",
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
