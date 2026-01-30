# entry.py
from __future__ import annotations

import math
from typing import Any, Dict, Tuple

import numpy as np
import pandas as pd

from short_pump.features import delta_ratio


def decide_entry_1m(cfg,
                    candles_1m: pd.DataFrame,
                    trades: pd.DataFrame,
                    context_score: float,
                    ctx_parts: Dict[str, Any],
                    peak_price_5m: float) -> Tuple[bool, Dict[str, Any]]:
    if len(candles_1m) < max(cfg.break_low_lookback + 2, cfg.no_new_high_lookback + 2):
        return False, {"reason": "not_enough_1m_data"}

    last = candles_1m.iloc[-1]
    now_ts = candles_1m["ts"].iloc[-1]
    price = float(last["close"])

    n = cfg.break_low_lookback
    prev_lows = candles_1m["low"].iloc[-(n+1):-1]
    break_low = price < float(prev_lows.min())

    dr1 = delta_ratio(trades, now_ts - pd.Timedelta(minutes=1))
    dr3 = delta_ratio(trades, now_ts - pd.Timedelta(minutes=3))
    dr30 = delta_ratio(trades, now_ts - pd.Timedelta(seconds=30))

    delta_ok = (dr1 <= cfg.delta_ratio_1m_max) or (dr3 <= cfg.delta_ratio_1m_max)

    m = cfg.no_new_high_lookback
    recent_highs = candles_1m["high"].iloc[-m:]
    max_idx = int(np.argmax(recent_highs.values))
    no_new_high = (max_idx < len(recent_highs) - 1)

    dist_to_peak = (peak_price_5m - price) / peak_price_5m if peak_price_5m > 0 else 0.0
    dist_pct = dist_to_peak * 100.0
    near_top = dist_to_peak <= cfg.dist_to_peak_max_pct

    # late penalty
    if dist_pct > cfg.late_dist_pct:
        strong_delta = (dr1 <= cfg.delta_ratio_early_late_max) or (dr3 <= cfg.delta_ratio_early_late_max)
        if not strong_delta:
            delta_ok = False

    if context_score >= 0.65:
        need = 2
    elif context_score >= 0.45:
        need = 3
    else:
        need = 4

    flags = {"break_low": break_low, "delta_ok": delta_ok, "no_new_high": no_new_high, "near_top": near_top}
    hit = sum(1 for v in flags.values() if v)

    entry_ok = (flags["delta_ok"] is True) and (hit >= need)

    dbg = {
        "time_utc": str(now_ts),
        "price": price,
        "dist_to_peak_pct": dist_pct,
        "delta_ratio_1m": dr1,
        "delta_ratio_3m": dr3,
        "delta_ratio_30s": dr30,
        "entry_source": "1m",
        "need": need,
        "hit": hit,
        "flags": flags,
        "context_score": context_score,
        "context_parts": ctx_parts,
        "late_mode": dist_pct > cfg.late_dist_pct,
    }

    return entry_ok, dbg


def decide_entry_fast(cfg,
                      trades: pd.DataFrame,
                      context_score: float,
                      ctx_parts: Dict[str, Any],
                      peak_price_5m: float) -> Tuple[bool, Dict[str, Any]]:
    if trades.empty:
        return False, {"reason": "no_trades"}

    now_ts = pd.Timestamp.now(tz="UTC")

    price = float(trades.iloc[-1].get("price", np.nan))
    if math.isnan(price):
        return False, {"reason": "no_trade_price"}

    dist_to_peak = (peak_price_5m - price) / peak_price_5m if peak_price_5m > 0 else 0.0
    dist_pct = dist_to_peak * 100.0
    near_top = dist_to_peak <= cfg.dist_to_peak_max_pct

    dr30 = delta_ratio(trades, now_ts - pd.Timedelta(seconds=30))
    dr60 = delta_ratio(trades, now_ts - pd.Timedelta(seconds=60))

    delta_ok = (dr30 <= cfg.delta_ratio_30s_max) or (dr60 <= cfg.delta_ratio_30s_max)

    if dist_pct > cfg.late_dist_pct:
        strong_delta = (dr30 <= cfg.delta_ratio_fast_late_max) or (dr60 <= cfg.delta_ratio_fast_late_max)
        if not strong_delta:
            delta_ok = False

    entry_ok = (context_score >= 0.65) and near_top and delta_ok

    dbg = {
        "time_utc": str(now_ts),
        "price": price,
        "dist_to_peak_pct": dist_pct,
        "delta_ratio_1m": None,
        "delta_ratio_3m": None,
        "delta_ratio_30s": dr30,
        "need": None,
        "hit": None,
        "flags": {"break_low": False, "delta_ok": delta_ok, "no_new_high": False, "near_top": near_top},
        "context_score": context_score,
        "context_parts": ctx_parts,
        "late_mode": dist_pct > cfg.late_dist_pct,
        "entry_source": "fast",
    }
    return entry_ok, dbg