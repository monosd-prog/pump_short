"""
Trade delta ratio — v2 port of short_pump/features.py:delta_ratio.

MIRRORS_V1_BEHAVIOR:
- Returns a fraction in [-1, 1], NOT percent ((buy - sell) / (buy + sell)).
- Empty trades, no rows in window, or zero total volume → 0.0 (never None).
- Window is trades with ts >= (evaluation_ts - window_seconds); history pre-filtered to ts <= evaluation_ts.
"""
from __future__ import annotations

from datetime import datetime

import pandas as pd

from pump_v2.core.indicator_base import Indicator
from pump_v2.indicators._trade_utils import normalize_trades_df as _normalize_trades_df


def _delta_ratio_v1_body(trades: pd.DataFrame, since_ts: pd.Timestamp) -> float:
    """Exact logic from short_pump/features.py:delta_ratio (lines 15–26)."""
    if trades.empty:
        return 0.0
    x = trades[trades["ts"] >= since_ts]
    if x.empty:
        return 0.0
    buy = x.loc[x["side"].str.lower() == "buy", "qty"].sum()
    sell = x.loc[x["side"].str.lower() == "sell", "qty"].sum()
    total = buy + sell
    if total <= 0:
        return 0.0
    return float((buy - sell) / total)


class DeltaRatio(Indicator):
    name = "delta_ratio"

    def __init__(self, window_seconds: int):
        if window_seconds <= 0:
            raise ValueError(f"window_seconds must be positive, got {window_seconds}")
        self.window_seconds = window_seconds

    def compute(self, symbol: str, ts: datetime, history: pd.DataFrame) -> float:
        """
        Buy/sell imbalance over [ts - window_seconds, ts] on trade tape.

        Behavior MUST match v1 short_pump/features.py:delta_ratio on the same frame.
        """
        _ = symbol
        trades = _normalize_trades_df(history, ts)
        t_end = pd.Timestamp(ts)
        if t_end.tzinfo is None:
            t_end = t_end.tz_localize("UTC")
        else:
            t_end = t_end.tz_convert("UTC")
        since_ts = t_end - pd.Timedelta(seconds=self.window_seconds)
        return _delta_ratio_v1_body(trades, since_ts)
