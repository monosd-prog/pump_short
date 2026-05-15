"""
CVD delta ratio — v2 port of short_pump/features.py:cvd_delta_ratio.

MIRRORS_V1_BEHAVIOR (short_pump canonical, not common/market_features duplicate):
- Returns fraction in [-1, 1]: (buy - sell) / (buy + sell).
- Empty trades, empty window, or total <= 0 → None (unlike delta_ratio which returns 0.0).
- Window: trades with ts >= (evaluation_ts - window_seconds); history filtered to ts <= evaluation_ts.
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

import pandas as pd

from pump_v2.core.indicator_base import Indicator
from pump_v2.indicators._trade_utils import normalize_trades_df


def _cvd_delta_ratio_v1_body(trades: pd.DataFrame, since_ts: pd.Timestamp) -> Optional[float]:
    """Exact logic from short_pump/features.py:cvd_delta_ratio (lines 153–167)."""
    if trades is None or trades.empty:
        return None

    x = trades[trades["ts"] >= since_ts]
    if x is None or x.empty:
        return None

    buy = x.loc[x["side"].str.lower() == "buy", "qty"].sum()
    sell = x.loc[x["side"].str.lower() == "sell", "qty"].sum()
    total = buy + sell

    if total <= 0:
        return None

    return float((buy - sell) / total)


class CVDDeltaRatio(Indicator):
    name = "cvd_delta_ratio"

    def __init__(self, window_seconds: int):
        if window_seconds <= 0:
            raise ValueError(f"window_seconds must be positive, got {window_seconds}")
        self.window_seconds = window_seconds

    def compute(self, symbol: str, ts: datetime, history: pd.DataFrame) -> Optional[float]:
        """
        Buy/sell imbalance over [ts - window_seconds, ts].

        Behavior MUST match v1 short_pump/features.py:cvd_delta_ratio.
        """
        _ = symbol
        trades = normalize_trades_df(history, ts)
        t_end = pd.Timestamp(ts)
        if t_end.tzinfo is None:
            t_end = t_end.tz_localize("UTC")
        else:
            t_end = t_end.tz_convert("UTC")
        since_ts = t_end - pd.Timedelta(seconds=self.window_seconds)
        return _cvd_delta_ratio_v1_body(trades, since_ts)
