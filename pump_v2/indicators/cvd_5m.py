"""
CVD over a rolling trade window — v2 port of common/market_features.py:cvd_5m.

MIRRORS_V1_BEHAVIOR (defaults bar_size_sec=60, window_bars=5 → 5 minutes):
- Groups trades into 1-minute bars (dt.floor("1min") when bar_size_sec==60).
- cvd_abs = sum(bar_buy - bar_sell) over bars in window.
- cvd_ratio = cvd_abs / total_volume in window, or None if total_volume==0.
- Empty/missing columns/no rows in window → (None, None).
- compute() returns cvd_abs (raw qty delta), not the ratio.
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional, Tuple

import pandas as pd

from pump_v2.core.indicator_base import Indicator
from pump_v2.indicators._trade_utils import normalize_trades_df as _normalize_trades_df


def _cvd_5m_v1_body(
    trades: pd.DataFrame,
    now_ts_utc: pd.Timestamp,
    *,
    bar_size_sec: int,
    window_bars: int,
) -> Tuple[Optional[float], Optional[float]]:
    """Logic from common/market_features.py:cvd_5m (lines 276–305)."""
    if trades is None or trades.empty:
        return None, None
    required = {"ts", "side", "qty"}
    if not required.issubset(set(trades.columns)):
        return None, None
    window_sec = bar_size_sec * window_bars
    since = now_ts_utc - pd.Timedelta(seconds=window_sec)
    x = trades[trades["ts"] >= since]
    if x is None or x.empty:
        return None, None
    try:
        x = x.copy()
        x["qty"] = x["qty"].astype(float)
        side_lower = x["side"].astype(str).str.lower()
        x["_buy"] = x["qty"].where(side_lower == "buy", 0.0)
        x["_sell"] = x["qty"].where(side_lower == "sell", 0.0)
        ts_col = x["ts"]
        if ts_col.dt.tz is None:
            ts_col = ts_col.dt.tz_localize("UTC")
        # MIRRORS_V1: canonical 1m bars use floor("1min"), not generic 60s label
        if bar_size_sec == 60:
            x["_bar"] = ts_col.dt.floor("1min")
        else:
            x["_bar"] = ts_col.dt.floor(f"{int(bar_size_sec)}s")
        bars = x.groupby("_bar").agg(
            bar_buy=("_buy", "sum"),
            bar_sell=("_sell", "sum"),
        )
        bars["bar_delta"] = bars["bar_buy"] - bars["bar_sell"]
        cvd_abs = float(bars["bar_delta"].sum())
        total_volume = float((bars["bar_buy"] + bars["bar_sell"]).sum())
        cvd_ratio = float(cvd_abs / total_volume) if total_volume > 0 else None
        return cvd_abs, cvd_ratio
    except Exception:
        return None, None


class CVD5m(Indicator):
    name = "cvd_5m"

    def __init__(self, bar_size_sec: int = 60, window_bars: int = 5):
        if bar_size_sec <= 0:
            raise ValueError(f"bar_size_sec must be positive, got {bar_size_sec}")
        if window_bars <= 0:
            raise ValueError(f"window_bars must be positive, got {window_bars}")
        self.bar_size_sec = bar_size_sec
        self.window_bars = window_bars

    def compute_full(
        self, symbol: str, ts: datetime, history: pd.DataFrame
    ) -> Tuple[Optional[float], Optional[float]]:
        """Returns (cvd_abs, cvd_ratio) like v1 cvd_5m()."""
        _ = symbol
        trades = _normalize_trades_df(history, ts)
        t_end = pd.Timestamp(ts)
        if t_end.tzinfo is None:
            t_end = t_end.tz_localize("UTC")
        else:
            t_end = t_end.tz_convert("UTC")
        return _cvd_5m_v1_body(
            trades,
            t_end,
            bar_size_sec=self.bar_size_sec,
            window_bars=self.window_bars,
        )

    def compute(self, symbol: str, ts: datetime, history: pd.DataFrame) -> Optional[float]:
        """Raw cumulative volume delta (base-asset qty) over the window."""
        cvd_abs, _ = self.compute_full(symbol, ts, history)
        return cvd_abs
