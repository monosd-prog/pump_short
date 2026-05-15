"""
Distance to window peak — v2 port of false_pump/detector dist formula + fixture replay.

MIRRORS_V1_BEHAVIOR:
- Formula: (max_high - last_close) / max_high * 100 (false_pump/detector.py:119-121).
- Fixture canon (_generate.replay_structure): max(high) over all candles <= ts (window_minutes=None).
- With window_minutes set: max(high) over (ts - N minutes, ts] on normalized candles.
- No clamp on negative dist when close > max high.
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

import pandas as pd

from pump_v2.core.indicator_base import Indicator
from pump_v2.indicators._candle_utils import normalize_candles_df


def _dist_to_window_peak_pct_v1_body(
    candles: pd.DataFrame,
    *,
    evaluation_ts: Optional[datetime] = None,
    window_minutes: Optional[int] = None,
) -> Optional[float]:
    if candles is None or candles.empty:
        return None
    if "high" not in candles.columns or "close" not in candles.columns:
        return None

    df = candles.sort_values("ts").reset_index(drop=True)
    if evaluation_ts is not None:
        t_end = pd.Timestamp(evaluation_ts)
        if t_end.tzinfo is None:
            t_end = t_end.tz_localize("UTC")
        else:
            t_end = t_end.tz_convert("UTC")
        df = df[df["ts"] <= t_end]
        if df.empty:
            return None
    else:
        t_end = df["ts"].iloc[-1]

    if window_minutes is not None:
        start = t_end - pd.Timedelta(minutes=int(window_minutes))
        df = df[(df["ts"] > start) & (df["ts"] <= t_end)]
        if df.empty:
            return None

    win_high = float(df["high"].max())
    last_close = float(df["close"].iloc[-1])
    if win_high <= 0:
        return None
    return float((win_high - last_close) / win_high * 100.0)


class DistToWindowPeakPct(Indicator):
    name = "dist_to_window_peak_pct"

    def __init__(self, window_minutes: Optional[int] = None):
        """
        window_minutes=None: max(high) over all candles <= ts (fixture / replay_structure canon).
        window_minutes>0: rolling window in minutes before evaluation ts.
        """
        if window_minutes is not None and window_minutes <= 0:
            raise ValueError(f"window_minutes must be > 0 when set, got {window_minutes}")
        self.window_minutes = window_minutes

    def compute(
        self,
        symbol: str,
        ts: datetime,
        history: pd.DataFrame,
    ) -> Optional[float]:
        _ = symbol
        candles = normalize_candles_df(history, ts)
        if candles.empty:
            return None
        return _dist_to_window_peak_pct_v1_body(
            candles,
            evaluation_ts=ts,
            window_minutes=self.window_minutes,
        )
