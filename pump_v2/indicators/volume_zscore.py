"""
Volume z-score on 5m candles — v2 port of short_pump/features.py:volume_zscore.

MIRRORS_V1_BEHAVIOR (features.py canonical, NOT context5m._volume_z nor market_features volume_zscore_20):
- tail(lookback) volumes INCLUDING current bar in mean/std.
- min rows: max(10, lookback // 3); else float('nan').
- std uses ddof=0; denominator std + 1e-9 (constant volumes → ~0.0, not None).
- Default lookback=50; input row order preserved (no sort).
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

import pandas as pd

from pump_v2.core.indicator_base import Indicator
from pump_v2.indicators._candle_utils import normalize_candles_df

V1_DEFAULT_LOOKBACK = 50


def _volume_zscore_v1_body(df: pd.DataFrame, lookback: int) -> float:
    """Exact logic from short_pump/features.py:volume_zscore (lines 8–12)."""
    v = df["volume"].tail(lookback)
    if len(v) < max(10, lookback // 3):
        return float("nan")
    return float((v.iloc[-1] - v.mean()) / (v.std(ddof=0) + 1e-9))


class VolumeZScore(Indicator):
    name = "volume_zscore"

    def __init__(self, lookback_bars: int = V1_DEFAULT_LOOKBACK):
        if lookback_bars <= 1:
            raise ValueError(f"lookback_bars must be > 1, got {lookback_bars}")
        self.lookback_bars = lookback_bars

    def compute(
        self, symbol: str, ts: datetime, history: pd.DataFrame
    ) -> Optional[float]:
        """
        Z-score of last bar volume vs tail(lookback) on 5m candles at or before ts.

        Behavior MUST match v1 short_pump/features.py:volume_zscore on the same frame.
        Returns float (possibly nan) exactly as v1 volume_zscore.
        """
        _ = symbol
        candles = normalize_candles_df(history, ts)
        if candles is None or candles.empty or "volume" not in candles.columns:
            return float("nan")
        return _volume_zscore_v1_body(candles, lookback=self.lookback_bars)
