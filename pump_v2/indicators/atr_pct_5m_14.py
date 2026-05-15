"""
ATR % on 5m candles — v2 port of short_pump/features.py:atr and atr_pct.

MIRRORS_V1_BEHAVIOR (features.py canonical, NOT context5m._atr_pct_14):
- True Range: max(high-low, |high-prev_close|, |low-prev_close|); tr1 without abs on (high-low).
- ATR = rolling(period).mean() on TR (SMA, not EWM).
- atr_pct = atr / last_close as FRACTION (e.g. 0.001288), not percent (*100).
- Needs period+1 rows; insufficient data → None; input row order preserved (no sort).
- context5m._atr_pct_14 uses list[dict], returns 0.0 when short, and multiplies by 100 — different path.
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

import pandas as pd

from pump_v2.core.indicator_base import Indicator
from pump_v2.indicators._candle_utils import normalize_candles_df


def _atr_v1_body(df: pd.DataFrame, period: int) -> Optional[float]:
    """Exact logic from short_pump/features.py:atr (lines 30–56)."""
    if df is None or df.empty:
        return None
    need = period + 1
    if len(df) < need:
        return None

    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)

    prev_close = close.shift(1)

    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()

    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr_series = tr.rolling(period).mean()

    val = atr_series.iloc[-1]
    return None if pd.isna(val) else float(val)


def _atr_pct_v1_body(df: pd.DataFrame, period: int) -> Optional[float]:
    """Exact logic from short_pump/features.py:atr_pct (lines 59–70)."""
    a = _atr_v1_body(df, period=period)
    if a is None or df is None or df.empty:
        return None
    last_close = float(df["close"].iloc[-1])
    if last_close == 0:
        return None
    return float(a / last_close)


class ATRPct5m14(Indicator):
    name = "atr_pct_5m_14"

    def __init__(self, period: int = 14, timeframe: str = "5m"):
        if period <= 0:
            raise ValueError(f"period must be positive, got {period}")
        tf = str(timeframe).strip().lower()
        if tf != "5m":
            raise NotImplementedError(f"only timeframe='5m' supported in phase 2, got {timeframe!r}")
        self.period = period
        self.timeframe = tf

    def compute(self, symbol: str, ts: datetime, history: pd.DataFrame) -> Optional[float]:
        """
        ATR(period) / last close on 5m candles at or before ts.

        Behavior MUST match v1 short_pump/features.py:atr_pct on the same frame.
        """
        _ = symbol
        candles = normalize_candles_df(history, ts)
        return _atr_pct_v1_body(candles, period=self.period)
