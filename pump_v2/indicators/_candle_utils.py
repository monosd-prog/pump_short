"""Shared candle DataFrame normalization for pump_v2 indicators."""
from __future__ import annotations

from datetime import datetime

import pandas as pd

_CANDLE_COLS = ("open", "high", "low", "close", "volume")


def normalize_candles_df(history: pd.DataFrame, ts: datetime) -> pd.DataFrame:
    """Map fixture columns to v1 shape, rows with ts <= evaluation ts (input row order preserved)."""
    if history is None or history.empty:
        return pd.DataFrame(columns=["ts", *list(_CANDLE_COLS)])

    df = history.copy()
    if "ts" in df.columns:
        ts_series = pd.to_datetime(df["ts"], utc=True)
    elif "ts_utc" in df.columns:
        ts_series = pd.to_datetime(df["ts_utc"], utc=True)
    else:
        return pd.DataFrame(columns=["ts", *list(_CANDLE_COLS)])

    for c in _CANDLE_COLS:
        if c not in df.columns:
            return pd.DataFrame(columns=["ts", *list(_CANDLE_COLS)])

    t_end = pd.Timestamp(ts)
    if t_end.tzinfo is None:
        t_end = t_end.tz_localize("UTC")
    else:
        t_end = t_end.tz_convert("UTC")

    out = pd.DataFrame()
    out["ts"] = ts_series
    for c in _CANDLE_COLS:
        out[c] = df[c].astype(float)
    # MIRRORS_V1_BEHAVIOR: short_pump/features.py:atr does not sort the input frame.
    return out[out["ts"] <= t_end].reset_index(drop=True)
