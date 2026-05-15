"""
OI change percent — v2 port of short_pump/features.py:oi_change_pct.

MIRRORS_V1_BEHAVIOR: uses last row of (filtered) history as "now"; lookback row is
last row with ts <= now_ts - lookback_minutes, else falls back to iloc[0] when len >= 2.
Returns percentage points (e.g. -2.5 means -2.5%), not a fraction.
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

import pandas as pd

from pump_v2.core.indicator_base import Indicator


def _normalize_oi_df(oi_history: pd.DataFrame, ts: datetime) -> pd.DataFrame:
    """Map SCHEMA columns to v1 shape (ts, openInterest), rows with ts <= evaluation ts."""
    if oi_history is None or oi_history.empty:
        return pd.DataFrame(columns=["ts", "openInterest"])

    df = oi_history.copy()
    if "openInterest" in df.columns:
        ts_col = "ts" if "ts" in df.columns else "ts_utc"
        oi_col = "openInterest"
    elif "oi" in df.columns:
        ts_col = "ts_utc" if "ts_utc" in df.columns else "ts"
        oi_col = "oi"
    else:
        return pd.DataFrame(columns=["ts", "openInterest"])

    out = pd.DataFrame()
    out["ts"] = pd.to_datetime(df[ts_col], utc=True)
    out["openInterest"] = df[oi_col].astype(float)

    t_end = pd.Timestamp(ts)
    if t_end.tzinfo is None:
        t_end = t_end.tz_localize("UTC")
    else:
        t_end = t_end.tz_convert("UTC")

    out = out[out["ts"] <= t_end].sort_values("ts").reset_index(drop=True)
    return out


def _oi_change_pct_v1_body(oi_df: pd.DataFrame, lookback_minutes: int) -> Optional[float]:
    """Exact logic from short_pump/features.py:oi_change_pct (lines 84–114)."""
    if oi_df is None or oi_df.empty:
        return None

    if "openInterest" not in oi_df.columns:
        return None

    if len(oi_df) < 2:
        return None

    now_ts = oi_df["ts"].iloc[-1]
    lookback_ts = now_ts - pd.Timedelta(minutes=lookback_minutes)

    past_oi_rows = oi_df[oi_df["ts"] <= lookback_ts]
    if past_oi_rows.empty:
        if len(oi_df) >= 2:
            past_oi = float(oi_df["openInterest"].iloc[0])
        else:
            return None
    else:
        past_oi = float(past_oi_rows["openInterest"].iloc[-1])

    current_oi = float(oi_df["openInterest"].iloc[-1])

    if past_oi == 0:
        return None

    change_pct = ((current_oi - past_oi) / past_oi) * 100.0
    return float(change_pct)


class OIChangePct(Indicator):
    name = "oi_change_pct"

    def __init__(self, lookback_minutes: int):
        if lookback_minutes <= 0:
            raise ValueError(f"lookback_minutes must be positive, got {lookback_minutes}")
        self.lookback_minutes = lookback_minutes

    def compute(
        self,
        symbol: str,
        ts: datetime,
        history: pd.DataFrame,
    ) -> Optional[float]:
        """
        OI % change from lookback_minutes before last row (at or before ts) to last row.

        Behavior MUST match v1 short_pump/features.py:oi_change_pct on the same frame.
        """
        _ = symbol  # reserved for logging / multi-symbol feeds
        oi_df = _normalize_oi_df(history, ts)
        return _oi_change_pct_v1_body(oi_df, self.lookback_minutes)
