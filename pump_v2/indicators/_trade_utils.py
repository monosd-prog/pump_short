"""Shared trade DataFrame normalization for pump_v2 indicators."""
from __future__ import annotations

from datetime import datetime

import pandas as pd


def normalize_trades_df(trades: pd.DataFrame, ts: datetime) -> pd.DataFrame:
    """Map fixture columns to v1 shape (ts, side, qty), rows with ts <= evaluation ts."""
    if trades is None or trades.empty:
        return pd.DataFrame(columns=["ts", "side", "qty"])

    df = trades.copy()
    if "ts" in df.columns:
        ts_series = pd.to_datetime(df["ts"], utc=True)
    elif "ts_ms" in df.columns:
        ts_series = pd.to_datetime(df["ts_ms"], unit="ms", utc=True)
    elif "ts_utc" in df.columns:
        ts_series = pd.to_datetime(df["ts_utc"], utc=True, format="mixed")
    else:
        return pd.DataFrame(columns=["ts", "side", "qty"])

    if "side" not in df.columns or "qty" not in df.columns:
        return pd.DataFrame(columns=["ts", "side", "qty"])

    t_end = pd.Timestamp(ts)
    if t_end.tzinfo is None:
        t_end = t_end.tz_localize("UTC")
    else:
        t_end = t_end.tz_convert("UTC")

    out = pd.DataFrame()
    out["ts"] = ts_series
    out["side"] = df["side"].astype(str)
    out["qty"] = df["qty"].astype(float)
    return out[out["ts"] <= t_end].sort_values("ts").reset_index(drop=True)
