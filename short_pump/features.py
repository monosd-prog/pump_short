# features.py
from __future__ import annotations

import pandas as pd


def volume_zscore(df: pd.DataFrame, lookback: int = 50) -> float:
    v = df["volume"].tail(lookback)
    if len(v) < max(10, lookback // 3):
        return float("nan")
    return float((v.iloc[-1] - v.mean()) / (v.std(ddof=0) + 1e-9))


def delta_ratio(trades: pd.DataFrame, since_ts: pd.Timestamp) -> float:
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

import pandas as pd

def atr(df: pd.DataFrame, period: int = 14) -> float:
    """
    Average True Range (ATR).
    df must have columns: high, low, close
    Returns last ATR value (float) or None if not enough data.
    """
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


def atr_pct(df: pd.DataFrame, period: int = 14) -> float:
    """
    ATR normalized to % of last close: atr / close.
    Returns float like 0.0123 (=1.23%) or None.
    """
    a = atr(df, period=period)
    if a is None or df is None or df.empty:
        return None
    last_close = float(df["close"].iloc[-1])
    if last_close == 0:
        return None
    return float(a / last_close)