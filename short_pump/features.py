# features.py
from __future__ import annotations

from typing import Optional
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


def oi_change_pct(oi_df: pd.DataFrame, lookback_minutes: int = 5) -> Optional[float]:
    """
    Calculate OI change percentage over lookback period.
    
    Args:
        oi_df: DataFrame with columns: ts, openInterest (or similar)
        lookback_minutes: Lookback period in minutes
    
    Returns:
        Percentage change (e.g., -2.5 for -2.5%) or None if insufficient data
    """
    if oi_df is None or oi_df.empty:
        return None
    
    if "openInterest" not in oi_df.columns:
        return None
    
    if len(oi_df) < 2:
        return None
    
    # Get current and lookback OI
    now_ts = oi_df["ts"].iloc[-1]
    lookback_ts = now_ts - pd.Timedelta(minutes=lookback_minutes)
    
    # Find OI at lookback time
    past_oi_rows = oi_df[oi_df["ts"] <= lookback_ts]
    if past_oi_rows.empty:
        # Use first available row if lookback is too far
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


def oi_divergence_5m(oi_change_5m_pct: Optional[float], dist_to_peak_pct: float) -> bool:
    """
    Detect OI divergence for 5m context.
    Divergence: price near top (dist <= 3.5%) AND OI is falling (negative change).
    
    Args:
        oi_change_5m_pct: OI change percentage over 5m (can be None)
        dist_to_peak_pct: Distance to peak in percentage
    
    Returns:
        True if divergence detected, False otherwise
    """
    if oi_change_5m_pct is None:
        return False
    
    # Price near top (within 3.5% of peak)
    near_top = dist_to_peak_pct <= 3.5
    
    # OI is falling (negative change)
    oi_falling = oi_change_5m_pct < 0
    
    return near_top and oi_falling


def cvd_delta_ratio(trades: pd.DataFrame, since_ts: pd.Timestamp) -> Optional[float]:
    """
    Calculate CVD delta ratio (same as delta_ratio but returns None if insufficient data).
    Used for CVD features in ARMED/FAST modes.
    
    Args:
        trades: DataFrame with columns: ts, side, qty
        since_ts: Timestamp to look back from
    
    Returns:
        Delta ratio (buy - sell) / (buy + sell) or None if insufficient data
    """
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


def compute_cvd_part(
    cvd_30s: Optional[float],
    cvd_1m: Optional[float],
    cvd_30s_max: float,
    cvd_1m_max: float,
    cvd_weight: float,
) -> float:
    """
    Compute CVD contribution to context_score.
    
    Args:
        cvd_30s: CVD delta ratio for 30s (can be None)
        cvd_1m: CVD delta ratio for 1m (can be None)
        cvd_30s_max: Threshold for 30s (e.g., -0.12)
        cvd_1m_max: Threshold for 1m (e.g., -0.05)
        cvd_weight: Weight to apply if conditions met (e.g., 0.2)
    
    Returns:
        CVD part (0.0 to cvd_weight) or 0.0 if invalid
    """
    if cvd_30s is None or cvd_1m is None:
        return 0.0
    
    # Both must pass thresholds (for SHORT: negative delta is good)
    if cvd_30s <= cvd_30s_max and cvd_1m <= cvd_1m_max:
        return float(cvd_weight)
    
    return 0.0


def normalize_funding(payload: Optional[dict]) -> tuple[Optional[float], Optional[str]]:
    """
    Normalize funding payload to (funding_rate, funding_rate_ts_utc).
    Returns (None, None) if not available.
    """
    if not payload:
        return None, None

    rate = None
    ts_utc = None

    if "fundingRate" in payload:
        try:
            rate = float(payload.get("fundingRate"))
        except Exception:
            rate = None
    elif "funding_rate" in payload:
        try:
            rate = float(payload.get("funding_rate"))
        except Exception:
            rate = None

    ts_val = payload.get("nextFundingTime") or payload.get("fundingRateTimestamp") or payload.get("time")
    if isinstance(ts_val, (int, float)):
        ts_f = float(ts_val)
        if ts_f > 10_000_000_000:  # ms
            ts_f = ts_f / 1000.0
        try:
            ts_utc = pd.Timestamp.fromtimestamp(ts_f, tz="UTC").isoformat()
        except Exception:
            ts_utc = None
    elif isinstance(ts_val, str):
        ts_utc = ts_val

    return rate, ts_utc