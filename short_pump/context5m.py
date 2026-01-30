import math
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import numpy as np
import pandas as pd

from short_pump.features import volume_zscore


def atr_14_5m_pct(candles_5m: pd.DataFrame, period: int = 14) -> Optional[float]:
    """ATR(period) as % of last close. Returns None if not enough data."""
    if candles_5m is None or candles_5m.empty:
        return None

    df = candles_5m.copy()
    for c in ("high", "low", "close"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["high", "low", "close"])
    if len(df) < period + 1:
        return None

    prev_close = df["close"].shift(1)
    tr = pd.concat(
        [
            (df["high"] - df["low"]).abs(),
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    atr = tr.rolling(period).mean()
    last_atr = float(atr.iloc[-1])
    last_close = float(df["close"].iloc[-1])
    if not (last_close > 0):
        return None
    return (last_atr / last_close) * 100.0


@dataclass
class StructureState:
    stage: int = 0
    peak_price: float = 0.0
    low1: float = 0.0
    high1: float = 0.0
    low2: float = 0.0
    high2: float = 0.0
    last_stage_change_ts: Optional[pd.Timestamp] = None


def update_structure(cfg, st: StructureState, price: float, peak_price: float) -> StructureState:
    """
    Stage machine:
      0: just tracking peak
      1: first drop from peak
      2: first bounce
      3: second drop
      4: second bounce (ARMED)
    """
    if st.peak_price <= 0:
        st.peak_price = peak_price
    st.peak_price = max(st.peak_price, peak_price)

    # helper thresholds (absolute)
    drop1 = st.peak_price * (1.0 - cfg.drop1_min_pct)
    bounce1 = st.peak_price * (1.0 - cfg.drop1_min_pct + cfg.bounce1_min_pct)

    if st.stage == 0:
        if price <= drop1:
            st.stage = 1
            st.low1 = price
    elif st.stage == 1:
        st.low1 = min(st.low1, price)
        if price >= bounce1:
            st.stage = 2
            st.high1 = price
    elif st.stage == 2:
        st.high1 = max(st.high1, price)
        # second drop relative to high1
        if price <= st.high1 * (1.0 - cfg.drop2_min_pct):
            st.stage = 3
            st.low2 = price
    elif st.stage == 3:
        st.low2 = min(st.low2, price)
        if price >= st.low2 * (1.0 + cfg.bounce2_min_pct):
            st.stage = 4
            st.high2 = price
    elif st.stage == 4:
        st.high2 = max(st.high2, price)

    return st


def build_dbg5(
    cfg,
    candles_5m: pd.DataFrame,
    oi: pd.DataFrame,
    trades: pd.DataFrame,
    st: StructureState,
) -> Dict[str, Any]:
    """Build 5m debug/features snapshot. 15m CVD feature removed by design."""
    last = candles_5m.iloc[-1]
    time_utc = pd.to_datetime(last["ts"], utc=True)
    price = float(last["close"])

    peak = float(st.peak_price) if st.peak_price > 0 else float(candles_5m["high"].max())
    dist_to_peak_pct = ((peak - price) / peak * 100.0) if peak > 0 else 0.0

    # OI features (5m step): use last two OI points if available
    oi_change_pct: Optional[float] = None
    oi_divergence: Optional[bool] = None
    try:
        if oi is not None and not oi.empty and len(oi) >= 2:
            oi_now = float(oi.iloc[-1]["openInterest"])
            oi_prev = float(oi.iloc[-2]["openInterest"])
            if oi_prev > 0:
                oi_change_pct = (oi_now - oi_prev) / oi_prev * 100.0
            oi_divergence = (oi_change_pct is not None) and (oi_change_pct < 0)
    except Exception:
        pass

    # Volume z-score (5m candles)
    vol_z: Optional[float] = None
    try:
        vol_z = float(volume_zscore(candles_5m, lookback=cfg.vol_z_lookback))
    except Exception:
        pass

    return {
        "time_utc": time_utc.strftime("%Y-%m-%d %H:%M:%S%z"),
        "stage": int(st.stage),
        "price": float(price),
        "peak_price": float(peak),
        "dist_to_peak_pct": float(dist_to_peak_pct),
        "oi_change_pct": oi_change_pct,
        "oi_divergence": bool(oi_divergence) if oi_divergence is not None else None,
        "vol_z": float(vol_z) if vol_z is not None else None,
        "atr_14_5m_pct": atr_14_5m_pct(candles_5m, period=14),
    }


def compute_context_score_5m(dbg5: Dict[str, Any]) -> Tuple[float, Dict[str, float]]:
    """Context score for 5m stage. 15m CVD component removed."""
    parts: Dict[str, float] = {}

    # stage weight
    stage = int(dbg5.get("stage", 0) or 0)
    if stage >= 4:
        parts["stage"] = 0.35
    elif stage == 3:
        parts["stage"] = 0.25
    elif stage == 2:
        parts["stage"] = 0.10
    else:
        parts["stage"] = 0.0

    # near top (distance from peak)
    dist = dbg5.get("dist_to_peak_pct")
    near_top_ok = (dist is not None) and (0.5 <= float(dist) <= 12.0)
    parts["near_top"] = 0.25 if near_top_ok else 0.0

    # OI divergence (OI falling while price is near peak is a common short context)
    oi_div = dbg5.get("oi_divergence")
    parts["oi"] = 0.25 if oi_div else 0.0

    # volume condition (allow slightly negative z)
    vz = dbg5.get("vol_z")
    vol_ok = (vz is not None) and (float(vz) >= -1.5)
    parts["vol"] = 0.10 if vol_ok else 0.0

    # ATR (5m): proxy for opportunity/volatility
    atr_pct = dbg5.get("atr_14_5m_pct")
    atr_ok = (atr_pct is not None) and (float(atr_pct) >= 0.25)
    parts["atr"] = 0.05 if atr_ok else 0.0

    score = float(sum(parts.values()))
    return score, parts