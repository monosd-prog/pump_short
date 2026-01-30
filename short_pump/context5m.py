import math
import pandas as pd
import numpy as np
from dataclasses import dataclass
from typing import Optional, Tuple, Dict, Any

from short_pump.features import volume_zscore, delta_ratio


def atr_14_5m_pct(candles_5m: pd.DataFrame, period: int = 14) -> Optional[float]:
    """ATR(period) as % of last close. Returns None if not enough data."""
    if candles_5m is None or candles_5m.empty:
        return None
    df = candles_5m.copy()
    # ensure numeric
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


def build_dbg5(cfg, candles_5m: pd.DataFrame, oi: pd.DataFrame, trades: pd.DataFrame, st: StructureState) -> Dict[str, Any]:
    last = candles_5m.iloc[-1]
    time_utc = pd.to_datetime(last["ts"], utc=True)
    price = float(last["close"])

    peak = float(st.peak_price) if st.peak_price > 0 else float(candles_5m["high"].max())
    dist_to_peak_pct = ((peak - price) / peak * 100.0) if peak > 0 else 0.0

    # OI features
    oi_change_15m_pct = None
    oi_divergence = None
    try:
        if oi is not None and not oi.empty and len(oi) >= 4:
            # approximate last 15m: 4 points if 5m cadence
            oi_now = float(oi.iloc[-1]["openInterest"])
            oi_prev = float(oi.iloc[-4]["openInterest"])
            if oi_prev > 0:
                oi_change_15m_pct = (oi_now - oi_prev) / oi_prev * 100.0
            oi_divergence = (oi_change_15m_pct is not None) and (oi_change_15m_pct < 0)
    except Exception:
        pass

    # Delta ratio (15m from trades)
    delta_ratio_15m = None
    try:
        delta_ratio_15m = float(delta_ratio(trades, seconds=15 * 60))
    except Exception:
        pass

    # Volume z-score
    vol_z = None
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
        "oi_change_15m_pct": oi_change_15m_pct,
        "oi_divergence": bool(oi_divergence) if oi_divergence is not None else None,
        "delta_ratio_15m": delta_ratio_15m,
        "vol_z": float(vol_z) if vol_z is not None else None,
        "atr_14_5m_pct": atr_14_5m_pct(candles_5m, period=14),
    }


def compute_context_score_5m(dbg5: Dict[str, Any]) -> Tuple[float, Dict[str, float]]:
    parts: Dict[str, float] = {}

    # stage weight
    stage = int(dbg5.get("stage", 0) or 0)
    if stage >= 4:
        parts["stage"] = 0.30
    elif stage == 3:
        parts["stage"] = 0.20
    elif stage == 2:
        parts["stage"] = 0.10
    else:
        parts["stage"] = 0.0

    # near top (distance from peak)
    dist = dbg5.get("dist_to_peak_pct")
    near_top_ok = (dist is not None) and (0.5 <= float(dist) <= 8.0)
    parts["near_top"] = 0.25 if near_top_ok else 0.0

    # OI divergence
    oi_div = dbg5.get("oi_divergence")
    parts["oi"] = 0.20 if oi_div else 0.0

    # delta ratio (15m) negative is good for short
    dr15 = dbg5.get("delta_ratio_15m")
    cvd_ok = (dr15 is not None) and (float(dr15) <= -0.03)
    parts["cvd15"] = 0.20 if cvd_ok else 0.0

    # volume condition
    vz = dbg5.get("vol_z")
    vol_ok = (vz is not None) and (float(vz) >= -1.5)  # allow slightly negative z
    parts["vol"] = 0.10 if vol_ok else 0.0

    # ATR (5m): proxy for opportunity/volatility
    atr_pct = dbg5.get("atr_14_5m_pct")
    atr_ok = (atr_pct is not None) and (atr_pct >= 0.25)
    parts["atr"] = 0.05 if atr_ok else 0.0

    score = float(sum(parts.values()))
    return score, parts