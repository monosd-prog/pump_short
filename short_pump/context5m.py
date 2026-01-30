# short_pump/context5m.py
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple, Optional
from datetime import datetime, timezone

import pandas as pd

# -----------------------------
# State (shared between ticks)
# -----------------------------

@dataclass
class StructureState:
    # structure stage (0..4)
    stage: int = 0

    # highest price seen since watch start
    peak_price: float = 0.0

    # rolling lows for swing logic
    low_after_peak: float = 0.0
    low_after_bounce: float = 0.0

    # how many bounces we have confirmed (0,1,2)
    bounce_count: int = 0

    # watcher uses this to send ARMED telegram only once
    armed_notified: bool = False

    # optional timestamp markers (string is enough for logging)
    armed_since_utc: str = ""


def _pct(a: float, b: float) -> float:
    """(a - b) / b in percent, safe."""
    if b == 0:
        return 0.0
    return (a - b) / b * 100.0


def update_structure(cfg: Any, st: StructureState, last_price: float, peak_price: float) -> None:
    """Update structure stage on each 5m tick.

    Defensive: getattr(...) with defaults so it won't crash if cfg lacks thresholds.
    """

    # --- thresholds (defaults tuned to match your logs) ---
    # Stage 0 -> 1: first pullback from peak
    drop1_min_pct = float(getattr(cfg, "drop1_min_pct", 3.0))

    # Stage 1 -> 2: first bounce from low_after_peak
    bounce1_min_pct = float(getattr(cfg, "bounce1_min_pct", 1.0))

    # Stage 2 -> 3: second pullback after bounce (can be shallower than drop1)
    drop2_min_pct = float(getattr(cfg, "drop2_min_pct", 2.0))

    # Stage 3 -> 4: second bounce (ARMED)
    bounce2_min_pct = float(getattr(cfg, "bounce2_min_pct", 0.8))

    # if we make a new peak — reset structure
    if peak_price and peak_price > 0:
        if peak_price > st.peak_price:
            st.peak_price = peak_price

    if st.peak_price <= 0:
        st.peak_price = peak_price or last_price or 0.0

    # dist to peak (positive below peak)
    dist_to_peak_pct = (st.peak_price - last_price) / st.peak_price * 100.0 if st.peak_price > 0 else 0.0

    # init lows
    if st.low_after_peak <= 0:
        st.low_after_peak = last_price
    if st.low_after_bounce <= 0:
        st.low_after_bounce = last_price

    # always track lowest price since peak
    st.low_after_peak = min(st.low_after_peak, last_price)

    # ----------------
    # stage transitions
    # ----------------
    if st.stage == 0:
        # wait first real pullback
        if dist_to_peak_pct >= drop1_min_pct:
            st.stage = 1
            st.low_after_peak = last_price
            st.low_after_bounce = last_price
            st.bounce_count = 0

    elif st.stage == 1:
        # track low, wait first bounce
        st.low_after_peak = min(st.low_after_peak, last_price)
        if last_price >= st.low_after_peak * (1.0 + bounce1_min_pct / 100.0):
            st.stage = 2
            st.bounce_count = 1
            st.low_after_bounce = last_price  # will be adjusted below

    elif st.stage == 2:
        # track local high after bounce
        st.low_after_bounce = max(st.low_after_bounce, last_price)
        # second pullback after bounce (use dist_to_peak as proxy; also allow dip from bounce high)
        dip_from_bounce_high_pct = (st.low_after_bounce - last_price) / st.low_after_bounce * 100.0 if st.low_after_bounce > 0 else 0.0
        if dist_to_peak_pct >= drop2_min_pct or dip_from_bounce_high_pct >= drop2_min_pct:
            st.stage = 3
            st.low_after_peak = last_price  # reuse as 'low after 2nd drop'
            st.low_after_bounce = last_price

    elif st.stage == 3:
        # track low in second drop
        st.low_after_peak = min(st.low_after_peak, last_price)
        if last_price >= st.low_after_peak * (1.0 + bounce2_min_pct / 100.0):
            st.stage = 4
            st.bounce_count = 2
            if not st.armed_since_utc:
                st.armed_since_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    # stage 4 is ARMED: we stay there until watcher finishes


# -----------------------------
# Debug row builder (5m)
# -----------------------------

def _atr_pct_14(candles_5m: List[Dict[str, Any]]) -> float:
    """ATR(14) as percent of last close, computed on 5m candles."""
    if len(candles_5m) < 15:
        return 0.0
    df = pd.DataFrame(candles_5m)
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)
    prev_close = close.shift(1)
    tr = pd.concat([(high - low).abs(),
                    (high - prev_close).abs(),
                    (low - prev_close).abs()], axis=1).max(axis=1)
    atr = tr.rolling(14).mean().iloc[-1]
    last_close = float(close.iloc[-1]) if float(close.iloc[-1]) else 0.0
    if last_close == 0 or pd.isna(atr):
        return 0.0
    return float(atr) / last_close * 100.0


def _volume_z(candles_5m: List[Dict[str, Any]], lookback: int = 48) -> float:
    if len(candles_5m) < 10:
        return 0.0
    df = pd.DataFrame(candles_5m)
    vol = df["volume"].astype(float)
    lb = min(lookback, len(vol))
    window = vol.iloc[-lb:]
    mu = float(window.mean())
    sd = float(window.std(ddof=0))
    if sd == 0:
        return 0.0
    return (float(vol.iloc[-1]) - mu) / sd


def build_dbg5(
    cfg: Any,
    candles_5m: List[Dict[str, Any]],
    oi: Optional[Dict[str, Any]],
    trades_fast: Optional[List[Dict[str, Any]]],
    st: StructureState,
) -> Dict[str, Any]:
    """Build a single 5m debug row (used for logs/csv)."""
    if not candles_5m:
        return {}

    last = candles_5m[-1]
    ts = last.get("time") or last.get("timestamp") or last.get("ts")
    if isinstance(ts, (int, float)):
        if ts > 10_000_000_000:  # ms
            dt = datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc)
        else:
            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    elif isinstance(ts, str):
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        except Exception:
            dt = datetime.now(timezone.utc)
    else:
        dt = datetime.now(timezone.utc)

    price = float(last.get("close") or last.get("price") or 0.0)
    peak = float(st.peak_price or 0.0)
    if peak <= 0:
        peak = price

    dist_to_peak_pct = (peak - price) / peak * 100.0 if peak > 0 else 0.0

    oi_change_15m_pct = None
    oi_divergence = False
    if isinstance(oi, dict):
        oi_change_15m_pct = oi.get("oi_change_15m_pct")
        oi_divergence = bool(oi.get("oi_divergence", False))

    vol_z = _volume_z(candles_5m)
    atr_14_5m_pct = _atr_pct_14(candles_5m)

    dbg5: Dict[str, Any] = {
        "run_id": getattr(cfg, "run_id", ""),
        "symbol": getattr(cfg, "symbol", ""),
        "time_utc": dt.strftime("%Y-%m-%d %H:%M:%S%z"),
        "stage": int(st.stage),
        "price": price,
        "peak_price": peak,
        "dist_to_peak_pct": dist_to_peak_pct,
        "oi_change_15m_pct": oi_change_15m_pct,
        "oi_divergence": oi_divergence,
        "vol_z": vol_z,
        "atr_14_5m_pct": atr_14_5m_pct,
    }
    return dbg5


# -----------------------------
# Context score (5m)
# -----------------------------

def compute_context_score_5m(dbg5: Dict[str, Any]) -> Tuple[float, Dict[str, float]]:
    """Compute 0..1 score and its parts from dbg5.

    Важно: parts["stage"] — это вклад (вес) в score, а не номер stage.
    """

    stage = int(dbg5.get("stage", 0))

    parts: Dict[str, float] = {
        "stage": 0.0,
        "near_top": 0.0,
        "oi": 0.0,
        "vol": 0.0,
        "atr": 0.0,
    }

    # stage weight: grows when structure matures
    if stage >= 4:
        parts["stage"] = 0.30
    elif stage >= 3:
        parts["stage"] = 0.25
    elif stage >= 2:
        parts["stage"] = 0.10
    else:
        parts["stage"] = 0.0

    # near-top: closer to peak is better for "short after pump" context
    dist = float(dbg5.get("dist_to_peak_pct") or 0.0)
    if dist <= 1.0:
        parts["near_top"] = 0.25
    elif dist <= 3.5:
        parts["near_top"] = 0.15
    else:
        parts["near_top"] = 0.0

    # oi divergence: boolean computed upstream
    if bool(dbg5.get("oi_divergence", False)):
        parts["oi"] = 0.25

    # volume spike
    vol_z = float(dbg5.get("vol_z") or 0.0)
    if vol_z >= 2.0:
        parts["vol"] = 0.10
    elif vol_z >= 1.0:
        parts["vol"] = 0.05

    # ATR filter: add only when vol enough
    atr_pct = float(dbg5.get("atr_14_5m_pct") or 0.0)
    if atr_pct >= 2.0:
        parts["atr"] = 0.05

    score = float(sum(parts.values()))
    score = max(0.0, min(1.0, score))
    return score, parts


__all__ = [
    "StructureState",
    "update_structure",
    "build_dbg5",
    "compute_context_score_5m",
]