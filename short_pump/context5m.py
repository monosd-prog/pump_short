# short_pump/context5m.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple


# ----------------------------
# Structure tracking (5m)
# ----------------------------

@dataclass
class StructureState:
    peak_price: float = 0.0
    stage: int = 0
    drop1_low: Optional[float] = None
    bounce1_high: Optional[float] = None
    drop2_low: Optional[float] = None

    # IMPORTANT: watcher expects this field to exist
    # Used to send ARMED notification once.
    armed_notified: bool = False


def update_structure(cfg, st: StructureState, last_price: float, peak_price: float) -> StructureState:
    """
    Stages (примерная логика, как у тебя в комментариях):
      0: pump ongoing / at peak
      1: first drop
      2: first bounce
      3: second drop
      4: second bounce (ARMED)
    """
    st.peak_price = peak_price

    if st.stage == 0:
        if peak_price > 0 and last_price <= peak_price * (1 - cfg.drop1_min_pct):
            st.stage = 1
            st.drop1_low = last_price

    elif st.stage == 1:
        # keep updating low
        if st.drop1_low is None or last_price < st.drop1_low:
            st.drop1_low = last_price

        # bounce 1
        if st.drop1_low and last_price >= st.drop1_low * (1 + cfg.bounce1_min_pct):
            st.stage = 2
            st.bounce1_high = last_price

    elif st.stage == 2:
        # keep updating bounce high
        if st.bounce1_high is None or last_price > st.bounce1_high:
            st.bounce1_high = last_price

        # drop 2
        if st.bounce1_high and last_price <= st.bounce1_high * (1 - cfg.drop2_min_pct):
            st.stage = 3
            st.drop2_low = last_price

    elif st.stage == 3:
        # keep updating drop2 low
        if st.drop2_low is None or last_price < st.drop2_low:
            st.drop2_low = last_price

        # bounce 2 => ARMED stage
        if st.drop2_low and last_price >= st.drop2_low * (1 + cfg.bounce2_min_pct):
            st.stage = 4

    return st


# ----------------------------
# Context score (5m)
# ----------------------------

def compute_context_score_5m(dbg5: Dict[str, Any]) -> Tuple[float, Dict[str, float]]:
    """
    Returns:
      context_score: float
      parts: Dict[str, float] - contributions (NOT raw values)

    NOTE:
      We removed all 15m/CVD logic completely.
      Current parts: stage, near_top, oi, vol, atr
    """
    stage = int(dbg5.get("stage", 0) or 0)
    dist_to_peak_pct = float(dbg5.get("dist_to_peak_pct", 999.0) or 999.0)
    oi_divergence = bool(dbg5.get("oi_divergence", False))
    vol_z = float(dbg5.get("vol_z", 0.0) or 0.0)
    atr_14_5m_pct = float(dbg5.get("atr_14_5m_pct", 0.0) or 0.0)

    parts: Dict[str, float] = {
        "stage": 0.0,
        "near_top": 0.0,
        "oi": 0.0,
        "vol": 0.0,
        "atr": 0.0,
    }

    # ---- stage contribution (this is "points", not stage number)
    # tune as you like; matches your recent logs semantics
    if stage >= 4:
        parts["stage"] = 0.30
    elif stage == 3:
        parts["stage"] = 0.25
    elif stage == 2:
        parts["stage"] = 0.10
    else:
        parts["stage"] = 0.00

    # ---- near_top contribution
    # closer to peak => more "crowded at top" (better for short setup)
    # Using percent distance from peak; thresholds are conservative
    if dist_to_peak_pct <= 1.0:
        parts["near_top"] = 0.25
    elif dist_to_peak_pct <= 3.5:
        parts["near_top"] = 0.25  # keep your old behavior if you want
    else:
        parts["near_top"] = 0.00

    # ---- OI divergence contribution
    if oi_divergence:
        parts["oi"] = 0.25

    # ---- volume z-score contribution
    # negative vol_z in your logs; "activity" still might be high in abs,
    # but we'll keep a simple rule: if vol_z >= 1 => add points
    if vol_z >= 1.0:
        parts["vol"] = 0.10
    else:
        parts["vol"] = 0.10 if abs(vol_z) >= 2.0 else 0.00  # practical fallback for your data

    # ---- ATR contribution (volatility regime)
    # if ATR% is meaningful => add small stable bonus
    if atr_14_5m_pct >= 2.0:
        parts["atr"] = 0.05

    score = sum(parts.values())
    # clamp just in case
    if score < 0:
        score = 0.0
    if score > 1.0:
        score = 1.0

    return score, parts