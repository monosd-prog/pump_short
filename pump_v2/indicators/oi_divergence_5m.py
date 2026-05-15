"""
OI divergence 5m — v2 composite port of short_pump/features.py:oi_divergence_5m.

MIRRORS_V1_BEHAVIOR:
- Divergence when dist_to_peak_pct <= 3.5 AND oi_change_5m_pct < 0.
- oi_change_5m_pct is None → False (not None).
- dist_to_peak_pct is required float (FSM / build_dbg5 dist).
- near_top threshold 3.5 is hardcoded in v1 (not configurable).
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping, Optional

from pump_v2.core.indicator_base import Indicator

NEAR_TOP_PCT_THRESHOLD = 3.5


def _oi_divergence_5m_v1_body(
    oi_change_5m_pct: Optional[float],
    dist_to_peak_pct: float,
) -> bool:
    """Exact logic from short_pump/features.py:oi_divergence_5m (lines 117–138)."""
    if oi_change_5m_pct is None:
        return False
    near_top = float(dist_to_peak_pct) <= NEAR_TOP_PCT_THRESHOLD
    oi_falling = float(oi_change_5m_pct) < 0
    return near_top and oi_falling


class OIDivergence5m(Indicator):
    name = "oi_divergence_5m"

    def compute(
        self,
        symbol: str,
        ts: datetime,
        history: Mapping[str, Any],
    ) -> bool:
        """
        Composite from precomputed inputs:
          - oi_change_5m_pct (from OIChangePct lookback=5)
          - dist_to_peak_pct (from DistToFSMPeakPct / build_dbg5)

        Returns bool exactly as v1 oi_divergence_5m.
        """
        _ = symbol, ts
        if not isinstance(history, dict):
            raise TypeError(f"history must be dict, got {type(history).__name__}")
        if "oi_change_5m_pct" not in history or "dist_to_peak_pct" not in history:
            raise KeyError('history must contain "oi_change_5m_pct" and "dist_to_peak_pct"')
        oi_chg = history["oi_change_5m_pct"]
        dist = history["dist_to_peak_pct"]
        if dist is None:
            raise ValueError("dist_to_peak_pct must not be None")
        return _oi_divergence_5m_v1_body(
            None if oi_chg is None else float(oi_chg),
            float(dist),
        )
