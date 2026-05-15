"""
Distance to FSM peak — v2 port of short_pump/context5m.py:build_dbg5 dist_to_peak_pct.

MIRRORS_V1_BEHAVIOR:
- peak = StructureState.peak_price; if peak <= 0 → peak = current_price (build_dbg5).
- dist_pct = (peak - current_price) / peak * 100 when peak > 0, else 0.0.
- Positive when price below peak; negative when price above peak (no clamp).
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping, Optional

from pump_v2.core.indicator_base import Indicator
from pump_v2.indicators.structure_state import StructureState


def _dist_to_fsm_peak_pct_v1_body(
    structure_state: StructureState,
    current_price: float,
) -> float:
    """Exact logic from build_dbg5 lines 199–204."""
    peak = float(structure_state.peak_price or 0.0)
    price = float(current_price)
    if peak <= 0:
        peak = price
    return (peak - price) / peak * 100.0 if peak > 0 else 0.0


class DistToFSMPeakPct(Indicator):
    name = "dist_to_fsm_peak_pct"

    def compute(
        self,
        symbol: str,
        ts: datetime,
        history: Mapping[str, Any],
    ) -> float:
        """
        history: {"structure_state": StructureState, "current_price": float}.

        Returns percent distance from FSM peak to current price.
        """
        _ = symbol, ts
        if not isinstance(history, dict):
            raise TypeError(f"history must be dict, got {type(history).__name__}")
        if "structure_state" not in history or "current_price" not in history:
            raise KeyError('history must contain "structure_state" and "current_price"')
        st = history["structure_state"]
        if st is None:
            raise ValueError("structure_state must not be None")
        if not isinstance(st, StructureState):
            raise TypeError(f"structure_state must be StructureState, got {type(st).__name__}")
        return _dist_to_fsm_peak_pct_v1_body(st, float(history["current_price"]))
