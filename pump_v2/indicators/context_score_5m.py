"""
Context score 5m — v2 port of short_pump/context5m.py:compute_context_score_5m.

MIRRORS_V1_BEHAVIOR:
- Parts: stage, near_top, oi, vol, atr (weights sum to score; NO cvd in this function).
- vol uses dbg5["vol_z"] (context5m._volume_z lookback 48), NOT features.volume_zscore(50).
- atr uses dbg5["atr_14_5m_pct"] (context5m._atr_pct_14 percent scale), NOT features.atr_pct fraction.
- score = clamp(sum(parts), 0, 1).
- Logged events_v3 context_parts may include legacy "cvd" key from watcher — not in this canon.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Mapping, Tuple

from pump_v2.core.indicator_base import Indicator

PART_KEYS = ("stage", "near_top", "oi", "vol", "atr")


@dataclass(frozen=True)
class ContextScore5m:
    score: float
    parts: Dict[str, float]

    def as_dict(self) -> Dict[str, Any]:
        return {"score": self.score, "parts": dict(self.parts)}


def _compute_context_score_5m_v1_body(dbg5: Mapping[str, Any]) -> Tuple[float, Dict[str, float]]:
    """Exact logic from short_pump/context5m.py:compute_context_score_5m (lines 249–305)."""
    stage = int(dbg5.get("stage", 0))

    parts: Dict[str, float] = {
        "stage": 0.0,
        "near_top": 0.0,
        "oi": 0.0,
        "vol": 0.0,
        "atr": 0.0,
    }

    if stage >= 4:
        parts["stage"] = 0.30
    elif stage >= 3:
        parts["stage"] = 0.25
    elif stage >= 2:
        parts["stage"] = 0.10
    else:
        parts["stage"] = 0.0

    dist = float(dbg5.get("dist_to_peak_pct") or 0.0)
    if dist <= 1.0:
        parts["near_top"] = 0.25
    elif dist <= 3.5:
        parts["near_top"] = 0.15
    else:
        parts["near_top"] = 0.0

    oi_change_5m = dbg5.get("oi_change_5m_pct")
    if dbg5.get("oi_divergence_5m", False):
        parts["oi"] = 0.20
    elif oi_change_5m is not None and oi_change_5m < -1.0:
        parts["oi"] = 0.10

    vol_z = float(dbg5.get("vol_z") or 0.0)
    if vol_z >= 2.0:
        parts["vol"] = 0.10
    elif vol_z >= 1.0:
        parts["vol"] = 0.05

    atr_pct = float(dbg5.get("atr_14_5m_pct") or 0.0)
    if atr_pct >= 2.0:
        parts["atr"] = 0.05

    score = float(sum(parts.values()))
    score = max(0.0, min(1.0, score))
    return score, parts


def _coerce_dbg5(history: Mapping[str, Any]) -> Dict[str, Any]:
    """
    Normalize history to dbg5 keys expected by compute_context_score_5m.

    Preferred: pass build_dbg5() output (vol_z, atr_14_5m_pct).
    """
    if not isinstance(history, Mapping):
        raise TypeError(f"history must be mapping, got {type(history).__name__}")

    out: Dict[str, Any] = {}

    if "stage" in history:
        out["stage"] = int(history["stage"])
    elif "structure_state" in history and history["structure_state"] is not None:
        out["stage"] = int(getattr(history["structure_state"], "stage", 0))
    else:
        out["stage"] = 0

    out["dist_to_peak_pct"] = float(
        history.get("dist_to_peak_pct", history.get("dist_to_fsm_peak_pct", 0.0)) or 0.0
    )

    out["oi_change_5m_pct"] = history.get("oi_change_5m_pct")
    if "oi_divergence_5m" in history:
        out["oi_divergence_5m"] = bool(history["oi_divergence_5m"])
    else:
        out["oi_divergence_5m"] = False

    if "vol_z" in history:
        out["vol_z"] = float(history.get("vol_z") or 0.0)
    else:
        out["vol_z"] = 0.0

    if "atr_14_5m_pct" in history:
        out["atr_14_5m_pct"] = float(history.get("atr_14_5m_pct") or 0.0)
    elif "atr_pct_5m_14" in history and history.get("atr_pct_5m_14") is not None:
        out["atr_14_5m_pct"] = float(history["atr_pct_5m_14"]) * 100.0
    else:
        out["atr_14_5m_pct"] = 0.0

    return out


class ContextScore5mIndicator(Indicator):
    name = "context_score_5m"

    def compute(self, symbol: str, ts: datetime, history: Mapping[str, Any]) -> ContextScore5m:
        _ = symbol, ts
        dbg5 = _coerce_dbg5(history)
        score, parts = _compute_context_score_5m_v1_body(dbg5)
        return ContextScore5m(score=score, parts=parts)
