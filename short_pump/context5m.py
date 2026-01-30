# short_pump/context5m.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple, Union


@dataclass
class StructureState:
    """
    Minimal state used by watcher/runtime while scanning 5m structure.
    Keep it small and backwards-compatible.
    """
    stage: int = 0
    peak_price: float = 0.0
    peak_ts: Optional[str] = None

    # watcher expects this field (you had AttributeError before)
    armed_notified: bool = False


TimeLike = Union[str, datetime]


def _to_utc_str(ts: Optional[TimeLike]) -> str:
    """
    Normalize time to string like: 'YYYY-mm-dd HH:MM:SS+0000'
    """
    if ts is None:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S%z")
    if isinstance(ts, datetime):
        dt = ts.astimezone(timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S%z")
    # assume already string
    return str(ts)


def build_dbg5(
    *,
    run_id: str,
    symbol: str,
    time_utc: Optional[TimeLike],
    stage: int,
    price: float,
    peak_price: float,
    oi_change_15m_pct: Optional[float] = None,   # legacy column (can be None)
    oi_divergence: bool = False,
    vol_z: Optional[float] = None,
    atr_14_5m_pct: Optional[float] = None,
    wall_time_utc: Optional[TimeLike] = None,
    candle_lag_sec: Optional[float] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Build debug row for 5m CSV/logging.
    Signature is keyword-only on purpose to be tolerant to watcher changes.
    """
    pp = float(peak_price or 0.0)
    pr = float(price or 0.0)

    if pp > 0:
        dist_to_peak_pct = max(0.0, (pp - pr) / pp * 100.0)
    else:
        dist_to_peak_pct = 0.0

    row: Dict[str, Any] = {
        "run_id": run_id,
        "symbol": symbol,
        "time_utc": _to_utc_str(time_utc),
        "stage": int(stage),
        "price": pr,
        "peak_price": pp,
        "dist_to_peak_pct": dist_to_peak_pct,

        # legacy / optional
        "oi_change_15m_pct": oi_change_15m_pct if oi_change_15m_pct is not None else "",
        "oi_divergence": bool(oi_divergence),

        "vol_z": float(vol_z) if vol_z is not None else "",
        "atr_14_5m_pct": float(atr_14_5m_pct) if atr_14_5m_pct is not None else "",
        "wall_time_utc": _to_utc_str(wall_time_utc),
        "candle_lag_sec": float(candle_lag_sec) if candle_lag_sec is not None else "",
    }

    if extra:
        # do not break csv schema too much; keep in one cell if needed by caller
        row["extra"] = extra

    return row


def compute_context_score_5m(dbg5: Dict[str, Any]) -> Tuple[float, Dict[str, float]]:
    """
    Returns:
      score in [0..1]
      parts dict with contributions (points), not the stage number.
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
    if stage >= 4:
        parts["stage"] = 0.30
    elif stage == 3:
        parts["stage"] = 0.25
    elif stage == 2:
        parts["stage"] = 0.10
    else:
        parts["stage"] = 0.00

    # ---- near_top contribution
    if dist_to_peak_pct <= 1.0:
        parts["near_top"] = 0.25
    elif dist_to_peak_pct <= 3.5:
        parts["near_top"] = 0.25
    else:
        parts["near_top"] = 0.00

    # ---- OI divergence contribution
    if oi_divergence:
        parts["oi"] = 0.25

    # ---- volume contribution
    if vol_z >= 1.0:
        parts["vol"] = 0.10
    else:
        parts["vol"] = 0.10 if abs(vol_z) >= 2.0 else 0.00

    # ---- ATR contribution
    if atr_14_5m_pct >= 2.0:
        parts["atr"] = 0.05

    score = sum(parts.values())
    if score < 0:
        score = 0.0
    if score > 1.0:
        score = 1.0

    return score, parts


__all__ = [
    "StructureState",
    "build_dbg5",
    "compute_context_score_5m",
]