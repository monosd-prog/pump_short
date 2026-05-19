"""
Dbg5 bundle builder — v2 composite port of short_pump/context5m.py:build_dbg5.

MIRRORS_V1_BEHAVIOR:
- vol_z via context5m._volume_z (lookback 48, ddof=0), NOT features.volume_zscore.
- atr_14_5m_pct via context5m._atr_pct_14 (percent scale), NOT features.atr_pct fraction.
- dist_to_peak_pct from StructureState.peak_price vs last close (FSM peak).
- oi_change_5m_pct / oi_divergence_5m via features.oi_change_pct / oi_divergence_5m.
- Returns only the 6 keys consumed by compute_context_score_5m.
"""
from __future__ import annotations

import dataclasses
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional

import pandas as pd

from pump_v2.core.indicator_base import Indicator
from pump_v2.indicators.dist_to_fsm_peak_pct import _dist_to_fsm_peak_pct_v1_body
from pump_v2.indicators.oi_change_pct import _normalize_oi_df, _oi_change_pct_v1_body
from pump_v2.indicators.oi_divergence_5m import _oi_divergence_5m_v1_body
from pump_v2.indicators.structure_state import StructureState


def _volume_z(candles_5m: List[Dict[str, Any]], lookback: int = 48) -> float:
    """Exact port of short_pump/context5m.py:_volume_z."""
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


def _atr_pct_14(candles_5m: List[Dict[str, Any]]) -> float:
    """Exact port of short_pump/context5m.py:_atr_pct_14."""
    if len(candles_5m) < 15:
        return 0.0
    df = pd.DataFrame(candles_5m)
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)
    prev_close = close.shift(1)
    tr = pd.concat(
        [
            (high - low).abs(),
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    atr = tr.rolling(14).mean().iloc[-1]
    last_close = float(close.iloc[-1]) if float(close.iloc[-1]) else 0.0
    if last_close == 0 or pd.isna(atr):
        return 0.0
    return float(atr) / last_close * 100.0


def _candles_to_list(candles: Any) -> List[Dict[str, Any]]:
    """Accept list[dict] or DataFrame with OHLCV columns."""
    if candles is None:
        return []
    if isinstance(candles, list):
        return [dict(r) for r in candles]
    if isinstance(candles, pd.DataFrame):
        if candles.empty:
            return []
        df = candles.copy()
        if "ts_utc" in df.columns and "ts" not in df.columns:
            df["ts"] = pd.to_datetime(df["ts_utc"], utc=True)
        return df.to_dict("records")
    raise TypeError(f"candles_5m must be list or DataFrame, got {type(candles).__name__}")


@dataclass(frozen=True)
class Dbg5Bundle:
    """Fields read by compute_context_score_5m / ContextScore5mIndicator._coerce_dbg5."""

    stage: int
    dist_to_peak_pct: float
    oi_change_5m_pct: Optional[float]
    oi_divergence_5m: bool
    vol_z: float
    atr_14_5m_pct: float

    def as_dict(self) -> Dict[str, Any]:
        """Compatible with ContextScore5mIndicator history / _coerce_dbg5."""
        return dataclasses.asdict(self)


class Dbg5BuilderIndicator(Indicator):
    name = "dbg5_builder"

    def compute(
        self,
        symbol: str,
        ts: datetime,
        history: Mapping[str, Any],
    ) -> Dbg5Bundle:
        """
        Build dbg5-shaped bundle from history.

        history keys:
          - candles_5m: list[dict] or pd.DataFrame (OHLCV)
          - structure_state: StructureState (stage + peak_price)
          - oi_history: optional pd.DataFrame (ts_utc/ts + oi/openInterest)
          - oi_df: optional v1-shaped DataFrame (ts, openInterest) — used if oi_history absent
        """
        _ = symbol
        if not isinstance(history, Mapping):
            raise TypeError(f"history must be mapping, got {type(history).__name__}")

        candles_5m = _candles_to_list(history.get("candles_5m"))
        if not candles_5m:
            return Dbg5Bundle(
                stage=0,
                dist_to_peak_pct=0.0,
                oi_change_5m_pct=None,
                oi_divergence_5m=False,
                vol_z=0.0,
                atr_14_5m_pct=0.0,
            )

        st = history.get("structure_state")
        if st is None:
            raise KeyError('history must contain "structure_state"')
        if not isinstance(st, StructureState):
            raise TypeError(f"structure_state must be StructureState, got {type(st).__name__}")

        last = candles_5m[-1]
        price = float(last.get("close") or last.get("price") or 0.0)
        stage = int(st.stage)
        dist_to_peak_pct = _dist_to_fsm_peak_pct_v1_body(st, price)

        oi_change_5m_pct: Optional[float] = None
        oi_divergence_5m_val = False

        oi_v1 = history.get("oi_df")
        if oi_v1 is None and history.get("oi_history") is not None:
            oi_raw = history.get("oi_history")
            if isinstance(oi_raw, pd.DataFrame) and not oi_raw.empty:
                oi_v1 = _normalize_oi_df(oi_raw, ts)
            elif isinstance(oi_raw, dict) and oi_raw.get("oi_df") is not None:
                oi_v1 = oi_raw["oi_df"]

        if isinstance(oi_v1, pd.DataFrame) and not oi_v1.empty:
            oi_change_5m_pct = _oi_change_pct_v1_body(oi_v1, lookback_minutes=5)
            oi_divergence_5m_val = _oi_divergence_5m_v1_body(oi_change_5m_pct, dist_to_peak_pct)

        vol_z = _volume_z(candles_5m)
        atr_14_5m_pct = _atr_pct_14(candles_5m)

        return Dbg5Bundle(
            stage=stage,
            dist_to_peak_pct=dist_to_peak_pct,
            oi_change_5m_pct=oi_change_5m_pct,
            oi_divergence_5m=oi_divergence_5m_val,
            vol_z=vol_z,
            atr_14_5m_pct=atr_14_5m_pct,
        )
