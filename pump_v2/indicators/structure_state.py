"""
FSM structure state — v2 port of short_pump/context5m.py StructureState + update_structure.

MIRRORS_V1_BEHAVIOR:
- Pure replay: no per-symbol persistent state in the indicator.
- Bar-by-bar replay matches pump_v2/tests/fixtures/_generate.py:replay_structure:
  sort candles by ts, peak_price = max(high) of last 20 bars in prefix, last_price = close.
- Threshold defaults match fixture cfg (percent points): drop1=3, bounce1=1, drop2=2, bounce2=0.8.
- Stage 4 sets armed_since_utc via datetime.now(UTC) when first entered (non-deterministic if stage=4).
- No rollbacks from stage 4; peak reset only when new high > st.peak_price.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Optional

import pandas as pd

from pump_v2.core.indicator_base import Indicator
from pump_v2.indicators._candle_utils import normalize_candles_df

PEAK_LOOKBACK_BARS = 20

# Fixture / update_structure getattr defaults (percent points, not config.py fractions).
DEFAULT_DROP1_MIN_PCT = 3.0
DEFAULT_BOUNCE1_MIN_PCT = 1.0
DEFAULT_DROP2_MIN_PCT = 2.0
DEFAULT_BOUNCE2_MIN_PCT = 0.8


@dataclass
class StructureState:
    """1:1 fields with short_pump/context5m.py:StructureState."""

    stage: int = 0
    peak_price: float = 0.0
    low_after_peak: float = 0.0
    low_after_bounce: float = 0.0
    bounce_count: int = 0
    armed_notified: bool = False
    armed_since_utc: str = ""


@dataclass(frozen=True)
class StructureFsmParams:
    drop1_min_pct: float = DEFAULT_DROP1_MIN_PCT
    bounce1_min_pct: float = DEFAULT_BOUNCE1_MIN_PCT
    drop2_min_pct: float = DEFAULT_DROP2_MIN_PCT
    bounce2_min_pct: float = DEFAULT_BOUNCE2_MIN_PCT

    def __post_init__(self) -> None:
        for name, val in (
            ("drop1_min_pct", self.drop1_min_pct),
            ("bounce1_min_pct", self.bounce1_min_pct),
            ("drop2_min_pct", self.drop2_min_pct),
            ("bounce2_min_pct", self.bounce2_min_pct),
        ):
            if val <= 0:
                raise ValueError(f"{name} must be > 0, got {val}")

    def as_cfg(self) -> Any:
        return SimpleNamespace(
            drop1_min_pct=self.drop1_min_pct,
            bounce1_min_pct=self.bounce1_min_pct,
            drop2_min_pct=self.drop2_min_pct,
            bounce2_min_pct=self.bounce2_min_pct,
        )


def _update_structure_v1_body(
    cfg: Any, st: StructureState, last_price: float, peak_price: float
) -> StructureState:
    """Exact logic from short_pump/context5m.py:update_structure (lines 49–130)."""
    drop1_min_pct = float(getattr(cfg, "drop1_min_pct", DEFAULT_DROP1_MIN_PCT))
    bounce1_min_pct = float(getattr(cfg, "bounce1_min_pct", DEFAULT_BOUNCE1_MIN_PCT))
    drop2_min_pct = float(getattr(cfg, "drop2_min_pct", DEFAULT_DROP2_MIN_PCT))
    bounce2_min_pct = float(getattr(cfg, "bounce2_min_pct", DEFAULT_BOUNCE2_MIN_PCT))

    if peak_price and peak_price > 0:
        if peak_price > st.peak_price:
            st.peak_price = peak_price

    if st.peak_price <= 0:
        st.peak_price = peak_price or last_price or 0.0

    dist_to_peak_pct = (st.peak_price - last_price) / st.peak_price * 100.0 if st.peak_price > 0 else 0.0

    if st.low_after_peak <= 0:
        st.low_after_peak = last_price
    if st.low_after_bounce <= 0:
        st.low_after_bounce = last_price

    st.low_after_peak = min(st.low_after_peak, last_price)

    if st.stage == 0:
        if dist_to_peak_pct >= drop1_min_pct:
            st.stage = 1
            st.low_after_peak = last_price
            st.low_after_bounce = last_price
            st.bounce_count = 0

    elif st.stage == 1:
        st.low_after_peak = min(st.low_after_peak, last_price)
        if last_price >= st.low_after_peak * (1.0 + bounce1_min_pct / 100.0):
            st.stage = 2
            st.bounce_count = 1
            st.low_after_bounce = last_price

    elif st.stage == 2:
        st.low_after_bounce = max(st.low_after_bounce, last_price)
        dip_from_bounce_high_pct = (
            (st.low_after_bounce - last_price) / st.low_after_bounce * 100.0 if st.low_after_bounce > 0 else 0.0
        )
        if dist_to_peak_pct >= drop2_min_pct or dip_from_bounce_high_pct >= drop2_min_pct:
            st.stage = 3
            st.low_after_peak = last_price
            st.low_after_bounce = last_price

    elif st.stage == 3:
        st.low_after_peak = min(st.low_after_peak, last_price)
        if last_price >= st.low_after_peak * (1.0 + bounce2_min_pct / 100.0):
            st.stage = 4
            st.bounce_count = 2
            if not st.armed_since_utc:
                st.armed_since_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    return st


def replay_structure_state(
    candles: pd.DataFrame,
    *,
    params: Optional[StructureFsmParams] = None,
    peak_lookback_bars: int = PEAK_LOOKBACK_BARS,
) -> StructureState:
    """
    Replay FSM bar-by-bar — mirrors pump_v2/tests/fixtures/_generate.py:replay_structure.
    """
    st = StructureState()
    if candles is None or candles.empty:
        return st

    required = {"open", "high", "low", "close"}
    if not required.issubset(set(candles.columns)):
        return st

    df = candles.sort_values("ts").reset_index(drop=True)
    cfg = (params or StructureFsmParams()).as_cfg()

    for i in range(len(df)):
        sub = df.iloc[: i + 1]
        peak_price = float(sub["high"].tail(max(1, peak_lookback_bars)).max())
        last_price = float(sub.iloc[-1]["close"])
        _update_structure_v1_body(cfg, st, last_price, peak_price)

    return st


class StructureStateIndicator(Indicator):
    name = "structure_state"

    def __init__(
        self,
        drop1_min_pct: float = DEFAULT_DROP1_MIN_PCT,
        bounce1_min_pct: float = DEFAULT_BOUNCE1_MIN_PCT,
        drop2_min_pct: float = DEFAULT_DROP2_MIN_PCT,
        bounce2_min_pct: float = DEFAULT_BOUNCE2_MIN_PCT,
        peak_lookback_bars: int = PEAK_LOOKBACK_BARS,
    ):
        self.params = StructureFsmParams(
            drop1_min_pct=drop1_min_pct,
            bounce1_min_pct=bounce1_min_pct,
            drop2_min_pct=drop2_min_pct,
            bounce2_min_pct=bounce2_min_pct,
        )
        if peak_lookback_bars <= 0:
            raise ValueError(f"peak_lookback_bars must be > 0, got {peak_lookback_bars}")
        self.peak_lookback_bars = peak_lookback_bars

    def compute(self, symbol: str, ts: datetime, history: pd.DataFrame) -> StructureState:
        _ = symbol
        candles = normalize_candles_df(history, ts)
        if candles.empty:
            return StructureState()
        return replay_structure_state(
            candles,
            params=self.params,
            peak_lookback_bars=self.peak_lookback_bars,
        )
