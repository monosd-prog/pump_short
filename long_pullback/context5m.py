from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Tuple

import pandas as pd

from common.context_score import compute_context_score
from long_pullback.config import Config


@dataclass
class LongPullbackState:
    stage: int = 0
    peak_price: float = 0.0
    peak_ts: pd.Timestamp | None = None
    last_5m_ts: pd.Timestamp | None = None


def update_context_5m(cfg: Config, st: LongPullbackState, candles_5m: pd.DataFrame) -> Tuple[LongPullbackState, Dict[str, float]]:
    """
    Update long pullback context stages:
      0: none
      1: pump detected
      2: pullback zone
    """
    parts: Dict[str, float] = {
        "pump": 0.0,
        "pullback": 0.0,
        "absorption": 0.0,
        "breakout": 0.0,
    }

    if candles_5m is None or candles_5m.empty:
        return st, parts

    last = candles_5m.iloc[-1]
    price = float(last["close"])
    ts = last["ts"]

    # Pump detected: price moved up in last N candles
    lookback = candles_5m.tail(6)
    if len(lookback) >= 3:
        start_price = float(lookback.iloc[0]["close"])
        pump_pct = (price - start_price) / start_price * 100.0 if start_price > 0 else 0.0
        if pump_pct >= cfg.pullback_max_pct:  # reuse threshold for MVP
            st.stage = max(st.stage, 1)
            st.peak_price = max(st.peak_price, float(lookback["high"].max()))
            st.peak_ts = ts
            parts["pump"] = cfg.weight_pump

    if st.stage >= 1 and st.peak_price > 0:
        pullback_pct = (st.peak_price - price) / st.peak_price * 100.0
        if st.peak_ts is not None:
            minutes_since_peak = (ts - st.peak_ts).total_seconds() / 60.0
        else:
            minutes_since_peak = 0.0

        if (
            cfg.pullback_min_pct <= pullback_pct <= cfg.pullback_max_pct
            and cfg.pullback_time_min_minutes <= minutes_since_peak <= cfg.pullback_time_max_minutes
        ):
            st.stage = max(st.stage, 2)
            parts["pullback"] = cfg.weight_pullback

    return st, parts
