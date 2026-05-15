"""
Pump shape on 5m candles — v2 port of common/market_features.py:pump_shape_features_5m.

MIRRORS_V1_BEHAVIOR:
- tail(lookback) on input row order (no sort); body=0 uses eps_body for wick_body_ratio_last.
- green_candles_5 stored as float; upper/lower wick ratios None when range_last == 0.
- Missing OHLC columns or empty frame → all fields None.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional

import pandas as pd

from pump_v2.core.indicator_base import Indicator
from pump_v2.indicators._candle_utils import normalize_candles_df

V1_DEFAULT_LOOKBACK = 5

_PUMP_SHAPE_FIELDS = (
    "green_candles_5",
    "max_candle_body_pct_5",
    "avg_candle_body_pct_5",
    "upper_wick_ratio_last",
    "lower_wick_ratio_last",
    "wick_body_ratio_last",
)


@dataclass(frozen=True)
class PumpShape5m:
    wick_body_ratio_last: Optional[float]
    upper_wick_ratio_last: Optional[float]
    lower_wick_ratio_last: Optional[float]
    green_candles_5: Optional[float]
    max_candle_body_pct_5: Optional[float]
    avg_candle_body_pct_5: Optional[float]


def _empty_pump_shape() -> PumpShape5m:
    return PumpShape5m(
        wick_body_ratio_last=None,
        upper_wick_ratio_last=None,
        lower_wick_ratio_last=None,
        green_candles_5=None,
        max_candle_body_pct_5=None,
        avg_candle_body_pct_5=None,
    )


def _pump_shape_features_5m_v1_body(
    candles_5m: Optional[pd.DataFrame], *, lookback: int = 5
) -> Dict[str, Optional[float]]:
    """Exact logic from common/market_features.py:pump_shape_features_5m (lines 98–160)."""
    out: Dict[str, Optional[float]] = {
        "green_candles_5": None,
        "max_candle_body_pct_5": None,
        "avg_candle_body_pct_5": None,
        "upper_wick_ratio_last": None,
        "lower_wick_ratio_last": None,
        "wick_body_ratio_last": None,
    }
    if candles_5m is None or candles_5m.empty:
        return out
    required = {"open", "high", "low", "close"}
    if not required.issubset(set(candles_5m.columns)):
        return out
    try:
        tail = candles_5m.tail(max(1, int(lookback))).copy()
        if tail.empty:
            return out
        o = tail["open"].astype(float)
        c = tail["close"].astype(float)
        h = tail["high"].astype(float)
        l = tail["low"].astype(float)

        greens = int((c > o).sum())
        body = (c - o).abs()
        body_pct = (body / o.replace(0, pd.NA)) * 100.0
        try:
            max_body = float(body_pct.max(skipna=True))
            avg_body = float(body_pct.mean(skipna=True))
        except Exception:
            max_body, avg_body = None, None

        o_last = float(o.iloc[-1])
        c_last = float(c.iloc[-1])
        h_last = float(h.iloc[-1])
        l_last = float(l.iloc[-1])
        body_last = abs(c_last - o_last)
        range_last = max(h_last - l_last, 0.0)
        upper_wick = max(0.0, h_last - max(o_last, c_last))
        lower_wick = max(0.0, min(o_last, c_last) - l_last)

        eps_body = 1e-9
        wick_body_ratio = float((upper_wick + lower_wick) / max(body_last, eps_body))
        upper_ratio = float(upper_wick / range_last) if range_last > 0 else None
        lower_ratio = float(lower_wick / range_last) if range_last > 0 else None

        out.update(
            {
                "green_candles_5": float(greens),
                "max_candle_body_pct_5": max_body,
                "avg_candle_body_pct_5": avg_body,
                "upper_wick_ratio_last": upper_ratio,
                "lower_wick_ratio_last": lower_ratio,
                "wick_body_ratio_last": wick_body_ratio,
            }
        )
    except Exception:
        return out
    return out


def _dict_to_pump_shape(d: Dict[str, Optional[float]]) -> PumpShape5m:
    return PumpShape5m(
        wick_body_ratio_last=d.get("wick_body_ratio_last"),
        upper_wick_ratio_last=d.get("upper_wick_ratio_last"),
        lower_wick_ratio_last=d.get("lower_wick_ratio_last"),
        green_candles_5=d.get("green_candles_5"),
        max_candle_body_pct_5=d.get("max_candle_body_pct_5"),
        avg_candle_body_pct_5=d.get("avg_candle_body_pct_5"),
    )


class PumpShape5mIndicator(Indicator):
    name = "pump_shape_5m"

    def __init__(self, n_candles: int = V1_DEFAULT_LOOKBACK):
        if n_candles <= 0:
            raise ValueError(f"n_candles must be > 0, got {n_candles}")
        self.n_candles = n_candles

    def compute(self, symbol: str, ts: datetime, history: pd.DataFrame) -> PumpShape5m:
        """
        Candle shape features from last n_candles 5m bars at or before ts.

        Values match common.market_features.pump_shape_features_5m on the same frame.
        """
        _ = symbol
        candles = normalize_candles_df(history, ts)
        raw = _pump_shape_features_5m_v1_body(candles, lookback=self.n_candles)
        return _dict_to_pump_shape(raw)
