"""
Liquidation rollups from CSV — Phase 2 aggregation matching fixture generator.

MIRRORS_V1 side mapping (short_pump/liquidations.py WS ingest, lines 567–610):
- Bybit "Buy"  → short liquidation → short_* rollups
- Bybit "Sell" → long liquidation  → long_* rollups

Window semantics match pump_v2/tests/fixtures/_generate.py:liquidation_rollups_from_csv:
- (window_end - N seconds, window_end] — left-exclusive, right-inclusive on ts.
- Empty / missing → all zeros (not None).

v1 liquidation_features uses in-memory get_liq_stats with inclusive lower bound; CSV path
is the Phase 2 canon per CONTRACTS.md.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

import pandas as pd

from pump_v2.core.indicator_base import Indicator

WINDOWS_SECONDS = (30, 60)


@dataclass(frozen=True)
class LiquidationRollups:
    long_count_30s: int
    long_usd_30s: float
    short_count_30s: int
    short_usd_30s: float
    long_count_60s: int
    long_usd_60s: float
    short_count_60s: int
    short_usd_60s: float

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _zero_rollups() -> LiquidationRollups:
    return LiquidationRollups(0, 0.0, 0, 0.0, 0, 0.0, 0, 0.0)


def _liq_side_bucket(side: str) -> str:
    """Same as pump_v2/tests/fixtures/_generate.py:liq_side_bucket."""
    s = str(side).strip().lower()
    if s == "buy":
        return "short"
    if s == "sell":
        return "long"
    return "unknown"


def _normalize_liq_ts(df: pd.DataFrame) -> pd.Series:
    if "ts" in df.columns:
        return pd.to_datetime(df["ts"], utc=True)
    if "ts_utc" in df.columns:
        return pd.to_datetime(df["ts_utc"], utc=True)
    if "ts_ms" in df.columns:
        return pd.to_datetime(df["ts_ms"], unit="ms", utc=True)
    raise KeyError("liquidations history needs ts, ts_utc, or ts_ms column")


def _liquidation_rollups_from_df(
    symbol: str,
    window_end: pd.Timestamp,
    df: pd.DataFrame,
    *,
    windows_seconds: Tuple[int, ...] = WINDOWS_SECONDS,
) -> LiquidationRollups:
    """Core rollup logic — mirrors _generate.liquidation_rollups_from_csv."""
    if df is None or df.empty:
        return _zero_rollups()

    work = df.copy()
    if "symbol" in work.columns:
        work = work[work["symbol"].astype(str).str.upper() == symbol.upper()]
    if work.empty:
        return _zero_rollups()

    work["ts"] = _normalize_liq_ts(work)
    we = pd.Timestamp(window_end)
    if we.tzinfo is None:
        we = we.tz_localize("UTC")
    else:
        we = we.tz_convert("UTC")

    buckets: Dict[str, Tuple[int, float, int, float]] = {}
    for sec in windows_seconds:
        tag = f"{sec}s"
        start = we - pd.Timedelta(seconds=sec)
        w = work[(work["ts"] > start) & (work["ts"] <= we)]
        lc = sc = 0
        lu = su = 0.0
        for _, row in w.iterrows():
            usd = float(row.get("value_usd") or 0.0)
            b = _liq_side_bucket(str(row.get("side", "")))
            if b == "long":
                lc += 1
                lu += usd
            elif b == "short":
                sc += 1
                su += usd
        buckets[tag] = (lc, lu, sc, su)

    b30 = buckets.get("30s", (0, 0.0, 0, 0.0))
    b60 = buckets.get("60s", (0, 0.0, 0, 0.0))
    return LiquidationRollups(
        long_count_30s=b30[0],
        long_usd_30s=b30[1],
        short_count_30s=b30[2],
        short_usd_30s=b30[3],
        long_count_60s=b60[0],
        long_usd_60s=b60[1],
        short_count_60s=b60[2],
        short_usd_60s=b60[3],
    )


class LiquidationRollupsIndicator(Indicator):
    name = "liquidation_rollups"

    def __init__(self, windows_seconds: Tuple[int, ...] = WINDOWS_SECONDS):
        if tuple(windows_seconds) != WINDOWS_SECONDS:
            raise NotImplementedError("Only (30, 60) supported in phase 2")
        self.windows_seconds = tuple(windows_seconds)

    def compute(
        self, symbol: str, ts: datetime, history: Optional[pd.DataFrame]
    ) -> LiquidationRollups:
        """
        Roll up liquidations CSV rows for symbol at evaluation ts (window end).

        history: DataFrame with ts_utc/ts/ts_ms, symbol, side, value_usd (collector schema).
        """
        if history is None:
            return _zero_rollups()
        return _liquidation_rollups_from_df(
            symbol, pd.Timestamp(ts), history, windows_seconds=self.windows_seconds
        )
