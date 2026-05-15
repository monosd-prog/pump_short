"""
Funding snapshot — v2 port of common/market_features.py:normalize_funding (3-tuple).

MIRRORS_V1_BEHAVIOR:
- Canon is common/market_features.normalize_funding (rate, ts_utc, rate_abs).
- short_pump/features.normalize_funding returns 2-tuple only (used by watcher v1).
- ts_utc stays str when Bybit sends nextFundingTime as digit string (no datetime parse).
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping, Optional, Union

from common import market_features as mf
from pump_v2.core.indicator_base import Indicator

HistoryInput = Union[Mapping[str, Any], str, None]


@dataclass(frozen=True)
class FundingSnapshot:
    funding_rate: Optional[float]
    funding_rate_abs: Optional[float]
    ts_utc: Optional[str]


def _coerce_funding_payload(history: HistoryInput) -> Optional[Mapping[str, Any]]:
    """Accept ticker dict, collector envelope, or JSON string."""
    if history is None:
        return None
    if isinstance(history, str):
        history = json.loads(history)
    if not isinstance(history, dict):
        raise TypeError(f"funding history must be dict or JSON str, got {type(history).__name__}")
    if "rest_tickers" in history or "ws_ticker_last" in history:
        inner = history.get("rest_tickers") or history.get("ws_ticker_last") or {}
        return inner if isinstance(inner, dict) else {}
    return history


def _normalize_funding_v1_body(payload: Optional[Mapping[str, Any]]) -> FundingSnapshot:
    rate, ts_utc, rate_abs = mf.normalize_funding(payload)
    return FundingSnapshot(
        funding_rate=rate,
        funding_rate_abs=rate_abs,
        ts_utc=ts_utc,
    )


class FundingSnapshotIndicator(Indicator):
    name = "funding_snapshot"

    def compute(self, symbol: str, ts: datetime, history: HistoryInput) -> FundingSnapshot:
        """
        Normalize Bybit ticker / funding REST payload at evaluation time.

        Values match common.market_features.normalize_funding; return type is dataclass.
        """
        _ = symbol, ts
        payload = _coerce_funding_payload(history)
        return _normalize_funding_v1_body(payload)
