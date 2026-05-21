"""Tests for prerun vs v1 comparison matching utilities."""
from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from pump_v2.validation.compare_prerun_vs_v1 import compare_prerun_to_v1, find_match


def _ts(ts: datetime) -> pd.Timestamp:
    t = pd.Timestamp(ts)
    return t.tz_convert("UTC") if t.tzinfo else t.tz_localize("UTC")


def _v1_row(symbol: str, ts: datetime, idx: int = 0) -> pd.Series:
    return pd.Series(
        {
            "symbol": symbol,
            "ts_utc": _ts(ts),
            "stage": 4,
            "dist_to_peak_pct": 4.0,
            "context_score": 0.5,
            "risk_profile": "short_pump_mid",
        },
        name=idx,
    )


def _v2_row(symbol: str, ts: datetime) -> pd.Series:
    return pd.Series(
        {
            "symbol": symbol,
            "ts_utc": _ts(ts),
            "risk_profile": "short_pump_mid",
        }
    )


def test_match_window_hit() -> None:
    t0 = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
    t1 = datetime(2026, 5, 1, 12, 3, tzinfo=timezone.utc)
    v1 = pd.DataFrame([_v1_row("BTCUSDT", t1)])
    v2 = _v2_row("BTCUSDT", t0)
    got = find_match(v2, v1, window_minutes=5)
    assert got is not None
    assert got["symbol"] == "BTCUSDT"


def test_match_window_miss() -> None:
    t0 = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
    t1 = datetime(2026, 5, 1, 12, 6, tzinfo=timezone.utc)
    v1 = pd.DataFrame([_v1_row("BTCUSDT", t1)])
    v2 = _v2_row("BTCUSDT", t0)
    assert find_match(v2, v1, window_minutes=5) is None


def test_match_same_symbol_only() -> None:
    t0 = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
    v1 = pd.DataFrame([_v1_row("BBB", t0)])
    v2 = _v2_row("AAA", t0)
    assert find_match(v2, v1, window_minutes=5) is None


def test_empty_prerun_no_crash() -> None:
    t0 = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
    v1 = pd.DataFrame([_v1_row("BTCUSDT", t0)])
    prerun = pd.DataFrame()
    matches, v2_unmatched, v1_unmatched = compare_prerun_to_v1(prerun, v1)
    assert matches == []
    assert v2_unmatched == []
    assert len(v1_unmatched) == 1
