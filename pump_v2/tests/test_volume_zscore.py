"""
Tests for pump_v2.indicators.volume_zscore — parity with short_pump/features.py:volume_zscore.

GROUP B skipped: logged_parity volume_zscore_20 is 1m/lookback=20 (market_features), not features.volume_zscore.
"""
from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Tuple

import pandas as pd
import pytest

from pump_v2.indicators.volume_zscore import V1_DEFAULT_LOOKBACK, VolumeZScore
from pump_v2.tests.conftest import LIVE_COLLECTED, LOGGED_PARITY_SAMPLES, SYMBOLS_FIXTURES
from short_pump import features as spf

LIVE_SYMBOL_DIRS = ["btcusdt", "ethusdt", "solusdt", "dogeusdt", "linkusdt"]
SYMBOL_EDGE_FIXTURES = [
    "btc_typical",
    "quiet_alt",
    "pump_recent",
    "no_liquidations",
    "no_oi",
]


def _parse_ts(s: str) -> datetime:
    t = pd.Timestamp(s)
    if t.tzinfo is None:
        t = t.tz_localize("UTC")
    return t.to_pydatetime()


def _klines_for_v1(k: pd.DataFrame) -> pd.DataFrame:
    """Same shape as pump_v2/tests/fixtures/_generate.py:klines_for_v1 (no sort)."""
    if k is None or k.empty:
        return pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume"])
    x = k.copy()
    x["ts"] = pd.to_datetime(x["ts_utc"], utc=True)
    for c in ("open", "high", "low", "close", "volume"):
        x[c] = x[c].astype(float)
    return x[["ts", "open", "high", "low", "close", "volume"]]


def _assert_v1_parity(got: Any, want: float, *, label: str = "") -> None:
    if pd.isna(want):
        assert got is not None and pd.isna(got), label
    else:
        assert got == want, label


def _assert_expected(got: Any, exp_val: Any, *, label: str = "") -> None:
    if exp_val is None:
        assert got is not None and pd.isna(got), label
    else:
        assert got is not None and not pd.isna(got), label
        assert abs(float(got) - float(exp_val)) < 1e-6, label


def _load_live_klines_case(sym_dir: str) -> Tuple[str, datetime, pd.DataFrame, Dict[str, Any]]:
    base = LIVE_COLLECTED / sym_dir
    meta = json.loads((base / "meta.json").read_text(encoding="utf-8"))
    expected = json.loads((base / "expected_indicators.json").read_text(encoding="utf-8"))
    kl = pd.read_parquet(base / "klines_5m.parquet")
    ts = _parse_ts(meta["window_end_utc"])
    return meta["symbol"], ts, kl, expected


# --- GROUP A: live_collected ---


@pytest.mark.parametrize("sym_dir", LIVE_SYMBOL_DIRS)
def test_live_collected_matches_expected(sym_dir: str) -> None:
    symbol, ts, kl, expected = _load_live_klines_case(sym_dir)
    exp_val = expected["volume_zscore"]["value"]
    lookback = expected["volume_zscore"]["params"].get("lookback", V1_DEFAULT_LOOKBACK)
    ind = VolumeZScore(lookback_bars=lookback)
    got = ind.compute(symbol, ts, kl)
    k1 = _klines_for_v1(kl)
    want = spf.volume_zscore(k1, lookback=lookback)
    _assert_v1_parity(got, want, label=f"v2 vs v1 {sym_dir}")
    _assert_expected(got, exp_val, label=f"{sym_dir} expected")


# --- GROUP B: logged_parity — SKIP ---


def test_logged_parity_group_b_skipped_by_design() -> None:
    """
    volume_zscore_20 in samples uses lookback=20 on 1m candles (market_features.volume_1m_features),
    v1 features.volume_zscore default is lookback=50 on 5m. Not directly comparable.
    """
    sample = json.loads((LOGGED_PARITY_SAMPLES / "sample_001.json").read_text(encoding="utf-8"))
    logged = sample["logged_indicators"]
    assert "volume_zscore_20" in logged
    assert "volume_zscore" not in logged
    assert V1_DEFAULT_LOOKBACK == 50


# --- GROUP C: edge cases ---


def test_empty_candles_returns_nan() -> None:
    ts = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
    empty = pd.DataFrame(columns=["ts_utc", "open", "high", "low", "close", "volume"])
    got = VolumeZScore().compute("X", ts, empty)
    want = spf.volume_zscore(_klines_for_v1(empty), lookback=50)
    _assert_v1_parity(got, want)
    assert math.isnan(got)


def test_one_bar_returns_nan() -> None:
    ts = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
    kl = pd.DataFrame(
        [
            {
                "ts_utc": pd.Timestamp(ts).isoformat(),
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 1000.0,
            }
        ]
    )
    got = VolumeZScore().compute("X", ts, kl)
    assert math.isnan(got)


def test_exactly_lookback_bars_computes() -> None:
    ts = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
    lb = 50
    rows = []
    for i in range(lb):
        t = pd.Timestamp(ts) - pd.Timedelta(minutes=5 * (lb - 1 - i))
        rows.append(
            {
                "ts_utc": t.isoformat(),
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.0,
                "volume": 1000.0 + i,
            }
        )
    kl = pd.DataFrame(rows)
    got = VolumeZScore(lookback_bars=lb).compute("X", ts, kl)
    want = spf.volume_zscore(_klines_for_v1(kl), lookback=lb)
    _assert_v1_parity(got, want)
    assert not math.isnan(got)


def test_constant_volumes_returns_zero() -> None:
    ts = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
    rows = []
    for i in range(20):
        t = pd.Timestamp(ts) - pd.Timedelta(minutes=5 * (19 - i))
        rows.append(
            {
                "ts_utc": t.isoformat(),
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.0,
                "volume": 500.0,
            }
        )
    kl = pd.DataFrame(rows)
    got = VolumeZScore(lookback_bars=50).compute("X", ts, kl)
    want = spf.volume_zscore(_klines_for_v1(kl), lookback=50)
    _assert_v1_parity(got, want)
    assert got == 0.0


@pytest.mark.parametrize("bad", [0, 1, -1])
def test_invalid_lookback_raises(bad: int) -> None:
    with pytest.raises(ValueError, match="lookback_bars must be > 1"):
        VolumeZScore(lookback_bars=bad)


@pytest.mark.parametrize("fixture_name", SYMBOL_EDGE_FIXTURES)
def test_symbols_fixture_matches_v1_at_snapshot(fixture_name: str) -> None:
    base = SYMBOLS_FIXTURES / fixture_name
    meta = json.loads((base / "meta.json").read_text(encoding="utf-8"))
    expected = json.loads((base / "expected_indicators.json").read_text(encoding="utf-8"))
    kp = base / "klines_5m.parquet"
    kl = pd.read_parquet(kp) if kp.is_file() and kp.stat().st_size > 0 else pd.DataFrame()
    ts = _parse_ts(meta["ts_snapshot_utc"])
    exp_val = expected["volume_zscore"]["value"]
    lookback = expected["volume_zscore"]["params"].get("lookback", V1_DEFAULT_LOOKBACK)
    k1 = _klines_for_v1(kl)
    got = VolumeZScore(lookback_bars=lookback).compute(meta["symbol"], ts, kl)
    want = spf.volume_zscore(k1, lookback=lookback)
    _assert_v1_parity(got, want)
    _assert_expected(got, exp_val)
