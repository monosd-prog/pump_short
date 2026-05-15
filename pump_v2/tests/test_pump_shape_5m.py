"""
Tests for pump_v2.indicators.pump_shape_5m — parity with common.market_features.pump_shape_features_5m.
"""
from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Tuple

import pandas as pd
import pytest

from common import market_features as mf
from pump_v2.indicators.pump_shape_5m import PumpShape5m, PumpShape5mIndicator, V1_DEFAULT_LOOKBACK
from pump_v2.tests.conftest import LIVE_COLLECTED, LOGGED_PARITY_SAMPLES, SYMBOLS_FIXTURES

PUMP_SHAPE_FIELDS = (
    "green_candles_5",
    "max_candle_body_pct_5",
    "avg_candle_body_pct_5",
    "upper_wick_ratio_last",
    "lower_wick_ratio_last",
    "wick_body_ratio_last",
)

LIVE_SYMBOL_DIRS = ["btcusdt", "ethusdt", "solusdt", "dogeusdt", "linkusdt"]
LOGGED_SHAPE_FIELDS = list(PUMP_SHAPE_FIELDS)
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
    if k is None or k.empty:
        return pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume"])
    x = k.copy()
    x["ts"] = pd.to_datetime(x["ts_utc"], utc=True)
    for c in ("open", "high", "low", "close", "volume"):
        x[c] = x[c].astype(float)
    return x[["ts", "open", "high", "low", "close", "volume"]]


def _assert_field_equal(got: Any, exp: Any, field: str, label: str = "") -> None:
    if exp is None:
        assert got is None, f"{label} {field}: expected None, got {got}"
    elif got is None:
        pytest.fail(f"{label} {field}: expected {exp}, got None")
    else:
        assert abs(float(got) - float(exp)) < 1e-6, f"{label} {field}: got={got} exp={exp}"


def _assert_shape_matches_dict(got: PumpShape5m, exp: Dict[str, Any], *, label: str = "") -> None:
    got_d = asdict(got)
    for field in PUMP_SHAPE_FIELDS:
        _assert_field_equal(got_d.get(field), exp.get(field), field, label)


def _load_live_case(sym_dir: str) -> Tuple[str, datetime, pd.DataFrame, Dict[str, Any]]:
    base = LIVE_COLLECTED / sym_dir
    meta = json.loads((base / "meta.json").read_text(encoding="utf-8"))
    expected = json.loads((base / "expected_indicators.json").read_text(encoding="utf-8"))
    kl = pd.read_parquet(base / "klines_5m.parquet")
    ts = _parse_ts(meta["window_end_utc"])
    return meta["symbol"], ts, kl, expected


# --- GROUP A: live_collected ---


@pytest.mark.parametrize("sym_dir", LIVE_SYMBOL_DIRS)
def test_live_collected_matches_expected(sym_dir: str) -> None:
    symbol, ts, kl, expected = _load_live_case(sym_dir)
    exp_val = expected["pump_shape_5m"]["value"]
    lookback = expected["pump_shape_5m"]["params"].get("lookback", V1_DEFAULT_LOOKBACK)
    got = PumpShape5mIndicator(n_candles=lookback).compute(symbol, ts, kl)
    k1 = _klines_for_v1(kl)
    want = mf.pump_shape_features_5m(k1, lookback=lookback)
    _assert_shape_matches_dict(got, want, label=f"v1 {sym_dir}")
    _assert_shape_matches_dict(got, exp_val, label=f"expected {sym_dir}")


# --- GROUP B: logged_parity smoke ---


@pytest.mark.parametrize(
    "sample_path",
    sorted(LOGGED_PARITY_SAMPLES.glob("sample_*.json")),
)
def test_logged_parity_shape_fields_smoke(sample_path: Path) -> None:
    sample = json.loads(sample_path.read_text(encoding="utf-8"))
    logged = sample["logged_indicators"]
    for field in LOGGED_SHAPE_FIELDS:
        ent = logged.get(field)
        if ent is None:
            continue
        val = ent.get("value")
        if val is None:
            continue
        fv = float(val)
        assert isinstance(val, (int, float))
        if field == "green_candles_5":
            assert 0 <= fv <= 5
        elif field in ("upper_wick_ratio_last", "lower_wick_ratio_last"):
            assert 0 <= fv <= 1.0 + 1e-6
        elif field == "wick_body_ratio_last":
            assert fv >= 0  # unbounded when body ~ 0
        else:
            assert fv >= 0


# --- GROUP C: edge cases ---


def test_empty_df_all_none() -> None:
    ts = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
    got = PumpShape5mIndicator().compute("X", ts, pd.DataFrame())
    want = mf.pump_shape_features_5m(_klines_for_v1(pd.DataFrame()), lookback=5)
    _assert_shape_matches_dict(got, want)


def test_fewer_than_lookback_still_computes() -> None:
    ts = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
    rows = []
    for i in range(3):
        t = pd.Timestamp(ts) - pd.Timedelta(minutes=5 * (2 - i))
        rows.append(
            {
                "ts_utc": t.isoformat(),
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 101.0,
                "volume": 1.0,
            }
        )
    kl = pd.DataFrame(rows)
    got = PumpShape5mIndicator(n_candles=5).compute("X", ts, kl)
    want = mf.pump_shape_features_5m(_klines_for_v1(kl), lookback=5)
    _assert_shape_matches_dict(got, want)
    assert got.green_candles_5 is not None


def test_doji_last_candle() -> None:
    ts = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
    rows = []
    for i in range(5):
        t = pd.Timestamp(ts) - pd.Timedelta(minutes=5 * (4 - i))
        rows.append(
            {
                "ts_utc": t.isoformat(),
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.0 if i == 4 else 101.0,
                "volume": 1.0,
            }
        )
    kl = pd.DataFrame(rows)
    got = PumpShape5mIndicator().compute("X", ts, kl)
    want = mf.pump_shape_features_5m(_klines_for_v1(kl), lookback=5)
    _assert_shape_matches_dict(got, want)
    assert got.wick_body_ratio_last is not None


def test_missing_ohlc_columns_all_none() -> None:
    ts = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
    kl = pd.DataFrame({"ts_utc": [pd.Timestamp(ts).isoformat()], "volume": [1.0]})
    got = PumpShape5mIndicator().compute("X", ts, kl)
    assert got.green_candles_5 is None
    assert got.wick_body_ratio_last is None


@pytest.mark.parametrize("bad", [0, -1])
def test_invalid_n_candles_raises(bad: int) -> None:
    with pytest.raises(ValueError, match="n_candles must be > 0"):
        PumpShape5mIndicator(n_candles=bad)


@pytest.mark.parametrize("fixture_name", SYMBOL_EDGE_FIXTURES)
def test_symbols_fixture_matches_expected(fixture_name: str) -> None:
    base = SYMBOLS_FIXTURES / fixture_name
    meta = json.loads((base / "meta.json").read_text(encoding="utf-8"))
    expected = json.loads((base / "expected_indicators.json").read_text(encoding="utf-8"))
    kp = base / "klines_5m.parquet"
    kl = pd.read_parquet(kp) if kp.is_file() and kp.stat().st_size > 0 else pd.DataFrame()
    ts = _parse_ts(meta["ts_snapshot_utc"])
    exp_val = expected["pump_shape_5m"]["value"]
    lookback = expected["pump_shape_5m"]["params"].get("lookback", V1_DEFAULT_LOOKBACK)
    got = PumpShape5mIndicator(n_candles=lookback).compute(meta["symbol"], ts, kl)
    want = mf.pump_shape_features_5m(_klines_for_v1(kl), lookback=lookback)
    _assert_shape_matches_dict(got, want)
    _assert_shape_matches_dict(got, exp_val)
