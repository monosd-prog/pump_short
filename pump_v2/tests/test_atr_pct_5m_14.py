"""
Tests for pump_v2.indicators.atr_pct_5m_14 — parity with short_pump/features.py:atr_pct.

GROUP B skipped: logged_parity has context_parts.atr (score weight), not atr_pct_5m_14.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd
import pytest

from pump_v2.indicators.atr_pct_5m_14 import ATRPct5m14
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
    exp_val = expected["atr_pct_5m_14"]["value"]
    ind = ATRPct5m14(period=14, timeframe="5m")
    got = ind.compute(symbol, ts, kl)
    k1 = _klines_for_v1(kl)
    want = spf.atr_pct(k1, period=14)
    assert got == want, f"v2 vs v1 atr_pct mismatch {sym_dir}"
    if exp_val is None:
        assert got is None
    else:
        assert got is not None
        assert abs(got - float(exp_val)) < 1e-6


# --- GROUP B: logged_parity — SKIP (documented) ---


def test_logged_parity_group_b_skipped_by_design() -> None:
    """events_v3 has no atr_pct_5m_14 column; context_parts.atr is score weight only."""
    sample = json.loads((LOGGED_PARITY_SAMPLES / "sample_001.json").read_text(encoding="utf-8"))
    parts = sample["logged_indicators"]["context_parts"]["value"]
    assert "atr" in parts
    assert "atr_pct_5m_14" not in sample["logged_indicators"]


# --- GROUP C: edge cases ---


def test_empty_candles_returns_none() -> None:
    ts = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
    assert ATRPct5m14().compute("X", ts, pd.DataFrame()) is None
    assert spf.atr_pct(pd.DataFrame(), period=14) is None


def test_insufficient_bars_returns_none() -> None:
    ts = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
    rows = []
    for i in range(10):
        t = pd.Timestamp(ts) - pd.Timedelta(minutes=5 * (9 - i))
        rows.append(
            {
                "ts_utc": t.isoformat(),
                "open": 100.0 + i,
                "high": 101.0 + i,
                "low": 99.0 + i,
                "close": 100.5 + i,
                "volume": 1000.0,
            }
        )
    kl = pd.DataFrame(rows)
    assert ATRPct5m14().compute("X", ts, kl) is None


def test_exactly_15_bars_computes() -> None:
    ts = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
    rows = []
    for i in range(15):
        t = pd.Timestamp(ts) - pd.Timedelta(minutes=5 * (14 - i))
        rows.append(
            {
                "ts_utc": t.isoformat(),
                "open": 100.0,
                "high": 102.0,
                "low": 98.0,
                "close": 101.0,
                "volume": 1000.0,
            }
        )
    got = ATRPct5m14().compute("X", ts, pd.DataFrame(rows))
    assert got is not None
    assert got > 0


def test_future_candles_excluded() -> None:
    ts_eval = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
    rows = []
    for i in range(20):
        tt = pd.Timestamp("2026-05-15 10:00:00", tz="UTC") + pd.Timedelta(minutes=5 * i)
        rows.append(
            {
                "ts_utc": tt.isoformat(),
                "open": 100.0,
                "high": 102.0,
                "low": 98.0,
                "close": 101.0,
                "volume": 1000.0,
            }
        )
    future = pd.Timestamp("2026-05-15 12:30:00", tz="UTC")
    rows.append(
        {
            "ts_utc": future.isoformat(),
            "open": 100.0,
            "high": 200.0,
            "low": 50.0,
            "close": 150.0,
            "volume": 99999.0,
        }
    )
    got = ATRPct5m14().compute("X", ts_eval, pd.DataFrame(rows))
    k1 = _klines_for_v1(pd.DataFrame([r for r in rows if pd.Timestamp(r["ts_utc"]) <= pd.Timestamp(ts_eval)]))
    assert got == spf.atr_pct(k1, period=14)


@pytest.mark.parametrize("bad", [0, -1])
def test_invalid_period_raises(bad: int) -> None:
    with pytest.raises(ValueError, match="period must be positive"):
        ATRPct5m14(period=bad)


def test_unsupported_timeframe_raises() -> None:
    with pytest.raises(NotImplementedError, match="only timeframe='5m'"):
        ATRPct5m14(timeframe="1m")


@pytest.mark.parametrize("fixture_name", SYMBOL_EDGE_FIXTURES)
def test_symbols_fixture_matches_v1_at_snapshot(fixture_name: str) -> None:
    base = SYMBOLS_FIXTURES / fixture_name
    meta = json.loads((base / "meta.json").read_text(encoding="utf-8"))
    expected = json.loads((base / "expected_indicators.json").read_text(encoding="utf-8"))
    kp = base / "klines_5m.parquet"
    kl = pd.read_parquet(kp) if kp.is_file() and kp.stat().st_size > 0 else pd.DataFrame()
    ts = _parse_ts(meta["ts_snapshot_utc"])
    exp_val = expected["atr_pct_5m_14"]["value"]
    k1 = _klines_for_v1(kl)
    got = ATRPct5m14().compute(meta["symbol"], ts, kl)
    want = spf.atr_pct(k1, period=14)
    assert got == want
    if exp_val is None:
        assert got is None
    else:
        assert abs(got - float(exp_val)) < 1e-6
