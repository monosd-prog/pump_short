"""
Tests for pump_v2.indicators.funding_snapshot — parity with common.market_features.normalize_funding.

GROUP B: logged_parity funding_rate columns are CSV snapshots, not Bybit payload replay.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Tuple

import pytest

from common import market_features as mf
from pump_v2.indicators.funding_snapshot import FundingSnapshot, FundingSnapshotIndicator
from pump_v2.tests.conftest import LIVE_COLLECTED, LOGGED_PARITY_SAMPLES, SYMBOLS_FIXTURES

LIVE_SYMBOL_DIRS = ["btcusdt", "ethusdt", "solusdt", "dogeusdt", "linkusdt"]
SYMBOL_EDGE_FIXTURES = [
    "btc_typical",
    "quiet_alt",
    "pump_recent",
    "no_liquidations",
    "no_oi",
]


def _parse_ts(s: str) -> datetime:
    t = __import__("pandas").Timestamp(s)
    if t.tzinfo is None:
        t = t.tz_localize("UTC")
    return t.to_pydatetime()


def _load_funding_payload(path: Path) -> Dict[str, Any]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if "rest_tickers" in raw or "ws_ticker_last" in raw:
        return raw.get("rest_tickers") or raw.get("ws_ticker_last") or {}
    return raw


def _load_live_case(sym_dir: str) -> Tuple[str, datetime, Dict[str, Any], Dict[str, Any]]:
    base = LIVE_COLLECTED / sym_dir
    meta = json.loads((base / "meta.json").read_text(encoding="utf-8"))
    expected = json.loads((base / "expected_indicators.json").read_text(encoding="utf-8"))
    funding_path = base / "funding.json"
    funding_raw = json.loads(funding_path.read_text(encoding="utf-8")) if funding_path.is_file() else {}
    ts = _parse_ts(meta["window_end_utc"])
    return meta["symbol"], ts, funding_raw, expected


def _assert_snapshot_matches_expected(got: FundingSnapshot, exp: Dict[str, Any]) -> None:
    if exp["funding_rate"] is None:
        assert got.funding_rate is None
    else:
        assert got.funding_rate is not None
        assert abs(got.funding_rate - float(exp["funding_rate"])) < 1e-6
    if exp["funding_rate_abs"] is None:
        assert got.funding_rate_abs is None
    else:
        assert got.funding_rate_abs is not None
        assert abs(got.funding_rate_abs - float(exp["funding_rate_abs"])) < 1e-6
    assert got.ts_utc == exp["ts_utc"]


# --- GROUP A: live_collected ---


@pytest.mark.parametrize("sym_dir", LIVE_SYMBOL_DIRS)
def test_live_collected_matches_expected(sym_dir: str) -> None:
    symbol, ts, funding_raw, expected = _load_live_case(sym_dir)
    exp_val = expected["funding_snapshot"]["value"]
    ind = FundingSnapshotIndicator()
    got = ind.compute(symbol, ts, funding_raw)
    payload = funding_raw.get("rest_tickers") or funding_raw.get("ws_ticker_last") or funding_raw
    want_r, want_ts, want_abs = mf.normalize_funding(payload)
    assert got.funding_rate == want_r
    assert got.funding_rate_abs == want_abs
    assert got.ts_utc == want_ts
    _assert_snapshot_matches_expected(got, exp_val)


# --- GROUP B: logged_parity smoke ---


@pytest.mark.parametrize(
    "sample_path",
    sorted(LOGGED_PARITY_SAMPLES.glob("sample_*.json")),
)
def test_logged_parity_funding_rate_abs_smoke(sample_path: Path) -> None:
    sample = json.loads(sample_path.read_text(encoding="utf-8"))
    logged = sample["logged_indicators"]
    fr_ent = logged.get("funding_rate")
    fa_ent = logged.get("funding_rate_abs")
    if fr_ent is None or fa_ent is None:
        pytest.skip("no funding columns")
    fr = fr_ent.get("value")
    fa = fa_ent.get("value")
    if fr is None or fa is None:
        return
    fr_f = float(fr)
    fa_f = float(fa)
    assert -0.05 <= fr_f <= 0.05, f"funding_rate out of range: {fr_f}"
    assert fa_f >= 0
    assert abs(fa_f - abs(fr_f)) < 1e-9


# --- GROUP C: edge cases ---


def test_empty_dict_returns_all_none() -> None:
    ts = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
    got = FundingSnapshotIndicator().compute("X", ts, {})
    want = mf.normalize_funding({})
    assert (got.funding_rate, got.ts_utc, got.funding_rate_abs) == want


def test_missing_funding_rate_key() -> None:
    ts = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
    payload = {"nextFundingTime": "1778832000000"}
    got = FundingSnapshotIndicator().compute("X", ts, payload)
    want = mf.normalize_funding(payload)
    assert (got.funding_rate, got.ts_utc, got.funding_rate_abs) == want
    assert got.funding_rate is None
    assert got.ts_utc == "1778832000000"


def test_funding_rate_none() -> None:
    ts = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
    payload = {"fundingRate": None, "nextFundingTime": "1"}
    got = FundingSnapshotIndicator().compute("X", ts, payload)
    assert got == _snapshot_from_v1(payload)


def test_funding_rate_empty_string() -> None:
    ts = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
    payload = {"fundingRate": "", "nextFundingTime": "2"}
    got = FundingSnapshotIndicator().compute("X", ts, payload)
    assert got == _snapshot_from_v1(payload)


def test_funding_rate_numeric_string() -> None:
    ts = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
    payload = {"fundingRate": "0.0001", "nextFundingTime": "3"}
    got = FundingSnapshotIndicator().compute("X", ts, payload)
    assert got.funding_rate == 0.0001
    assert got.funding_rate_abs == 0.0001


def test_history_none() -> None:
    ts = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
    got = FundingSnapshotIndicator().compute("X", ts, None)
    assert got == FundingSnapshot(None, None, None)


def test_invalid_json_string_raises() -> None:
    ts = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
    with pytest.raises(json.JSONDecodeError):
        FundingSnapshotIndicator().compute("X", ts, "not json")


def test_non_dict_raises_type_error() -> None:
    ts = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
    with pytest.raises(TypeError, match="must be dict"):
        FundingSnapshotIndicator().compute("X", ts, 42)


def test_collector_envelope_rest_tickers() -> None:
    ts = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
    envelope = {
        "rest_tickers": {"fundingRate": "-0.00002408", "nextFundingTime": "1778832000000"},
        "ws_ticker_last": {"symbol": "BTCUSDT"},
    }
    got = FundingSnapshotIndicator().compute("X", ts, envelope)
    assert got == _snapshot_from_v1(envelope["rest_tickers"])


def _snapshot_from_v1(payload: Dict[str, Any]) -> FundingSnapshot:
    r, t, a = mf.normalize_funding(payload)
    return FundingSnapshot(funding_rate=r, funding_rate_abs=a, ts_utc=t)


@pytest.mark.parametrize("fixture_name", SYMBOL_EDGE_FIXTURES)
def test_symbols_fixture_matches_expected(fixture_name: str) -> None:
    base = SYMBOLS_FIXTURES / fixture_name
    meta = json.loads((base / "meta.json").read_text(encoding="utf-8"))
    expected = json.loads((base / "expected_indicators.json").read_text(encoding="utf-8"))
    fp = base / "funding.json"
    funding = json.loads(fp.read_text(encoding="utf-8")) if fp.is_file() else {}
    ts = _parse_ts(meta["ts_snapshot_utc"])
    exp_val = expected["funding_snapshot"]["value"]
    got = FundingSnapshotIndicator().compute(meta["symbol"], ts, funding)
    assert got == _snapshot_from_v1(funding)
    _assert_snapshot_matches_expected(got, exp_val)
