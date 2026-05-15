"""
Tests for pump_v2.indicators.context_score_5m — parity with context5m.compute_context_score_5m.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, Tuple

import pandas as pd
import pytest

from pump_v2.indicators.context_score_5m import PART_KEYS, ContextScore5m, ContextScore5mIndicator
from pump_v2.tests.conftest import LIVE_COLLECTED, LOGGED_PARITY_SAMPLES, SYMBOLS_FIXTURES
from pump_v2.tests.fixtures import _generate as _gen
from short_pump.context5m import build_dbg5, compute_context_score_5m

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


def _fixture_cfg(symbol: str) -> SimpleNamespace:
    return SimpleNamespace(
        symbol=symbol,
        run_id=f"test_{symbol}",
        drop1_min_pct=3.0,
        bounce1_min_pct=1.0,
        drop2_min_pct=2.0,
        bounce2_min_pct=0.8,
    )


def _build_dbg5_from_fixture(symbol: str, kl: pd.DataFrame, oi_raw: pd.DataFrame) -> Dict[str, Any]:
    cfg = _fixture_cfg(symbol)
    st, _ = _gen.replay_structure(cfg, kl)
    recs = [dict(r) for r in _gen.klines_for_v1(kl).to_dict("records")]
    if oi_raw is None or oi_raw.empty:
        oi_v1 = pd.DataFrame(columns=["ts", "openInterest"])
    else:
        oi_schema = oi_raw.copy()
        oi_schema["ts_utc"] = pd.to_datetime(oi_schema["ts_utc"], utc=True)
        oi_v1 = _gen.oi_for_v1(oi_schema)
    oi_block = {"oi_df": oi_v1} if not oi_v1.empty else None
    return build_dbg5(cfg, recs, oi_block, None, st)


def _load_live_case(sym_dir: str) -> Tuple[str, datetime, pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    base = LIVE_COLLECTED / sym_dir
    meta = json.loads((base / "meta.json").read_text(encoding="utf-8"))
    expected = json.loads((base / "expected_indicators.json").read_text(encoding="utf-8"))
    kl = pd.read_parquet(base / "klines_5m.parquet")
    oi_path = base / "oi_history.parquet"
    oi = pd.read_parquet(oi_path) if oi_path.is_file() and oi_path.stat().st_size > 0 else pd.DataFrame()
    ts = _parse_ts(meta["window_end_utc"])
    return meta["symbol"], ts, kl, oi, expected


def _assert_score_parts(got: ContextScore5m, exp_score: float, exp_parts: Dict[str, Any]) -> None:
    assert abs(got.score - float(exp_score)) < 1e-6
    for k in PART_KEYS:
        assert abs(got.parts[k] - float(exp_parts[k])) < 1e-6


# --- GROUP A: live_collected via build_dbg5 ---


@pytest.mark.parametrize("sym_dir", LIVE_SYMBOL_DIRS)
def test_live_collected_matches_expected(sym_dir: str) -> None:
    symbol, ts, kl, oi, expected = _load_live_case(sym_dir)
    dbg5 = _build_dbg5_from_fixture(symbol, kl, oi)
    got = ContextScore5mIndicator().compute(symbol, ts, dbg5)
    want_score, want_parts = compute_context_score_5m(dbg5)
    _assert_score_parts(got, want_score, want_parts)
    exp = expected["context_score_5m"]
    _assert_score_parts(got, exp["value"], exp["parts"])


@pytest.mark.parametrize("fixture_name", SYMBOL_EDGE_FIXTURES)
def test_symbols_fixture_matches_expected(fixture_name: str) -> None:
    base = SYMBOLS_FIXTURES / fixture_name
    meta = json.loads((base / "meta.json").read_text(encoding="utf-8"))
    expected = json.loads((base / "expected_indicators.json").read_text(encoding="utf-8"))
    kp = base / "klines_5m.parquet"
    kl = pd.read_parquet(kp) if kp.is_file() and kp.stat().st_size > 0 else pd.DataFrame()
    op = base / "oi.parquet"
    oi = pd.read_parquet(op) if op.is_file() and op.stat().st_size > 0 else pd.DataFrame()
    ts = _parse_ts(meta["ts_snapshot_utc"])
    dbg5 = _build_dbg5_from_fixture(meta["symbol"], kl, oi)
    got = ContextScore5mIndicator().compute(meta["symbol"], ts, dbg5)
    exp = expected["context_score_5m"]
    _assert_score_parts(got, exp["value"], exp["parts"])


# --- GROUP B: logged_parity (smoke — vol_z/atr not in CSV; parts may include legacy cvd) ---


def _partial_dbg5_from_logged(logged: Dict[str, Any]) -> Dict[str, Any]:
    stage = logged.get("stage", {}).get("value")
    dist = logged.get("dist_to_peak_pct", {}).get("value")
    oi5 = logged.get("oi_change_5m_pct", {}).get("value")
    dbg5: Dict[str, Any] = {
        "stage": int(stage) if stage is not None else 0,
        "dist_to_peak_pct": float(dist) if dist is not None else 0.0,
        "oi_change_5m_pct": float(oi5) if oi5 is not None else None,
        "oi_divergence_5m": False,
        "vol_z": 0.0,
        "atr_14_5m_pct": 0.0,
    }
    if oi5 is not None and dist is not None:
        from short_pump import features as spf

        dbg5["oi_divergence_5m"] = spf.oi_divergence_5m(float(oi5), float(dist))
    return dbg5


@pytest.mark.parametrize("sample_path", sorted(LOGGED_PARITY_SAMPLES.glob("sample_*.json")))
def test_logged_parity_smoke(sample_path: Path) -> None:
    sample = json.loads(sample_path.read_text(encoding="utf-8"))
    logged = sample["logged_indicators"]
    cs = logged.get("context_score", {}).get("value")
    if cs is None:
        pytest.skip("no context_score")
    dbg5 = _partial_dbg5_from_logged(logged)
    got = ContextScore5mIndicator().compute("X", datetime.now(timezone.utc), dbg5)
    assert 0.0 <= got.score <= 1.0
    assert set(got.parts.keys()) == set(PART_KEYS)
    assert abs(sum(got.parts.values()) - got.score) < 1e-9


@pytest.mark.parametrize("sample_path", sorted(LOGGED_PARITY_SAMPLES.glob("sample_*.json")))
def test_logged_parity_parts_keys_when_present(sample_path: Path) -> None:
    sample = json.loads(sample_path.read_text(encoding="utf-8"))
    parts_ent = sample["logged_indicators"].get("context_parts", {}).get("value")
    if parts_ent is None:
        return
    dbg5 = _partial_dbg5_from_logged(sample["logged_indicators"])
    got = ContextScore5mIndicator().compute("X", datetime.now(timezone.utc), dbg5)
    for k in PART_KEYS:
        assert k in got.parts
    if "cvd" in parts_ent:
        pass


# --- GROUP C: edge cases ---


def test_minimal_dbg5_all_zero() -> None:
    dbg5 = {
        "stage": 0,
        "dist_to_peak_pct": 10.0,
        "oi_change_5m_pct": None,
        "oi_divergence_5m": False,
        "vol_z": 0.0,
        "atr_14_5m_pct": 0.0,
    }
    got = ContextScore5mIndicator().compute("X", datetime.now(timezone.utc), dbg5)
    assert got.score == 0.0
    assert all(v == 0.0 for v in got.parts.values())


def test_stage_four_max_contributions_without_vol_atr() -> None:
    dbg5 = {
        "stage": 4,
        "dist_to_peak_pct": 0.5,
        "oi_change_5m_pct": -2.0,
        "oi_divergence_5m": True,
        "vol_z": 0.0,
        "atr_14_5m_pct": 0.0,
    }
    got = ContextScore5mIndicator().compute("X", datetime.now(timezone.utc), dbg5)
    assert got.parts["stage"] == 0.30
    assert got.parts["near_top"] == 0.25
    assert got.parts["oi"] == 0.20
    assert abs(got.score - 0.75) < 1e-9


def test_max_unclamped_score_is_nine_tenths() -> None:
    dbg5 = {
        "stage": 4,
        "dist_to_peak_pct": 0.0,
        "oi_change_5m_pct": -5.0,
        "oi_divergence_5m": True,
        "vol_z": 3.0,
        "atr_14_5m_pct": 5.0,
    }
    got = ContextScore5mIndicator().compute("X", datetime.now(timezone.utc), dbg5)
    assert got.score == 0.9


def test_v2_matches_v1_function() -> None:
    dbg5 = {
        "stage": 3,
        "dist_to_peak_pct": 2.0,
        "oi_change_5m_pct": -0.5,
        "oi_divergence_5m": False,
        "vol_z": 1.5,
        "atr_14_5m_pct": 2.5,
    }
    s1, p1 = compute_context_score_5m(dbg5)
    got = ContextScore5mIndicator().compute("X", datetime.now(timezone.utc), dbg5)
    assert got.score == s1
    assert got.parts == p1
