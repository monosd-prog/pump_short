"""
Tests for pump_v2.indicators.oi_divergence_5m — composite parity with features.oi_divergence_5m.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Tuple

import pandas as pd
import pytest

from pump_v2.indicators.oi_divergence_5m import NEAR_TOP_PCT_THRESHOLD, OIDivergence5m
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


def _load_live_case(sym_dir: str) -> Tuple[str, datetime, Dict[str, Any]]:
    base = LIVE_COLLECTED / sym_dir
    meta = json.loads((base / "meta.json").read_text(encoding="utf-8"))
    expected = json.loads((base / "expected_indicators.json").read_text(encoding="utf-8"))
    ts = _parse_ts(meta["window_end_utc"])
    return meta["symbol"], ts, expected


def _history_from_expected(expected: Dict[str, Any]) -> Dict[str, Any]:
    oi5 = expected["oi_change_pct_5m"]["value"]
    dist = expected["dist_to_fsm_peak_pct"]["value"]
    return {
        "oi_change_5m_pct": oi5,
        "dist_to_peak_pct": dist if dist is not None else 0.0,
    }


# --- GROUP A: live_collected ---


@pytest.mark.parametrize("sym_dir", LIVE_SYMBOL_DIRS)
def test_live_collected_matches_expected(sym_dir: str) -> None:
    symbol, ts, expected = _load_live_case(sym_dir)
    exp_div = expected["oi_divergence_5m"]
    exp_val = bool(exp_div["value"])
    hist = _history_from_expected(expected)
    got = OIDivergence5m().compute(symbol, ts, hist)
    want = spf.oi_divergence_5m(
        hist["oi_change_5m_pct"],
        float(hist["dist_to_peak_pct"]),
    )
    assert got == want
    assert got == exp_val, f"{sym_dir}: got={got} exp={exp_val}"


@pytest.mark.parametrize("fixture_name", SYMBOL_EDGE_FIXTURES)
def test_symbols_fixture_matches_expected(fixture_name: str) -> None:
    base = SYMBOLS_FIXTURES / fixture_name
    meta = json.loads((base / "meta.json").read_text(encoding="utf-8"))
    expected = json.loads((base / "expected_indicators.json").read_text(encoding="utf-8"))
    ts = _parse_ts(meta["ts_snapshot_utc"])
    hist = _history_from_expected(expected)
    got = OIDivergence5m().compute(meta["symbol"], ts, hist)
    assert got == bool(expected["oi_divergence_5m"]["value"])


def test_btc_typical_positive_oi_not_divergence() -> None:
    """btc_typical: oi_change > 0 → divergence false."""
    base = SYMBOLS_FIXTURES / "btc_typical"
    expected = json.loads((base / "expected_indicators.json").read_text(encoding="utf-8"))
    assert expected["oi_divergence_5m"]["value"] is False
    hist = _history_from_expected(expected)
    assert OIDivergence5m().compute("BTCUSDT", datetime.now(timezone.utc), hist) is False


# --- GROUP B: logged_parity (formula on logged inputs; no oi_divergence column) ---


@pytest.mark.parametrize("sample_path", sorted(LOGGED_PARITY_SAMPLES.glob("sample_*.json")))
def test_logged_parity_formula_on_csv_inputs(sample_path: Path) -> None:
    """
    events_v3 has no oi_divergence_5m column; verify v2 == v1 on logged
    oi_change_5m_pct + dist_to_peak_pct when both present.
    """
    sample = json.loads(sample_path.read_text(encoding="utf-8"))
    logged = sample["logged_indicators"]
    oi_ent = logged.get("oi_change_5m_pct")
    dist_ent = logged.get("dist_to_peak_pct")
    if oi_ent is None or dist_ent is None:
        pytest.skip("missing oi_change_5m_pct or dist_to_peak_pct")
    oi_val = oi_ent.get("value")
    dist_val = dist_ent.get("value")
    if oi_val is None or dist_val is None:
        return
    hist = {"oi_change_5m_pct": float(oi_val), "dist_to_peak_pct": float(dist_val)}
    got = OIDivergence5m().compute("X", datetime.now(timezone.utc), hist)
    want = spf.oi_divergence_5m(hist["oi_change_5m_pct"], hist["dist_to_peak_pct"])
    assert got == want
    assert isinstance(got, bool)


# --- GROUP C: edge cases ---


def test_oi_change_none_returns_false() -> None:
    ts = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
    got = OIDivergence5m().compute("X", ts, {"oi_change_5m_pct": None, "dist_to_peak_pct": 1.0})
    assert got is False
    assert spf.oi_divergence_5m(None, 1.0) is False


def test_divergence_true_when_near_top_and_oi_falling() -> None:
    ts = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
    hist = {"oi_change_5m_pct": -1.0, "dist_to_peak_pct": NEAR_TOP_PCT_THRESHOLD}
    assert OIDivergence5m().compute("X", ts, hist) is True


def test_divergence_false_when_dist_above_threshold() -> None:
    ts = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
    hist = {"oi_change_5m_pct": -1.0, "dist_to_peak_pct": NEAR_TOP_PCT_THRESHOLD + 0.01}
    assert OIDivergence5m().compute("X", ts, hist) is False


def test_divergence_false_when_oi_positive() -> None:
    ts = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
    hist = {"oi_change_5m_pct": 0.1, "dist_to_peak_pct": 0.0}
    assert OIDivergence5m().compute("X", ts, hist) is False


def test_dist_zero_near_top() -> None:
    ts = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
    assert OIDivergence5m().compute("X", ts, {"oi_change_5m_pct": -0.5, "dist_to_peak_pct": 0.0}) is True


def test_missing_keys_raise() -> None:
    with pytest.raises(KeyError):
        OIDivergence5m().compute("X", datetime.now(timezone.utc), {"oi_change_5m_pct": 1.0})


def test_dist_none_raises() -> None:
    with pytest.raises(ValueError, match="dist_to_peak_pct must not be None"):
        OIDivergence5m().compute(
            "X",
            datetime.now(timezone.utc),
            {"oi_change_5m_pct": -1.0, "dist_to_peak_pct": None},
        )


def test_non_dict_history_raises() -> None:
    with pytest.raises(TypeError):
        OIDivergence5m().compute("X", datetime.now(timezone.utc), [])
