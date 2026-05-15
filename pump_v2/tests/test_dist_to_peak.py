"""
Tests for dist_to_fsm_peak_pct and dist_to_window_peak_pct.
"""
from __future__ import annotations

import importlib.util
import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, Tuple

import pandas as pd
import pytest

from pump_v2.indicators.dist_to_fsm_peak_pct import DistToFSMPeakPct
from pump_v2.indicators.dist_to_window_peak_pct import DistToWindowPeakPct
from pump_v2.indicators.structure_state import StructureState, StructureStateIndicator
from pump_v2.tests.conftest import LIVE_COLLECTED, LOGGED_PARITY_SAMPLES, SYMBOLS_FIXTURES

_gen_path = Path(__file__).resolve().parent / "fixtures" / "_generate.py"
_spec = importlib.util.spec_from_file_location("fixture_gen", _gen_path)
assert _spec and _spec.loader
_gen = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_gen)

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


def _fixture_cfg() -> SimpleNamespace:
    return SimpleNamespace(
        drop1_min_pct=3.0,
        bounce1_min_pct=1.0,
        drop2_min_pct=2.0,
        bounce2_min_pct=0.8,
    )


def _klines_for_v1(k: pd.DataFrame) -> pd.DataFrame:
    if k is None or k.empty:
        return pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume"])
    x = k.copy()
    x["ts"] = pd.to_datetime(x["ts_utc"], utc=True)
    for c in ("open", "high", "low", "close", "volume"):
        x[c] = x[c].astype(float)
    return x[["ts", "open", "high", "low", "close", "volume"]]


def _load_live_case(sym_dir: str) -> Tuple[str, datetime, pd.DataFrame, Dict[str, Any]]:
    base = LIVE_COLLECTED / sym_dir
    meta = json.loads((base / "meta.json").read_text(encoding="utf-8"))
    expected = json.loads((base / "expected_indicators.json").read_text(encoding="utf-8"))
    kl = pd.read_parquet(base / "klines_5m.parquet")
    ts = _parse_ts(meta["window_end_utc"])
    return meta["symbol"], ts, kl, expected


def _last_close_at_ts(kl: pd.DataFrame, ts: datetime) -> float:
    k1 = _klines_for_v1(kl).sort_values("ts")
    t_end = pd.Timestamp(ts)
    if t_end.tzinfo is None:
        t_end = t_end.tz_localize("UTC")
    else:
        t_end = t_end.tz_convert("UTC")
    sub = k1[k1["ts"] <= t_end]
    if sub.empty:
        return 0.0
    return float(sub.iloc[-1]["close"])


# --- GROUP A: live_collected ---


@pytest.mark.parametrize("sym_dir", LIVE_SYMBOL_DIRS)
def test_live_fsm_dist_matches_expected(sym_dir: str) -> None:
    symbol, ts, kl, expected = _load_live_case(sym_dir)
    exp_val = float(expected["dist_to_fsm_peak_pct"]["value"])
    state = StructureStateIndicator().compute(symbol, ts, kl)
    last_close = _last_close_at_ts(kl, ts)
    got = DistToFSMPeakPct().compute(
        symbol,
        ts,
        {"structure_state": state, "current_price": last_close},
    )
    st, dist_win = _gen.replay_structure(_fixture_cfg(), kl)
    k1 = _klines_for_v1(kl).sort_values("ts")
    peak = float(st.peak_price or 0.0)
    if peak <= 0:
        peak = last_close
    want_fsm = (peak - last_close) / peak * 100.0 if peak > 0 else 0.0
    assert abs(got - want_fsm) < 1e-6
    assert abs(got - exp_val) < 1e-6


@pytest.mark.parametrize("sym_dir", LIVE_SYMBOL_DIRS)
def test_live_window_dist_matches_expected(sym_dir: str) -> None:
    symbol, ts, kl, expected = _load_live_case(sym_dir)
    exp_val = float(expected["dist_to_window_peak_pct"]["value"])
    got = DistToWindowPeakPct(window_minutes=None).compute(symbol, ts, kl)
    _, dist_win = _gen.replay_structure(_fixture_cfg(), kl)
    assert got is not None
    assert abs(got - dist_win) < 1e-6
    assert abs(got - exp_val) < 1e-6


@pytest.mark.parametrize("fixture_name", SYMBOL_EDGE_FIXTURES)
def test_symbols_window_dist_matches_expected(fixture_name: str) -> None:
    base = SYMBOLS_FIXTURES / fixture_name
    meta = json.loads((base / "meta.json").read_text(encoding="utf-8"))
    expected = json.loads((base / "expected_indicators.json").read_text(encoding="utf-8"))
    kp = base / "klines_5m.parquet"
    kl = pd.read_parquet(kp) if kp.is_file() and kp.stat().st_size > 0 else pd.DataFrame()
    ts = _parse_ts(meta["ts_snapshot_utc"])
    exp_val = float(expected["dist_to_window_peak_pct"]["value"])
    got = DistToWindowPeakPct(window_minutes=None).compute(meta["symbol"], ts, kl)
    _, dist_win = _gen.replay_structure(_fixture_cfg(), kl)
    assert got is not None
    assert abs(got - dist_win) < 1e-6
    assert abs(got - exp_val) < 1e-6


# --- GROUP B: logged_parity smoke ---


@pytest.mark.parametrize("sample_path", sorted(LOGGED_PARITY_SAMPLES.glob("sample_*.json")))
def test_logged_parity_dist_to_peak_smoke(sample_path: Path) -> None:
    sample = json.loads(sample_path.read_text(encoding="utf-8"))
    ent = sample["logged_indicators"].get("dist_to_peak_pct")
    if ent is None:
        pytest.skip("no dist_to_peak_pct")
    val = ent.get("value")
    if val is None:
        return
    fv = float(val)
    assert -5.0 <= fv <= 100.0


# --- GROUP C: dist_to_fsm_peak_pct edge cases ---


def test_fsm_peak_zero_uses_current_price() -> None:
    st = StructureState(peak_price=0.0)
    got = DistToFSMPeakPct().compute(
        "X",
        datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc),
        {"structure_state": st, "current_price": 100.0},
    )
    assert got == 0.0


def test_fsm_current_above_peak_negative() -> None:
    st = StructureState(peak_price=100.0)
    got = DistToFSMPeakPct().compute(
        "X",
        datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc),
        {"structure_state": st, "current_price": 105.0},
    )
    assert got < 0
    assert abs(got - (-5.0)) < 1e-6


def test_fsm_missing_keys_raise() -> None:
    with pytest.raises(KeyError):
        DistToFSMPeakPct().compute(
            "X",
            datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc),
            {"structure_state": StructureState()},
        )


def test_fsm_none_structure_raises() -> None:
    with pytest.raises(ValueError, match="structure_state must not be None"):
        DistToFSMPeakPct().compute(
            "X",
            datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc),
            {"structure_state": None, "current_price": 1.0},
        )


# --- GROUP D: dist_to_window_peak_pct edge cases ---


def test_window_empty_df_returns_none() -> None:
    ts = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
    assert DistToWindowPeakPct().compute("X", ts, pd.DataFrame()) is None


def test_window_single_candle() -> None:
    ts = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
    kl = pd.DataFrame(
        [
            {
                "ts_utc": pd.Timestamp(ts).isoformat(),
                "open": 100.0,
                "high": 110.0,
                "low": 99.0,
                "close": 105.0,
                "volume": 1.0,
            }
        ]
    )
    got = DistToWindowPeakPct().compute("X", ts, kl)
    assert got is not None
    assert abs(got - (110.0 - 105.0) / 110.0 * 100.0) < 1e-6


@pytest.mark.parametrize("bad", [0, -1])
def test_window_invalid_minutes_raises(bad: int) -> None:
    with pytest.raises(ValueError, match="window_minutes must be > 0"):
        DistToWindowPeakPct(window_minutes=bad)


def test_window_all_outside_window_returns_none() -> None:
    ts = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
    t_old = pd.Timestamp(ts) - pd.Timedelta(minutes=30)
    kl = pd.DataFrame(
        [
            {
                "ts_utc": t_old.isoformat(),
                "open": 100.0,
                "high": 120.0,
                "low": 99.0,
                "close": 100.0,
                "volume": 1.0,
            }
        ]
    )
    got = DistToWindowPeakPct(window_minutes=5).compute("X", ts, kl)
    assert got is None


def test_window_close_above_high_negative() -> None:
    ts = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
    kl = pd.DataFrame(
        [
            {
                "ts_utc": pd.Timestamp(ts).isoformat(),
                "open": 100.0,
                "high": 100.0,
                "low": 99.0,
                "close": 105.0,
                "volume": 1.0,
            }
        ]
    )
    got = DistToWindowPeakPct().compute("X", ts, kl)
    assert got is not None and got < 0
