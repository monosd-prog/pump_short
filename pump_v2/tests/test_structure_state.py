"""
Tests for pump_v2.indicators.structure_state — FSM replay vs v1 update_structure.
"""
from __future__ import annotations

import importlib.util
import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, Tuple

import pandas as pd
import pytest

from pump_v2.indicators.structure_state import (
    StructureFsmParams,
    StructureState,
    StructureStateIndicator,
    replay_structure_state,
)
from pump_v2.tests.conftest import LIVE_COLLECTED, LOGGED_PARITY_SAMPLES, SYMBOLS_FIXTURES
from short_pump.context5m import StructureState as V1StructureState
from short_pump.context5m import update_structure

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

STATE_FIELDS = (
    "stage",
    "peak_price",
    "low_after_peak",
    "low_after_bounce",
    "bounce_count",
    "armed_notified",
    "armed_since_utc",
)


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


def _assert_state_equal(got: StructureState, exp: Dict[str, Any], *, label: str = "") -> None:
    g = asdict(got)
    for field in STATE_FIELDS:
        gv, ev = g[field], exp[field]
        if field == "armed_since_utc":
            if ev in ("", None):
                assert gv in ("", None) or not gv, f"{label} armed_since_utc"
            else:
                assert gv == ev, f"{label} armed_since_utc"
        elif field in ("stage", "bounce_count"):
            assert gv == ev, f"{label} {field}: got={gv} exp={ev}"
        elif field == "armed_notified":
            assert bool(gv) == bool(ev), f"{label} armed_notified"
        else:
            assert abs(float(gv) - float(ev)) < 1e-6, f"{label} {field}: got={gv} exp={ev}"


def _load_live_case(sym_dir: str) -> Tuple[str, datetime, pd.DataFrame, Dict[str, Any]]:
    base = LIVE_COLLECTED / sym_dir
    meta = json.loads((base / "meta.json").read_text(encoding="utf-8"))
    expected = json.loads((base / "expected_indicators.json").read_text(encoding="utf-8"))
    kl = pd.read_parquet(base / "klines_5m.parquet")
    ts = _parse_ts(meta["window_end_utc"])
    return meta["symbol"], ts, kl, expected


# --- GROUP A ---


@pytest.mark.parametrize("sym_dir", LIVE_SYMBOL_DIRS)
def test_live_collected_matches_expected(sym_dir: str) -> None:
    symbol, ts, kl, expected = _load_live_case(sym_dir)
    exp_val = expected["structure_state"]["value"]
    got = StructureStateIndicator().compute(symbol, ts, kl)
    want_st, _ = _gen.replay_structure(_fixture_cfg(), kl)
    _assert_state_equal(got, asdict(want_st), label=f"gen {sym_dir}")
    _assert_state_equal(got, exp_val, label=f"expected {sym_dir}")


@pytest.mark.parametrize("fixture_name", SYMBOL_EDGE_FIXTURES)
def test_symbols_fixture_matches_expected(fixture_name: str) -> None:
    base = SYMBOLS_FIXTURES / fixture_name
    meta = json.loads((base / "meta.json").read_text(encoding="utf-8"))
    expected = json.loads((base / "expected_indicators.json").read_text(encoding="utf-8"))
    kp = base / "klines_5m.parquet"
    kl = pd.read_parquet(kp) if kp.is_file() and kp.stat().st_size > 0 else pd.DataFrame()
    ts = _parse_ts(meta["ts_snapshot_utc"])
    exp_val = expected["structure_state"]["value"]
    got = StructureStateIndicator().compute(meta["symbol"], ts, kl)
    want_st, _ = _gen.replay_structure(_fixture_cfg(), kl)
    _assert_state_equal(got, asdict(want_st))
    _assert_state_equal(got, exp_val)


# --- GROUP B ---


@pytest.mark.parametrize("sample_path", sorted(LOGGED_PARITY_SAMPLES.glob("sample_*.json")))
def test_logged_parity_stage_smoke(sample_path: Path) -> None:
    sample = json.loads(sample_path.read_text(encoding="utf-8"))
    logged = sample["logged_indicators"]
    stage_ent = logged.get("stage")
    if stage_ent is None:
        pytest.skip("no stage")
    val = stage_ent.get("value")
    if val is None:
        return
    assert isinstance(val, (int, float))
    assert 0 <= int(val) <= 4
    dist = logged.get("dist_to_peak_pct", {}).get("value")
    if dist is not None:
        assert float(dist) >= 0


# --- GROUP C ---


def test_empty_df_default_state() -> None:
    ts = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
    got = StructureStateIndicator().compute("X", ts, pd.DataFrame())
    assert got == StructureState()


def test_one_bar_stage_zero() -> None:
    ts = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
    kl = pd.DataFrame(
        [
            {
                "ts_utc": pd.Timestamp(ts).isoformat(),
                "open": 100.0,
                "high": 100.0,
                "low": 99.0,
                "close": 100.0,
                "volume": 1.0,
            }
        ]
    )
    got = StructureStateIndicator().compute("X", ts, kl)
    assert got.stage == 0


def test_flat_prices_stay_stage_zero() -> None:
    """No pullback from peak → stage stays 0 (monotonic drop >3% would enter stage 1)."""
    ts = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
    rows = []
    for i in range(10):
        rows.append(
            {
                "ts_utc": (pd.Timestamp(ts) - pd.Timedelta(minutes=5 * (9 - i))).isoformat(),
                "open": 100.0,
                "high": 100.0,
                "low": 100.0,
                "close": 100.0,
                "volume": 1.0,
            }
        )
    got = StructureStateIndicator().compute("X", ts, pd.DataFrame(rows))
    assert got.stage == 0


@pytest.mark.parametrize(
    "bad",
    [
        ("drop1_min_pct", -1.0),
        ("bounce1_min_pct", 0.0),
    ],
)
def test_invalid_threshold_raises(bad: Tuple[str, float]) -> None:
    kw = {bad[0]: bad[1]}
    with pytest.raises(ValueError, match="must be > 0"):
        StructureStateIndicator(**kw)


# --- GROUP D: FSM transitions ---


def _candles_from_closes(closes: list[float], *, base_ts: datetime) -> pd.DataFrame:
    rows = []
    for i, close in enumerate(closes):
        t = pd.Timestamp(base_ts) - pd.Timedelta(minutes=5 * (len(closes) - 1 - i))
        h = close if i == 0 else max(close, closes[i - 1])
        rows.append(
            {
                "ts_utc": t.isoformat(),
                "open": close,
                "high": h + 0.5,
                "low": close - 0.5,
                "close": close,
                "volume": 1.0,
            }
        )
    x = pd.DataFrame(rows)
    x["ts"] = pd.to_datetime(x["ts_utc"], utc=True)
    return x[["ts", "open", "high", "low", "close", "volume"]]


def test_synthetic_reaches_stage_four() -> None:
    """
  100 peak → 96 (-4%) stage1 → 97.5 bounce stage2 → 95 dip stage3 → 96.5 bounce stage4.
    """
    base = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
    closes = [100.0, 96.0, 97.5, 95.0, 96.5]
    candles = _candles_from_closes(closes, base_ts=base)
    st = replay_structure_state(candles, params=StructureFsmParams())
    assert st.stage == 4
    assert st.bounce_count == 2


def test_v2_matches_v1_update_on_same_replay_path() -> None:
    base = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
    closes = [100.0, 96.0, 97.5, 95.0, 96.5]
    candles = _candles_from_closes(closes, base_ts=base)
    v2_st = replay_structure_state(candles, params=StructureFsmParams())
    v1_st = V1StructureState()
    cfg = _fixture_cfg()
    df = candles.sort_values("ts").reset_index(drop=True)
    for i in range(len(df)):
        sub = df.iloc[: i + 1]
        peak = float(sub["high"].tail(20).max())
        last = float(sub.iloc[-1]["close"])
        update_structure(cfg, v1_st, last, peak)
    assert asdict(v2_st) == asdict(v1_st)
