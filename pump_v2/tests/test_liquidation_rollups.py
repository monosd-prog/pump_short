"""
Tests for pump_v2.indicators.liquidation_rollups.

GROUP A parity vs pump_v2/tests/fixtures/_generate.py:liquidation_rollups_from_csv.
GROUP B: logged_parity liq_* columns — smoke only (in-memory v1 path).
"""
from __future__ import annotations

import importlib.util
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Tuple

import pandas as pd
import pytest

from pump_v2.indicators.liquidation_rollups import (
    LiquidationRollups,
    LiquidationRollupsIndicator,
    _zero_rollups,
)
from pump_v2.tests.conftest import LIVE_COLLECTED, LOGGED_PARITY_SAMPLES, SYMBOLS_FIXTURES

_gen_path = Path(__file__).resolve().parent / "fixtures" / "_generate.py"
_spec = importlib.util.spec_from_file_location("fixture_gen", _gen_path)
assert _spec and _spec.loader
_gen = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_gen)

LIVE_SYMBOL_DIRS = ["btcusdt", "ethusdt", "solusdt", "dogeusdt", "linkusdt"]
SYMBOL_EDGE_FIXTURES = ["btc_typical", "quiet_alt", "pump_recent", "no_liquidations", "no_oi"]

ROLLUP_FIELDS = (
    "long_count_30s",
    "long_usd_30s",
    "short_count_30s",
    "short_usd_30s",
    "long_count_60s",
    "long_usd_60s",
    "short_count_60s",
    "short_usd_60s",
)

LOGGED_LIQ_FIELDS = (
    "liq_long_count_30s",
    "liq_long_usd_30s",
    "liq_short_count_30s",
    "liq_short_usd_30s",
    "liq_long_count_1m",
    "liq_long_usd_1m",
    "liq_short_count_1m",
    "liq_short_usd_1m",
)


def _parse_ts(s: str) -> datetime:
    t = pd.Timestamp(s)
    if t.tzinfo is None:
        t = t.tz_localize("UTC")
    return t.to_pydatetime()


def _assert_rollups_equal(got: LiquidationRollups, exp: Dict[str, Any], *, label: str = "") -> None:
    got_d = got.as_dict()
    for field in ROLLUP_FIELDS:
        gv, ev = got_d[field], exp[field]
        if isinstance(ev, int) or field.endswith("_count"):
            assert gv == ev, f"{label} {field}: got={gv} exp={ev}"
        else:
            assert abs(float(gv) - float(ev)) < 1e-6, f"{label} {field}: got={gv} exp={ev}"


def _load_live_case(sym_dir: str) -> Tuple[str, datetime, pd.DataFrame, Dict[str, Any]]:
    base = LIVE_COLLECTED / sym_dir
    meta = json.loads((base / "meta.json").read_text(encoding="utf-8"))
    expected = json.loads((base / "expected_indicators.json").read_text(encoding="utf-8"))
    liq_path = base / "liquidations.csv"
    liq = pd.read_csv(liq_path) if liq_path.is_file() else pd.DataFrame()
    ts = _parse_ts(meta["window_end_utc"])
    return meta["symbol"], ts, liq, expected


# --- GROUP A ---


@pytest.mark.parametrize("sym_dir", LIVE_SYMBOL_DIRS)
def test_live_collected_matches_expected(sym_dir: str) -> None:
    symbol, ts, liq, expected = _load_live_case(sym_dir)
    exp_val = expected["liquidation_rollups"]["value"]
    ind = LiquidationRollupsIndicator()
    got = ind.compute(symbol, ts, liq)
    want = _gen.liquidation_rollups_from_csv(symbol, pd.Timestamp(ts), LIVE_COLLECTED / sym_dir / "liquidations.csv")
    _assert_rollups_equal(got, want, label=f"gen {sym_dir}")
    _assert_rollups_equal(got, exp_val, label=f"expected {sym_dir}")


def test_doge_liquidations_outside_snapshot_window_are_zero() -> None:
    """4 DOGE rows exist in CSV but all are before window_end-60s → zeros at snapshot."""
    symbol, ts, liq, expected = _load_live_case("dogeusdt")
    got = LiquidationRollupsIndicator().compute(symbol, ts, liq)
    assert got == _zero_rollups()
    assert expected["liquidation_rollups"]["value"]["long_count_60s"] == 0


def test_link_single_row_outside_60s_window() -> None:
    symbol, ts, liq, _ = _load_live_case("linkusdt")
    assert len(liq) == 1
    got = LiquidationRollupsIndicator().compute(symbol, ts, liq)
    assert got == _zero_rollups()


def test_synthetic_event_inside_30s_window() -> None:
    ts = datetime(2026, 5, 15, 12, 0, 0, tzinfo=timezone.utc)
    t_in = pd.Timestamp(ts) - pd.Timedelta(seconds=10)
    liq = pd.DataFrame(
        [
            {
                "ts_utc": t_in.isoformat(),
                "ts_ms": int(t_in.timestamp() * 1000),
                "symbol": "DOGEUSDT",
                "side": "Sell",
                "qty": 100.0,
                "price": 1.0,
                "value_usd": 100.0,
            }
        ]
    )
    got = LiquidationRollupsIndicator().compute("DOGEUSDT", ts, liq)
    assert got.long_count_30s == 1
    assert abs(got.long_usd_30s - 100.0) < 1e-6
    assert got.short_count_30s == 0


# --- GROUP B ---


@pytest.mark.parametrize("sample_path", sorted(LOGGED_PARITY_SAMPLES.glob("sample_*.json")))
def test_logged_parity_liq_fields_smoke(sample_path: Path) -> None:
    sample = json.loads(sample_path.read_text(encoding="utf-8"))
    logged = sample["logged_indicators"]
    for field in LOGGED_LIQ_FIELDS:
        ent = logged.get(field)
        assert ent is not None, f"missing {field} in {sample_path.name}"
        val = ent.get("value")
        if val is None:
            continue
        if field.endswith("_count"):
            assert isinstance(val, (int, float))
            assert int(val) >= 0
        else:
            assert isinstance(val, (int, float))
            assert float(val) >= 0
    lc30 = logged["liq_long_count_30s"].get("value")
    lu30 = logged["liq_long_usd_30s"].get("value")
    if lc30 is not None and lu30 is not None and int(lc30) == 0:
        assert float(lu30) == 0.0


# --- GROUP C ---


def test_empty_df_all_zeros() -> None:
    ts = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
    got = LiquidationRollupsIndicator().compute("X", ts, pd.DataFrame())
    assert got == _zero_rollups()


def test_history_none_all_zeros() -> None:
    ts = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
    got = LiquidationRollupsIndicator().compute("X", ts, None)
    assert got == _zero_rollups()


def test_all_buy_maps_to_short_only() -> None:
    ts = datetime(2026, 5, 15, 12, 0, 0, tzinfo=timezone.utc)
    t0 = pd.Timestamp(ts) - pd.Timedelta(seconds=5)
    liq = pd.DataFrame(
        [
            {
                "ts_utc": t0.isoformat(),
                "symbol": "X",
                "side": "Buy",
                "value_usd": 50.0,
            }
        ]
    )
    got = LiquidationRollupsIndicator().compute("X", ts, liq)
    assert got.short_count_30s == 1
    assert got.long_count_30s == 0


def test_all_sell_maps_to_long_only() -> None:
    ts = datetime(2026, 5, 15, 12, 0, 0, tzinfo=timezone.utc)
    t0 = pd.Timestamp(ts) - pd.Timedelta(seconds=5)
    liq = pd.DataFrame(
        [
            {
                "ts_utc": t0.isoformat(),
                "symbol": "X",
                "side": "Sell",
                "value_usd": 75.0,
            }
        ]
    )
    got = LiquidationRollupsIndicator().compute("X", ts, liq)
    assert got.long_count_30s == 1
    assert abs(got.long_usd_30s - 75.0) < 1e-6
    assert got.short_count_30s == 0


def test_events_outside_window_all_zeros() -> None:
    ts = datetime(2026, 5, 15, 12, 0, 0, tzinfo=timezone.utc)
    t_old = pd.Timestamp(ts) - pd.Timedelta(seconds=120)
    liq = pd.DataFrame(
        [
            {
                "ts_utc": t_old.isoformat(),
                "symbol": "X",
                "side": "Sell",
                "value_usd": 999.0,
            }
        ]
    )
    got = LiquidationRollupsIndicator().compute("X", ts, liq)
    assert got == _zero_rollups()


def test_partial_window_includes_only_in_range() -> None:
    ts = datetime(2026, 5, 15, 12, 0, 0, tzinfo=timezone.utc)
    t_in = pd.Timestamp(ts) - pd.Timedelta(seconds=20)
    t_out = pd.Timestamp(ts) - pd.Timedelta(seconds=90)
    liq = pd.DataFrame(
        [
            {"ts_utc": t_in.isoformat(), "symbol": "X", "side": "Sell", "value_usd": 10.0},
            {"ts_utc": t_out.isoformat(), "symbol": "X", "side": "Sell", "value_usd": 90.0},
        ]
    )
    got = LiquidationRollupsIndicator().compute("X", ts, liq)
    assert got.long_count_60s == 1
    assert abs(got.long_usd_60s - 10.0) < 1e-6


def test_unsupported_windows_raises() -> None:
    with pytest.raises(NotImplementedError, match="Only \\(30, 60\\)"):
        LiquidationRollupsIndicator(windows_seconds=(30, 45))


@pytest.mark.parametrize("fixture_name", SYMBOL_EDGE_FIXTURES)
def test_symbols_fixture_matches_expected(fixture_name: str) -> None:
    base = SYMBOLS_FIXTURES / fixture_name
    meta = json.loads((base / "meta.json").read_text(encoding="utf-8"))
    expected = json.loads((base / "expected_indicators.json").read_text(encoding="utf-8"))
    liq_path = base / "liquidations.csv"
    liq = pd.read_csv(liq_path) if liq_path.is_file() else pd.DataFrame()
    ts = _parse_ts(meta["ts_snapshot_utc"])
    exp_val = expected["liquidation_rollups"]["value"]
    got = LiquidationRollupsIndicator().compute(meta["symbol"], ts, liq)
    want = _gen.liquidation_rollups_from_csv(meta["symbol"], pd.Timestamp(ts), liq_path)
    _assert_rollups_equal(got, want)
    _assert_rollups_equal(got, exp_val)
