"""
Tests for pump_v2.indicators.oi_change_pct — parity with v1 + fixture suites.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import pytest

from pump_v2.indicators.oi_change_pct import OIChangePct
from pump_v2.tests.conftest import LIVE_COLLECTED, LOGGED_PARITY_SAMPLES, SYMBOLS_FIXTURES
from short_pump import features as spf

LIVE_SYMBOL_DIRS = ["btcusdt", "ethusdt", "solusdt", "dogeusdt", "linkusdt"]
LOOKBACK_CASES = [(1, "oi_change_pct_1m"), (3, "oi_change_pct_3m"), (5, "oi_change_pct_5m")]

SYMBOL_EDGE_FIXTURES = [
    ("btc_typical", True),
    ("quiet_alt", True),
    ("pump_recent", True),
    ("no_liquidations", True),
    ("no_oi", False),
]


def _parse_ts(s: str) -> datetime:
    t = pd.Timestamp(s)
    if t.tzinfo is None:
        t = t.tz_localize("UTC")
    return t.to_pydatetime()


def _load_live_case(sym_dir: str) -> Tuple[str, datetime, pd.DataFrame, Dict[str, Any]]:
    base = LIVE_COLLECTED / sym_dir
    meta = json.loads((base / "meta.json").read_text(encoding="utf-8"))
    expected = json.loads((base / "expected_indicators.json").read_text(encoding="utf-8"))
    oi = pd.read_parquet(base / "oi_history.parquet")
    ts = _parse_ts(meta["window_end_utc"])
    symbol = meta["symbol"]
    return symbol, ts, oi, expected


def _v1_on_history(oi_raw: pd.DataFrame, ts: datetime, lookback: int) -> Optional[float]:
    """Reference: v1 on frame truncated to ts (same as v2 normalization + v1 body)."""
    from pump_v2.indicators.oi_change_pct import _normalize_oi_df, _oi_change_pct_v1_body

    return _oi_change_pct_v1_body(_normalize_oi_df(oi_raw, ts), lookback)


# --- GROUP A: live_collected ---


@pytest.mark.parametrize("sym_dir", LIVE_SYMBOL_DIRS)
@pytest.mark.parametrize("lookback,exp_key", LOOKBACK_CASES)
def test_live_collected_matches_expected(sym_dir: str, lookback: int, exp_key: str) -> None:
    symbol, ts, oi, expected = _load_live_case(sym_dir)
    exp_entry = expected[exp_key]
    exp_val = exp_entry["value"]
    ind = OIChangePct(lookback_minutes=lookback)
    got = ind.compute(symbol, ts, oi)
    v1_ref = spf.oi_change_pct(
        pd.DataFrame(
            {
                "ts": pd.to_datetime(oi["ts_utc"], utc=True),
                "openInterest": oi["oi"].astype(float),
            }
        ).sort_values("ts").reset_index(drop=True),
        lookback_minutes=lookback,
    )
    assert got == v1_ref, f"v2 vs v1 mismatch for {sym_dir} lb={lookback}"
    if exp_val is None:
        assert got is None, f"{sym_dir} {exp_key}: expected None, got {got}"
    else:
        assert got is not None
        assert abs(got - float(exp_val)) < 1e-6, f"{sym_dir} {exp_key}: got={got} exp={exp_val}"


# --- GROUP B: logged_parity smoke ---


def _logged_parity_sample_paths() -> List[Path]:
    return sorted(LOGGED_PARITY_SAMPLES.glob("sample_*.json"))


@pytest.mark.parametrize("sample_path", _logged_parity_sample_paths(), ids=lambda p: p.stem)
def test_logged_parity_oi_fields_smoke(sample_path: Path) -> None:
    sample = json.loads(sample_path.read_text(encoding="utf-8"))
    logged = sample["logged_indicators"]
    ind = OIChangePct(lookback_minutes=5)
    # empty history must not raise; type is None or float
    got = ind.compute("SYM", datetime.now(timezone.utc), pd.DataFrame())
    assert got is None

    for key in ("oi_change_1m_pct", "oi_change_5m_pct"):
        ent = logged.get(key, {})
        val = ent.get("value") if isinstance(ent, dict) else None
        if val is None:
            continue
        assert isinstance(val, (int, float)), f"{sample_path.name} {key} not numeric: {val!r}"
        fv = float(val)
        # logged values are fractions in many rows (e.g. 0.85 = 85%?) — fixtures use mixed;
        # smoke: reasonable magnitude (fraction -1..1 or percent -100..100)
        assert -100.0 <= fv <= 100.0, f"{sample_path.name} {key} out of range: {fv}"


# --- GROUP C: edge cases (mirror v1) ---


def test_empty_history_returns_none() -> None:
    ind = OIChangePct(lookback_minutes=5)
    ts = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
    assert ind.compute("X", ts, pd.DataFrame()) is None
    assert spf.oi_change_pct(pd.DataFrame(), lookback_minutes=5) is None


def test_single_row_returns_none() -> None:
    ts = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
    df = pd.DataFrame(
        {"ts_utc": [ts.isoformat()], "oi": [100.0]},
    )
    ind = OIChangePct(lookback_minutes=5)
    assert ind.compute("X", ts, df) is None


def test_lookback_fallback_to_first_row_mirrors_v1() -> None:
    """MIRRORS_V1_BEHAVIOR: no row at lookback_ts → uses iloc[0]."""
    t0 = pd.Timestamp("2026-05-15 07:00:00", tz="UTC")
    t1 = pd.Timestamp("2026-05-15 07:45:00", tz="UTC")
    raw = pd.DataFrame(
        {
            "ts_utc": [t0.isoformat(), t1.isoformat()],
            "oi": [100.0, 110.0],
        }
    )
    ts = t1.to_pydatetime()
    ind = OIChangePct(lookback_minutes=30)
    got = ind.compute("X", ts, raw)
    v1_df = pd.DataFrame({"ts": [t0, t1], "openInterest": [100.0, 110.0]})
    want = spf.oi_change_pct(v1_df, lookback_minutes=30)
    assert got == want == 10.0


def test_future_rows_excluded_by_ts() -> None:
    t0 = pd.Timestamp("2026-05-15 07:00:00", tz="UTC")
    t_mid = pd.Timestamp("2026-05-15 07:30:00", tz="UTC")
    t_end = pd.Timestamp("2026-05-15 07:50:00", tz="UTC")
    raw = pd.DataFrame(
        {
            "ts_utc": [t0.isoformat(), t_mid.isoformat(), t_end.isoformat()],
            "oi": [100.0, 105.0, 999.0],
        }
    )
    ind = OIChangePct(lookback_minutes=5)
    got = ind.compute("X", t_mid.to_pydatetime(), raw)
    # last row at eval time is t_mid (105), not t_end (999)
    assert got is not None
    assert got == pytest.approx(5.0)


@pytest.mark.parametrize("bad_lb", [0, -1])
def test_invalid_lookback_raises(bad_lb: int) -> None:
    with pytest.raises(ValueError, match="lookback_minutes must be positive"):
        OIChangePct(lookback_minutes=bad_lb)


def _symbols_oi_v1_frame(oi: pd.DataFrame) -> pd.DataFrame:
    if oi is None or oi.empty:
        return pd.DataFrame(columns=["ts", "openInterest"])
    return pd.DataFrame(
        {
            "ts": pd.to_datetime(oi["ts_utc"], utc=True),
            "openInterest": oi["oi"].astype(float),
        }
    ).sort_values("ts").reset_index(drop=True)


@pytest.mark.parametrize("fixture_name,has_oi", SYMBOL_EDGE_FIXTURES)
@pytest.mark.parametrize("lookback,exp_key", LOOKBACK_CASES)
def test_symbols_fixture_matches_v1_at_last_oi_ts(
    fixture_name: str, has_oi: bool, lookback: int, exp_key: str
) -> None:
    """REST symbols/ parquet is latest OI snapshot; v1 used iloc[-1]. Evaluate at last ts."""
    base = SYMBOLS_FIXTURES / fixture_name
    meta = json.loads((base / "meta.json").read_text(encoding="utf-8"))
    expected = json.loads((base / "expected_indicators.json").read_text(encoding="utf-8"))
    oi_path = base / "oi.parquet"
    if oi_path.is_file() and oi_path.stat().st_size > 0:
        oi = pd.read_parquet(oi_path)
    else:
        oi = pd.DataFrame(columns=["ts_utc", "oi"])
    symbol = meta["symbol"]
    exp_val = expected[exp_key]["value"]
    oi_v1 = _symbols_oi_v1_frame(oi)

    if not has_oi or oi_v1.empty:
        ts = _parse_ts(meta["ts_snapshot_utc"])
        got = OIChangePct(lookback_minutes=lookback).compute(symbol, ts, oi)
        assert got is None
        assert exp_val is None
        return

    ts_last = oi_v1["ts"].iloc[-1].to_pydatetime()
    got = OIChangePct(lookback_minutes=lookback).compute(symbol, ts_last, oi)
    want = spf.oi_change_pct(oi_v1, lookback_minutes=lookback)
    assert got == want
    if exp_val is None:
        assert got is None
    else:
        assert got is not None
        assert abs(got - float(exp_val)) < 1e-6


def test_symbols_historical_snapshot_before_oi_data_returns_none() -> None:
    """MIRRORS_V1 edge: REST oi.parquet is newer than ts_snapshot → no rows at snapshot."""
    base = SYMBOLS_FIXTURES / "btc_typical"
    meta = json.loads((base / "meta.json").read_text(encoding="utf-8"))
    oi = pd.read_parquet(base / "oi.parquet")
    ts_snap = _parse_ts(meta["ts_snapshot_utc"])
    got = OIChangePct(lookback_minutes=5).compute(meta["symbol"], ts_snap, oi)
    assert got is None
