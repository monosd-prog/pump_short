"""
Tests for pump_v2.indicators.cvd_5m — parity with common.market_features.cvd_5m.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd
import pytest

from common import market_features as mf
from pump_v2.indicators.cvd_5m import CVD5m
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
    t = pd.Timestamp(s)
    if t.tzinfo is None:
        t = t.tz_localize("UTC")
    return t.to_pydatetime()


def _trades_for_v1(trades_raw: pd.DataFrame) -> pd.DataFrame:
    if trades_raw is None or trades_raw.empty:
        return pd.DataFrame(columns=["ts", "side", "qty"])
    x = trades_raw.copy()
    if "ts" not in x.columns:
        if "ts_ms" in x.columns:
            x["ts"] = pd.to_datetime(x["ts_ms"], unit="ms", utc=True)
        else:
            x["ts"] = pd.to_datetime(x["ts_utc"], utc=True, format="mixed")
    return x[["ts", "side", "qty"]].sort_values("ts").reset_index(drop=True)


def _load_live_case(sym_dir: str) -> Tuple[str, datetime, pd.DataFrame, Dict[str, Any]]:
    base = LIVE_COLLECTED / sym_dir
    meta = json.loads((base / "meta.json").read_text(encoding="utf-8"))
    expected = json.loads((base / "expected_indicators.json").read_text(encoding="utf-8"))
    trades = pd.read_parquet(base / "trades.parquet")
    ts = _parse_ts(meta["window_end_utc"])
    return meta["symbol"], ts, trades, expected


# --- GROUP A: live_collected ---


@pytest.mark.parametrize("sym_dir", LIVE_SYMBOL_DIRS)
def test_live_collected_cvd_abs_matches_expected(sym_dir: str) -> None:
    symbol, ts, trades, expected = _load_live_case(sym_dir)
    ent = expected["cvd_5m"]
    exp_abs = ent["value"]
    exp_ratio = ent.get("cvd_ratio_5m")
    ind = CVD5m()
    got_abs, got_ratio = ind.compute_full(symbol, ts, trades)
    tr_v1 = _trades_for_v1(trades)
    want_abs, want_ratio = mf.cvd_5m(tr_v1, pd.Timestamp(ts))
    assert got_abs == want_abs, f"v2 vs v1 cvd_abs mismatch {sym_dir}"
    assert got_ratio == want_ratio, f"v2 vs v1 cvd_ratio mismatch {sym_dir}"
    if exp_abs is None:
        assert got_abs is None
    else:
        assert got_abs is not None
        assert abs(got_abs - float(exp_abs)) < 1e-6
    if exp_ratio is not None and got_ratio is not None:
        assert abs(got_ratio - float(exp_ratio)) < 1e-6


# --- GROUP B: logged_parity smoke ---


def _logged_parity_sample_paths() -> List[Path]:
    return sorted(LOGGED_PARITY_SAMPLES.glob("sample_*.json"))


@pytest.mark.parametrize("sample_path", _logged_parity_sample_paths(), ids=lambda p: p.stem)
def test_logged_parity_cvd_abs_smoke(sample_path: Path) -> None:
    sample = json.loads(sample_path.read_text(encoding="utf-8"))
    logged = sample["logged_indicators"]
    ind = CVD5m()
    got = ind.compute("SYM", datetime.now(timezone.utc), pd.DataFrame())
    assert got is None

    ent = logged.get("cvd_abs_5m", {})
    val = ent.get("value") if isinstance(ent, dict) else None
    if val is None:
        return
    assert isinstance(val, (int, float))
    fv = float(val)
    # raw CVD in base qty — wide range; smoke only
    assert -1e9 < fv < 1e9


# --- GROUP C: edge cases ---


def test_empty_trades_returns_none() -> None:
    ts = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
    ind = CVD5m()
    assert ind.compute("X", ts, pd.DataFrame()) is None
    assert mf.cvd_5m(pd.DataFrame(), pd.Timestamp(ts)) == (None, None)


def test_all_buy_positive_cvd() -> None:
    t0 = pd.Timestamp("2026-05-15 11:46:00", tz="UTC")
    t1 = pd.Timestamp("2026-05-15 11:50:00", tz="UTC")
    trades = pd.DataFrame(
        {
            "ts_utc": [t0.isoformat(), t1.isoformat()],
            "side": ["Buy", "Buy"],
            "qty": [10.0, 5.0],
            "price": [1.0, 1.0],
        }
    )
    ts = t1.to_pydatetime()
    got = CVD5m().compute("X", ts, trades)
    assert got is not None and got > 0


def test_all_sell_negative_cvd() -> None:
    t0 = pd.Timestamp("2026-05-15 11:46:00", tz="UTC")
    t1 = pd.Timestamp("2026-05-15 11:50:00", tz="UTC")
    trades = pd.DataFrame(
        {
            "ts_utc": [t0.isoformat(), t1.isoformat()],
            "side": ["Sell", "Sell"],
            "qty": [10.0, 5.0],
            "price": [1.0, 1.0],
        }
    )
    ts = t1.to_pydatetime()
    got = CVD5m().compute("X", ts, trades)
    assert got is not None and got < 0


def test_all_trades_after_evaluation_ts_returns_none() -> None:
    ts = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
    future = datetime(2026, 5, 15, 12, 10, tzinfo=timezone.utc)
    trades = pd.DataFrame(
        {
            "ts_utc": [future.isoformat()],
            "side": ["Buy"],
            "qty": [100.0],
            "price": [1.0],
        }
    )
    assert CVD5m().compute("X", ts, trades) is None


def test_partial_window_still_computes() -> None:
    """MIRRORS_V1: fewer than window_bars 1m buckets still sums available bars."""
    t0 = pd.Timestamp("2026-05-15 11:49:30", tz="UTC")
    t1 = pd.Timestamp("2026-05-15 11:50:00", tz="UTC")
    trades = pd.DataFrame(
        {
            "ts_utc": [t0.isoformat(), t1.isoformat()],
            "side": ["Buy", "Sell"],
            "qty": [10.0, 4.0],
            "price": [1.0, 1.0],
        }
    )
    got = CVD5m().compute("X", t1.to_pydatetime(), trades)
    assert got == pytest.approx(6.0)


@pytest.mark.parametrize("bar_size,window", [(0, 5), (60, 0), (-1, 5), (60, -1)])
def test_invalid_params_raise(bar_size: int, window: int) -> None:
    with pytest.raises(ValueError):
        CVD5m(bar_size_sec=bar_size, window_bars=window)


@pytest.mark.parametrize("fixture_name", SYMBOL_EDGE_FIXTURES)
def test_symbols_fixture_matches_v1_at_snapshot(fixture_name: str) -> None:
    base = SYMBOLS_FIXTURES / fixture_name
    meta = json.loads((base / "meta.json").read_text(encoding="utf-8"))
    expected = json.loads((base / "expected_indicators.json").read_text(encoding="utf-8"))
    tp = base / "trades.parquet"
    trades = pd.read_parquet(tp) if tp.is_file() and tp.stat().st_size > 0 else pd.DataFrame()
    ts = _parse_ts(meta["ts_snapshot_utc"])
    exp_val = expected["cvd_5m"]["value"]
    tr_v1 = _trades_for_v1(trades)
    got = CVD5m().compute(meta["symbol"], ts, trades)
    want_abs, _ = mf.cvd_5m(tr_v1, pd.Timestamp(ts))
    assert got == want_abs
    if exp_val is None:
        assert got is None
    else:
        assert abs(got - float(exp_val)) < 1e-6
