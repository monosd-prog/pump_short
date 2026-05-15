"""
Tests for pump_v2.indicators.cvd_delta_ratio — parity with short_pump/features.py.

v1 format: fraction in [-1, 1], NOT percent (same formula as delta_ratio, None on empty).
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd
import pytest

from pump_v2.indicators.cvd_delta_ratio import CVDDeltaRatio
from pump_v2.tests.conftest import LIVE_COLLECTED, LOGGED_PARITY_SAMPLES, SYMBOLS_FIXTURES
from short_pump import features as spf

LIVE_SYMBOL_DIRS = ["btcusdt", "ethusdt", "solusdt", "dogeusdt", "linkusdt"]
# expected_indicators only has 30s and 1m for cvd_delta_ratio (no 3m)
WINDOW_CASES = [
    (30, "cvd_delta_ratio_30s"),
    (60, "cvd_delta_ratio_1m"),
]

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


def _load_live_trades_case(sym_dir: str) -> Tuple[str, datetime, pd.DataFrame, Dict[str, Any]]:
    base = LIVE_COLLECTED / sym_dir
    meta = json.loads((base / "meta.json").read_text(encoding="utf-8"))
    expected = json.loads((base / "expected_indicators.json").read_text(encoding="utf-8"))
    trades = pd.read_parquet(base / "trades.parquet")
    ts = _parse_ts(meta["window_end_utc"])
    return meta["symbol"], ts, trades, expected


# --- GROUP A: live_collected ---


@pytest.mark.parametrize("sym_dir", LIVE_SYMBOL_DIRS)
@pytest.mark.parametrize("window_sec,exp_key", WINDOW_CASES)
def test_live_collected_matches_expected(sym_dir: str, window_sec: int, exp_key: str) -> None:
    symbol, ts, trades, expected = _load_live_trades_case(sym_dir)
    exp_val = expected[exp_key]["value"]
    ind = CVDDeltaRatio(window_seconds=window_sec)
    got = ind.compute(symbol, ts, trades)
    tr_v1 = _trades_for_v1(trades)
    since = pd.Timestamp(ts) - pd.Timedelta(seconds=window_sec)
    want = spf.cvd_delta_ratio(tr_v1, since)
    assert got == want, f"v2 vs v1 mismatch {sym_dir} window={window_sec}s"
    if exp_val is None:
        assert got is None, f"{sym_dir} {exp_key}: expected None, got {got}"
    else:
        assert got is not None
        assert abs(got - float(exp_val)) < 1e-6, f"{sym_dir} {exp_key}: got={got} exp={exp_val}"


# --- GROUP B: logged_parity smoke ---


def _logged_parity_sample_paths() -> List[Path]:
    return sorted(LOGGED_PARITY_SAMPLES.glob("sample_*.json"))


@pytest.mark.parametrize("sample_path", _logged_parity_sample_paths(), ids=lambda p: p.stem)
def test_logged_parity_cvd_delta_ratio_smoke(sample_path: Path) -> None:
    sample = json.loads(sample_path.read_text(encoding="utf-8"))
    logged = sample["logged_indicators"]
    ind = CVDDeltaRatio(window_seconds=60)
    got = ind.compute("SYM", datetime.now(timezone.utc), pd.DataFrame())
    assert got is None

    for key in ("cvd_delta_ratio_30s", "cvd_delta_ratio_1m"):
        ent = logged.get(key, {})
        val = ent.get("value") if isinstance(ent, dict) else None
        if val is None:
            continue
        assert isinstance(val, (int, float)), f"{sample_path.name} {key} not numeric"
        fv = float(val)
        # v1 short_pump: fraction in [-1, 1]
        assert -1.0 <= fv <= 1.0, f"{sample_path.name} {key}={fv} outside [-1,1]"


# --- GROUP C: edge cases ---


def test_empty_trades_returns_none() -> None:
    ts = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
    ind = CVDDeltaRatio(window_seconds=30)
    assert ind.compute("X", ts, pd.DataFrame()) is None
    assert spf.cvd_delta_ratio(pd.DataFrame(), pd.Timestamp(ts)) is None


def test_all_buy_returns_one() -> None:
    ts = datetime(2026, 5, 15, 12, 0, 0, tzinfo=timezone.utc)
    trades = pd.DataFrame(
        {"ts_utc": [ts.isoformat()], "side": ["Buy"], "qty": [5.0], "price": [1.0]}
    )
    got = CVDDeltaRatio(window_seconds=60).compute("X", ts, trades)
    assert got == 1.0


def test_all_sell_returns_minus_one() -> None:
    ts = datetime(2026, 5, 15, 12, 0, 0, tzinfo=timezone.utc)
    trades = pd.DataFrame(
        {"ts_utc": [ts.isoformat()], "side": ["Sell"], "qty": [5.0], "price": [1.0]}
    )
    got = CVDDeltaRatio(window_seconds=60).compute("X", ts, trades)
    assert got == -1.0


def test_all_trades_outside_window_returns_none() -> None:
    ts = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
    old = datetime(2026, 5, 15, 11, 0, tzinfo=timezone.utc)
    trades = pd.DataFrame(
        {"ts_utc": [old.isoformat()], "side": ["Buy"], "qty": [100.0], "price": [1.0]}
    )
    assert CVDDeltaRatio(window_seconds=30).compute("X", ts, trades) is None


def test_one_trade_in_window() -> None:
    ts = datetime(2026, 5, 15, 12, 0, 0, tzinfo=timezone.utc)
    trades = pd.DataFrame(
        {"ts_utc": [ts.isoformat()], "side": ["Buy"], "qty": [3.0], "price": [1.0]}
    )
    assert CVDDeltaRatio(window_seconds=60).compute("X", ts, trades) == 1.0


@pytest.mark.parametrize("bad", [0, -1])
def test_invalid_window_raises(bad: int) -> None:
    with pytest.raises(ValueError, match="window_seconds must be positive"):
        CVDDeltaRatio(window_seconds=bad)


@pytest.mark.parametrize("fixture_name", SYMBOL_EDGE_FIXTURES)
@pytest.mark.parametrize("window_sec,exp_key", WINDOW_CASES)
def test_symbols_fixture_matches_v1_at_snapshot(
    fixture_name: str, window_sec: int, exp_key: str
) -> None:
    base = SYMBOLS_FIXTURES / fixture_name
    meta = json.loads((base / "meta.json").read_text(encoding="utf-8"))
    expected = json.loads((base / "expected_indicators.json").read_text(encoding="utf-8"))
    tp = base / "trades.parquet"
    trades = pd.read_parquet(tp) if tp.is_file() and tp.stat().st_size > 0 else pd.DataFrame()
    ts = _parse_ts(meta["ts_snapshot_utc"])
    exp_val = expected[exp_key]["value"]
    tr_v1 = _trades_for_v1(trades)
    since = pd.Timestamp(ts) - pd.Timedelta(seconds=window_sec)
    got = CVDDeltaRatio(window_seconds=window_sec).compute(meta["symbol"], ts, trades)
    want = spf.cvd_delta_ratio(tr_v1, since)
    assert got == want
    if exp_val is None:
        assert got is None
    else:
        assert abs(got - float(exp_val)) < 1e-6
