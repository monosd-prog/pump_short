"""
Tests for pump_v2.indicators.dbg5_builder — parity with context5m helpers and smoke build.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import pytest

from pump_v2.indicators.dbg5_builder import (
    Dbg5Bundle,
    Dbg5BuilderIndicator,
    _atr_pct_14,
    _volume_z,
)
from pump_v2.indicators.structure_state import StructureState
from short_pump.context5m import _atr_pct_14 as v1_atr_pct_14
from short_pump.context5m import _volume_z as v1_volume_z


def _sample_candles(n: int = 20) -> list[dict]:
    rows = []
    for i in range(n):
        base = 100.0 + i * 0.5
        rows.append(
            {
                "open": base,
                "high": base + 1.0,
                "low": base - 0.5,
                "close": base + 0.2,
                "volume": 1000.0 + i * 50.0,
            }
        )
    return rows


def test_dbg5_bundle_as_dict() -> None:
    bundle = Dbg5Bundle(
        stage=3,
        dist_to_peak_pct=2.5,
        oi_change_5m_pct=-1.2,
        oi_divergence_5m=True,
        vol_z=1.5,
        atr_14_5m_pct=2.1,
    )
    d = bundle.as_dict()
    assert set(d.keys()) == {
        "stage",
        "dist_to_peak_pct",
        "oi_change_5m_pct",
        "oi_divergence_5m",
        "vol_z",
        "atr_14_5m_pct",
    }
    assert d["stage"] == 3
    assert d["oi_divergence_5m"] is True


def test_volume_z_parity() -> None:
    candles = _sample_candles(30)
    got = _volume_z(candles)
    want = v1_volume_z(candles)
    assert got == want


def test_atr_14_parity() -> None:
    candles = _sample_candles(20)
    got = _atr_pct_14(candles)
    want = v1_atr_pct_14(candles)
    assert got == want


def test_dbg5_builder_smoke() -> None:
    candles = _sample_candles(15)
    st = StructureState(stage=2, peak_price=float(candles[-1]["close"]) + 2.0)
    ts = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
    got = Dbg5BuilderIndicator().compute(
        "BTCUSDT",
        ts,
        {
            "candles_5m": candles,
            "structure_state": st,
        },
    )
    assert isinstance(got, Dbg5Bundle)
    assert isinstance(got.stage, int)
    assert isinstance(got.dist_to_peak_pct, float)
    assert got.oi_change_5m_pct is None
    assert got.oi_divergence_5m is False
    assert isinstance(got.vol_z, float)
    assert isinstance(got.atr_14_5m_pct, float)
    assert got.atr_14_5m_pct >= 0.0
