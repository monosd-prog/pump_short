"""Tests for pump_v2.prerun context builder and signal logger."""
from __future__ import annotations

import csv
from datetime import datetime, timezone

import pytest

from pump_v2.core.strategy_base import Signal
from pump_v2.prerun.context_builder import build_market_context_from_watcher
from pump_v2.prerun import signal_logger


def test_build_ctx_from_dbg5_dict() -> None:
    dbg5 = {
        "stage": 4,
        "dist_to_peak_pct": 4.2,
        "oi_change_5m_pct": -1.5,
        "oi_divergence_5m": True,
        "vol_z": 1.2,
        "atr_14_5m_pct": 2.1,
    }
    ctx = build_market_context_from_watcher(
        symbol="BTCUSDT",
        candles_5m=[{"close": 1.0}],
        oi_dict=None,
        funding_rate=0.0,
        dbg5=dbg5,
        context_score=0.55,
        ctx_parts={"stage": 0.3, "near_top": 0.0},
    )
    assert ctx.dbg5 is not None
    assert ctx.dbg5.stage == 4
    assert ctx.dbg5.dist_to_peak_pct == pytest.approx(4.2)
    assert ctx.indicators["context_score_5m"].score == pytest.approx(0.55)


def test_build_ctx_funding_stored() -> None:
    ctx = build_market_context_from_watcher(
        symbol="ETHUSDT",
        candles_5m=[{"close": 2.0}],
        oi_dict=None,
        funding_rate=-0.0003,
        dbg5={"stage": 4, "dist_to_peak_pct": 3.8},
        context_score=0.5,
        ctx_parts={},
    )
    assert ctx.funding == pytest.approx(-0.0003)


def test_build_ctx_none_oi_dict() -> None:
    ctx = build_market_context_from_watcher(
        symbol="SOLUSDT",
        candles_5m=[],
        oi_dict=None,
        funding_rate=0.0,
        dbg5={"stage": 3, "dist_to_peak_pct": 4.0},
        context_score=0.45,
        ctx_parts={},
    )
    assert ctx.symbol == "SOLUSDT"


def test_log_signal_creates_csv(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    csv_path = tmp_path / "prerun_signals_v2.csv"
    monkeypatch.setattr(signal_logger, "_CSV_PATH", csv_path)

    sig = Signal(
        strategy="short_pump",
        symbol="BTCUSDT",
        side="short",
        entry_price=1.0,
        sl_price=1.004,
        tp_price=0.994,
        notional_usd=7.0,
        leverage=4,
        ts_utc=datetime(2026, 1, 1, tzinfo=timezone.utc),
        metadata={
            "risk_profile": "short_pump_mid",
            "stage": 4,
            "dist_to_peak_pct": 4.2,
            "context_score": 0.55,
            "risk_mult": 0.7,
        },
    )
    signal_logger.log_prerun_signal(sig, "BTCUSDT")

    assert csv_path.exists()
    with csv_path.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 1
    assert rows[0]["risk_profile"] == "short_pump_mid"
    assert rows[0]["symbol"] == "BTCUSDT"


def test_log_none_signal_no_write(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    csv_path = tmp_path / "prerun_signals_v2.csv"
    monkeypatch.setattr(signal_logger, "_CSV_PATH", csv_path)

    signal_logger.log_prerun_signal(None, "BTCUSDT")
    assert not csv_path.exists()
