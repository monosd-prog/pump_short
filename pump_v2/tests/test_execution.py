"""Tests for pump_v2 execution: entry gate, signal adapter, enqueuer."""
from __future__ import annotations

import time
from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from pump_v2.core.market_context import MarketContext
from pump_v2.core.strategy_base import Signal
from pump_v2.execution.entry_gate import EntryGate
from pump_v2.execution.enqueuer import maybe_enqueue_v2_signal
from pump_v2.execution.signal_adapter import v2_signal_to_queue_dict


def _v2_signal(**meta) -> Signal:
    m = {
        "stage": 4,
        "dist_to_peak_pct": 4.2,
        "context_score": 0.5,
        "risk_profile": "short_pump_mid",
        **meta,
    }
    return Signal(
        strategy="short_pump",
        symbol="BTCUSDT",
        side="short",
        entry_price=100.0,
        tp_price=99.4,
        sl_price=100.4,
        notional_usd=10.0,
        leverage=4,
        ts_utc=datetime(2026, 5, 22, 12, 0, 0, tzinfo=timezone.utc),
        metadata=m,
    )


def _ctx(**indicators) -> MarketContext:
    ctx = MarketContext(
        symbol="BTCUSDT",
        ts_utc=datetime(2026, 5, 22, 12, 0, 0, tzinfo=timezone.utc),
        funding=0.0,
    )
    ctx.indicators.update(indicators)
    return ctx


def test_gate_allows_first_call() -> None:
    gate = EntryGate(cooldown_seconds=120)
    assert gate.allow_and_record("BTCUSDT") is True


def test_gate_blocks_second_call() -> None:
    gate = EntryGate(cooldown_seconds=120)
    gate.allow_and_record("BTCUSDT")
    assert gate.allow_and_record("BTCUSDT") is False


def test_gate_allows_after_cooldown() -> None:
    gate = EntryGate(cooldown_seconds=0)
    gate.allow_and_record("BTCUSDT")
    time.sleep(0.01)
    assert gate.allow_and_record("BTCUSDT") is True


def test_gate_independent_symbols() -> None:
    gate = EntryGate(cooldown_seconds=120)
    assert gate.allow_and_record("AAA") is True
    assert gate.allow_and_record("BBB") is True


def test_adapter_side_uppercase() -> None:
    d = v2_signal_to_queue_dict(_v2_signal(), _ctx())
    assert d["side"] == "SHORT"


def test_adapter_event_id_has_v2_prefix() -> None:
    d = v2_signal_to_queue_dict(_v2_signal(), _ctx())
    assert str(d["event_id"]).startswith("v2_")


def test_adapter_stage_from_metadata() -> None:
    sig = _v2_signal(stage=4, dist_to_peak_pct=4.2, context_score=0.5)
    d = v2_signal_to_queue_dict(sig, _ctx())
    assert d["stage"] == 4
    assert d["dist_to_peak_pct"] == pytest.approx(4.2)
    assert d["context_score"] == pytest.approx(0.5)


def test_adapter_liq_from_ctx_indicators() -> None:
    ctx = _ctx(liq_long_usd_30s=45.0)
    d = v2_signal_to_queue_dict(_v2_signal(), ctx)
    assert d["liq_long_usd_30s"] == 45.0


def test_adapter_funding_from_metadata() -> None:
    sig = _v2_signal(funding_rate_abs=0.0007)
    d = v2_signal_to_queue_dict(sig, _ctx())
    assert d["funding_rate_abs"] == pytest.approx(0.0007)


def test_enqueue_none_signal_returns_false() -> None:
    assert maybe_enqueue_v2_signal(None, _ctx()) is False


def test_enqueue_blocked_by_cooldown() -> None:
    gate = EntryGate(cooldown_seconds=120)
    gate.allow_and_record("BTCUSDT")
    sig = _v2_signal()
    assert maybe_enqueue_v2_signal(sig, _ctx(), gate=gate) is False


@patch("trading.queue.enqueue_signal_dict")
def test_enqueue_success(mock_enqueue) -> None:
    gate = EntryGate(cooldown_seconds=120)
    sig = _v2_signal()
    assert maybe_enqueue_v2_signal(sig, _ctx(), gate=gate) is True
    mock_enqueue.assert_called_once()
    payload = mock_enqueue.call_args[0][0]
    assert payload["symbol"] == "BTCUSDT"
    assert payload["source"] == "pump_v2"
