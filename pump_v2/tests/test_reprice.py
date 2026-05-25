"""Tests for TP/SL reprice from actual fill."""
from __future__ import annotations

from trading.reprice_tpsl import REPRICE_DRIFT_THRESHOLD, compute_repriced_tpsl


def test_reprice_triggers_on_drift() -> None:
    result = compute_repriced_tpsl(
        signal_entry=1.0,
        actual_entry=1.004,
        sl_price=1.004,
        tp_price=0.994,
        side="SHORT",
        sl_pct=0.004,
        tp_pct=0.006,
        drift_threshold=REPRICE_DRIFT_THRESHOLD,
    )
    assert result is not None
    new_sl, new_tp = result
    assert new_sl == 1.004 * 1.004
    assert new_tp == 1.004 * 0.994


def test_reprice_skips_on_small_drift() -> None:
    result = compute_repriced_tpsl(
        signal_entry=1.0,
        actual_entry=1.0005,
        sl_price=1.004,
        tp_price=0.994,
        side="SHORT",
        sl_pct=0.004,
        drift_threshold=REPRICE_DRIFT_THRESHOLD,
    )
    assert result is None


def test_reprice_long_side() -> None:
    result = compute_repriced_tpsl(
        signal_entry=1.0,
        actual_entry=0.996,
        sl_price=0.996,
        tp_price=1.006,
        side="LONG",
        sl_pct=0.004,
        drift_threshold=0.0,
    )
    assert result is not None
    new_sl, new_tp = result
    assert new_sl == 0.996 * (1.0 - 0.004)
    assert new_tp == 0.996 * (1.0 + 0.006)


def test_new_sl_correct_direction_short() -> None:
    actual_entry = 1.004
    result = compute_repriced_tpsl(
        signal_entry=1.0,
        actual_entry=actual_entry,
        sl_price=1.004,
        tp_price=0.994,
        side="SHORT",
        sl_pct=0.004,
        tp_pct=0.006,
        drift_threshold=0.0,
    )
    assert result is not None
    new_sl, new_tp = result
    assert new_sl > actual_entry
    assert new_tp < actual_entry
