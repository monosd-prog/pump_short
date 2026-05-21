"""Tests for pump_v2.strategies.short_pump — ShortPumpStrategy (mid + funding_1R)."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from pump_v2.core.market_context import MarketContext
from pump_v2.indicators.context_score_5m import ContextScore5m
from pump_v2.indicators.dbg5_builder import Dbg5Bundle
from pump_v2.strategies.short_pump import ShortPumpStrategy


def _make_ctx(
    stage: int,
    dist: float,
    ctx_score: float,
    close: float = 1.0,
    funding: float = 0.0,
) -> MarketContext:
    ts = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    return MarketContext(
        symbol="BTCUSDT",
        ts_utc=ts,
        funding=funding,
        dbg5=Dbg5Bundle(
            stage=stage,
            dist_to_peak_pct=dist,
            oi_change_5m_pct=None,
            oi_divergence_5m=False,
            vol_z=0.0,
            atr_14_5m_pct=0.0,
        ),
        indicators={
            "context_score_5m": ContextScore5m(score=ctx_score, parts={}),
        },
        candles={"5m": [{"close": close}]},
    )


@pytest.fixture
def strategy() -> ShortPumpStrategy:
    return ShortPumpStrategy(params={}, risk={})


def test_mid_signal_returned(strategy: ShortPumpStrategy) -> None:
    sig = strategy.check_signal(_make_ctx(stage=4, dist=4.2, ctx_score=0.50))
    assert sig is not None
    assert sig.metadata["risk_profile"] == "short_pump_mid"
    assert sig.notional_usd == 7.0
    assert sig.leverage == 4
    assert sig.side == "short"


def test_mid_stage3_also_triggers(strategy: ShortPumpStrategy) -> None:
    # Y-fix: classify_profile accepts stage 3, but tradeable gate requires stage == 4
    sig = strategy.check_signal(_make_ctx(stage=3, dist=4.2, ctx_score=0.50))
    assert sig is None


def test_dist_below_tradeable_min(strategy: ShortPumpStrategy) -> None:
    assert strategy.check_signal(_make_ctx(stage=4, dist=2.9, ctx_score=0.50)) is None


def test_dist_above_mid_range(strategy: ShortPumpStrategy) -> None:
    assert strategy.check_signal(_make_ctx(stage=4, dist=5.5, ctx_score=0.50)) is None


def test_ctx_below_mid_range(strategy: ShortPumpStrategy) -> None:
    assert strategy.check_signal(_make_ctx(stage=4, dist=4.2, ctx_score=0.35)) is None


def test_ctx_above_mid_range(strategy: ShortPumpStrategy) -> None:
    assert strategy.check_signal(_make_ctx(stage=4, dist=4.2, ctx_score=0.65)) is None


def test_tp_sl_prices_correct(strategy: ShortPumpStrategy) -> None:
    sig = strategy.check_signal(_make_ctx(stage=4, dist=4.2, ctx_score=0.50, close=1.0))
    assert sig is not None
    assert sig.entry_price == pytest.approx(1.0)
    assert sig.tp_price == pytest.approx(0.994)
    assert sig.sl_price == pytest.approx(1.004)


def test_no_dbg5_returns_none(strategy: ShortPumpStrategy) -> None:
    ctx = _make_ctx(stage=4, dist=4.2, ctx_score=0.50)
    ctx.dbg5 = None
    assert strategy.check_signal(ctx) is None


def test_funding_band1_signal_returned(strategy: ShortPumpStrategy) -> None:
    sig = strategy.check_signal(
        _make_ctx(stage=4, dist=4.2, ctx_score=0.50, funding=-0.0007)
    )
    assert sig is not None
    assert sig.metadata["risk_profile"] == "short_pump_funding_1R"
    assert sig.metadata["funding_rate_abs"] == pytest.approx(0.0007)
    assert sig.notional_usd == 10.0
    assert sig.leverage == 4


def test_funding_band2_signal_returned(strategy: ShortPumpStrategy) -> None:
    sig = strategy.check_signal(
        _make_ctx(stage=4, dist=6.0, ctx_score=0.30, funding=0.007)
    )
    assert sig is not None
    assert sig.metadata["risk_profile"] == "short_pump_funding_1R"
    assert sig.metadata["funding_rate_abs"] == pytest.approx(0.007)


def test_funding_before_mid(strategy: ShortPumpStrategy) -> None:
    # mid-range dist/ctx but funding band → funding_1R wins
    sig = strategy.check_signal(
        _make_ctx(stage=4, dist=4.2, ctx_score=0.50, funding=0.0008)
    )
    assert sig is not None
    assert sig.metadata["risk_profile"] == "short_pump_funding_1R"


def test_funding_stage3_blocked_by_tradeable_gate(strategy: ShortPumpStrategy) -> None:
    # F-fix: classify accepts stage 3, tradeable gate still stage == 4
    sig = strategy.check_signal(
        _make_ctx(stage=3, dist=4.2, ctx_score=0.50, funding=0.0008)
    )
    assert sig is None


def test_funding_outside_bands_falls_through_to_mid(strategy: ShortPumpStrategy) -> None:
    sig = strategy.check_signal(
        _make_ctx(stage=4, dist=4.2, ctx_score=0.50, funding=0.002)
    )
    assert sig is not None
    assert sig.metadata["risk_profile"] == "short_pump_mid"


def test_funding_boundary_upper_excluded(strategy: ShortPumpStrategy) -> None:
    # fr=0.001 / 0.01 not in [0.0005,0.001) nor [0.005,0.01) → mid when dist/ctx ok
    for fr in (0.001, 0.01):
        sig = strategy.check_signal(
            _make_ctx(stage=4, dist=4.2, ctx_score=0.50, funding=fr)
        )
        assert sig is not None
        assert sig.metadata["risk_profile"] == "short_pump_mid"
