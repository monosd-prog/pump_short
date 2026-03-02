"""Smoke: BybitLiveBroker imports and basic sanity (no network)."""
from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

_repo_root = Path(__file__).resolve().parents[1]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

os.environ["BYBIT_API_KEY"] = "test_key"
os.environ["BYBIT_API_SECRET"] = "test_secret"
os.environ["BYBIT_TESTNET"] = "true"
# Use temp state so smoke never touches /root/pump_short/datasets/trading_state.json
_SMOKE_STATE_PATH = os.path.join(tempfile.gettempdir(), "trading_state_smoke.json")
os.environ["TRADING_STATE_PATH"] = _SMOKE_STATE_PATH

from trading.bybit_live import BybitLiveBroker, _round_price_to_tick, _sign, side_to_position_idx


def test_sign() -> None:
    """Sanity: HMAC sign produces hex string."""
    sig = _sign("key", "secret", "1234567890", "5000", "category=linear&symbol=BTCUSDT")
    assert isinstance(sig, str)
    assert len(sig) == 64
    assert all(c in "0123456789abcdef" for c in sig)
    print("OK: _sign produces 64-char hex")


def test_broker_init() -> None:
    """Broker init with dry_run does not require network."""
    b = BybitLiveBroker(api_key="k", api_secret="s", dry_run=True)
    assert b.dry_run is True
    assert "testnet" in b._base
    print("OK: BybitLiveBroker init (dry_run)")


def test_side_to_position_idx() -> None:
    """side -> positionIdx: LONG/Buy -> 1, SHORT/Sell -> 2."""
    assert side_to_position_idx("LONG") == 1
    assert side_to_position_idx("Buy") == 1
    assert side_to_position_idx("BUY") == 1
    assert side_to_position_idx("SHORT") == 2
    assert side_to_position_idx("Sell") == 2
    assert side_to_position_idx("SELL") == 2
    assert side_to_position_idx("") == 2
    print("OK: side_to_position_idx mapping")


def test_hedge_position_idx_in_payload() -> None:
    """When mode=hedge, _get_position_idx returns 1 or 2; payload would contain positionIdx."""
    b = BybitLiveBroker(api_key="k", api_secret="s", dry_run=True)
    b._position_mode = "hedge"
    assert b._get_position_idx("BTCUSDT", "SHORT") == 2
    assert b._get_position_idx("BTCUSDT", "LONG") == 1
    assert b._get_position_idx("BTCUSDT", "Sell") == 2
    assert b._get_position_idx("BTCUSDT", "Buy") == 1
    print("OK: hedge mode positionIdx in payload (1=LONG, 2=SHORT)")


def test_round_price_to_tick() -> None:
    """Price rounding to tick step."""
    assert _round_price_to_tick(1.2345, 0.01) == 1.23
    assert _round_price_to_tick(1.2365, 0.01) == 1.24
    assert _round_price_to_tick(95000.5, 0.1) == 95000.5
    print("OK: _round_price_to_tick")


def test_set_trading_stop_dry_run() -> None:
    """set_trading_stop with dry_run=True returns empty, no network call."""
    b = BybitLiveBroker(api_key="k", api_secret="s", dry_run=True)
    res = b.set_trading_stop("BTCUSDT", 2, 95000.0, 105000.0)
    assert res == {}
    print("OK: set_trading_stop dry_run returns {} (no network)")


def test_set_leverage_retcode_110043() -> None:
    """set_leverage treats retCode=110043 (leverage not modified) as success, does not raise."""
    def stub_110043(*a, **kw):
        return {"retCode": 110043, "retMsg": "leverage not modified", "result": {}}

    with patch("trading.bybit_live._request", side_effect=stub_110043):
        b = BybitLiveBroker(api_key="k", api_secret="s", dry_run=False)
        b.set_leverage("BTCUSDT", 4)
    print("OK: set_leverage retCode=110043 does not raise")


def test_set_leverage_other_retcode_raises() -> None:
    """set_leverage raises for other non-zero retCode (via _request)."""
    mock_resp = Mock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"retCode": 12345, "retMsg": "some other error", "result": {}}
    mock_resp.raise_for_status = Mock()

    with patch("trading.bybit_live.requests.post", return_value=mock_resp):
        b = BybitLiveBroker(api_key="k", api_secret="s", dry_run=False)
        try:
            b.set_leverage("BTCUSDT", 4)
            assert False, "expected RuntimeError"
        except RuntimeError as e:
            assert "12345" in str(e)
    print("OK: set_leverage other retCode raises")


def test_broker_get_broker_factory() -> None:
    """get_broker returns BybitLiveBroker when mode=live."""
    from trading.broker import get_broker
    b = get_broker("live", dry_run_live=True)
    assert isinstance(b, BybitLiveBroker)
    assert b.dry_run is True
    print("OK: get_broker('live') returns BybitLiveBroker")


def test_resolve_live_outcome_tp_hit() -> None:
    """resolve_live_outcome parses mocked closed-pnl and returns TP_hit for SHORT profit."""
    from trading.bybit_live_outcome import resolve_live_outcome

    mock_closed = [
        {
            "symbol": "POWERUSDT",
            "side": "Sell",
            "avgEntryPrice": "2.10",
            "avgExitPrice": "2.07",
            "closedPnl": "1.43",
            "updatedTime": "1730473200000",
            "orderId": "close-order-1",
        },
    ]
    broker = Mock()
    broker.get_closed_pnl = Mock(return_value=mock_closed)

    result = resolve_live_outcome(
        symbol="POWERUSDT",
        order_id="entry-order-1",
        position_idx=2,
        opened_ts="2024-11-01 12:00:00+00:00",
        entry_price=2.10,
        tp_price=2.07,
        sl_price=2.13,
        side="SHORT",
        broker=broker,
        timeout_sec=2,
        poll_sec=0.1,
    )
    assert result is not None
    assert result.get("status") == "TP_hit"
    assert abs(float(result.get("exit_price", 0)) - 2.07) < 0.001
    assert float(result.get("pnl_pct", 0)) > 0
    print("OK: resolve_live_outcome TP_hit for SHORT")


def test_resolve_live_outcome_sl_hit() -> None:
    """resolve_live_outcome parses mocked closed-pnl and returns SL_hit for SHORT loss."""
    from trading.bybit_live_outcome import resolve_live_outcome

    mock_closed = [
        {
            "symbol": "POWERUSDT",
            "side": "Sell",
            "avgEntryPrice": "2.10",
            "avgExitPrice": "2.1198183",
            "closedPnl": "-0.94",
            "updatedTime": "1730473300000",
            "orderId": "close-order-2",
        },
    ]
    broker = Mock()
    broker.get_closed_pnl = Mock(return_value=mock_closed)

    result = resolve_live_outcome(
        symbol="POWERUSDT",
        order_id="entry-order-1",
        position_idx=2,
        opened_ts="2024-11-01 12:00:00+00:00",
        entry_price=2.10,
        tp_price=2.07,
        sl_price=2.13,
        side="SHORT",
        broker=broker,
        timeout_sec=2,
        poll_sec=0.1,
    )
    assert result is not None
    assert result.get("status") == "SL_hit"
    assert abs(float(result.get("exit_price", 0)) - 2.1198183) < 0.0001
    assert float(result.get("pnl_pct", 0)) < 0
    print("OK: resolve_live_outcome SL_hit for SHORT")


def test_live_outcome_retry_no_candle_fallback_on_timeout() -> None:
    """Resolver raises TimeoutError 3x -> outcome stays pending, track_outcome (candle fallback) NOT called."""
    from short_pump.fast0_sampler import STRATEGY, _run_fast0_outcome_watcher

    track_outcome_called = []

    def track_outcome_spy(*a, **kw):
        track_outcome_called.append(1)
        return {"end_reason": "TIMEOUT", "outcome": "TIMEOUT", "pnl_pct": 0.0, "hold_seconds": 0, "exit_price": 0.0}

    mock_position = {
        "strategy": STRATEGY,
        "symbol": "POWERUSDT",
        "side": "SHORT",
        "order_id": "ord-123",
        "position_idx": 2,
        "entry": 2.10,
        "tp": 2.07,
        "sl": 2.13,
    }

    def mock_load_state():
        return {
            "open_positions": {
                STRATEGY: {
                    f"{STRATEGY}:run1:e1:POWERUSDT": mock_position,
                },
            },
        }

    with patch("short_pump.fast0_sampler.LIVE_OUTCOME_RETRY_MAX_SEC", 4):
        with patch("short_pump.fast0_sampler.LIVE_OUTCOME_RETRY_INTERVAL_SEC", 1):
            with patch("short_pump.fast0_sampler.track_outcome", side_effect=track_outcome_spy):
                with patch("trading.state.load_state", side_effect=mock_load_state):
                    with patch("trading.state.make_position_id", return_value=f"{STRATEGY}:run1:e1:POWERUSDT"):
                        with patch("trading.bybit_live_outcome.resolve_live_outcome", side_effect=TimeoutError("mock timeout")):
                            _run_fast0_outcome_watcher(
                                symbol="POWERUSDT",
                                run_id="run1",
                                event_id="e1",
                                trade_id="t1",
                                entry_price=2.10,
                                tp_price=2.07,
                                sl_price=2.13,
                                entry_time_utc="2024-11-01 12:00:00+00:00",
                                base_dir=None,
                                mode="live",
                            )

    assert len(track_outcome_called) == 0
    print("OK: live outcome retry on TimeoutError -> no candle fallback")


def _make_short_signal(
    entry: float = 100_000.0, tp: float = 98_000.0, sl: float = 102_000.0
) -> object:
    """Minimal Signal for short_pump (stage=4, dist_to_peak_pct >= 3.5)."""
    from short_pump.signals import Signal
    return Signal(
        strategy="short_pump",
        symbol="BTCUSDT",
        side="SHORT",
        ts_utc="2026-02-26 12:00:00+0000",
        run_id="r1",
        event_id="e1",
        entry_price=entry,
        tp_price=tp,
        sl_price=sl,
        tp_pct=None,
        sl_pct=None,
        stage=4,
        dist_to_peak_pct=5.0,
        context_score=None,
        cvd_30s=None,
        cvd_1m=None,
        liq_long_usd_30s=None,
        liq_short_usd_30s=None,
    )


def test_open_position_tpsl_fail_then_retry_fail_closes() -> None:
    """TPSL first fail, retry fails -> close_position called, open_position returns None."""
    signal = _make_short_signal()
    place_calls = []
    close_calls = []

    def mock_place(symbol, side, qty, reduce_only=False, **kw):
        place_calls.append({"symbol": symbol, "side": side, "qty": qty, "reduce_only": reduce_only})
        if reduce_only:
            close_calls.append(place_calls[-1])
        return {"orderId": "mock", "orderLinkId": "mock"}

    def mock_set_tpsl(*a, **kw):
        raise RuntimeError("Bybit API error retCode=10001 retMsg=StopLoss for Sell position should greater base_price")

    def mock_get_position(symbol, side=None):
        return {"size": 0.01, "side": "Sell", "avgPrice": 100_000.0}

    def mock_ticker(symbol):
        return {"lastPrice": 102_500.0, "markPrice": 102_500.0}

    patches = [
        patch("trading.bybit_live._request", side_effect=lambda *a, **kw: {"retCode": 0, "result": {"list": []}}),
        patch.object(BybitLiveBroker, "place_market_order", side_effect=mock_place),
        patch.object(BybitLiveBroker, "set_trading_stop", side_effect=mock_set_tpsl),
        patch.object(BybitLiveBroker, "get_open_position", side_effect=mock_get_position),
        patch.object(BybitLiveBroker, "get_ticker_price", side_effect=mock_ticker),
        patch.object(BybitLiveBroker, "get_instrument_limits", return_value={
            "min_qty": 0.001, "min_notional_usd": 5, "lot_step": 0.001, "qty_precision": 3,
            "tick_size": 0.1,
        }),
        patch.object(BybitLiveBroker, "set_leverage"),
    ]
    with patch("trading.bybit_live.requests.get") as mock_get:
        mock_get.return_value.json.return_value = {
            "retCode": 0, "result": {"list": [{"symbol": "BTCUSDT", "positionIdx": 2}]}
        }
        mock_get.return_value.raise_for_status = Mock()
        with patch.object(BybitLiveBroker, "_ensure_position_mode", return_value="hedge"):
            for p in patches:
                p.start()
            try:
                b = BybitLiveBroker(api_key="k", api_secret="s", dry_run=False)
                pos = b.open_position(signal, qty_notional_usd=1000, risk_usd=20, leverage=4, opened_ts="")
                assert pos is None
                assert len(close_calls) >= 1
                assert close_calls[0]["reduce_only"] is True
                assert close_calls[0]["side"] == "Buy"
            finally:
                for p in patches:
                    p.stop()
    print("OK: open_position TPSL fail+retry fail -> close, return None")


def test_open_position_tpsl_fail_then_retry_success() -> None:
    """TPSL first fail, retry succeeds -> open_position returns position dict (trade accepted)."""
    signal = _make_short_signal()
    set_tpsl_calls = []

    def mock_set_tpsl(symbol, position_idx, take_profit, stop_loss):
        set_tpsl_calls.append((take_profit, stop_loss))
        if len(set_tpsl_calls) == 1:
            raise RuntimeError("Bybit API error retCode=10001 retMsg=StopLoss invalid")
        return {}

    patches = [
        patch("trading.bybit_live._request", side_effect=lambda *a, **kw: {"retCode": 0, "result": {"list": []}}),
        patch.object(BybitLiveBroker, "place_market_order", return_value={"orderId": "mock"}),
        patch.object(BybitLiveBroker, "set_trading_stop", side_effect=mock_set_tpsl),
        patch.object(BybitLiveBroker, "get_open_position", return_value={"size": 0.01, "side": "Sell", "avgPrice": 100_000.0}),
        patch.object(BybitLiveBroker, "get_ticker_price", return_value={"lastPrice": 102_500.0, "markPrice": 102_500.0}),
        patch.object(BybitLiveBroker, "get_instrument_limits", return_value={
            "min_qty": 0.001, "min_notional_usd": 5, "lot_step": 0.001, "qty_precision": 3,
            "tick_size": 0.1,
        }),
        patch.object(BybitLiveBroker, "set_leverage"),
    ]
    with patch("trading.bybit_live.requests.get") as mock_get:
        mock_get.return_value.json.return_value = {
            "retCode": 0, "result": {"list": [{"symbol": "BTCUSDT", "positionIdx": 2}]}
        }
        mock_get.return_value.raise_for_status = Mock()
        with patch.object(BybitLiveBroker, "_ensure_position_mode", return_value="hedge"):
            for p in patches:
                p.start()
            try:
                b = BybitLiveBroker(api_key="k", api_secret="s", dry_run=False)
                pos = b.open_position(signal, qty_notional_usd=1000, risk_usd=20, leverage=4, opened_ts="")
                assert pos is not None
                assert pos.get("symbol") == "BTCUSDT"
                assert pos.get("side") == "SHORT"
                assert len(set_tpsl_calls) == 2
            finally:
                for p in patches:
                    p.stop()
    print("OK: open_position TPSL fail+retry success -> trade accepted")


def test_runner_proceeds_with_2plus_positions_when_disable_max_concurrent() -> None:
    """With DISABLE_MAX_CONCURRENT_TRADES=true and 2+ open positions, runner proceeds to broker.open_position in live mode."""
    signal = _make_short_signal()
    open_position_calls = []

    mock_broker = Mock()
    mock_broker.get_balance = Mock(return_value=10000.0)

    def mock_open(signal, *args, **kwargs):
        open_position_calls.append(1)
        return {
            "strategy": getattr(signal, "strategy", "short_pump"),
            "symbol": getattr(signal, "symbol", "BTCUSDT"),
            "side": getattr(signal, "side", "SHORT"),
            "mode": "live",
            "order_id": "mock-ord-123",
            "position_idx": 2,
            "run_id": getattr(signal, "run_id", ""),
            "event_id": getattr(signal, "event_id", ""),
            "entry": float(getattr(signal, "entry_price", 100000)),
            "tp": float(getattr(signal, "tp_price", 98000)),
            "sl": float(getattr(signal, "sl_price", 102000)),
        }
    mock_broker.open_position = Mock(side_effect=mock_open)

    state_with_2 = {
        "open_positions": {
            "short_pump": {
                "sp:r1:e1:BTCUSDT": {"symbol": "BTCUSDT", "entry": 100000, "tp": 98000, "sl": 102000},
                "sp:r2:e2:ETHUSDT": {"symbol": "ETHUSDT", "entry": 3000, "tp": 2950, "sl": 3050},
            }
        },
        "last_signal_ids": {},
    }

    patches = [
        patch("trading.runner.EXECUTION_MODE", "live"),
        patch("trading.runner.DISABLE_MAX_CONCURRENT_TRADES", True),
        patch("trading.runner._acquire_lock", return_value=(True, None)),
        patch("trading.runner.get_latest_signals", return_value=([signal], ["line1"])),
        patch("trading.runner.load_state", return_value=state_with_2),
        patch("trading.runner.get_broker", return_value=mock_broker),
        patch("trading.runner.AUTO_TRADING_ENABLE", 1),
    ]
    for p in patches:
        p.start()
    try:
        from trading.runner import run_once
        run_once(dry_run_live=True)
    finally:
        for p in patches:
            p.stop()

    assert len(open_position_calls) >= 1, "broker.open_position should be called when DISABLE_MAX_CONCURRENT_TRADES=true"
    print("OK: runner proceeds to open_position with 2+ positions when DISABLE_MAX_CONCURRENT_TRADES=true")


def test_outcome_worker_closes_on_tp_resolved() -> None:
    """Outcome worker calls close_from_live_outcome when resolve_live_outcome returns TP_hit."""
    from trading.outcome_worker import run_outcome_worker

    close_calls = []

    def mock_close_from_live_outcome(*a, **kw):
        close_calls.append({"args": a, "kwargs": kw})
        return True

    mock_broker = Mock()
    state = {
        "open_positions": {
            "short_pump": {
                "short_pump:run1:e1:BTCUSDT": {
                    "strategy": "short_pump",
                    "symbol": "BTCUSDT",
                    "side": "SHORT",
                    "entry": 100000.0,
                    "tp": 98000.0,
                    "sl": 102000.0,
                    "opened_ts": "2026-02-26 12:00:00+00:00",
                    "mode": "live",
                    "order_id": "ord-123",
                    "position_idx": 2,
                    "run_id": "run1",
                    "event_id": "e1",
                },
            },
        },
    }
    outcome_tp = {
        "status": "TP_hit",
        "exit_price": 97950.0,
        "exit_ts": "2026-02-26 12:05:00+00:00",
        "pnl_pct": 0.05,
    }

    with patch("trading.bybit_live_outcome.resolve_live_outcome", return_value=outcome_tp):
        with patch("trading.paper_outcome.close_from_live_outcome", side_effect=mock_close_from_live_outcome):
            with patch("trading.state.save_state"):
                run_outcome_worker(state, mock_broker)

    assert len(close_calls) == 1, "close_from_live_outcome should be called once"
    kw = close_calls[0]["kwargs"]
    assert kw.get("res") == "TP_hit"
    assert kw.get("symbol") == "BTCUSDT"
    assert kw.get("run_id") == "run1"
    assert kw.get("event_id") == "e1"
    print("OK: outcome worker calls close_from_live_outcome on TP_hit")


def main() -> None:
    test_sign()
    test_side_to_position_idx()
    test_round_price_to_tick()
    test_hedge_position_idx_in_payload()
    test_set_trading_stop_dry_run()
    test_set_leverage_retcode_110043()
    test_set_leverage_other_retcode_raises()
    test_resolve_live_outcome_tp_hit()
    test_resolve_live_outcome_sl_hit()
    test_live_outcome_retry_no_candle_fallback_on_timeout()
    test_open_position_tpsl_fail_then_retry_fail_closes()
    test_open_position_tpsl_fail_then_retry_success()
    test_runner_proceeds_with_2plus_positions_when_disable_max_concurrent()
    test_outcome_worker_closes_on_tp_resolved()
    test_broker_init()
    test_broker_get_broker_factory()
    print("smoke_bybit_live: OK")
    # Cleanup temp state
    try:
        os.remove(_SMOKE_STATE_PATH)
    except OSError:
        pass


if __name__ == "__main__":
    main()
