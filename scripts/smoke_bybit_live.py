"""Smoke: BybitLiveBroker imports and basic sanity (no network)."""
from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import Mock, patch

_repo_root = Path(__file__).resolve().parents[1]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

os.environ["BYBIT_API_KEY"] = "test_key"
os.environ["BYBIT_API_SECRET"] = "test_secret"
os.environ["BYBIT_TESTNET"] = "true"

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


def main() -> None:
    test_sign()
    test_side_to_position_idx()
    test_round_price_to_tick()
    test_hedge_position_idx_in_payload()
    test_set_trading_stop_dry_run()
    test_set_leverage_retcode_110043()
    test_set_leverage_other_retcode_raises()
    test_open_position_tpsl_fail_then_retry_fail_closes()
    test_open_position_tpsl_fail_then_retry_success()
    test_broker_init()
    test_broker_get_broker_factory()
    print("smoke_bybit_live: OK")


if __name__ == "__main__":
    main()
