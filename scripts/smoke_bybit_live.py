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


def main() -> None:
    test_sign()
    test_side_to_position_idx()
    test_round_price_to_tick()
    test_hedge_position_idx_in_payload()
    test_set_trading_stop_dry_run()
    test_set_leverage_retcode_110043()
    test_set_leverage_other_retcode_raises()
    test_broker_init()
    test_broker_get_broker_factory()
    print("smoke_bybit_live: OK")


if __name__ == "__main__":
    main()
