"""Smoke: BybitLiveBroker imports and basic sanity (no network)."""
from __future__ import annotations

import os
import sys
from pathlib import Path

_repo_root = Path(__file__).resolve().parents[1]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

os.environ["BYBIT_API_KEY"] = "test_key"
os.environ["BYBIT_API_SECRET"] = "test_secret"
os.environ["BYBIT_TESTNET"] = "true"

from trading.bybit_live import BybitLiveBroker, _sign


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


def test_broker_get_broker_factory() -> None:
    """get_broker returns BybitLiveBroker when mode=live."""
    from trading.broker import get_broker
    b = get_broker("live", dry_run_live=True)
    assert isinstance(b, BybitLiveBroker)
    assert b.dry_run is True
    print("OK: get_broker('live') returns BybitLiveBroker")


def main() -> None:
    test_sign()
    test_broker_init()
    test_broker_get_broker_factory()
    print("smoke_bybit_live: OK")


if __name__ == "__main__":
    main()
