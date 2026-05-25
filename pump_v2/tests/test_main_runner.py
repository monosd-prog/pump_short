"""Tests for pump_v2 standalone main_runner (Phase 7)."""
from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timedelta, timezone
from http.client import HTTPConnection
from http.server import HTTPServer
from unittest.mock import patch

import pytest

import pump_v2.main_runner as mr


@pytest.fixture(autouse=True)
def _reset_registry() -> None:
    mr.reset_registry_for_tests()
    yield
    mr.reset_registry_for_tests()


def test_v2_config_stub() -> None:
    cfg = mr.V2WatchConfig(symbol="XYZUSDT", category="linear")
    assert cfg.symbol == "XYZUSDT"
    assert cfg.run_id.startswith("v2_")


def test_webhook_accepts_bybit_symbol() -> None:
    status, body = mr.accept_pump("BTCUSDT", exchange="bybit")
    assert status == 200
    assert body["status"] == "ok"
    with mr._active_lock:
        assert "BTCUSDT" in mr._active


def test_webhook_rejects_non_bybit() -> None:
    status, body = mr.accept_pump("BTCUSDT", exchange="binance")
    assert status == 400
    assert body["reason"] == "exchange_not_bybit"
    with mr._active_lock:
        assert len(mr._active) == 0


def test_webhook_respects_max_concurrent() -> None:
    with patch.object(mr, "MAX_CONCURRENT", 2):
        mr.accept_pump("AAAUSDT", exchange="bybit")
        mr.accept_pump("BBBUSDT", exchange="bybit")
        status, body = mr.accept_pump("CCCUSDT", exchange="bybit")
        assert status == 429
        assert body["reason"] == "max_concurrent"
        with mr._active_lock:
            assert "CCCUSDT" not in mr._active


def test_symbol_expires_after_watch_window() -> None:
    sym = "TESTUSDT"
    with mr._active_lock:
        mr._active[sym] = datetime.now(timezone.utc) - timedelta(seconds=1)

    with patch.object(mr, "POLL_SECONDS", 0):
        mr.watch_symbol(sym)

    with mr._active_lock:
        assert sym not in mr._active


def test_http_post_accepts_bybit() -> None:
    port_holder: list[int] = []

    def _run() -> None:
        server = HTTPServer(("127.0.0.1", 0), mr.PumpWebhookHandler)
        port_holder.append(server.server_address[1])
        server.serve_forever()

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    for _ in range(50):
        if port_holder:
            break
        time.sleep(0.05)
    assert port_holder, "server did not bind"

    conn = HTTPConnection("127.0.0.1", port_holder[0], timeout=5)
    payload = json.dumps({"symbol": "ETHUSDT", "exchange": "bybit"})
    conn.request("POST", "/pump", body=payload, headers={"Content-Type": "application/json"})
    resp = conn.getresponse()
    assert resp.status == 200
    with mr._active_lock:
        assert "ETHUSDT" in mr._active
    conn.close()
