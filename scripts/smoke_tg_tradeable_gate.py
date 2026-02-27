"""Smoke: TG notifications gated by tradeable signals.
- short_pump: stage==4 AND dist_to_peak_pct >= TG_ENTRY_DIST_MIN (default 3.5)
- short_pump_fast0: liq_long_usd_30s > 0
"""
from __future__ import annotations

import os
import sys
from unittest.mock import patch

_repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

os.environ["TG_ENTRY_DIST_MIN"] = "3.5"
os.environ["TG_ENTRY_STAGE"] = "4"

from short_pump.telegram import is_fast0_tg_entry_allowed, is_tradeable_short_pump, send_telegram


def test_short_pump_tradeable_gate() -> None:
    """short_pump: TG only when stage==4 and dist>=3.5."""
    # stage!=4 -> not tradeable
    assert is_tradeable_short_pump(3, 5.0) is False
    assert is_tradeable_short_pump(5, 5.0) is False
    # stage==4, dist<3.5 -> not tradeable
    assert is_tradeable_short_pump(4, 0.0) is False
    assert is_tradeable_short_pump(4, 3.0) is False
    assert is_tradeable_short_pump(4, 3.49) is False
    # stage==4, dist>=3.5 -> tradeable
    assert is_tradeable_short_pump(4, 3.5) is True
    assert is_tradeable_short_pump(4, 4.0) is True
    assert is_tradeable_short_pump(4, 10.0) is True
    # missing/invalid -> not tradeable
    assert is_tradeable_short_pump(4, None) is False
    print("OK: short_pump tradeable gate (stage=4, dist>=3.5)")


def test_fast0_tg_gate() -> None:
    """short_pump_fast0: is_fast0_tg_entry_allowed returns True only when liq>0."""
    assert is_fast0_tg_entry_allowed({"liq_long_usd_30s": 0}) is False
    assert is_fast0_tg_entry_allowed({"liq_long_usd_30s": 0.0}) is False
    assert is_fast0_tg_entry_allowed({"liq_long_usd_30s": None}) is False
    assert is_fast0_tg_entry_allowed({}) is False
    assert is_fast0_tg_entry_allowed({"liq_long_usd_30s": -1}) is False

    assert is_fast0_tg_entry_allowed({"liq_long_usd_30s": 1}) is True
    assert is_fast0_tg_entry_allowed({"liq_long_usd_30s": 0.01}) is True
    assert is_fast0_tg_entry_allowed({"liq_long_usd_30s": 100.0}) is True
    print("OK: fast0 TG gate (liq_long_usd_30s>0)")


def test_fast0_no_tg_send_when_liq_zero() -> None:
    """Gate ensures send_telegram is only called when liq>0 (mock verifies no call for liq=0)."""
    payload_liq0 = {"liq_long_usd_30s": 0, "context_score": 0.65}
    payload_liq100 = {"liq_long_usd_30s": 100, "context_score": 0.65}
    with patch("short_pump.telegram.send_telegram") as mock_send:
        import short_pump.telegram as tg
        # Simulate fast0 ENTRY path: only send when is_fast0_tg_entry_allowed
        mock_send.reset_mock()
        if is_fast0_tg_entry_allowed(payload_liq0):
            tg.send_telegram("test", strategy="short_pump_fast0", side="SHORT", mode="FAST0",
                            event_id="evt", context_score=0.65, entry_ok=True, formatted=True)
        assert mock_send.call_count == 0, "liq=0 must not trigger TG send"

        mock_send.reset_mock()
        if is_fast0_tg_entry_allowed(payload_liq100):
            tg.send_telegram("test", strategy="short_pump_fast0", side="SHORT", mode="FAST0",
                            event_id="evt", context_score=0.65, entry_ok=True, formatted=True)
        assert mock_send.call_count == 1, "liq>0 must trigger TG send"
    print("OK: fast0 liq=0 -> no TG send; liq>0 -> TG send")


def main() -> None:
    test_short_pump_tradeable_gate()
    test_fast0_tg_gate()
    test_fast0_no_tg_send_when_liq_zero()
    print("smoke_tg_tradeable_gate: TG tradeable gating OK")


if __name__ == "__main__":
    main()
