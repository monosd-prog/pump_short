"""Smoke: risk profile selection for short_pump and fast0, notional 10 USD, TG payload."""
from __future__ import annotations

import os
import sys
from pathlib import Path

_repo_root = Path(__file__).resolve().parents[1]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

os.environ.setdefault("LIVE_FIXED_NOTIONAL_USD", "10")
os.environ.setdefault("LIVE_LEVERAGE", "4")
os.environ.setdefault("LIVE_MARGIN_MODE", "isolated")
os.environ.setdefault("SHORT_PUMP_AUTO_DIST_MIN", "3.5")
os.environ.setdefault("FAST0_AUTO_ENABLE", "1")
os.environ.setdefault("FAST0_LIQ_5K_25K_ENABLE", "1")
os.environ.setdefault("FAST0_LIQ_100K_ENABLE", "1")

from trading.broker import allow_entry_short_pump, allow_entry_short_pump_fast0
from trading.risk_profile import (
    get_risk_profile,
    get_notional_and_leverage,
    LIVE_FIXED_NOTIONAL_USD,
    LIVE_LEVERAGE,
    LIVE_MARGIN_MODE,
)
from trading.risk import calc_position_size
from short_pump.signals import Signal
from short_pump.telegram import is_fast0_tg_entry_allowed
from notifications.tg_format import format_tg, format_fast0_outcome_message, format_outcome


def _signal(strategy: str, **kwargs) -> Signal:
    return Signal(
        strategy=strategy,
        symbol=kwargs.get("symbol", "XUSDT"),
        side="SHORT",
        ts_utc="2026-02-26T12:00:00Z",
        run_id="smoke",
        event_id=kwargs.get("event_id", "evt1"),
        entry_price=1.0,
        tp_price=0.99,
        sl_price=1.01,
        tp_pct=1.0,
        sl_pct=1.0,
        stage=kwargs.get("stage"),
        dist_to_peak_pct=kwargs.get("dist_to_peak_pct"),
        context_score=kwargs.get("context_score", 0.7),
        cvd_30s=kwargs.get("cvd_30s", -0.1),
        cvd_1m=kwargs.get("cvd_1m", -0.05),
        liq_long_usd_30s=kwargs.get("liq_long_usd_30s"),
        liq_short_usd_30s=kwargs.get("liq_short_usd_30s"),
    )


def main() -> None:
    errors = []

    # 1. short_pump: stage4 + dist>=3.5 -> short_pump_active_1R
    sig = _signal("short_pump", stage=4, dist_to_peak_pct=4.0)
    ok, _ = allow_entry_short_pump(sig)
    assert ok, "short_pump stage4 dist=4 should pass"
    profile, risk_mult, _ = get_risk_profile("short_pump", stage=4, dist_to_peak_pct=4.0, symbol="XUSDT")
    assert profile == "short_pump_active_1R", f"expected short_pump_active_1R, got {profile}"
    assert risk_mult == 1.0, f"expected risk_mult=1.0, got {risk_mult}"
    print("OK: short_pump stage4 dist>=3.5 -> short_pump_active_1R, 1R")

    # short_pump stage3 -> reject
    sig3 = _signal("short_pump", stage=3, dist_to_peak_pct=5.0)
    ok3, _ = allow_entry_short_pump(sig3)
    assert not ok3, "short_pump stage3 should reject"
    profile3, _, _ = get_risk_profile("short_pump", stage=3, dist_to_peak_pct=5.0)
    assert profile3 == "", f"expected no profile for stage3, got {profile3}"
    print("OK: short_pump stage3 -> reject")

    # 2a. fast0 liq=0 dist=1.0 -> fast0_base_1R (pass)
    sig_b = _signal("short_pump_fast0", liq_long_usd_30s=0, dist_to_peak_pct=1.0)
    ok_b, _ = allow_entry_short_pump_fast0(sig_b)
    assert ok_b, "fast0 liq=0 dist=1.0 (base) should pass"
    profile_b, risk_b, _ = get_risk_profile("short_pump_fast0", liq_long_usd_30s=0, dist_to_peak_pct=1.0)
    assert profile_b == "fast0_base_1R", f"expected fast0_base_1R, got {profile_b}"
    assert risk_b == 1.0, f"expected risk_mult=1.0, got {risk_b}"
    print("OK: fast0 liq=0 dist=1.0 -> fast0_base_1R, 1R")

    # 2b. fast0 liq=2k dist=1.0 -> reject (liq not in bucket)
    sig_b2 = _signal("short_pump_fast0", liq_long_usd_30s=2000, dist_to_peak_pct=1.0)
    ok_b2, reason_b2 = allow_entry_short_pump_fast0(sig_b2)
    assert not ok_b2, "fast0 liq=2k (0,5k] should reject"
    assert reason_b2 == "fast0_liq_not_in_allowed_bucket", f"expected fast0_liq_not_in_allowed_bucket, got {reason_b2}"
    profile_b2, _, _ = get_risk_profile("short_pump_fast0", liq_long_usd_30s=2000, dist_to_peak_pct=1.0)
    assert profile_b2 == "", f"expected no profile for liq 2k, got {profile_b2}"
    print("OK: fast0 liq=2k -> reject fast0_liq_not_in_allowed_bucket")

    # 2c. fast0 dist=1.6 -> reject (dist>1.5)
    sig_d = _signal("short_pump_fast0", liq_long_usd_30s=15000, dist_to_peak_pct=1.6)
    ok_d, reason_d = allow_entry_short_pump_fast0(sig_d)
    assert not ok_d, "fast0 dist=1.6 should reject"
    assert reason_d == "fast0_dist_gt_1_5", f"expected fast0_dist_gt_1_5, got {reason_d}"
    print("OK: fast0 dist=1.6 -> reject fast0_dist_gt_1_5")

    # 3. fast0 5k-25k dist=1.5 -> fast0_1p5R
    sig_m = _signal("short_pump_fast0", liq_long_usd_30s=15000, dist_to_peak_pct=1.5)
    ok_m, _ = allow_entry_short_pump_fast0(sig_m)
    assert ok_m, "fast0 liq=15k dist=1.5 should pass"
    profile_m, risk_m, _ = get_risk_profile("short_pump_fast0", liq_long_usd_30s=15000, dist_to_peak_pct=1.5)
    assert profile_m == "fast0_1p5R", f"expected fast0_1p5R, got {profile_m}"
    assert risk_m == 1.5, f"expected risk_mult=1.5, got {risk_m}"
    print("OK: fast0 liq=15k (5k-25k) dist=1.5 -> fast0_1p5R, 1.5R")

    # 4. fast0 100k+ dist=1.0 -> fast0_2R
    sig_h = _signal("short_pump_fast0", liq_long_usd_30s=150000, dist_to_peak_pct=1.0)
    ok_h, _ = allow_entry_short_pump_fast0(sig_h)
    assert ok_h, "fast0 liq=150k dist=1.0 should pass"
    profile_h, risk_h, _ = get_risk_profile("short_pump_fast0", liq_long_usd_30s=150000, dist_to_peak_pct=1.0)
    assert profile_h == "fast0_2R", f"expected fast0_2R, got {profile_h}"
    assert risk_h == 2.0, f"expected risk_mult=2.0, got {risk_h}"
    print("OK: fast0 liq=150k dist=1.0 -> fast0_2R, 2R")

    # 5. notional from 10 USD
    nt, lev, mm = get_notional_and_leverage(1.0)
    assert abs(nt - 10.0) < 0.1, f"expected notional~10 USD, got {nt}"
    assert lev == 4, f"expected leverage=4, got {lev}"
    assert mm == "isolated", f"expected margin=isolated, got {mm}"
    nt_15, _, _ = get_notional_and_leverage(1.5)
    assert abs(nt_15 - 15.0) < 0.1, f"expected 1.5R notional~15, got {nt_15}"
    nt_20, _, _ = get_notional_and_leverage(2.0)
    assert abs(nt_20 - 20.0) < 0.1, f"expected 2R notional~20, got {nt_20}"
    print("OK: notional 10/15/20 USD for 1R/1.5R/2R, lev=4, margin=isolated")

    # 6. calc_position_size with fixed_notional_override
    notional, risk_usd, reject = calc_position_size(
        1.0, 0.99, 1000.0, "XUSDT", 0.0025, 0.01,
        fixed_notional_override=10.0,
    )
    assert reject is None, f"expected no reject, got {reject}"
    assert 9.5 <= notional <= 10.5, f"expected notional~10, got {notional}"
    print("OK: calc_position_size fixed_notional_override=10 -> notional~10")

    # 7. TG payload contains risk_profile, 10 USD, x4, isolated
    sig_tg = _signal("short_pump_fast0", liq_long_usd_30s=15000, dist_to_peak_pct=1.5)
    msg = format_tg(sig_tg)
    assert "fast0_1p5R" in msg or "risk_profile=" in msg, f"TG msg missing risk_profile: {msg[:200]}"
    assert "10" in msg or "15" in msg, f"TG msg missing notional: {msg[:200]}"
    assert "x4" in msg or "lev=" in msg, f"TG msg missing leverage: {msg[:200]}"
    assert "isolated" in msg.lower(), f"TG msg missing margin: {msg[:200]}"
    print("OK: TG entry contains risk_profile, notional, lev, margin")

    msg_out = format_fast0_outcome_message(
        symbol="XUSDT", run_id="smoke", event_id="evt1",
        res="TP_hit", entry_price=1.0, tp_price=0.99, sl_price=1.01,
        exit_price=0.99, pnl_pct=1.0, hold_seconds=60,
        risk_profile="fast0_1p5R",
        notional_usd=15.0, leverage=4, margin_mode="isolated",
    )
    assert "fast0_1p5R" in msg_out, f"outcome msg missing risk_profile: {msg_out}"
    assert "15" in msg_out, f"outcome msg missing notional: {msg_out}"
    assert "x4" in msg_out or "lev=" in msg_out, f"outcome msg missing leverage: {msg_out}"
    assert "isolated" in msg_out.lower(), f"outcome msg missing margin: {msg_out}"
    print("OK: TG outcome contains risk_profile, notional, lev, margin")

    # 8. TG entry logic: dist<=1.5 and liq in [0, (5k,25k], >100k]
    assert is_fast0_tg_entry_allowed({"liq_long_usd_30s": 0, "dist_to_peak_pct": 1.0}), "liq=0 dist=1 -> TG allowed"
    assert not is_fast0_tg_entry_allowed({"liq_long_usd_30s": 2000, "dist_to_peak_pct": 1.0}), "liq=2k -> TG not allowed"
    assert not is_fast0_tg_entry_allowed({"liq_long_usd_30s": 15000, "dist_to_peak_pct": 2.0}), "dist=2 -> TG not allowed"
    assert is_fast0_tg_entry_allowed({"liq_long_usd_30s": 15000, "dist_to_peak_pct": 1.5}), "5k-25k dist=1.5 -> TG allowed"
    assert is_fast0_tg_entry_allowed({"liq_long_usd_30s": 150000, "dist_to_peak_pct": 1.0}), "100k+ dist=1 -> TG allowed"
    print("OK: TG entry matches risk_profile logic (dist<=1.5, liq in allowed buckets)")

    print("smoke_risk_profile: all checks passed")


if __name__ == "__main__":
    main()
