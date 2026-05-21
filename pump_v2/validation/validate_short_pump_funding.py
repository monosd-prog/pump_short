#!/usr/bin/env python3
"""Synthetic parity validation: ShortPumpStrategy short_pump_funding_1R."""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pump_v2.core.market_context import MarketContext
from pump_v2.indicators.context_score_5m import ContextScore5m
from pump_v2.indicators.dbg5_builder import Dbg5Bundle
from pump_v2.strategies.short_pump import ShortPumpStrategy

POS_CASES = [
    {"label": "band1_low", "stage": 4, "dist": 4.0, "ctx": 0.50, "funding": 0.0005},
    {"label": "band1_mid", "stage": 4, "dist": 4.2, "ctx": 0.50, "funding": -0.0007},
    {"label": "band2_low", "stage": 4, "dist": 6.0, "ctx": 0.30, "funding": 0.005},
    {"label": "band2_mid", "stage": 4, "dist": 4.0, "ctx": 0.50, "funding": 0.007},
    {"label": "f_before_mid", "stage": 4, "dist": 4.2, "ctx": 0.50, "funding": 0.0008},
]

NEG_CASES = [
    {"label": "fr_between_bands", "stage": 4, "dist": 6.0, "ctx": 0.50, "funding": 0.002},
    {"label": "fr_too_low", "stage": 4, "dist": 4.2, "ctx": 0.50, "funding": 0.0001},
    {"label": "fr_band1_upper", "stage": 4, "dist": 4.2, "ctx": 0.50, "funding": 0.001},
    {"label": "fr_band2_upper", "stage": 4, "dist": 4.2, "ctx": 0.50, "funding": 0.01},
    {"label": "stage3_tradeable", "stage": 3, "dist": 4.2, "ctx": 0.50, "funding": 0.0008},
    {"label": "no_funding", "stage": 4, "dist": 4.2, "ctx": 0.50, "funding": 0.0},
]


def _build_ctx(case: dict) -> MarketContext:
    funding = float(case["funding"])
    bundle = Dbg5Bundle(
        stage=int(case["stage"]),
        dist_to_peak_pct=float(case["dist"]),
        oi_change_5m_pct=0.0,
        oi_divergence_5m=False,
        vol_z=0.0,
        atr_14_5m_pct=0.0,
    )
    ctx = MarketContext(
        symbol="PARITY_FUNDING",
        ts_utc=datetime.now(timezone.utc),
        funding=funding,
        dbg5=bundle,
    )
    ctx.candles["5m"] = [{"close": 1.0}]
    ctx.indicators["context_score_5m"] = ContextScore5m(score=float(case["ctx"]), parts={})
    return ctx


def main() -> int:
    print("ShortPumpStrategy parity — short_pump_funding_1R (synthetic)\n")
    strategy = ShortPumpStrategy(params={}, risk={})

    pos_results = []
    for case in POS_CASES:
        sig = strategy.check_signal(_build_ctx(case))
        ok = sig is not None and sig.metadata.get("risk_profile") == "short_pump_funding_1R"
        pos_results.append({**case, "ok": ok, "got": sig.metadata.get("risk_profile") if sig else "None"})

    neg_results = []
    for case in NEG_CASES:
        sig = strategy.check_signal(_build_ctx(case))
        ok = sig is None or sig.metadata.get("risk_profile") != "short_pump_funding_1R"
        neg_results.append({**case, "ok": ok, "got": sig.metadata.get("risk_profile") if sig else "None"})

    passed_pos = sum(r["ok"] for r in pos_results)
    failed_pos = [r for r in pos_results if not r["ok"]]
    print(f"=== POSITIVES (synthetic): {passed_pos}/{len(pos_results)} ===")
    if failed_pos:
        for r in failed_pos:
            print(f"  FAIL {r['label']}: funding={r['funding']} got={r['got']}")

    passed_neg = sum(r["ok"] for r in neg_results)
    failed_neg = [r for r in neg_results if not r["ok"]]
    print(f"\n=== NEGATIVES: {passed_neg}/{len(neg_results)} ===")
    if failed_neg:
        for r in failed_neg:
            print(f"  FAIL {r['label']}: funding={r['funding']} got={r['got']}")

    if failed_pos or failed_neg:
        print("\n✗ PARITY CHECKS FAILED")
        return 1
    print("\n✓ ALL PARITY CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
