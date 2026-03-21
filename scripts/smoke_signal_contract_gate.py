#!/usr/bin/env python3
"""
Local smoke: verify signal contract adherence via `trading.broker.allow_entry()`.

Requirements (from project_ops/tasks/TASK_B2.md):
- no runner
- no queues
- no datasets writes
- deterministic env + exact/tolerant reason assertions
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from typing import Any, Callable


# Ensure repo root is on PYTHONPATH when executed as `python3 scripts/...`
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)


from trading.broker import allow_entry  # noqa: E402
import trading.risk_profile as risk_profile  # noqa: E402


@dataclass
class _SignalLike:
    """Minimal attribute container for broker contract checks."""

    strategy: str
    stage: Any = None
    dist_to_peak_pct: Any = None
    liq_long_usd_30s: Any = None


def _assert_exact_reason(case_id: str, ok: bool, reason: str, expected_reason: str) -> None:
    if ok:
        raise AssertionError(f"[{case_id}] expected reject, got allowed=True")
    if reason != expected_reason:
        raise AssertionError(f"[{case_id}] reason exact mismatch.\nexpected: {expected_reason}\nactual:   {reason}")


def _assert_tolerant_reason_contains(case_id: str, ok: bool, reason: str, must_contain: list[str]) -> None:
    if ok:
        raise AssertionError(f"[{case_id}] expected reject, got allowed=True")
    for s in must_contain:
        if s not in reason:
            raise AssertionError(
                f"[{case_id}] reason tolerant mismatch: missing substring {s!r}.\nactual: {reason}"
            )


def _run_case(case_id: str, signal: _SignalLike, expect_allowed: bool, reason_check: Callable[[bool, str], None]) -> None:
    allowed, reason = allow_entry(signal)
    if allowed != expect_allowed:
        raise AssertionError(
            f"[{case_id}] allowed mismatch.\nexpected: {expect_allowed}\nactual:   {allowed}\nreason:   {reason}"
        )
    reason_check(allowed, reason)


def main() -> int:
    # Track failures to produce a single PASS/FAIL summary.
    failures: list[str] = []

    def try_case(case_id: str, fn: Callable[[], None]) -> None:
        try:
            fn()
        except AssertionError as e:
            failures.append(str(e))

    # --------------------
    # short_pump fixtures
    # --------------------
    short_pump = "short_pump"

    # SP-ACCEPT-1
    try_case(
        "SP-ACCEPT-1",
        lambda: _run_case(
            "SP-ACCEPT-1",
            _SignalLike(strategy=short_pump, stage=4, dist_to_peak_pct=4.0),
            True,
            lambda _ok, _reason: None,
        ),
    )

    # SP-REJECT-1 exact reason
    try_case(
        "SP-REJECT-1",
        lambda: _run_case(
            "SP-REJECT-1",
            _SignalLike(strategy=short_pump, stage=3, dist_to_peak_pct=4.0),
            False,
            lambda ok, reason: _assert_exact_reason("SP-REJECT-1", ok, reason, "stage=3 (require 4)"),
        ),
    )

    # SP-REJECT-2 tolerant reason: dist_to_peak_pct=... < ...
    try_case(
        "SP-REJECT-2",
        lambda: _run_case(
            "SP-REJECT-2",
            _SignalLike(strategy=short_pump, stage=4, dist_to_peak_pct=3.4),
            False,
            lambda ok, reason: _assert_tolerant_reason_contains(
                "SP-REJECT-2",
                ok,
                reason,
                must_contain=["dist_to_peak_pct=", " < "],
            ),
        ),
    )

    # SP-REJECT-3 exact reason
    try_case(
        "SP-REJECT-3",
        lambda: _run_case(
            "SP-REJECT-3",
            _SignalLike(strategy=short_pump, stage=None, dist_to_peak_pct=4.0),
            False,
            lambda ok, reason: _assert_exact_reason("SP-REJECT-3", ok, reason, "missing stage"),
        ),
    )

    # SP-REJECT-4 exact reason
    try_case(
        "SP-REJECT-4",
        lambda: _run_case(
            "SP-REJECT-4",
            _SignalLike(strategy=short_pump, stage=4, dist_to_peak_pct=None),
            False,
            lambda ok, reason: _assert_exact_reason("SP-REJECT-4", ok, reason, "missing dist_to_peak_pct"),
        ),
    )

    # --------------------
    # short_pump_fast0 fixtures
    # --------------------
    fast0 = "short_pump_fast0"

    original_fast0_auto_enable = getattr(risk_profile, "FAST0_AUTO_ENABLE", True)

    try:
        # F0-REJECT-1: FAST0_AUTO_ENABLE=0 exact reason
        risk_profile.FAST0_AUTO_ENABLE = False
        try_case(
            "F0-REJECT-1",
            lambda: _run_case(
                "F0-REJECT-1",
                _SignalLike(strategy=fast0, dist_to_peak_pct=1.0, liq_long_usd_30s=20000),
                False,
                lambda ok, reason: _assert_exact_reason("F0-REJECT-1", ok, reason, "FAST0_AUTO_ENABLE=0"),
            ),
        )

        # F0-ACCEPT-1
        risk_profile.FAST0_AUTO_ENABLE = True
        try_case(
            "F0-ACCEPT-1",
            lambda: _run_case(
                "F0-ACCEPT-1",
                _SignalLike(strategy=fast0, dist_to_peak_pct=1.0, liq_long_usd_30s=20000),
                True,
                lambda _ok, _reason: None,
            ),
        )

        # F0-REJECT-2 exact reason fast0_dist_gt_1_5
        try_case(
            "F0-REJECT-2",
            lambda: _run_case(
                "F0-REJECT-2",
                _SignalLike(strategy=fast0, dist_to_peak_pct=1.6, liq_long_usd_30s=20000),
                False,
                lambda ok, reason: _assert_exact_reason("F0-REJECT-2", ok, reason, "fast0_dist_gt_1_5"),
            ),
        )

        # F0-REJECT-3 exact reason fast0_liq_not_in_allowed_bucket
        try_case(
            "F0-REJECT-3",
            lambda: _run_case(
                "F0-REJECT-3",
                _SignalLike(strategy=fast0, dist_to_peak_pct=1.0, liq_long_usd_30s=2000),
                False,
                lambda ok, reason: _assert_exact_reason(
                    "F0-REJECT-3", ok, reason, "fast0_liq_not_in_allowed_bucket"
                ),
            ),
        )

        # F0-MALFORM-1 exact reason liq_missing
        try_case(
            "F0-MALFORM-1",
            lambda: _run_case(
                "F0-MALFORM-1",
                _SignalLike(strategy=fast0, dist_to_peak_pct=1.0, liq_long_usd_30s="bad"),
                False,
                lambda ok, reason: _assert_exact_reason("F0-MALFORM-1", ok, reason, "liq_missing"),
            ),
        )
    finally:
        risk_profile.FAST0_AUTO_ENABLE = original_fast0_auto_enable

    # --------------------
    # malformed input fixtures
    # --------------------
    # MAL-REJECT-1: short_pump with invalid dist_to_peak_pct
    try_case(
        "MAL-REJECT-1",
        lambda: _run_case(
            "MAL-REJECT-1",
            _SignalLike(strategy=short_pump, stage=4, dist_to_peak_pct="bad"),
            False,
            lambda ok, reason: _assert_exact_reason("MAL-REJECT-1", ok, reason, "invalid dist_to_peak_pct='bad'"),
        ),
    )

    # MAL-REJECT-2: unknown strategy exact
    try_case(
        "MAL-REJECT-2",
        lambda: _run_case(
            "MAL-REJECT-2",
            _SignalLike(strategy="nope", stage=4, dist_to_peak_pct=4.0),
            False,
            lambda ok, reason: _assert_exact_reason("MAL-REJECT-2", ok, reason, "unknown strategy='nope'"),
        ),
    )

    # --------------------
    # Summary
    # --------------------
    if failures:
        print("FAIL: smoke_signal_contract_gate")
        print("Failures:")
        for f in failures:
            print("-", f)
        return 1

    print("PASS: smoke_signal_contract_gate")
    print("Checked groups: short_pump (SP), short_pump_fast0 (F0), malformed input (MAL)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

