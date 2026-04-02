#!/usr/bin/env python3
"""
Regression check: _fast0_skip_paper_outcome_live_canonical would skip paper write
when live outcomes_v3 already has the row (known duplicate cases).

Run from repo root: PYTHONPATH=. python3 scripts/verify_fast0_paper_suppression.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

os.environ.setdefault("PYTHONPATH", str(_ROOT))


def main() -> int:
    from short_pump.fast0_sampler import STRATEGY, _fast0_skip_paper_outcome_live_canonical

    base_dir = str(_ROOT / "datasets")
    cases = [
        {
            "name": "SWARMSUSDT 20260401",
            "symbol": "SWARMSUSDT",
            "run_id": "20260401_213811",
            "event_id": "20260401_213811_fast0_1_0d6e1fc0",
            "trade_id": "short_pump_fast0:20260401_213811:20260401_213811_fast0_1_0d6e1fc0:SWARMSUSDT",
            "outcome_time_utc": "2026-04-01T18:39:00+00:00",
        },
        {
            "name": "STOUSDT 20260402",
            "symbol": "STOUSDT",
            "run_id": "20260402_113320",
            "event_id": "20260402_113320_fast0_10_6ffcda0e",
            "trade_id": "short_pump_fast0:20260402_113320:20260402_113320_fast0_10_6ffcda0e:STOUSDT",
            "outcome_time_utc": "2026-04-02T08:37:00+00:00",
        },
    ]
    ok_all = True
    for c in cases:
        skip, reason = _fast0_skip_paper_outcome_live_canonical(
            strategy_name=STRATEGY,
            run_id=c["run_id"],
            event_id=c["event_id"],
            symbol=c["symbol"],
            trade_id=c["trade_id"],
            outcome_time_utc=c["outcome_time_utc"],
            base_dir=base_dir,
        )
        # Before fix: paper row coexisted with live; helper must detect live_outcomes_v3_row.
        if not skip or reason != "live_outcomes_v3_row":
            print(f"FAIL {c['name']}: skip={skip} reason={reason!r} (expected live_outcomes_v3_row)")
            ok_all = False
        else:
            print(f"OK   {c['name']}: skip paper write (reason={reason})")
    return 0 if ok_all else 1


if __name__ == "__main__":
    raise SystemExit(main())
