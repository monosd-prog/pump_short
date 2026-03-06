"""Smoke: short_pump_fast0 entry gated by 5000 < liq_long_usd_30s <= 25000 (FAST0_LIQ_MIN/MAX_USD)."""
from __future__ import annotations

import csv
import os
import sys
import tempfile
from pathlib import Path

_repo_root = Path(__file__).resolve().parents[1]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

os.environ["EXECUTION_MODE"] = "paper"
os.environ["FAST0_LIQ_MIN_USD"] = "5000"
os.environ["FAST0_LIQ_MAX_USD"] = "25000"
os.environ["FAST0_ENTRY_CONTEXT_MIN"] = "0.60"
os.environ["FAST0_ENTRY_DIST_MIN"] = "0.5"
os.environ["FAST0_ENTRY_CVD30S_MAX"] = "-0.10"
os.environ["FAST0_ENTRY_CVD1M_MAX"] = "-0.05"
os.environ["FAST0_ENTRY_MIN_TICK"] = "0"

from common.io_dataset import get_dataset_dir, write_event_row, write_trade_row
from short_pump.fast0_sampler import STRATEGY, should_fast0_entry_ok


def main() -> None:
    wall_time_utc = "2026-02-26T12:00:00Z"
    strategy = STRATEGY

    with tempfile.TemporaryDirectory(prefix="smoke_fast0_liq_") as base:
        path = get_dataset_dir(strategy, wall_time_utc, base_dir=base, path_mode="live")
        Path(path).mkdir(parents=True, exist_ok=True)
        trades_file = Path(path) / "trades_v3.csv"

        # Payload that would pass context/dist/cvd; liq varies per case
        base_payload = {
            "context_score": 0.65,
            "dist_to_peak_pct": 1.0,
            "cvd_delta_ratio_30s": -0.15,
            "cvd_delta_ratio_1m": -0.08,
        }

        # liq=0 → blocked (liq_le_5000)
        payload_fail_liq = {**base_payload, "liq_long_usd_30s": 0}
        pass_ok, reason = should_fast0_entry_ok(payload_fail_liq, tick=5)
        assert pass_ok is False, f"liq=0 must fail: {reason}"
        assert reason == "liq_le_5000", f"expected liq_le_5000, got {reason}"

        # liq=3000 → blocked (liq_le_5000)
        pass_ok, reason = should_fast0_entry_ok({**base_payload, "liq_long_usd_30s": 3000}, tick=5)
        assert pass_ok is False, f"liq=3000 must fail: {reason}"
        assert reason == "liq_le_5000", f"expected liq_le_5000, got {reason}"

        # liq=50000 → blocked (liq_gt_25000)
        pass_ok, reason = should_fast0_entry_ok({**base_payload, "liq_long_usd_30s": 50000}, tick=5)
        assert pass_ok is False, f"liq=50000 must fail: {reason}"
        assert reason == "liq_gt_25000", f"expected liq_gt_25000, got {reason}"

        # liq=10000 → allowed (in (5000, 25000])
        pass_ok, reason = should_fast0_entry_ok({**base_payload, "liq_long_usd_30s": 10000}, tick=5)
        assert pass_ok is True, f"liq=10000 must pass: {reason}"
        assert reason == "ok", f"expected ok, got {reason}"
        # Write event with entry_ok=0, skip_reasons=liq_le_5000 (sampling)
        write_event_row(
            {
                "run_id": "smoke_fast0_liq",
                "event_id": "evt_liq_gate",
                "symbol": "XUSDT", "strategy": strategy, "side": "SHORT",
                "wall_time_utc": wall_time_utc, "time_utc": wall_time_utc,
                "stage": 0, "entry_ok": False, "skip_reasons": "liq_le_5000",
                "context_score": 0.65, "price": 1.0, "dist_to_peak_pct": 1.0,
                "payload_json": '{"liq_long_usd_30s":0}',
            },
            strategy=strategy, mode="live", wall_time_utc=wall_time_utc, base_dir=base, schema_version=3,
        )

        rows = []
        if trades_file.exists():
            with open(trades_file, encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
        assert len(rows) == 0, (
            f"liq_long_usd_30s=0 must not produce fast0 trade row; found {len(rows)}"
        )
        print("OK: no trade row when liq_long_usd_30s=0")

        # liq=10000 in (5000,25000] -> pass -> write trade
        payload_ok = {**base_payload, "liq_long_usd_30s": 10000.0}
        pass_ok, reason = should_fast0_entry_ok(payload_ok, tick=5)
        assert pass_ok is True, f"liq=10000 should pass: {reason}"
        if pass_ok:
            write_trade_row(
                {
                    "trade_id": "smoke_fast0_ok",
                    "event_id": "evt_ok",
                    "run_id": "run_ok",
                    "symbol": "YUSDT",
                    "strategy": strategy,
                    "side": "SHORT",
                    "entry_time_utc": wall_time_utc,
                    "entry_price": 1.0,
                    "tp_price": 0.99,
                    "sl_price": 1.01,
                    "trade_type": "FAST0_PAPER",
                },
                strategy=strategy,
                mode="live",
                wall_time_utc=wall_time_utc,
                base_dir=base,
                schema_version=3,
            )
        with open(trades_file, encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        assert len(rows) >= 1, "liq_long_usd_30s in (5000,25000] must produce trade row"
        print("OK: trade row written when liq_long_usd_30s=10000 (in range)")

    print("smoke_fast0_liq_trade_gate: fast0 liq range gate OK (block 0/3k/50k, allow 10k)")


if __name__ == "__main__":
    main()
