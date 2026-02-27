"""Smoke: short_pump_fast0 trade row write gated by liq_long_usd_30s > 0."""
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
        path = get_dataset_dir(strategy, wall_time_utc, base_dir=base)
        Path(path).mkdir(parents=True, exist_ok=True)
        trades_file = Path(path) / "trades_v3.csv"

        # Payload that would pass all checks except liq (context, dist, cvd ok)
        base_payload = {
            "context_score": 0.65,
            "dist_to_peak_pct": 1.0,
            "cvd_delta_ratio_30s": -0.15,
            "cvd_delta_ratio_1m": -0.08,
        }
        payload_fail_liq = {**base_payload, "liq_long_usd_30s": 0}
        pass_ok, reason = should_fast0_entry_ok(payload_fail_liq, tick=5)
        assert pass_ok is False, f"liq=0 must fail: {reason}"
        assert reason == "liq_gate", f"expected liq_gate, got {reason}"
        # Simulate fast0: only write trade when pass
        if pass_ok:
            write_trade_row(
                {"trade_id": "would_be_wrong", "event_id": "evt", "run_id": "run", "symbol": "XUSDT",
                 "strategy": strategy, "side": "SHORT", "entry_time_utc": wall_time_utc, "entry_price": 1.0,
                 "tp_price": 0.99, "sl_price": 1.01, "trade_type": "FAST0_PAPER"},
                strategy=strategy, mode="live", wall_time_utc=wall_time_utc, base_dir=base, schema_version=3,
            )
        # Write event with entry_ok=0, skip_reasons=liq_gate (sampling)
        write_event_row(
            {
                "run_id": "smoke_fast0_liq",
                "event_id": "evt_liq_gate",
                "symbol": "XUSDT", "strategy": strategy, "side": "SHORT",
                "wall_time_utc": wall_time_utc, "time_utc": wall_time_utc,
                "stage": 0, "entry_ok": False, "skip_reasons": "liq_gate",
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

        # liq>0 -> pass -> write trade
        payload_ok = {**base_payload, "liq_long_usd_30s": 100.0}
        pass_ok, reason = should_fast0_entry_ok(payload_ok, tick=5)
        assert pass_ok is True, f"liq=100 should pass: {reason}"
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
        assert len(rows) >= 1, "liq_long_usd_30s>0 must produce trade row"
        assert rows[-1].get("mode") == "paper"
        print("OK: trade row written when liq_long_usd_30s>0")

    print("smoke_fast0_liq_trade_gate: fast0 liq gate OK")


if __name__ == "__main__":
    main()
