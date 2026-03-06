"""Smoke: path uses caller's mode (paper|live) when provided; events/trades/outcomes in mode=live for live runs.
Fix: FAST0 events/trades in live runs now write to mode=live even when API server has EXECUTION_MODE=paper.
"""
from __future__ import annotations

import csv
import json
import os
import sys
import tempfile
from pathlib import Path

_repo_root = Path(__file__).resolve().parents[1]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from common.io_dataset import get_dataset_dir, write_event_row, write_outcome_row, write_trade_row


def main() -> None:
    strategy = "short_pump_fast0"
    wall_time_utc = "2026-03-02T19:17:10+00:00"

    with tempfile.TemporaryDirectory(prefix="smoke_exec_mode_") as base:
        # --- CASE 1: EXECUTION_MODE=paper, caller mode="live" → path must be mode=live
        os.environ["EXECUTION_MODE"] = "paper"
        path_from_caller_live = get_dataset_dir(
            strategy, wall_time_utc, base_dir=base, path_mode="live"
        )
        assert "mode=live" in Path(path_from_caller_live).parts, (
            f"path_mode=live → path must contain mode=live, got: {path_from_caller_live}"
        )
        print(f"OK: caller mode=live overrides EXECUTION_MODE=paper for path → {path_from_caller_live}")

        # --- 2) Write event/trade/outcome with mode="live" → must land in mode=live (even EXECUTION_MODE=paper)
        write_event_row(
            {
                "run_id": "smoke_exec_mode",
                "event_id": "evt_smoke_1",
                "symbol": "BTCUSDT",
                "strategy": strategy,
                "side": "SHORT",
                "wall_time_utc": wall_time_utc,
                "time_utc": wall_time_utc,
                "stage": 2,
                "entry_ok": False,
                "skip_reasons": "smoke",
                "context_score": 0.0,
                "price": 1000.0,
                "dist_to_peak_pct": 0.0,
                "payload_json": "{}",
            },
            strategy=strategy,
            mode="live",
            wall_time_utc=wall_time_utc,
            base_dir=base,
            schema_version=3,
        )
        write_trade_row(
            {
                "trade_id": "smoke_trade_1",
                "event_id": "evt_smoke_1",
                "run_id": "smoke_exec_mode",
                "symbol": "BTCUSDT",
                "strategy": strategy,
                "side": "SHORT",
                "entry_time_utc": wall_time_utc,
                "entry_price": 1000.0,
                "tp_price": 994.0,
                "sl_price": 1006.0,
                "trade_type": "FAST0_PAPER",
            },
            strategy=strategy,
            mode="live",
            wall_time_utc=wall_time_utc,
            base_dir=base,
            schema_version=3,
        )
        write_outcome_row(
            {
                "trade_id": "smoke_trade_1",
                "event_id": "evt_smoke_1",
                "run_id": "smoke_exec_mode",
                "symbol": "BTCUSDT",
                "strategy": strategy,
                "side": "SHORT",
                "outcome_time_utc": wall_time_utc,
                "outcome": "TP",
                "pnl_pct": 0.6,
                "hold_seconds": 120.0,
                "mae_pct": 0.0,
                "mfe_pct": 0.7,
                "details_json": "{}",
            },
            strategy=strategy,
            mode="live",
            wall_time_utc=wall_time_utc,
            base_dir=base,
            schema_version=3,
        )

        # Files MUST exist under mode=live (this was the bug: events/trades went to mode=paper)
        expected_dir = Path(base) / "date=20260302" / f"strategy={strategy}" / "mode=live"
        for fname in ("events.csv", "events_v3.csv", "trades.csv", "trades_v3.csv", "outcomes.csv", "outcomes_v3.csv"):
            p = expected_dir / fname
            assert p.exists(), f"mode=live must have {fname}, got: {list(expected_dir.glob('*'))}"
        print("OK: events/trades/outcomes in mode=live when caller passes mode=live")

        # And they MUST NOT be written under mode=paper for this case
        paper_dir_case1 = Path(base) / "date=20260302" / f"strategy={strategy}" / "mode=paper"
        assert not paper_dir_case1.exists(), (
            f"caller mode=live with EXECUTION_MODE=paper must not create mode=paper dir, found: {paper_dir_case1}"
        )
        print("OK: no files under mode=paper when caller mode=live and EXECUTION_MODE=paper")

        # --- CASE 2: EXECUTION_MODE=paper, caller mode="paper" → writes go to mode=paper
        path_from_caller_paper = get_dataset_dir(
            strategy, wall_time_utc, base_dir=base, path_mode="paper"
        )
        assert "mode=paper" in Path(path_from_caller_paper).parts, (
            f"path_mode=paper → path must contain mode=paper, got: {path_from_caller_paper}"
        )

        # Write with mode="paper" → must land in mode=paper
        write_event_row(
            {
                "run_id": "smoke_exec_mode_paper",
                "event_id": "evt_smoke_paper_1",
                "symbol": "BTCUSDT",
                "strategy": strategy,
                "side": "SHORT",
                "wall_time_utc": wall_time_utc,
                "time_utc": wall_time_utc,
                "stage": 2,
                "entry_ok": False,
                "skip_reasons": "smoke",
                "context_score": 0.0,
                "price": 1000.0,
                "dist_to_peak_pct": 0.0,
                "payload_json": "{}",
            },
            strategy=strategy,
            mode="paper",
            wall_time_utc=wall_time_utc,
            base_dir=base,
            schema_version=3,
        )
        paper_dir = Path(base) / "date=20260302" / f"strategy={strategy}" / "mode=paper"
        assert (paper_dir / "events_v3.csv").exists(), "mode=paper must have events_v3.csv"
        print("OK: events in mode=paper when caller passes mode=paper")

        # --- CASE 3: no caller mode/path_mode → fallback to EXECUTION_MODE
        os.environ["EXECUTION_MODE"] = "paper"
        path_fallback = get_dataset_dir(strategy, wall_time_utc, base_dir=base)
        assert "mode=paper" in Path(path_fallback).parts, (
            f"no path_mode + EXECUTION_MODE=paper → path=paper, got: {path_fallback}"
        )

        os.environ["EXECUTION_MODE"] = "live"
        path_fallback_live = get_dataset_dir(strategy, wall_time_utc, base_dir=base)
        assert "mode=live" in Path(path_fallback_live).parts, (
            f"no path_mode + EXECUTION_MODE=live → path=live, got: {path_fallback_live}"
        )
        print("OK: fallback to EXECUTION_MODE only when caller mode/path_mode is absent")

    print("smoke_dataset_exec_mode: caller mode controls path when paper|live; EXECUTION_MODE used only as fallback")


if __name__ == "__main__":
    main()
