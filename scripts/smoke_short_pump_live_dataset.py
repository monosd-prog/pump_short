#!/usr/bin/env python3
"""
Smoke: short_pump live dataset writing.
- Rows in mode=live files must have mode=live, source_mode=live (not paper).
- No duplicate contradictory outcomes for same event_id/trade_id.

Runs with EXECUTION_MODE=paper and caller mode="live" to simulate API server with paper
env while writing live path (path=live, row must also be live).
"""
from __future__ import annotations

import csv
import os
import sys
import tempfile
from pathlib import Path

_repo_root = Path(__file__).resolve().parents[1]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from common.io_dataset import write_event_row, write_trade_row, write_outcome_row


def _read_csv_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path, "r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        return list(r)


def main() -> None:
    strategy = "short_pump"
    wall_time_utc = "2026-03-10T12:00:00+00:00"
    run_id = "smoke_live_20260310"
    event_id = f"{run_id}_entry_fast"
    trade_id = f"{run_id}_trade_1"

    with tempfile.TemporaryDirectory(prefix="smoke_short_pump_live_") as base:
        os.environ["EXECUTION_MODE"] = "paper"
        base_path = Path(base)

        # Write event, trade, outcome with caller mode="live" (path=live)
        write_event_row(
            {
                "run_id": run_id,
                "event_id": event_id,
                "symbol": "PHAUSDT",
                "strategy": strategy,
                "side": "SHORT",
                "wall_time_utc": wall_time_utc,
                "time_utc": wall_time_utc,
                "stage": 4,
                "entry_ok": True,
                "dist_to_peak_pct": 6.4149,
                "payload_json": "{}",
            },
            strategy=strategy,
            mode="live",
            wall_time_utc=wall_time_utc,
            base_dir=str(base_path),
            schema_version=3,
        )
        write_trade_row(
            {
                "run_id": run_id,
                "event_id": event_id,
                "trade_id": trade_id,
                "symbol": "PHAUSDT",
                "strategy": strategy,
                "side": "SHORT",
                "entry_time_utc": wall_time_utc,
                "entry_price": 1.0,
                "tp_price": 0.98,
                "sl_price": 1.02,
                "trade_type": "LIVE",
            },
            strategy=strategy,
            mode="live",
            wall_time_utc=wall_time_utc,
            base_dir=str(base_path),
            schema_version=3,
        )
        write_outcome_row(
            {
                "run_id": run_id,
                "event_id": event_id,
                "trade_id": trade_id,
                "symbol": "PHAUSDT",
                "strategy": strategy,
                "side": "SHORT",
                "outcome_time_utc": wall_time_utc,
                "outcome": "TIMEOUT",
                "pnl_pct": 0.0,
                "details_json": "{}",
            },
            strategy=strategy,
            mode="live",
            wall_time_utc=wall_time_utc,
            base_dir=str(base_path),
            schema_version=3,
        )

        live_dir = base_path / "date=20260310" / f"strategy={strategy}" / "mode=live"
        assert live_dir.exists(), f"mode=live dir must exist: {live_dir}"

        # Verify row mode/source_mode = live
        for fname, key_col in [("events_v3.csv", "event_id"), ("trades_v3.csv", "trade_id"), ("outcomes_v3.csv", "trade_id")]:
            path = live_dir / fname
            assert path.exists(), f"{fname} must exist"
            rows = _read_csv_rows(path)
            assert rows, f"{fname} must have rows"
            for r in rows:
                row_mode = (r.get("mode") or "").strip().lower()
                row_src = (r.get("source_mode") or "").strip().lower()
                assert row_mode == "live", f"{fname} row must have mode=live, got mode={row_mode!r}"
                assert row_src == "live", f"{fname} row must have source_mode=live, got source_mode={row_src!r}"
        print("OK: rows in mode=live files have mode=live, source_mode=live")

        # Verify no duplicate outcomes for same event_id
        out_path = live_dir / "outcomes_v3.csv"
        rows = _read_csv_rows(out_path)
        event_ids = [r.get("event_id") or r.get("eventId") for r in rows]
        assert event_ids.count(event_id) <= 1, f"duplicate outcomes for event_id={event_id}: {event_ids}"
        trade_ids = [r.get("trade_id") or r.get("tradeId") for r in rows]
        assert trade_ids.count(trade_id) <= 1, f"duplicate outcomes for trade_id={trade_id}: {trade_ids}"
        print("OK: no duplicate outcomes for same event_id/trade_id")

    print("smoke_short_pump_live_dataset: PASS")


if __name__ == "__main__":
    main()
    sys.exit(0)
