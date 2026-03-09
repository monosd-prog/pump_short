#!/usr/bin/env python3
"""
Smoke: short_pump_fast0 live dataset writing.
- Rows in mode=live files must have mode=live, source_mode=live (not paper).
- Verifies events_v3, trades_v3, outcomes_v3 under mode=live path.

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

STRATEGY = "short_pump_fast0"


def _read_csv_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path, "r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        return list(r)


def main() -> None:
    wall_time_utc = "2026-03-10T12:00:00+00:00"
    run_id = "smoke_fast0_live_20260310"
    event_id = f"{run_id}_fast0_1_abc123"
    trade_id = f"{event_id}_trade"

    with tempfile.TemporaryDirectory(prefix="smoke_fast0_live_") as base:
        os.environ["EXECUTION_MODE"] = "paper"
        base_path = Path(base)

        # Write event, trade, outcome with caller mode="live" (path=live)
        write_event_row(
            {
                "run_id": run_id,
                "event_id": event_id,
                "symbol": "PHAUSDT",
                "strategy": STRATEGY,
                "side": "SHORT",
                "wall_time_utc": wall_time_utc,
                "time_utc": wall_time_utc,
                "stage": 0,
                "entry_ok": 1,
                "dist_to_peak_pct": 5.0,
                "context_score": 0.65,
                "payload_json": "{}",
            },
            strategy=STRATEGY,
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
                "strategy": STRATEGY,
                "side": "SHORT",
                "entry_time_utc": wall_time_utc,
                "entry_price": 1.0,
                "tp_price": 0.988,
                "sl_price": 1.012,
                "trade_type": "FAST0_PAPER",
            },
            strategy=STRATEGY,
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
                "strategy": STRATEGY,
                "side": "SHORT",
                "outcome_time_utc": wall_time_utc,
                "outcome": "TIMEOUT",
                "pnl_pct": 0.0,
                "details_json": "{}",
            },
            strategy=STRATEGY,
            mode="live",
            wall_time_utc=wall_time_utc,
            base_dir=str(base_path),
            schema_version=3,
        )

        live_dir = base_path / "date=20260310" / f"strategy={STRATEGY}" / "mode=live"
        assert live_dir.exists(), f"mode=live dir must exist: {live_dir}"

        # Verify row mode/source_mode = live in all three files
        for fname in ["events_v3.csv", "trades_v3.csv", "outcomes_v3.csv"]:
            path = live_dir / fname
            assert path.exists(), f"{fname} must exist"
            rows = _read_csv_rows(path)
            assert rows, f"{fname} must have rows"
            for r in rows:
                row_mode = (r.get("mode") or "").strip().lower()
                row_src = (r.get("source_mode") or "").strip().lower()
                assert row_mode == "live", f"{fname} row must have mode=live, got mode={row_mode!r}"
                assert row_src == "live", f"{fname} row must have source_mode=live, got source_mode={row_src!r}"
        print("OK: FAST0 rows in mode=live files have mode=live, source_mode=live")

        # Verify no paper row fields in live path
        paper_dir = base_path / "date=20260310" / f"strategy={STRATEGY}" / "mode=paper"
        assert not (paper_dir / "events_v3.csv").exists(), "live write must not create mode=paper events"
        print("OK: no paper path created when caller mode=live")

    print("smoke_fast0_live_dataset: PASS")


if __name__ == "__main__":
    main()
    sys.exit(0)
