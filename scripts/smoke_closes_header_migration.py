#!/usr/bin/env python3
"""Smoke: _ensure_closes_header migrates old CSV (no mfe/mae) to full header, preserves rows."""
from __future__ import annotations

import csv
import sys
import tempfile
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

from trading.paper_outcome import _ensure_closes_header

OLD_HEADER = [
    "ts", "strategy", "symbol", "side", "entry", "tp", "sl", "exit_price",
    "close_reason", "pnl_r", "pnl_usd", "run_id", "event_id",
]


def main() -> int:
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(OLD_HEADER)
        w.writerow([
            "2026-02-05 12:00:00+00:00", "short_pump_fast0", "XUSDT", "SHORT",
            "1.0", "0.99", "1.01", "0.99", "tp", "1.0", "10.0", "r1", "e1",
        ])
        temp_path = f.name
    path = str(temp_path)
    result = _ensure_closes_header(path)
    if result != "migrated":
        print(f"FAIL: expected result 'migrated', got {result!r}")
        Path(temp_path).unlink(missing_ok=True)
        return 1
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        rows = list(reader)
    for col in ("mfe_pct", "mae_pct", "mfe_r", "mae_r"):
        if col not in header:
            print(f"FAIL: header missing column {col!r}")
            Path(temp_path).unlink(missing_ok=True)
            return 1
    if len(rows) != 1:
        print(f"FAIL: expected 1 data row, got {len(rows)}")
        Path(temp_path).unlink(missing_ok=True)
        return 1
    old_cells = rows[0][: len(OLD_HEADER)]
    expected_first = ["2026-02-05 12:00:00+00:00", "short_pump_fast0", "XUSDT", "SHORT", "1.0", "0.99", "1.01", "0.99", "tp", "1.0", "10.0", "r1", "e1"]
    if old_cells != expected_first:
        print(f"FAIL: first row prefix changed: got {old_cells!r}")
        Path(temp_path).unlink(missing_ok=True)
        return 1
    Path(temp_path).unlink(missing_ok=True)
    print("smoke_closes_header_migration: OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
