#!/usr/bin/env python3
"""Smoke: backfill_timeout_mfe_mae fills empty mfe/mae for timeout rows only."""
from __future__ import annotations

import csv
import sys
import tempfile
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT / "scripts"))

from backfill_timeout_mfe_mae import backfill_timeout_mfe_mae

HEADER = [
    "ts", "strategy", "symbol", "side", "entry", "tp", "sl", "exit_price",
    "close_reason", "pnl_r", "pnl_usd", "run_id", "event_id",
    "mfe_pct", "mae_pct", "mfe_r", "mae_r",
]


def main() -> int:
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(HEADER)
        # timeout row: last 4 columns empty (old style)
        w.writerow([
            "2026-02-05 12:00:00+00:00", "short_pump_fast0", "XUSDT", "SHORT",
            "1.0", "0.99", "1.01", "1.0", "timeout", "0.0", "0.0", "r1", "e1",
            "", "", "", "",
        ])
        # non-timeout row: last 4 have values
        w.writerow([
            "2026-02-05 12:01:00+00:00", "short_pump_fast0", "YUSDT", "SHORT",
            "1.0", "0.99", "1.01", "0.99", "tp", "1.0", "10.0", "r2", "e2",
            "0.5000", "0.2000", "0.50", "0.20",
        ])
        temp_path = f.name

    stats = backfill_timeout_mfe_mae(temp_path)
    if stats["updated_rows"] != 1:
        print(f"FAIL: expected updated_rows=1, got {stats['updated_rows']}")
        Path(temp_path).unlink(missing_ok=True)
        return 1

    with open(temp_path, "r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        rows = list(reader)

    idx = {name: header.index(name) for name in ("close_reason", "mfe_pct", "mae_pct", "mfe_r", "mae_r")}
    # Row 0: timeout, must be 0.0000
    r0 = rows[0]
    for col in ("mfe_pct", "mae_pct", "mfe_r", "mae_r"):
        if r0[idx[col]] != "0.0000":
            print(f"FAIL: timeout row {col} expected 0.0000, got {r0[idx[col]]!r}")
            Path(temp_path).unlink(missing_ok=True)
            return 1
    # Row 1: non-timeout, must be unchanged
    r1 = rows[1]
    if r1[idx["mfe_pct"]] != "0.5000" or r1[idx["mae_pct"]] != "0.2000":
        print(f"FAIL: non-timeout row changed: mfe_pct={r1[idx['mfe_pct']]!r} mae_pct={r1[idx['mae_pct']]!r}")
        Path(temp_path).unlink(missing_ok=True)
        return 1

    Path(temp_path).unlink(missing_ok=True)
    print("smoke_backfill_timeout_mfe_mae: OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
