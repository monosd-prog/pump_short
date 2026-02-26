#!/usr/bin/env python3
"""
Smoke: MFE/MAE columns in trading_closes.csv.
Create a fake state with one SHORT position (entry=1.0, sl=1.01), close with outcome_meta
mfe_pct=0.02, mae_pct=0.01; verify CSV row has mfe_pct, mae_pct, mfe_r~2.0, mae_r~1.0.
"""
from __future__ import annotations

import csv
import os
import sys
import tempfile
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))
os.chdir(_ROOT)

# Use temp paths so we don't touch real datasets
_TMP = Path(tempfile.mkdtemp(prefix="smoke_mfe_mae_"))
STATE_PATH = _TMP / "trading_state.json"
CLOSES_PATH = _TMP / "trading_closes.csv"
os.environ["TRADING_STATE_PATH"] = str(STATE_PATH)
os.environ["TRADING_CLOSES_PATH"] = str(CLOSES_PATH)

from trading.state import load_state, make_position_id, record_open, save_state
from trading.paper_outcome import close_from_outcome


def main() -> int:
    strategy = "short_pump_fast0"
    symbol = "SMOKEUSDT"
    run_id = "smoke_mfe"
    event_id = "e1"
    entry, sl, tp = 1.0, 1.01, 0.99  # SHORT: risk 1% per unit

    state = load_state()
    pid = record_open(
        state,
        {
            "strategy": strategy,
            "symbol": symbol,
            "side": "SHORT",
            "run_id": run_id,
            "event_id": event_id,
            "entry": entry,
            "tp": tp,
            "sl": sl,
            "opened_ts": "2026-02-05 12:00:00+00:00",
            "risk_usd": 10.0,
        },
    )
    save_state(state)
    if not pid:
        print("FAIL: record_open returned None")
        return 1

    # Close with outcome_meta: mfe_pct=2%, mae_pct=1% (percent as in outcome_tracker) -> mfe_r=2.0, mae_r=1.0 (risk_pct=1%)
    ok = close_from_outcome(
        strategy=strategy,
        symbol=symbol,
        run_id=run_id,
        event_id=event_id,
        res="TP_hit",
        pnl_pct=2.0,
        ts_utc="2026-02-05 12:05:00+00:00",
        outcome_meta={"mfe_pct": 2.0, "mae_pct": 1.0},
    )
    if not ok:
        print("FAIL: close_from_outcome returned False")
        return 1

    if not CLOSES_PATH.exists():
        print("FAIL: trading_closes.csv was not created")
        return 1

    with open(CLOSES_PATH, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    if not rows:
        print("FAIL: no rows in trading_closes.csv")
        return 1
    row = rows[0]

    for col in ("mfe_pct", "mae_pct", "mfe_r", "mae_r"):
        if col not in row:
            print(f"FAIL: column {col} missing in CSV")
            return 1

    mfe_pct_val = float(row["mfe_pct"]) if row["mfe_pct"] else None
    mae_pct_val = float(row["mae_pct"]) if row["mae_pct"] else None
    mfe_r_val = float(row["mfe_r"]) if row["mfe_r"] else None
    mae_r_val = float(row["mae_r"]) if row["mae_r"] else None

    if mfe_pct_val is None or abs(mfe_pct_val - 2.0) > 0.01:
        print(f"FAIL: mfe_pct expected ~2.0 (%), got {mfe_pct_val}")
        return 1
    if mae_pct_val is None or abs(mae_pct_val - 1.0) > 0.01:
        print(f"FAIL: mae_pct expected ~1.0 (%), got {mae_pct_val}")
        return 1
    if mfe_r_val is None or abs(mfe_r_val - 2.0) > 0.1:
        print(f"FAIL: mfe_r expected ~2.0, got {mfe_r_val}")
        return 1
    if mae_r_val is None or abs(mae_r_val - 1.0) > 0.1:
        print(f"FAIL: mae_r expected ~1.0, got {mae_r_val}")
        return 1

    print("smoke_mfe_mae_columns: OK (mfe_pct/mae_pct/mfe_r/mae_r in CSV)")
    try:
        _TMP.rmdir()
    except OSError:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
