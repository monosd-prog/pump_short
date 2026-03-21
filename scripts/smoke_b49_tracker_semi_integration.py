#!/usr/bin/env python3
"""
Semi-integration smoke for B49: run the real tracker entrypoint with synthetic klines
to verify it can process an SP-PAPER position and produce non-zero MFE/MAE or TP outcome.

This harness does NOT modify production code. It monkeypatches the klines provider at runtime.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone

TEST_ROOT = Path(os.getenv("B49_TEST_ROOT", "/tmp/b49_semi")).resolve()
DATASET_BASE = TEST_ROOT / "datasets"
STATE_PATH = TEST_ROOT / "datasets" / "trading_state.json"
OUT_COMBINED = TEST_ROOT / "result.json"
KLINES_CSV = TEST_ROOT / "klines.csv"

def ensure_dirs():
    DATASET_BASE.mkdir(parents=True, exist_ok=True)

def fail(msg: str, code: int = 2):
    print("FAIL:", msg)
    sys.exit(code)

def main():
    ensure_dirs()
    # minimal imports guarded
    try:
        import pandas as pd
        import numpy as np
    except Exception as e:
        fail(f"missing runtime dependency: pandas/numpy required ({e})")

    # Set env vars so tracker config reads test paths
    os.environ["EXECUTION_MODE"] = "paper"
    os.environ["TRADING_STATE_PATH"] = str(STATE_PATH)
    os.environ["DATASET_BASE_DIR"] = str(DATASET_BASE)

    # Build synthetic klines: 1m candles that move from entry -> hit TP (short TP below entry)
    # Position: short, entry=100, tp=95, sl=105. We create candles that drop to 94 within watch window.
    entry_ts = datetime.now(timezone.utc) - timedelta(minutes=5)
    entry_iso = entry_ts.isoformat()
    entry_price = 100.0
    tp_price = 95.0
    sl_price = 105.0

    periods = 10
    idx = pd.date_range(start=entry_ts, periods=periods, freq="1min", tz="UTC")
    # start flat then drop below TP
    opens = np.linspace(entry_price, 96.0, periods)
    highs = opens + 0.5
    lows = opens - 0.5
    closes = opens.copy()
    # force final candle to drop to 94 (below TP)
    closes[-1] = 94.0
    lows[-1] = 93.5
    highs[-1] = 95.0
    df = pd.DataFrame(
        {
            "open": opens,
            "high": highs,
            "low": lows,
            "close": closes,
            "volume": np.random.randint(1, 10, size=periods),
        },
        index=idx,
    ).reset_index().rename(columns={"index": "timestamp"})
    # normalize to expected tracker schema: 'ts','open','high','low','close','volume'
    df = df.rename(columns={"timestamp": "ts"})
    # ensure ts is timezone-aware string/pandas Timestamp
    df["ts"] = pd.to_datetime(df["ts"])
    # Save klines for inspection
    df.to_csv(KLINES_CSV, index=False)

    # monkeypatch provider
    try:
        from short_pump import bybit_api

        def get_test_klines(cat: str, symbol: str, limit=300):
            # ignore args, return our df (with timezone-aware timestamps)
            return df.copy()

        bybit_api.get_klines_1m = get_test_klines
    except Exception as e:
        fail(f"failed to monkeypatch klines provider: {e}")

    # Prepare a single SP-PAPER position (not written to state is ok)
    position = {
        "strategy": "short_pump",
        "mode": "paper",
        "symbol": "TESTUSDT",
        "entry": entry_price,
        "tp": tp_price,
        "sl": sl_price,
        "opened_ts": entry_iso,
        "run_id": "smoke_r1",
        "event_id": "smoke_e1",
    }

    # Call real tracker wrapper
    try:
        from short_pump.outcome import track_outcome_short
        from short_pump.config import Config

        cfg = Config.from_env()
        entry_ts_pd = pd.to_datetime(entry_iso)

        res = track_outcome_short(
            cfg=cfg,
            entry_ts_utc=entry_ts_pd,
            entry_price=float(position["entry"]),
            tp_price=float(position["tp"]),
            sl_price=float(position["sl"]),
            entry_source="smoke_test",
            entry_type="paper",
            run_id=position["run_id"],
            symbol=position["symbol"],
        )
    except Exception as e:
        fail(f"tracker invocation failed: {e}")

    # Save result
    with open(OUT_COMBINED, "w", encoding="utf-8") as f:
        json.dump({"result": res}, f, indent=2, ensure_ascii=False, default=str)

    # Basic PASS checks
    mfe = None
    mae = None
    try:
        mfe = float(res.get("mfe", 0) or 0)
        mae = float(res.get("mae", 0) or 0)
    except Exception:
        pass

    outcome = res.get("outcome") or res.get("final_outcome") or ""
    passed = False
    if (mfe and abs(mfe) > 0.0) or (mae and abs(mae) > 0.0):
        passed = True
    if isinstance(outcome, str) and outcome.upper().startswith(("TP", "SL")):
        passed = True

    print("RESULT:", "PASS" if passed else "FAIL")
    print("mfe=", mfe, "mae=", mae, "outcome=", outcome)
    print("artifacts:", OUT_COMBINED, KLINES_CSV)
    sys.exit(0 if passed else 3)

if __name__ == "__main__":
    main()

