#!/usr/bin/env python3
"""
Minimal integration wrapper for B49:
- Reads persisted state (TRADING_STATE_PATH)
- Finds one open SP-PAPER position
- Verifies outcome_attached/outcome_tracker_id present
- Monkeypatches klines provider and calls real tracker entrypoint
- Writes result.json, klines.csv, and a short log summary

Usage:
  PYTHONPATH=. /path/to/python3 scripts/smoke_b49_runner_attach_wrapper_integration.py
Ensure env vars point to test paths (defaults to /tmp/b49_integ).
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone

TEST_ROOT = Path(os.getenv("B49_INTEG_ROOT", "/tmp/b49_integ")).resolve()
DATASET_BASE = TEST_ROOT / "datasets"
STATE_PATH = Path(os.getenv("TRADING_STATE_PATH", str(DATASET_BASE / "trading_state.json")))
OUT_RESULT = TEST_ROOT / "result_integration.json"
KLINES_CSV = TEST_ROOT / "klines_integration.csv"
LOG_SUM = TEST_ROOT / "summary.txt"

def ensure_dirs():
    DATASET_BASE.mkdir(parents=True, exist_ok=True)

def fail(msg: str, code: int = 2):
    print("FAIL:", msg)
    with open(LOG_SUM, "a", encoding="utf-8") as f:
        f.write("FAIL: " + msg + "\n")
    sys.exit(code)

def load_state() -> dict:
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {"open_positions": {}, "last_signal_ids": {}}
    except Exception as e:
        fail(f"failed to read state: {e}")

def find_one_sp_paper(state: dict):
    op = state.get("open_positions") or {}
    sp = op.get("short_pump") or {}
    if not isinstance(sp, dict) or not sp:
        return None, None
    # pick first
    for pid, pos in sp.items():
        return pid, pos
    return None, None

def build_synthetic_klines(entry_ts: datetime, entry_price: float, tp: float, sl: float):
    import pandas as pd
    import numpy as np

    periods = 10
    idx = pd.date_range(start=entry_ts - timedelta(minutes=1), periods=periods, freq="1min", tz="UTC")
    opens = np.linspace(entry_price, entry_price - 6.0, periods)
    highs = opens + 0.5
    lows = opens - 0.5
    closes = opens.copy()
    closes[-1] = entry_price - 6.0  # ensure drop to hit TP for short
    df = pd.DataFrame(
        {
            "open": opens,
            "high": highs,
            "low": lows,
            "close": closes,
            "volume": np.random.randint(1, 10, size=periods),
        },
        index=idx,
    ).reset_index().rename(columns={"index": "ts"})
    df["ts"] = pd.to_datetime(df["ts"])
    return df

def main():
    ensure_dirs()
    # import heavy deps lazily
    try:
        import pandas as pd  # noqa: F401
    except Exception as e:
        fail(f"missing pandas dependency: {e}")

    state = load_state()
    pid, pos = find_one_sp_paper(state)
    if not pid or not pos:
        fail("no open short_pump paper position found in state")

    # Verify attach metadata present
    attached = bool(pos.get("outcome_attached"))
    tracker_id = pos.get("outcome_tracker_id")
    if not attached or not tracker_id:
        fail(f"position {pid} missing attach metadata (outcome_attached={attached})")

    # Build synthetic klines aligned to opened_ts
    opened_ts = pos.get("opened_ts") or pos.get("opened_ts", "")
    try:
        import pandas as pd
        entry_ts = pd.to_datetime(opened_ts)
        if not hasattr(entry_ts, "tz"):
            entry_ts = pd.Timestamp(entry_ts).tz_localize("UTC")
    except Exception:
        entry_ts = datetime.now(timezone.utc) - timedelta(minutes=5)

    entry_price = float(pos.get("entry", 0) or 0.0)
    tp_price = float(pos.get("tp", 0) or 0.0)
    sl_price = float(pos.get("sl", 0) or 0.0)

    df = build_synthetic_klines(entry_ts.to_pydatetime() if hasattr(entry_ts, "to_pydatetime") else entry_ts, entry_price, tp_price, sl_price)
    df.to_csv(KLINES_CSV, index=False)
    df_used = df

    # Monkeypatch provider
    try:
        from short_pump import bybit_api

        def get_test_klines(cat: str, symbol: str, limit=300):
            return df.copy()

        bybit_api.get_klines_1m = get_test_klines
    except Exception as e:
        fail(f"failed to monkeypatch klines provider: {e}")

    # Call real tracker wrapper
    try:
        from short_pump.config import Config
        from common.outcome_tracker import track_outcome

        cfg = Config.from_env()
        import pandas as pd
        entry_ts_pd = pd.to_datetime(opened_ts)
        now_pd = pd.Timestamp.now(tz="UTC")
        # If persisted entry is far in the past (so end_ts already passed), shift entry to recent test window
        if entry_ts_pd + pd.Timedelta(minutes=getattr(cfg, "outcome_watch_minutes", 120)) < now_pd:
            entry_ts_pd = now_pd - pd.Timedelta(minutes=3)
            # rebuild synthetic klines aligned to adjusted entry_ts and re-monkeypatch
            df_new = build_synthetic_klines(entry_ts_pd.to_pydatetime(), entry_price, tp_price, sl_price)
            df_new.to_csv(KLINES_CSV, index=False)
            df_used = df_new
            try:
                from short_pump import bybit_api as _by

                def get_test_klines(cat: str, symbol: str, limit=300):
                    return df_new.copy()

                _by.get_klines_1m = get_test_klines
            except Exception:
                pass
        # Ensure entry_ts aligns with produced klines to let tracker find future candles
        try:
            entry_ts_pd = pd.to_datetime(df_used["ts"].iloc[0])
        except Exception:
            pass
        # Call core tracker directly with explicit fetch_klines to ensure synthetic data used
        def _fetch(cat: str, symbol: str, limit=300):
            return df_used.copy()

        res = track_outcome(
            cfg,
            side=str(pos.get("side", "short")),
            entry_ts_utc=entry_ts_pd,
            entry_price=entry_price,
            tp_price=tp_price,
            sl_price=sl_price,
            entry_source="integration_wrapper",
            entry_type="paper",
            conflict_policy=getattr(cfg, "conflict_policy", None),
            run_id=str(pos.get("run_id", "") or ""),
            symbol=str(pos.get("symbol", "") or ""),
            category=getattr(cfg, "category", None),
            fetch_klines_1m=_fetch,
            strategy_name="short_pump",
        )
    except Exception as e:
        fail(f"tracker invocation failed: {e}")

    # Save result and summary
    with open(OUT_RESULT, "w", encoding="utf-8") as f:
        json.dump({"pid": pid, "tracker_id": tracker_id, "result": res}, f, indent=2, default=str)

    summary_lines = []
    summary_lines.append(f"pid={pid}")
    summary_lines.append(f"outcome_attached={attached} outcome_tracker_id={tracker_id}")
    summary_lines.append(f"result_outcome={res.get('outcome')} mfe={res.get('mfe_pct')} mae={res.get('mae_pct')}")
    with open(LOG_SUM, "w", encoding="utf-8") as f:
        f.write("\n".join(summary_lines))

    # Basic pass/fail
    outcome = res.get("outcome") or ""
    mfe = float(res.get("mfe_pct") or 0.0)
    mae = float(res.get("mae_pct") or 0.0)
    passed = (mfe != 0.0) or (mae != 0.0) or (isinstance(outcome, str) and outcome.upper().startswith(("TP", "SL")))

    print("INTEGRATION RESULT:", "PASS" if passed else "FAIL")
    print("artifacts:", OUT_RESULT, KLINES_CSV, LOG_SUM)
    sys.exit(0 if passed else 3)

if __name__ == "__main__":
    main()

