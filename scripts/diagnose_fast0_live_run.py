#!/usr/bin/env python3
"""
Diagnostic for FAST0 live trade: TRADEABLE sent, position opened/closed, but LIVE_OPEN/OUTCOME not received.

Usage:
  cd /root/pump_short
  PYTHONPATH=. python3 scripts/diagnose_fast0_live_run.py --run-id 20260313_151135 --symbol DOODUSDT

Checks: state, closes, datasets, signals queue, env TG_SEND_OUTCOME.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", required=True, help="run_id (e.g. 20260313_151135)")
    ap.add_argument("--symbol", required=True, help="symbol (e.g. DOODUSDT)")
    ap.add_argument("--data-dir", default=None, help="datasets root (default: cwd/datasets or /root/pump_short/datasets)")
    args = ap.parse_args()

    run_id = args.run_id.strip()
    symbol = args.symbol.strip().upper()
    if args.data_dir:
        data_dir = Path(args.data_dir)
    else:
        cwd = Path(os.getcwd())
        if (cwd / "datasets" / "trading_state.json").exists():
            data_dir = cwd / "datasets"
        elif (cwd / "trading_state.json").exists():
            data_dir = cwd
        else:
            data_dir = Path("/root/pump_short/datasets")

    print("=" * 70)
    print(f"FAST0 LIVE DIAGNOSTIC: run_id={run_id} symbol={symbol}")
    print(f"data_dir={data_dir}")
    print("=" * 70)

    # 1. trading_state.json
    state_path = data_dir / "trading_state.json"
    print("\n1. TRADING STATE")
    if not state_path.exists():
        print(f"   NOT FOUND: {state_path}")
    else:
        try:
            d = json.loads(state_path.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"   ERROR: {e}")
        else:
            op = d.get("open_positions") or {}
            sp = op.get("short_pump_fast0") or {}
            found_open = []
            for pid, pos in list(sp.items()):
                if not isinstance(pos, dict):
                    continue
                if pos.get("symbol") == symbol or run_id in str(pos.get("run_id", "")):
                    found_open.append((pid, pos))
            if found_open:
                for pid, pos in found_open:
                    print(f"   OPEN: {pid}")
                    print(f"      run_id={pos.get('run_id')} event_id={pos.get('event_id')} symbol={pos.get('symbol')}")
                    print(f"      order_id={pos.get('order_id')} position_idx={pos.get('position_idx')}")
            else:
                print(f"   No open position for {symbol} / {run_id}")
            sent = d.get("outcome_tg_sent") or []
            found_sent = [k for k in sent if symbol in k or run_id in k]
            if found_sent:
                print(f"   outcome_tg_sent (matches): {found_sent[:5]}")
            else:
                print(f"   outcome_tg_sent: no match (total {len(sent)})")

    # 2. trading_closes
    print("\n2. TRADING CLOSES")
    closes_glob = list(data_dir.glob("trading_closes*.csv"))
    found_closes = False
    for p in closes_glob:
        if not p.exists():
            continue
        for line in p.read_text(encoding="utf-8", errors="replace").splitlines():
            if run_id in line or symbol in line:
                print(f"   {p.name}: {line[:120]}")
                found_closes = True
    if not found_closes:
        print("   No matching closes")

    # 3. outcomes_v3 in date=*
    print("\n3. DATASETS OUTCOMES")
    date_dirs = list(data_dir.glob("date=*"))
    for dd in sorted(date_dirs)[-10:]:
        ev = dd / "strategy=short_pump_fast0" / "mode=live" / "outcomes_v3.csv"
        if ev.exists():
            for line in ev.read_text(encoding="utf-8", errors="replace").splitlines():
                if run_id in line or symbol in line:
                    print(f"   {ev}: {line[:140]}")

    # 4. signals_queue.jsonl
    print("\n4. SIGNALS QUEUE")
    q_path = data_dir / "signals_queue.jsonl"
    if not q_path.exists():
        print("   signals_queue.jsonl NOT FOUND")
    else:
        for line in q_path.read_text(encoding="utf-8", errors="replace").strip().splitlines():
            if run_id in line or symbol in line:
                print(f"   {line[:200]}...")

    # 5. TG_SEND_OUTCOME
    print("\n5. ENV TG_SEND_OUTCOME")
    v = os.getenv("TG_SEND_OUTCOME", "not set")
    print(f"   TG_SEND_OUTCOME={v}")
    if v != "1":
        print("   -> OUTCOME TG will NOT be sent (_send_live_outcome_telegram returns early)")

    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()
