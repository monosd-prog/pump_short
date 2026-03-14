#!/usr/bin/env python3
"""
Backfill TPSL_SETUP_FAILED_CLOSED case: record to trading_closes, outcomes_v3, TG.

Use when a live trade filled entry but TPSL setup failed and position was force-closed,
and the case was lost (no record). Run manually with known params from logs/Bybit.

Usage:
  PYTHONPATH=. python3 scripts/backfill_tpsl_failed_closed.py \\
    --run-id 20260313_151135 --symbol DOODUSDT --strategy short_pump_fast0 \\
    --entry 0.1234 --exit 0.1250 --risk-profile fast0_1p5R

If --exit omitted, uses --entry (pnl≈0). Provide from Bybit closed-pnl or logs.
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from typing import Any


@dataclass
class SimpleSignal:
    strategy: str
    symbol: str
    side: str
    run_id: str
    event_id: str
    ts_utc: str


def main() -> None:
    ap = argparse.ArgumentParser(description="Backfill TPSL_SETUP_FAILED_CLOSED case")
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--symbol", required=True)
    ap.add_argument("--strategy", default="short_pump_fast0")
    ap.add_argument("--entry", type=float, required=True, help="Entry/avgPrice from logs")
    ap.add_argument("--exit", type=float, default=None, help="Exit price from closed-pnl; default=entry")
    ap.add_argument("--risk-profile", default="fast0_1p5R")
    ap.add_argument("--notional", type=float, default=100.0)
    ap.add_argument("--leverage", type=int, default=4)
    ap.add_argument("--data-dir", default=None, help="Datasets root")
    ap.add_argument("--dry-run", action="store_true", help="Print payload, do not write")
    args = ap.parse_args()

    run_id = args.run_id.strip()
    symbol = args.symbol.strip().upper()
    entry = float(args.entry)
    exit_price = args.exit if args.exit is not None else entry
    sl_pct = 0.010
    tp_pct = 0.012
    sl = entry * (1.0 + sl_pct)
    tp = entry * (1.0 - tp_pct)
    risk_abs = abs(entry - sl)
    risk_pct = (risk_abs / entry * 100.0) if entry > 0 else 1.0
    if (args.strategy or "").lower() in ("short_pump_fast0", "short_pump"):
        pnl_pct = (entry - exit_price) / entry * 100.0
    else:
        pnl_pct = (exit_price - entry) / entry * 100.0

    from trading.state import make_position_id
    event_id = f"{run_id}_fast0_backfill"
    trade_id = make_position_id(args.strategy, run_id, event_id, symbol)

    position: dict[str, Any] = {
        "strategy": args.strategy,
        "symbol": symbol,
        "side": "SHORT",
        "entry": entry,
        "tp": tp,
        "sl": sl,
        "exit_price": exit_price,
        "pnl_pct": pnl_pct,
        "close_reason": "tpsl_setup_failed",
        "run_id": run_id,
        "event_id": event_id,
        "trade_id": trade_id,
        "mode": "live",
        "order_id": "",
        "position_idx": 0,
        "notional_usd": args.notional,
        "risk_usd": args.notional * 0.01,
        "leverage": args.leverage,
        "risk_profile": args.risk_profile,
        "opened_ts": f"2026-03-13T15:11:35.000000+00:00",
    }

    signal = SimpleSignal(
        strategy=args.strategy,
        symbol=symbol,
        side="SHORT",
        run_id=run_id,
        event_id=event_id,
        ts_utc=position["opened_ts"],
    )

    if args.data_dir:
        os.environ["DATASET_BASE_DIR"] = str(args.data_dir)
        os.environ["TRADING_CLOSES_PATH"] = str(args.data_dir) + "/trading_closes.csv"

    if args.dry_run:
        print("DRY RUN - would record:")
        print("  position:", position)
        return

    from trading.paper_outcome import record_tpsl_failed_closed
    record_tpsl_failed_closed(position, signal, outcome_source="backfill")
    print("OK: TPSL_FAILED_CLOSED backfilled for", run_id, symbol)


if __name__ == "__main__":
    main()
