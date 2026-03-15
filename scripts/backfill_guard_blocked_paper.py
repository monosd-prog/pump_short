#!/usr/bin/env python3
"""
Backfill GUARD_BLOCKED paper records for historical cases.

Before the fix, AUTO_RISK_GUARD_BLOCKED did not write paper trace to datasets.
This script backfills missing records for known blocked cases.

Usage:
  PYTHONPATH=. python3 scripts/backfill_guard_blocked_paper.py \\
    --run-id 20260315_021220 --symbol LYNUSDT --strategy short_pump_fast0 \\
    --risk-profile fast0_base_1R --reason "state=DISABLED"

  # Dry-run (no write):
  PYTHONPATH=. python3 scripts/backfill_guard_blocked_paper.py \\
    --run-id 20260315_021220 --symbol LYNUSDT --strategy short_pump_fast0 \\
    --risk-profile fast0_base_1R --dry-run
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _parse_run_id_date(run_id: str) -> str:
    """Derive date from run_id (e.g. 20260315_021220 -> 20260315)."""
    s = (run_id or "").strip()
    if len(s) >= 8 and s[:8].isdigit():
        return s[:8]
    return datetime.now(timezone.utc).strftime("%Y%m%d")


def _run_id_to_ts_utc(run_id: str) -> str:
    """Derive approximate UTC timestamp from run_id for partitioning."""
    s = (run_id or "").strip()
    if len(s) >= 15 and s[:8].isdigit() and s[9:15].isdigit():
        # 20260315_021220 -> 2026-03-15T02:12:20
        date_part = s[:8]
        time_part = s[9:15]
        try:
            dt = datetime.strptime(
                f"{date_part} {time_part[:2]}:{time_part[2:4]}:{time_part[4:6]}",
                "%Y%m%d %H:%M:%S",
            )
            return dt.replace(tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000000+00:00")
        except ValueError:
            pass
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000000+00:00")


def backfill_guard_blocked_paper(
    run_id: str,
    symbol: str,
    strategy: str,
    risk_profile: str,
    reason: str = "",
    *,
    entry: float | None = None,
    tp: float | None = None,
    sl: float | None = None,
    base_dir: str | None = None,
    dry_run: bool = False,
) -> bool:
    """
    Write GUARD_BLOCKED paper outcome to datasets.
    Only mode=paper. No live datasets touched.
    Returns True if written (or would write in dry-run).
    """
    try:
        from common.outcome_tracker import build_outcome_row
        from common.io_dataset import write_outcome_row
        from trading.config import DATASET_BASE_DIR
        from trading.state import make_position_id
    except ImportError as e:
        print(f"ERROR: import failed: {e}")
        return False

    run_id = run_id.strip()
    symbol = symbol.strip().upper()
    strategy = (strategy or "short_pump_fast0").strip()
    risk_profile = (risk_profile or "").strip()

    if not run_id or not symbol or not strategy:
        print("ERROR: run_id, symbol, strategy are required")
        return False

    # Placeholder when entry/tp/sl unknown (backfill for historical cases)
    entry_val = float(entry) if entry is not None and entry > 0 else 1.0
    tp_val = float(tp) if tp is not None and tp > 0 else entry_val
    sl_val = float(sl) if sl is not None and sl > 0 else entry_val
    exit_price = entry_val if (entry is not None and entry > 0) else 0.0

    event_id = f"{run_id}_guard_blocked"
    trade_id = make_position_id(strategy, run_id, event_id, symbol)
    ts_utc = _run_id_to_ts_utc(run_id)
    ds_base = base_dir or DATASET_BASE_DIR

    summary = {
        "end_reason": "GUARD_BLOCKED",
        "outcome": "GUARD_BLOCKED",
        "pnl_pct": 0.0,
        "hold_seconds": 0.0,
        "mae_pct": 0.0,
        "mfe_pct": 0.0,
        "entry_price": entry_val,
        "tp_price": tp_val,
        "sl_price": sl_val,
        "exit_price": exit_price,
        "pnl_r": 0.0,
        "pnl_usd": 0.0,
        "opened_ts": ts_utc,
        "trade_type": "PAPER",
        "risk_profile": risk_profile,
        "details_payload": json.dumps({
            "guard_blocked": True,
            "reason": reason or "",
            "backfill": True,
        }, ensure_ascii=False),
    }

    orow = build_outcome_row(
        summary,
        trade_id=trade_id,
        event_id=event_id,
        run_id=run_id,
        symbol=symbol,
        strategy=strategy,
        mode="paper",
        side="SHORT",
        outcome_time_utc=ts_utc,
    )

    if not orow:
        print("WARN: build_outcome_row returned None (possible duplicate trade_id)")
        return False

    orow["outcome_source"] = "guard_blocked_paper_backfill"

    if dry_run:
        print("[DRY-RUN] Would write:")
        print(f"  strategy={strategy} symbol={symbol} run_id={run_id} event_id={event_id}")
        print(f"  risk_profile={risk_profile} outcome=GUARD_BLOCKED trade_type=PAPER")
        print(f"  path: {ds_base}/date={_parse_run_id_date(run_id)}/strategy={strategy}/mode=paper/outcomes_v3.csv")
        return True

    write_outcome_row(
        orow,
        strategy=strategy,
        mode="paper",
        wall_time_utc=ts_utc,
        schema_version=3,
        base_dir=ds_base,
        path_mode="paper",
    )
    print(f"OK: GUARD_BLOCKED_PAPER_BACKFILL | strategy={strategy} symbol={symbol} run_id={run_id} event_id={event_id}")
    return True


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Backfill GUARD_BLOCKED paper records for historical cases",
    )
    ap.add_argument("--run-id", required=True, help="e.g. 20260315_021220")
    ap.add_argument("--symbol", required=True, help="e.g. LYNUSDT")
    ap.add_argument("--strategy", default="short_pump_fast0")
    ap.add_argument("--risk-profile", required=True, help="e.g. fast0_base_1R")
    ap.add_argument("--reason", default="", help="Guard reason (e.g. state=DISABLED)")
    ap.add_argument("--entry", type=float, default=None, help="Entry price if known")
    ap.add_argument("--tp", type=float, default=None, help="TP price if known")
    ap.add_argument("--sl", type=float, default=None, help="SL price if known")
    ap.add_argument("--base-dir", default=None, help="Datasets root (default: DATASET_BASE_DIR)")
    ap.add_argument("--dry-run", action="store_true", help="Print only, do not write")
    args = ap.parse_args()

    ok = backfill_guard_blocked_paper(
        run_id=args.run_id,
        symbol=args.symbol,
        strategy=args.strategy,
        risk_profile=args.risk_profile,
        reason=args.reason,
        entry=args.entry,
        tp=args.tp,
        sl=args.sl,
        base_dir=args.base_dir,
        dry_run=args.dry_run,
    )
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
