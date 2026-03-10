#!/usr/bin/env python3
"""
Audit: Historical reconciliation of live TIMEOUT outcomes vs Bybit actual closes.

Phase 1: Audit-only. Find live outcomes recorded as TIMEOUT and try to match them
with actual Bybit closed-pnl. Report probable real outcome (SL_hit, TP_hit) or unresolved.
No data modification. Designed for future Phase 2 (backfill) re-use.

Phase 2 (backfill) - not implemented yet:
  - Add --apply flag to update outcomes_v3.csv in-place for resolved rows
  - Backup before modify: cp outcomes_v3.csv outcomes_v3.csv.bak_{ts}
  - For each row with resolved_outcome in (SL_hit, TP_hit) and confidence=high:
    - Replace outcome col; update pnl_pct, hold_seconds from Bybit if needed
  - Log all changes to artifacts/backfill_log.csv

Usage:
  python3 scripts/audit_live_timeout_reconciliation.py --root /root/pump_short
  python3 scripts/audit_live_timeout_reconciliation.py --root /root/pump_short --output artifacts/audit.csv

Output:
  - Summary to stdout
  - artifacts/live_timeout_reconciliation_audit.csv (or --output path)
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

# Project root for imports
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from trading.config import DATASET_BASE_DIR

AUDIT_COLUMNS = [
    "date",
    "strategy",
    "symbol",
    "run_id",
    "event_id",
    "trade_id",
    "order_id",
    "position_idx",
    "opened_ts",
    "timeout_recorded_ts",
    "entry_state",
    "tp_state",
    "sl_state",
    "bybit_entry",
    "bybit_exit",
    "bybit_closed_pnl",
    "bybit_closed_ts",
    "resolved_outcome",
    "confidence",
    "reason",
]

_ENTRY_TOLERANCE_PCT = 0.025  # 2.5% for historical matching (slippage)


def _parse_ts_ms(s: str) -> int | None:
    """Parse ISO/space timestamp to ms. Return None on failure."""
    if not s or not str(s).strip():
        return None
    ss = str(s).strip().replace("Z", "+00:00").replace("+0000", "+00:00")
    for fmt in (
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S",
    ):
        try:
            dt = datetime.strptime(ss, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        except ValueError:
            continue
    try:
        dt = datetime.fromisoformat(ss)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except ValueError:
        return None


def _find_live_outcome_files(root: Path) -> list[tuple[Path, str, str]]:
    """Find outcomes_v3.csv under date=*/strategy=*/mode=live. Return (path, date, strategy)."""
    root = Path(root).resolve()
    candidates = [root, root / "datasets"]
    found: list[tuple[Path, str, str]] = []
    for base in candidates:
        if not base.exists():
            continue
        for date_dir in sorted(base.glob("date=*")):
            if not date_dir.is_dir():
                continue
            date_val = date_dir.name.replace("date=", "")
            for strat_dir in date_dir.iterdir():
                if not strat_dir.is_dir() or not strat_dir.name.startswith("strategy="):
                    continue
                strategy = strat_dir.name.replace("strategy=", "")
                live_dir = strat_dir / "mode=live"
                p = live_dir / "outcomes_v3.csv"
                if p.exists():
                    found.append((p, date_val, strategy))
    return found


def _read_csv(path: Path) -> list[dict]:
    with open(path, "r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        return list(r)


def _load_trades_for_outcomes(outcomes_path: Path) -> dict[str, dict]:
    """Load trades_v3 from same dir, keyed by trade_id."""
    trades_path = outcomes_path.parent / "trades_v3.csv"
    if not trades_path.exists():
        return {}
    rows = _read_csv(trades_path)
    return {str(r.get("trade_id", "")).strip(): r for r in rows if r.get("trade_id")}


def _is_timeout(row: dict) -> bool:
    o = (row.get("outcome") or row.get("end_reason") or "").strip().upper()
    return o == "TIMEOUT"


def _is_live(row: dict) -> bool:
    mode = (row.get("mode") or "").strip().lower()
    src = (row.get("source_mode") or "").strip().lower()
    tt = (row.get("trade_type") or "").strip().upper()
    return mode == "live" or src == "live" or tt == "LIVE"


def _match_bybit_closed_pnl(
    broker,
    symbol: str,
    side: str,
    entry: float,
    tp: float,
    sl: float,
    opened_ms: int,
    timeout_ms: int,
) -> tuple[str, float, float, float, str, str]:
    """
    Query Bybit closed-pnl and match. Returns (resolved_outcome, bybit_entry, bybit_exit, bybit_pnl, bybit_ts, reason).
    resolved_outcome: SL_hit | TP_hit | unresolved
    """
    if not broker or not symbol or entry <= 0 or opened_ms is None:
        return ("unresolved", 0.0, 0.0, 0.0, "", "missing_params")
    want_side = "Sell" if (side or "").strip().upper() in ("SHORT", "SELL") else "Buy"
    end_ms = timeout_ms + 300_000  # 5 min after timeout
    try:
        records = broker.get_closed_pnl(
            symbol,
            start_time_ms=opened_ms - 60_000,
            end_time_ms=end_ms,
            limit=100,
            raise_on_network_error=False,
        )
    except Exception as e:
        return ("unresolved", 0.0, 0.0, 0.0, "", f"api_error:{type(e).__name__}")

    for rec in records:
        if rec.get("side") != want_side:
            continue
        avg_entry = float(rec.get("avgEntryPrice") or 0)
        avg_exit = float(rec.get("avgExitPrice") or 0)
        if avg_entry <= 0 or avg_exit <= 0:
            continue
        if abs(avg_entry - entry) / max(entry, 1e-9) > _ENTRY_TOLERANCE_PCT:
            continue
        updated_ms = int(rec.get("updatedTime") or 0)
        if updated_ms < opened_ms:
            continue
        if updated_ms > timeout_ms + 120_000:
            continue
        closed_pnl = float(rec.get("closedPnl") or 0)
        bybit_ts = datetime.fromtimestamp(updated_ms / 1000.0, tz=timezone.utc).isoformat()
        if want_side == "Sell":
            resolved = "TP_hit" if avg_exit < entry else "SL_hit"
        else:
            resolved = "TP_hit" if avg_exit > entry else "SL_hit"
        if abs(avg_exit - tp) / max(tp, 1e-9) < 0.005:
            resolved = "TP_hit"
        elif abs(avg_exit - sl) / max(sl, 1e-9) < 0.005:
            resolved = "SL_hit"
        return (resolved, avg_entry, avg_exit, closed_pnl, bybit_ts, "closed_pnl_match")
    return ("unresolved", 0.0, 0.0, 0.0, "", "no_match")


def _get_broker():
    """Get live broker for Bybit API. Returns None if not configured."""
    try:
        from trading.broker import get_broker
        return get_broker("live", dry_run_live=False)
    except Exception:
        return None


def run_audit(
    root: Path,
    output_path: Path | None = None,
    use_bybit: bool = True,
) -> dict:
    """
    Run audit. Returns summary dict.
    If use_bybit=True and broker available, queries Bybit. Else marks all as unresolved.
    """
    root = Path(root).resolve()
    candidates = [root, root / "datasets"]
    base = root
    for c in candidates:
        if c.exists() and list(c.glob("date=*")):
            base = c
            break

    files = _find_live_outcome_files(base)
    if not files:
        files = _find_live_outcome_files(root)

    broker = _get_broker() if use_bybit else None
    if use_bybit and broker is None:
        print("WARN: Bybit broker not configured (BYBIT_API_KEY etc); resolution will be unresolved", file=sys.stderr)

    total_live = 0
    total_timeout = 0
    resolved_sl = 0
    resolved_tp = 0
    unresolved = 0
    by_strategy: dict[str, dict] = defaultdict(lambda: {"total": 0, "timeout": 0, "sl": 0, "tp": 0, "unresolved": 0})
    by_date: dict[str, dict] = defaultdict(lambda: {"total": 0, "timeout": 0, "sl": 0, "tp": 0, "unresolved": 0})
    by_symbol: dict[str, dict] = defaultdict(lambda: {"total": 0, "timeout": 0, "sl": 0, "tp": 0, "unresolved": 0})
    audit_rows: list[dict] = []

    for outcomes_path, date_val, strategy in files:
        rows = _read_csv(outcomes_path)
        trades = _load_trades_for_outcomes(outcomes_path)
        for row in rows:
            if not _is_live(row):
                continue
            total_live += 1
            symbol = (row.get("symbol") or "").strip()
            by_strategy[strategy]["total"] += 1
            by_date[date_val]["total"] += 1
            by_symbol[symbol]["total"] += 1
            if not _is_timeout(row):
                continue
            total_timeout += 1
            by_strategy[strategy]["timeout"] += 1
            by_date[date_val]["timeout"] += 1
            by_symbol[symbol]["timeout"] += 1

            trade_id = (row.get("trade_id") or "").strip()
            event_id = (row.get("event_id") or row.get("eventId") or "").strip()
            run_id = (row.get("run_id") or "").strip()
            timeout_ts = (row.get("outcome_time_utc") or "").strip()
            timeout_ms = _parse_ts_ms(timeout_ts)
            trade = trades.get(trade_id, {})
            entry = float(trade.get("entry_price") or row.get("entry_price") or 0)
            tp = float(trade.get("tp_price") or 0)
            sl = float(trade.get("sl_price") or 0)
            opened_ts = (trade.get("entry_time_utc") or "").strip()
            opened_ms = _parse_ts_ms(opened_ts)
            side = (row.get("side") or trade.get("side") or "SHORT").strip()

            order_id = trade.get("order_id", "")
            position_idx = trade.get("position_idx", "")

            resolved = "unresolved"
            bybit_entry = 0.0
            bybit_exit = 0.0
            bybit_pnl = 0.0
            bybit_ts = ""
            reason = "no_bybit"
            confidence = ""

            if broker and entry > 0 and tp > 0 and sl > 0 and opened_ms:
                resolved, bybit_entry, bybit_exit, bybit_pnl, bybit_ts, reason = _match_bybit_closed_pnl(
                    broker, symbol, side, entry, tp, sl, opened_ms, timeout_ms or int(1e15),
                )
                if resolved == "SL_hit":
                    resolved_sl += 1
                    by_strategy[strategy]["sl"] += 1
                    by_date[date_val]["sl"] += 1
                    by_symbol[symbol]["sl"] += 1
                    confidence = "high" if reason == "closed_pnl_match" else "medium"
                elif resolved == "TP_hit":
                    resolved_tp += 1
                    by_strategy[strategy]["tp"] += 1
                    by_date[date_val]["tp"] += 1
                    by_symbol[symbol]["tp"] += 1
                    confidence = "high" if reason == "closed_pnl_match" else "medium"
                else:
                    unresolved += 1
                    by_strategy[strategy]["unresolved"] += 1
                    by_date[date_val]["unresolved"] += 1
                    by_symbol[symbol]["unresolved"] += 1
            else:
                unresolved += 1
                by_strategy[strategy]["unresolved"] += 1
                by_date[date_val]["unresolved"] += 1
                by_symbol[symbol]["unresolved"] += 1
                if not broker:
                    reason = "no_broker"
                elif entry <= 0 or not opened_ms:
                    reason = "missing_entry_or_opened_ts"

            audit_rows.append({
                "date": date_val,
                "strategy": strategy,
                "symbol": symbol,
                "run_id": run_id,
                "event_id": event_id,
                "trade_id": trade_id,
                "order_id": str(order_id) if order_id else "",
                "position_idx": str(position_idx) if position_idx else "",
                "opened_ts": opened_ts,
                "timeout_recorded_ts": timeout_ts,
                "entry_state": f"{entry:.6f}" if entry else "",
                "tp_state": f"{tp:.6f}" if tp else "",
                "sl_state": f"{sl:.6f}" if sl else "",
                "bybit_entry": f"{bybit_entry:.6f}" if bybit_entry else "",
                "bybit_exit": f"{bybit_exit:.6f}" if bybit_exit else "",
                "bybit_closed_pnl": f"{bybit_pnl:.4f}" if bybit_pnl else "",
                "bybit_closed_ts": bybit_ts,
                "resolved_outcome": resolved,
                "confidence": confidence,
                "reason": reason,
            })

    out_path = output_path or Path("artifacts") / "live_timeout_reconciliation_audit.csv"
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=AUDIT_COLUMNS, extrasaction="ignore")
        w.writeheader()
        w.writerows(audit_rows)

    summary = {
        "total_live_outcomes": total_live,
        "total_timeout": total_timeout,
        "timeout_resolved_sl": resolved_sl,
        "timeout_resolved_tp": resolved_tp,
        "timeout_unresolved": unresolved,
        "by_strategy": dict(by_strategy),
        "by_date": dict(by_date),
        "by_symbol": dict(by_symbol),
        "audit_csv": str(out_path),
    }
    return summary


def _print_summary(s: dict) -> None:
    print("\n--- Live Timeout Reconciliation Audit ---")
    print(f"  total_live_outcomes:     {s['total_live_outcomes']}")
    print(f"  total_TIMEOUT:           {s['total_timeout']}")
    print(f"  timeout → SL_hit:        {s['timeout_resolved_sl']}")
    print(f"  timeout → TP_hit:        {s['timeout_resolved_tp']}")
    print(f"  timeout unresolved:      {s['timeout_unresolved']}")
    print(f"\n  audit_csv: {s['audit_csv']}")
    print("\n--- By strategy ---")
    for k, v in sorted(s["by_strategy"].items()):
        if v["timeout"]:
            print(f"  {k}: timeout={v['timeout']} sl={v['sl']} tp={v['tp']} unresolved={v['unresolved']}")
    print("\n--- By date ---")
    for k in sorted(s["by_date"].keys(), reverse=True)[:15]:
        v = s["by_date"][k]
        if v["timeout"]:
            print(f"  {k}: timeout={v['timeout']} sl={v['sl']} tp={v['tp']} unresolved={v['unresolved']}")
    if len(s["by_date"]) > 15:
        print("  ...")
    print("\n--- By symbol ---")
    for k in sorted(s["by_symbol"].keys(), key=lambda x: -s["by_symbol"][x]["timeout"])[:10]:
        v = s["by_symbol"][k]
        if v["timeout"]:
            print(f"  {k}: timeout={v['timeout']} sl={v['sl']} tp={v['tp']} unresolved={v['unresolved']}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Audit live TIMEOUT outcomes vs Bybit closed-pnl (no data modification)"
    )
    parser.add_argument("--root", type=Path, default=Path(DATASET_BASE_DIR), help="Datasets root")
    parser.add_argument("--output", "-o", type=Path, default=None, help="Output CSV path")
    parser.add_argument("--no-bybit", action="store_true", help="Skip Bybit API calls")
    args = parser.parse_args()
    summary = run_audit(args.root, output_path=args.output, use_bybit=not args.no_bybit)
    _print_summary(summary)
    return 0


if __name__ == "__main__":
    sys.exit(main())
