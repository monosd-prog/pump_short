#!/usr/bin/env python3
"""
Audit: Reconcile live TIMEOUT outcomes using Bybit CSV export (offline, no API).

Uses CSV with columns:
  Market, Filled Type, Filled Quantity, Filled Price, Order Price, Fee Rate, Trading Fee,
  feeCoin, ExecFeeV2, Direction, Order Type, Transaction ID, Order No., Transaction Time(UTC+0)

Logic:
  - SHORT rows = candidate entry
  - LONG rows = candidate exit (for SHORT positions)
  - Match by symbol + time window near opened_ts/timeout_ts
  - Aggregate partial fills by Order No. (weighted avg exit price)
  - Classify as SL_hit or TP_hit from exit_price vs tp/sl/entry

Audit-only / preview mode. No data modification.

Usage:
  python3 scripts/audit_timeout_reconciliation_from_bybit_csv.py --root /root/pump_short --csv /path/to/bybit_trades.csv
  python3 scripts/audit_timeout_reconciliation_from_bybit_csv.py --root . --csv bybit.csv --output artifacts/reconciliation.csv
"""
from __future__ import annotations

import argparse
import csv
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from trading.config import DATASET_BASE_DIR

OUTPUT_COLUMNS = [
    "date",
    "strategy",
    "symbol",
    "run_id",
    "event_id",
    "opened_ts",
    "timeout_recorded_ts",
    "entry_state",
    "tp_state",
    "sl_state",
    "csv_entry_time",
    "csv_entry_order_no",
    "csv_entry_price",
    "csv_entry_qty",
    "csv_exit_time",
    "csv_exit_order_no",
    "csv_exit_price",
    "csv_exit_qty",
    "resolved_outcome",
    "confidence",
    "reason",
]

_TIME_WINDOW_START_SEC = 5 * 60   # 5 min before opened
_TIME_WINDOW_END_SEC = 20 * 60    # 20 min after timeout (allow late CSV export)
_ENTRY_TOLERANCE_PCT = 0.05       # 5% match tolerance for entry price
_TP_SL_NEAR_PCT = 0.005           # 0.5% to consider exit at TP/SL


def _normalize_header(h: str) -> str:
    return h.strip().replace(" ", "_").lower().replace("(", "").replace(")", "")


def _parse_bybit_time(s: str) -> Optional[datetime]:
    """
    Parse Bybit CSV time. Supports:
      - 19:05 2026-03-10
      - 19:05:00 2026-03-10
      - 2026-03-10 19:05
      - 2026-03-10 19:05:00
    Returns UTC-aware datetime or None.
    """
    if not s or not str(s).strip():
        return None
    ss = str(s).strip()
    for fmt in (
        "%H:%M %Y-%m-%d",
        "%H:%M:%S %Y-%m-%d",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
    ):
        try:
            dt = datetime.strptime(ss, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue
    return None


def _safe_float(val: Any) -> float:
    """Parse float, support scientific notation."""
    if val is None or val == "":
        return 0.0
    s = str(val).strip()
    if not s:
        return 0.0
    try:
        return float(Decimal(s))
    except Exception:
        return 0.0


def _parse_ts_ms(s: str) -> Optional[int]:
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
    with open(path, "r", newline="", encoding="utf-8", errors="replace") as f:
        r = csv.DictReader(f, skipinitialspace=True)
        return list(r)


def _load_bybit_csv(path: Path) -> list[dict]:
    """
    Load Bybit CSV with flexible header matching. Normalize keys for robustness.
    Expected columns: Market, Direction, Order No., Filled Price, Filled Quantity, Transaction Time(UTC+0)
    """
    rows = _read_csv(path)
    if not rows:
        return []

    header_map: dict[str, str] = {}
    raw_headers = list(rows[0].keys())
    norm_to_raw: dict[str, str] = {}
    for h in raw_headers:
        n = _normalize_header(h)
        norm_to_raw[n] = h

    def _find_col(keys: list[str]) -> Optional[str]:
        for k in keys:
            if k in norm_to_raw:
                return norm_to_raw[k]
        for k in list(norm_to_raw.keys()):
            if any(a in k for a in keys):
                return norm_to_raw[k]
        return None

    market_col = _find_col(["market"])
    direction_col = _find_col(["direction", "side"])
    order_no_col = _find_col(["order_no", "order_no.", "orderno"])
    price_col = _find_col(["filled_price", "filledprice"])
    qty_col = _find_col(["filled_quantity", "filledquantity", "filled_qty"])
    time_col = _find_col(["transaction_time", "transaction_timeutc", "transaction", "time"])

    if not all([market_col, direction_col, price_col, qty_col, time_col]):
        raise ValueError(
            f"Bybit CSV must have Market, Direction, Filled Price, Filled Quantity, Transaction Time. "
            f"Found: {list(raw_headers)[:15]}"
        )

    out: list[dict] = []
    for r in rows:
        symbol = str(r.get(market_col, "")).strip()
        direction = str(r.get(direction_col, "")).strip().upper()
        order_no = str(r.get(order_no_col, "")).strip() if order_no_col else ""
        price = _safe_float(r.get(price_col))
        qty = _safe_float(r.get(qty_col))
        ts_str = str(r.get(time_col, "")).strip()
        dt = _parse_bybit_time(ts_str)
        if not symbol or price <= 0 or qty <= 0 or not dt:
            continue
        out.append({
            "symbol": symbol,
            "direction": direction,
            "order_no": order_no,
            "price": price,
            "qty": qty,
            "ts": dt,
            "ts_ms": int(dt.timestamp() * 1000),
        })
    return out


def _load_trades_for_outcomes(outcomes_path: Path) -> dict[str, dict]:
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


@dataclass
class CsvFill:
    price: float
    qty: float
    ts: datetime
    ts_ms: int
    order_no: str


def _aggregate_fills_by_order(rows: list[dict]) -> dict[str, CsvFill]:
    """Group rows by order_no, compute weighted avg price and total qty."""
    by_order: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        ono = r.get("order_no", "") or ""
        by_order[ono].append(r)

    result: dict[str, CsvFill] = {}
    for order_no, grp in by_order.items():
        total_qty = sum(r["qty"] for r in grp)
        if total_qty <= 0:
            continue
        wavg_price = sum(r["price"] * r["qty"] for r in grp) / total_qty
        latest = max(grp, key=lambda x: x["ts_ms"])
        result[order_no] = CsvFill(
            price=wavg_price,
            qty=total_qty,
            ts=latest["ts"],
            ts_ms=latest["ts_ms"],
            order_no=order_no,
        )
    return result


def _match_timeout_via_csv(
    symbol: str,
    entry: float,
    tp: float,
    sl: float,
    opened_ms: int,
    timeout_ms: int,
    csv_rows: list[dict],
) -> tuple[str, Optional[CsvFill], Optional[CsvFill], str]:
    """
    Match timeout deal with CSV data.
    Returns (resolved_outcome, entry_fill, exit_fill, reason).
    resolved_outcome: SL_hit | TP_hit | unresolved | ambiguous_csv_match | no_csv_match
    """
    if not symbol or entry <= 0 or tp <= 0 or sl <= 0 or not opened_ms:
        return ("unresolved", None, None, "missing_params")
    if not csv_rows:
        return ("no_csv_match", None, None, "no_csv_match")

    start_ms = opened_ms - _TIME_WINDOW_START_SEC * 1000
    end_ms = (timeout_ms or opened_ms + 3600_000) + _TIME_WINDOW_END_SEC * 1000

    symbol_rows = [r for r in csv_rows if (r.get("symbol") or "").strip() == symbol.strip()]
    if not symbol_rows:
        return ("no_csv_match", None, None, "no_csv_match_symbol")

    short_rows = [r for r in symbol_rows if r.get("direction") in ("SELL", "SHORT")]
    long_rows = [r for r in symbol_rows if r.get("direction") in ("BUY", "LONG")]

    short_agg = _aggregate_fills_by_order(short_rows)
    long_agg = _aggregate_fills_by_order(long_rows)

    entry_candidates: list[CsvFill] = []
    for fill in short_agg.values():
        if start_ms <= fill.ts_ms <= end_ms:
            if abs(fill.price - entry) / max(entry, 1e-9) <= _ENTRY_TOLERANCE_PCT:
                entry_candidates.append(fill)

    exit_candidates: list[CsvFill] = []
    for fill in long_agg.values():
        if opened_ms <= fill.ts_ms <= end_ms:
            exit_candidates.append(fill)

    if len(entry_candidates) > 1 or len(exit_candidates) > 1:
        return ("ambiguous_csv_match", None, None, "ambiguous_csv_match")

    if not entry_candidates and not exit_candidates:
        return ("no_csv_match", None, None, "no_csv_match")

    if not entry_candidates:
        entry_fill = None
        if len(exit_candidates) == 1:
            exit_fill = exit_candidates[0]
            exit_price = exit_fill.price
            want_side = "Sell"  # SHORT positions
            resolved = "TP_hit" if exit_price < entry else "SL_hit"
            if abs(exit_price - tp) / max(tp, 1e-9) < _TP_SL_NEAR_PCT:
                resolved = "TP_hit"
            elif abs(exit_price - sl) / max(sl, 1e-9) < _TP_SL_NEAR_PCT:
                resolved = "SL_hit"
            return (resolved, None, exit_fill, "csv_exit_only_no_entry_match")
        return ("no_csv_match", None, None, "no_entry_match")

    entry_fill = entry_candidates[0]
    if not exit_candidates:
        return ("no_csv_match", entry_fill, None, "no_exit_match")

    exit_fill = exit_candidates[0]
    exit_price = exit_fill.price
    want_side = "Sell"
    resolved = "TP_hit" if exit_price < entry else "SL_hit"
    if abs(exit_price - tp) / max(tp, 1e-9) < _TP_SL_NEAR_PCT:
        resolved = "TP_hit"
    elif abs(exit_price - sl) / max(sl, 1e-9) < _TP_SL_NEAR_PCT:
        resolved = "SL_hit"
    return (resolved, entry_fill, exit_fill, "csv_match")


def run_audit(
    root: Path,
    csv_path: Path,
    output_path: Optional[Path] = None,
) -> dict:
    root = Path(root).resolve()
    csv_path = Path(csv_path).resolve()
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    candidates = [root, root / "datasets"]
    base = root
    for c in candidates:
        if c.exists() and list(c.glob("date=*")):
            base = c
            break

    files = _find_live_outcome_files(base)
    if not files:
        files = _find_live_outcome_files(root)

    csv_rows = _load_bybit_csv(csv_path)

    total_timeout = 0
    resolved_sl = 0
    resolved_tp = 0
    unresolved = 0
    ambiguous = 0
    no_match = 0
    reason_counts: dict[str, int] = defaultdict(int)
    by_strategy: dict[str, dict] = defaultdict(
        lambda: {"timeout": 0, "sl": 0, "tp": 0, "unresolved": 0, "ambiguous": 0, "no_match": 0}
    )
    by_date: dict[str, dict] = defaultdict(
        lambda: {"timeout": 0, "sl": 0, "tp": 0, "unresolved": 0, "ambiguous": 0, "no_match": 0}
    )
    audit_rows: list[dict] = []

    for outcomes_path, date_val, strategy in files:
        rows = _read_csv(outcomes_path)
        trades = _load_trades_for_outcomes(outcomes_path)
        for row in rows:
            if not _is_live(row):
                continue
            if not _is_timeout(row):
                continue
            total_timeout += 1
            by_strategy[strategy]["timeout"] += 1
            by_date[date_val]["timeout"] += 1

            trade_id = (row.get("trade_id") or "").strip()
            event_id = (row.get("event_id") or row.get("eventId") or "").strip()
            run_id = (row.get("run_id") or "").strip()
            timeout_ts = (row.get("outcome_time_utc") or "").strip()
            timeout_ms = _parse_ts_ms(timeout_ts)
            trade = trades.get(trade_id, {})
            entry = _safe_float(trade.get("entry_price") or row.get("entry_price") or row.get("entry"))
            tp = _safe_float(trade.get("tp_price") or row.get("tp_price") or row.get("tp"))
            sl = _safe_float(trade.get("sl_price") or row.get("sl_price") or row.get("sl"))
            opened_ts = (trade.get("entry_time_utc") or "").strip()
            opened_ms = _parse_ts_ms(opened_ts) or 0
            symbol = (row.get("symbol") or trade.get("symbol") or "").strip()

            resolved, entry_fill, exit_fill, reason = _match_timeout_via_csv(
                symbol, entry, tp, sl, opened_ms, timeout_ms or 0, csv_rows,
            )

            reason_counts[reason] += 1
            if resolved == "SL_hit":
                resolved_sl += 1
                by_strategy[strategy]["sl"] += 1
                by_date[date_val]["sl"] += 1
            elif resolved == "TP_hit":
                resolved_tp += 1
                by_strategy[strategy]["tp"] += 1
                by_date[date_val]["tp"] += 1
            elif resolved == "ambiguous_csv_match":
                ambiguous += 1
                by_strategy[strategy]["ambiguous"] += 1
                by_date[date_val]["ambiguous"] += 1
            elif resolved == "no_csv_match":
                no_match += 1
                by_strategy[strategy]["no_match"] += 1
                by_date[date_val]["no_match"] += 1
            else:
                unresolved += 1
                by_strategy[strategy]["unresolved"] += 1
                by_date[date_val]["unresolved"] += 1

            confidence = "high" if resolved in ("SL_hit", "TP_hit") and reason == "csv_match" else (
                "medium" if resolved in ("SL_hit", "TP_hit") else ""
            )

            def _fmt_fill(f: Optional[CsvFill]) -> tuple[str, str, str, str]:
                if not f:
                    return "", "", "", ""
                return (
                    f.ts.strftime("%Y-%m-%d %H:%M:%S+00:00"),
                    f.order_no or "",
                    f"{f.price:.6f}" if f.price else "",
                    f"{f.qty:.6f}" if f.qty else "",
                )

            e_time, e_ono, e_price, e_qty = _fmt_fill(entry_fill)
            x_time, x_ono, x_price, x_qty = _fmt_fill(exit_fill)

            audit_rows.append({
                "date": date_val,
                "strategy": strategy,
                "symbol": symbol,
                "run_id": run_id,
                "event_id": event_id,
                "opened_ts": opened_ts,
                "timeout_recorded_ts": timeout_ts,
                "entry_state": f"{entry:.6f}" if entry else "",
                "tp_state": f"{tp:.6f}" if tp else "",
                "sl_state": f"{sl:.6f}" if sl else "",
                "csv_entry_time": e_time,
                "csv_entry_order_no": e_ono,
                "csv_entry_price": e_price,
                "csv_entry_qty": e_qty,
                "csv_exit_time": x_time,
                "csv_exit_order_no": x_ono,
                "csv_exit_price": x_price,
                "csv_exit_qty": x_qty,
                "resolved_outcome": resolved,
                "confidence": confidence,
                "reason": reason,
            })

    out_path = output_path or Path("artifacts") / "live_timeout_reconciliation_from_csv.csv"
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS, extrasaction="ignore")
        w.writeheader()
        w.writerows(audit_rows)

    return {
        "total_timeout": total_timeout,
        "resolved_sl": resolved_sl,
        "resolved_tp": resolved_tp,
        "unresolved": unresolved,
        "ambiguous": ambiguous,
        "no_match": no_match,
        "reason_counts": dict(reason_counts),
        "by_strategy": dict(by_strategy),
        "by_date": dict(by_date),
        "audit_csv": str(out_path),
        "csv_rows_loaded": len(csv_rows),
    }


def _print_summary(s: dict) -> None:
    print("\n--- Live Timeout Reconciliation (Bybit CSV) ---")
    print(f"  CSV rows loaded:        {s.get('csv_rows_loaded', 0)}")
    print(f"  total timeouts checked: {s['total_timeout']}")
    print(f"  resolved via CSV SL_hit: {s['resolved_sl']}")
    print(f"  resolved via CSV TP_hit: {s['resolved_tp']}")
    print(f"  unresolved:             {s['unresolved']}")
    print(f"  ambiguous_csv_match:    {s['ambiguous']}")
    print(f"  no_csv_match:           {s['no_match']}")
    print(f"\n  audit_csv: {s['audit_csv']}")
    rc = s.get("reason_counts") or {}
    if rc:
        print("\n--- Reason breakdown ---")
        for k in sorted(rc.keys(), key=lambda x: -rc[x]):
            print(f"  {k}: {rc[k]}")
    print("\n--- By strategy ---")
    for k in sorted(s.get("by_strategy", {}).keys()):
        v = s["by_strategy"][k]
        if v.get("timeout", 0):
            print(
                f"  {k}: timeout={v['timeout']} sl={v['sl']} tp={v['tp']} "
                f"unresolved={v['unresolved']} ambiguous={v['ambiguous']} no_match={v['no_match']}"
            )
    print("\n--- By date ---")
    for k in sorted(s.get("by_date", {}).keys(), reverse=True)[:15]:
        v = s["by_date"][k]
        if v.get("timeout", 0):
            print(
                f"  {k}: timeout={v['timeout']} sl={v['sl']} tp={v['tp']} "
                f"unresolved={v['unresolved']} ambiguous={v['ambiguous']} no_match={v['no_match']}"
            )
    if len(s.get("by_date", {})) > 15:
        print("  ...")
    print()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Reconcile live TIMEOUT outcomes using Bybit CSV export (audit-only)"
    )
    parser.add_argument("--root", type=Path, default=Path(DATASET_BASE_DIR), help="Datasets root")
    parser.add_argument("--csv", "-c", type=Path, required=True, help="Path to Bybit CSV export")
    parser.add_argument("--output", "-o", type=Path, default=None, help="Output CSV path")
    args = parser.parse_args()
    try:
        summary = run_audit(args.root, args.csv, output_path=args.output)
        _print_summary(summary)
        return 0
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
