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
import os
import sys
from typing import Any
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

# Project root for imports
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _load_env_file_manual(path: Path) -> bool:
    """Simple key=value parser when dotenv not available. Skips if key already set."""
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
        for line in text.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                k, v = line.split("=", 1)
                k = k.strip()
                v = v.strip().strip('"').strip("'")
                if k and k not in os.environ:
                    os.environ[k] = v
        return True
    except Exception:
        return False


def _load_env(root: Path | None = None) -> tuple[bool, str]:
    """
    Load env from .env files (BYBIT_API_KEY etc). Must run before get_broker.
    Returns (loaded_any, source_path). Tries: root/.env, project/.env, /root/pump_short/.env, /etc/pump-short-live.env.
    Uses python-dotenv if available, else simple key=value parser.
    """
    root = Path(root) if root else _ROOT
    candidates = [
        root / ".env",
        _ROOT / ".env",
        Path("/root/pump_short/.env"),
        Path("/etc/pump-short-live.env"),
    ]
    loader = None
    try:
        from dotenv import load_dotenv
        loader = lambda p: load_dotenv(p, override=False)
    except ImportError:
        loader = _load_env_file_manual
    for p in candidates:
        if p.exists() and p.is_file():
            loader(p)
            return True, str(p)
    return False, "no_env_file_found"


def _check_bybit_env() -> tuple[bool, str]:
    """Check if BYBIT_API_KEY and BYBIT_API_SECRET are set. Returns (ok, reason)."""
    key = (os.getenv("BYBIT_API_KEY") or "").strip()
    secret = (os.getenv("BYBIT_API_SECRET") or "").strip()
    if not key:
        return False, "BYBIT_API_KEY empty or unset"
    if not secret:
        return False, "BYBIT_API_SECRET empty or unset"
    return True, "ok"


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
    "history_source",
    "fallback_endpoint",
    "fallback_match_reason",
]

_ENTRY_TOLERANCE_STRICT_PCT = 0.025   # 2.5%
_ENTRY_TOLERANCE_RELAXED_PCT = 0.05   # 5%
_ENTRY_TOLERANCE_PLAUSIBLE_PCT = 0.10  # 10% nearest match
_TIME_WINDOW_START_MS = 5 * 60 * 1000   # 5 min before opened
_TIME_WINDOW_END_MS = 15 * 60 * 1000    # 15 min after timeout


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
    Query Bybit closed-pnl (all pages), executions fallback. Returns (resolved_outcome, bybit_entry, bybit_exit, bybit_pnl, bybit_ts, reason).
    Uses widened time window, relaxed entry tolerance, and granular reason codes for unresolved.
    """
    if not broker or not symbol or entry <= 0 or opened_ms is None:
        return ("unresolved", 0.0, 0.0, 0.0, "", "missing_params")
    want_side = "Sell" if (side or "").strip().upper() in ("SHORT", "SELL") else "Buy"
    start_ms = opened_ms - _TIME_WINDOW_START_MS
    end_ms = (timeout_ms or opened_ms + 3600_000) + _TIME_WINDOW_END_MS  # 15 min after timeout
    if end_ms - start_ms > 7 * 24 * 3600 * 1000:
        end_ms = start_ms + 7 * 24 * 3600 * 1000 - 1000  # Bybit limit 7 days

    records: list[dict] = []
    fetch_method = "closed_pnl"
    try:
        if hasattr(broker, "get_closed_pnl_all_pages"):
            records = broker.get_closed_pnl_all_pages(
                symbol,
                start_time_ms=start_ms,
                end_time_ms=end_ms,
                limit=100,
                max_pages=5,
                raise_on_network_error=False,
            )
        else:
            records = broker.get_closed_pnl(
                symbol,
                start_time_ms=start_ms,
                end_time_ms=end_ms,
                limit=100,
                raise_on_network_error=False,
            )
    except Exception as e:
        return ("unresolved", 0.0, 0.0, 0.0, "", f"api_error:{type(e).__name__}")

    if not records:
        return ("unresolved", 0.0, 0.0, 0.0, "", "no_closed_pnl_rows")

    same_side = [r for r in records if r.get("side") == want_side]
    if not same_side:
        return ("unresolved", 0.0, 0.0, 0.0, "", "closed_pnl_side_mismatch")

    in_time: list[dict] = []
    for r in same_side:
        um = int(r.get("updatedTime") or 0)
        if um >= opened_ms and um <= end_ms:
            in_time.append(r)
    if not in_time:
        return ("unresolved", 0.0, 0.0, 0.0, "", "outside_time_window")

    for tol_pct, tol_name in [
        (_ENTRY_TOLERANCE_STRICT_PCT, "closed_pnl_match"),
        (_ENTRY_TOLERANCE_RELAXED_PCT, "closed_pnl_match_relaxed"),
        (_ENTRY_TOLERANCE_PLAUSIBLE_PCT, "closed_pnl_match_plausible"),
    ]:
        matches: list[tuple[dict, float]] = []
        for r in in_time:
            ae = float(r.get("avgEntryPrice") or 0)
            ax = float(r.get("avgExitPrice") or 0)
            if ae <= 0 or ax <= 0:
                continue
            if abs(ae - entry) / max(entry, 1e-9) <= tol_pct:
                matches.append((r, ae))

        if len(matches) == 1:
            r, _ = matches[0]
            closed_pnl = float(r.get("closedPnl") or 0)
            avg_exit = float(r.get("avgExitPrice") or 0)
            avg_entry = float(r.get("avgEntryPrice") or 0)
            um = int(r.get("updatedTime") or 0)
            bybit_ts = datetime.fromtimestamp(um / 1000.0, tz=timezone.utc).isoformat()
            if want_side == "Sell":
                resolved = "TP_hit" if avg_exit < entry else "SL_hit"
            else:
                resolved = "TP_hit" if avg_exit > entry else "SL_hit"
            if abs(avg_exit - tp) / max(tp, 1e-9) < 0.005:
                resolved = "TP_hit"
            elif abs(avg_exit - sl) / max(sl, 1e-9) < 0.005:
                resolved = "SL_hit"
            return (resolved, avg_entry, avg_exit, closed_pnl, bybit_ts, tol_name)
        if len(matches) > 1:
            return ("unresolved", 0.0, 0.0, 0.0, "", "multiple_candidates_ambiguous")

    best: dict | None = None
    best_diff = float("inf")
    for r in in_time:
        ae = float(r.get("avgEntryPrice") or 0)
        if ae <= 0:
            continue
        diff = abs(ae - entry) / max(entry, 1e-9)
        if diff < best_diff and diff <= 0.15:
            best_diff = diff
            best = r
    if best:
        return (
            "unresolved", 0.0, 0.0, 0.0, "",
            f"closed_pnl_entry_mismatch_nearest={best_diff*100:.2f}pct",
        )
    return ("unresolved", 0.0, 0.0, 0.0, "", "closed_pnl_entry_mismatch")


def _match_bybit_fallback(
    broker,
    symbol: str,
    side: str,
    entry: float,
    tp: float,
    sl: float,
    opened_ms: int,
    timeout_ms: int,
) -> tuple[str, float, float, float, str, str, str, str, str]:
    """
    Fallback for no_closed_pnl_rows: try order history and execution list.
    Returns (resolved_outcome, bybit_entry, bybit_exit, bybit_pnl, bybit_ts, reason,
             history_source, fallback_endpoint, fallback_match_reason).
    """
    want_side = "Sell" if (side or "").strip().upper() in ("SHORT", "SELL") else "Buy"
    close_side = "Buy" if want_side == "Sell" else "Sell"  # close SHORT = Buy
    start_ms = opened_ms - _TIME_WINDOW_START_MS
    end_ms = (timeout_ms or opened_ms + 3600_000) + _TIME_WINDOW_END_MS
    if end_ms - start_ms > 7 * 24 * 3600 * 1000:
        end_ms = start_ms + 7 * 24 * 3600 * 1000 - 1000

    def _classify_exit(avg_exit: float) -> str:
        if want_side == "Sell":
            return "TP_hit" if avg_exit < entry else "SL_hit"
        return "TP_hit" if avg_exit > entry else "SL_hit"

    if hasattr(broker, "get_order_history"):
        try:
            orders = broker.get_order_history(
                symbol,
                start_time_ms=start_ms,
                end_time_ms=end_ms,
                order_status=None,
                limit=50,
                max_pages=5,
                raise_on_network_error=False,
            )
            reduce_only = [
                o for o in orders
                if (o.get("reduceOnly") in (True, "true", "True", 1, "1"))
                and o.get("side") == close_side
            ]
            in_time = []
            for o in reduce_only:
                um = int(o.get("updatedTime") or o.get("createdTime") or 0)
                if um >= opened_ms and um <= end_ms:
                    ap = float(o.get("avgPrice") or 0)
                    if ap > 0:
                        in_time.append((o, ap, um))
            if len(in_time) == 1:
                o, avg_exit, um = in_time[0]
                bybit_ts = datetime.fromtimestamp(um / 1000.0, tz=timezone.utc).isoformat()
                resolved = _classify_exit(avg_exit)
                if abs(avg_exit - tp) / max(tp, 1e-9) < 0.005:
                    resolved = "TP_hit"
                elif abs(avg_exit - sl) / max(sl, 1e-9) < 0.005:
                    resolved = "SL_hit"
                return (
                    resolved, entry, avg_exit, 0.0, bybit_ts,
                    "order_history_match",
                    "order_history",
                    "/v5/order/history",
                    "reduceOnly_order_avgPrice",
                )
            if len(in_time) > 1:
                return (
                    "unresolved", 0.0, 0.0, 0.0, "",
                    "execution_rows_found_ambiguous",
                    "order_history",
                    "/v5/order/history",
                    "multiple_reduceOnly_orders",
                )
            if reduce_only:
                return (
                    "unresolved", 0.0, 0.0, 0.0, "",
                    "outside_time_window",
                    "order_history",
                    "/v5/order/history",
                    "reduceOnly_orders_outside_window",
                )
            return (
                "unresolved", 0.0, 0.0, 0.0, "",
                "order_history_no_reduceOnly",
                "order_history",
                "/v5/order/history",
                "no_filled_reduceOnly",
            )
        except Exception as e:
            return (
                "unresolved", 0.0, 0.0, 0.0, "",
                f"order_history_api_error:{type(e).__name__}",
                "order_history",
                "/v5/order/history",
                str(e),
            )

    if hasattr(broker, "get_executions_all_pages"):
        try:
            execs = broker.get_executions_all_pages(
                symbol,
                start_time_ms=start_ms,
                end_time_ms=end_ms,
                limit=100,
                max_pages=5,
                raise_on_network_error=False,
            )
            closing: list[dict] = []
            for ex in execs:
                cs = ex.get("closedSize")
                try:
                    cs_val = float(cs) if cs else 0
                except (TypeError, ValueError):
                    cs_val = 0
                if cs_val > 0 and ex.get("side") == close_side:
                    ep = float(ex.get("execPrice") or 0)
                    eq = float(ex.get("execQty") or ex.get("closedSize") or 0)
                    try:
                        eq = float(eq) if eq else 0
                    except (TypeError, ValueError):
                        eq = 0
                    et = int(ex.get("execTime") or 0)
                    if ep > 0 and et >= opened_ms and et <= end_ms:
                        closing.append({
                            "execPrice": ep, "execQty": eq, "execTime": et,
                            "orderId": ex.get("orderId", ""),
                        })
            if not closing:
                return (
                    "unresolved", 0.0, 0.0, 0.0, "",
                    "execution_list_no_closing_fills",
                    "execution_list",
                    "/v5/execution/list",
                    "no_closedSize_side_match",
                )
            total_qty = sum(c["execQty"] for c in closing)
            if total_qty <= 0:
                return (
                    "unresolved", 0.0, 0.0, 0.0, "",
                    "execution_rows_found_ambiguous",
                    "execution_list",
                    "/v5/execution/list",
                    "closing_fills_zero_qty",
                )
            avg_exit = sum(c["execPrice"] * c["execQty"] for c in closing) / total_qty
            latest_et = max(c["execTime"] for c in closing)
            bybit_ts = datetime.fromtimestamp(latest_et / 1000.0, tz=timezone.utc).isoformat()
            resolved = _classify_exit(avg_exit)
            if abs(avg_exit - tp) / max(tp, 1e-9) < 0.005:
                resolved = "TP_hit"
            elif abs(avg_exit - sl) / max(sl, 1e-9) < 0.005:
                resolved = "SL_hit"
            if len(closing) == 1:
                return (
                    resolved, entry, avg_exit, 0.0, bybit_ts,
                    "execution_list_match",
                    "execution_list",
                    "/v5/execution/list",
                    "single_closing_fill",
                )
            return (
                resolved, entry, avg_exit, 0.0, bybit_ts,
                "execution_list_match",
                "execution_list",
                "/v5/execution/list",
                "closedSize_weighted_avg",
            )
        except Exception as e:
            return (
                "unresolved", 0.0, 0.0, 0.0, "",
                f"execution_list_api_error:{type(e).__name__}",
                "execution_list",
                "/v5/execution/list",
                str(e),
            )

    return (
        "unresolved", 0.0, 0.0, 0.0, "",
        "bybit_history_no_fallback_available",
        "",
        "",
        "no_order_history_or_executions",
    )


def _get_broker() -> tuple[Any | None, bool, str]:
    """
    Get live broker for Bybit API. Call _load_env() first.
    Returns (broker_or_None, enabled, reason).
    """
    from trading.broker import get_broker
    try:
        broker = get_broker("live", dry_run_live=False)
        if broker is None:
            return None, False, "get_broker_returned_None"
        return broker, True, "ok"
    except ValueError as e:
        return None, False, str(e).strip() or "ValueError"
    except Exception as e:
        return None, False, f"{type(e).__name__}: {e}"


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

    env_loaded, source_env = _load_env(root)
    broker = None
    broker_enabled = False
    broker_reason = "no_bybit_skip"
    if use_bybit:
        if not env_loaded:
            broker_reason = f"env_not_loaded:{source_env}"
        else:
            key_ok, key_reason = _check_bybit_env()
            if not key_ok:
                broker_reason = key_reason
            else:
                broker, broker_enabled, broker_reason = _get_broker()
                if broker_enabled:
                    broker_reason = "ok"
                source_env = source_env or "env_loaded"

        if not broker_enabled:
            msg = (
                f"AUDIT_BROKER_FAILED | broker_enabled=no broker_reason={broker_reason!r} source_env={source_env!r}\n"
                "  Load env from .env (project root, /root/pump_short/.env, or /etc/pump-short-live.env).\n"
                "  Set BYBIT_API_KEY and BYBIT_API_SECRET. Use --no-bybit to skip Bybit reconciliation."
            )
            print(msg, file=sys.stderr)
            raise SystemExit(1)

    total_live = 0
    total_timeout = 0
    resolved_sl = 0
    resolved_tp = 0
    unresolved = 0
    no_closed_pnl_rows_count = 0
    no_closed_pnl_rows_resolved = 0
    reason_counts: dict[str, int] = defaultdict(int)
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
            history_source = ""
            fallback_endpoint = ""
            fallback_match_reason = ""

            if broker and entry > 0 and tp > 0 and sl > 0 and opened_ms:
                resolved, bybit_entry, bybit_exit, bybit_pnl, bybit_ts, reason = _match_bybit_closed_pnl(
                    broker, symbol, side, entry, tp, sl, opened_ms, timeout_ms or int(1e15),
                )
                if reason == "no_closed_pnl_rows":
                    no_closed_pnl_rows_count += 1
                    (
                        resolved, bybit_entry, bybit_exit, bybit_pnl, bybit_ts, reason,
                        history_source, fallback_endpoint, fallback_match_reason,
                    ) = _match_bybit_fallback(
                        broker, symbol, side, entry, tp, sl, opened_ms, timeout_ms or int(1e15),
                    )
                    if resolved in ("SL_hit", "TP_hit"):
                        no_closed_pnl_rows_resolved += 1
                if resolved == "SL_hit":
                    resolved_sl += 1
                    by_strategy[strategy]["sl"] += 1
                    by_date[date_val]["sl"] += 1
                    by_symbol[symbol]["sl"] += 1
                    confidence = "high" if reason == "closed_pnl_match" else ("medium" if history_source else "medium")
                    reason_counts[reason] += 1
                elif resolved == "TP_hit":
                    resolved_tp += 1
                    by_strategy[strategy]["tp"] += 1
                    by_date[date_val]["tp"] += 1
                    by_symbol[symbol]["tp"] += 1
                    confidence = "high" if reason == "closed_pnl_match" else ("medium" if history_source else "medium")
                    reason_counts[reason] += 1
                else:
                    unresolved += 1
                    by_strategy[strategy]["unresolved"] += 1
                    by_date[date_val]["unresolved"] += 1
                    by_symbol[symbol]["unresolved"] += 1
                    reason_counts[reason] += 1
            else:
                unresolved += 1
                by_strategy[strategy]["unresolved"] += 1
                by_date[date_val]["unresolved"] += 1
                by_symbol[symbol]["unresolved"] += 1
                if not broker:
                    reason = "no_broker"
                elif entry <= 0 or not opened_ms:
                    reason = "missing_entry_or_opened_ts"
                reason_counts[reason] += 1

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
                "history_source": history_source,
                "fallback_endpoint": fallback_endpoint,
                "fallback_match_reason": fallback_match_reason,
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
        "no_closed_pnl_rows_total": no_closed_pnl_rows_count,
        "no_closed_pnl_rows_resolved_fallback": no_closed_pnl_rows_resolved,
        "reason_counts": dict(reason_counts),
        "broker_enabled": "yes" if broker_enabled else "no",
        "broker_reason": broker_reason,
        "source_env": source_env or "(none)",
        "by_strategy": dict(by_strategy),
        "by_date": dict(by_date),
        "by_symbol": dict(by_symbol),
        "audit_csv": str(out_path),
    }
    return summary


def _print_summary(s: dict) -> None:
    print("\n--- Live Timeout Reconciliation Audit ---")
    print(f"  broker_enabled:          {s.get('broker_enabled', 'N/A')}")
    print(f"  broker_reason:           {s.get('broker_reason', 'N/A')}")
    print(f"  source_env:              {s.get('source_env', 'N/A')}")
    print(f"  total_live_outcomes:     {s['total_live_outcomes']}")
    print(f"  total_TIMEOUT:           {s['total_timeout']}")
    print(f"  timeout → SL_hit:        {s['timeout_resolved_sl']}")
    print(f"  timeout → TP_hit:        {s['timeout_resolved_tp']}")
    print(f"  timeout unresolved:      {s['timeout_unresolved']}")
    ncp = s.get("no_closed_pnl_rows_total", 0)
    ncp_r = s.get("no_closed_pnl_rows_resolved_fallback", 0)
    if ncp > 0:
        print(f"  no_closed_pnl_rows:      {ncp} (resolved via fallback: {ncp_r})")
    print(f"\n  audit_csv: {s['audit_csv']}")
    rc = s.get("reason_counts") or {}
    if rc:
        print("\n--- Reason breakdown ---")
        for k in sorted(rc.keys(), key=lambda x: -rc[x]):
            print(f"  {k}: {rc[k]}")
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
