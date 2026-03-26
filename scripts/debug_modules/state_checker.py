"""
state_checker.py — MODULE 3
Derive factual state summary from RawData.

Read-only. Produces StateReport dict — facts only, no Issue objects.
issue_detector consumes StateReport.
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from scripts.debug_modules.log_reader import RawData


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------

StateReport = Dict[str, Any]
"""
Keys:
  service_active          : bool
  service_status_line     : str          — first meaningful line of systemctl output
  last_signal_ts          : str | None   — ISO timestamp of last trade/log
  last_signal_symbol      : str | None
  last_signal_source      : str          — "trading_trades" | "log_file" | "unknown"
  open_positions_count    : int
  open_positions_detail   : list[dict]   — [{strategy, symbol, run_id, ts, mode, order_id_present, age_hours}]
  stuck_positions         : list[dict]   — positions older than stuck_threshold_hours (live mode)
                                           each entry adds: in_closes: bool (run_id found in trading_closes.csv)
  queue_depth             : int
  processing_stuck        : bool         — .processing exists AND age >= _RUNNER_STUCK_MINUTES (5 min)
  processing_age_minutes  : float | None
  lock_stale              : bool         — .lock exists AND age >= _LOCK_STALE_MINUTES (30 min)
  lock_stuck              : bool         — alias for lock_stale (backward compat, same value)
  lock_age_minutes        : float | None
  state_valid             : bool
  last_log_file_ts        : float | None  — mtime epoch
  last_log_file_name      : str | None
  hours_since_last_log    : float | None
  stuck_threshold_hours   : float
  no_signal_threshold_hours: float
"""

_STUCK_POSITION_HOURS = 24.0
# Threshold for .processing file: runner should never hold .processing > 5 min.
# A signal that takes longer than this to open indicates a genuine hang.
_RUNNER_STUCK_MINUTES = 5.0
# Threshold for .lock file: distinct from .processing.
# trading_runner.lock is a persistent lock held for the lifetime of the runner
# process (not per-signal). Flagging it at 5 min produces false positives when
# uvicorn/watcher are alive. 30 min is the minimum plausible hang duration
# when the lock file exists with NO supporting liveness evidence.
_LOCK_STALE_MINUTES = 30.0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_ts_to_epoch(ts_str: Optional[str]) -> Optional[float]:
    """Try to parse an ISO/mixed timestamp string to epoch float. Returns None on failure."""
    if not ts_str:
        return None
    ts_str = str(ts_str).strip()
    for fmt in (
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
    ):
        try:
            dt = datetime.strptime(ts_str[:len(fmt)+6], fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.timestamp()
        except ValueError:
            continue
    return None


def _age_hours(epoch: Optional[float], now: float) -> Optional[float]:
    if epoch is None:
        return None
    return max(0.0, (now - epoch) / 3600.0)


def _age_minutes(epoch: Optional[float], now: float) -> Optional[float]:
    if epoch is None:
        return None
    return max(0.0, (now - epoch) / 60.0)


def _service_active(service_status: str) -> bool:
    """Returns True if service status line indicates active/running."""
    lower = service_status.lower()
    return "active (running)" in lower or "active: active" in lower


def _service_status_line(service_status: str) -> str:
    """Extract the first 'Active:' line or first non-empty line."""
    for line in service_status.splitlines():
        stripped = line.strip()
        if stripped.lower().startswith("active:"):
            return stripped
    for line in service_status.splitlines():
        stripped = line.strip()
        if stripped:
            return stripped
    return service_status[:120] if service_status else "unknown"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build(raw: RawData, no_signal_threshold_hours: float = 2.0) -> StateReport:
    """
    Derive a StateReport from RawData. Never raises.
    """
    now = time.time()
    report: StateReport = {
        "service_active": False,
        "service_status_line": "unknown",
        "last_signal_ts": None,
        "last_signal_symbol": None,
        "last_signal_source": "unknown",
        "open_positions_count": 0,
        "open_positions_detail": [],
        "stuck_positions": [],
        "queue_depth": 0,
        "processing_stuck": False,
        "processing_age_minutes": None,
        "lock_stale": False,
        "lock_stuck": False,   # kept for backward compatibility — same value as lock_stale
        "lock_age_minutes": None,
        "state_valid": False,
        "last_log_file_ts": None,
        "last_log_file_name": None,
        "hours_since_last_log": None,
        "stuck_threshold_hours": _STUCK_POSITION_HOURS,
        "no_signal_threshold_hours": no_signal_threshold_hours,
    }

    # --- Service status ---
    try:
        report["service_active"] = _service_active(raw.get("service_status", ""))
        report["service_status_line"] = _service_status_line(raw.get("service_status", ""))
    except Exception:
        pass

    # --- Last log file ---
    try:
        log_files: List[Dict[str, Any]] = raw.get("log_files", [])
        if log_files:
            # already sorted by mtime desc in log_reader
            newest = log_files[0]
            report["last_log_file_ts"] = newest.get("mtime")
            report["last_log_file_name"] = newest.get("name")
            hours = _age_hours(newest.get("mtime"), now)
            report["hours_since_last_log"] = round(hours, 2) if hours is not None else None
    except Exception:
        pass

    # --- Last signal from trading_trades.csv ---
    try:
        trades_rows: List[Dict[str, Any]] = raw.get("trades_rows", [])
        if trades_rows:
            last_trade = trades_rows[-1]
            ts_str = (
                last_trade.get("entry_time_utc")
                or last_trade.get("wall_time_utc")
                or last_trade.get("ts_utc")
            )
            symbol = last_trade.get("symbol", "")
            if ts_str:
                report["last_signal_ts"] = ts_str
                report["last_signal_symbol"] = symbol
                report["last_signal_source"] = "trading_trades"
    except Exception:
        pass

    # Fallback: use log file mtime if no trades
    if report["last_signal_ts"] is None and report.get("last_log_file_ts"):
        try:
            ts_epoch = report["last_log_file_ts"]
            dt = datetime.fromtimestamp(ts_epoch, tz=timezone.utc)
            report["last_signal_ts"] = dt.isoformat()
            # try to get symbol from newest log filename
            log_files = raw.get("log_files", [])
            if log_files:
                report["last_signal_symbol"] = log_files[0].get("symbol", "")
            report["last_signal_source"] = "log_file"
        except Exception:
            pass

    # --- Open positions ---
    try:
        state_json = raw.get("state_json")
        report["state_valid"] = state_json is not None and raw.get("state_parse_error") is None

        if state_json and isinstance(state_json, dict):
            open_positions: Dict[str, Any] = state_json.get("open_positions") or {}
            detail: List[Dict[str, Any]] = []
            stuck: List[Dict[str, Any]] = []

            # Pre-build closes_run_ids set for O(1) lookup
            closes_run_ids: set[str] = set()
            try:
                closes_rows: List[Dict[str, Any]] = raw.get("closes_rows", [])
                for row in closes_rows:
                    run_id = row.get("run_id", "")
                    if run_id:
                        closes_run_ids.add(run_id)
            except Exception:
                closes_run_ids = set()

            for strategy, positions in open_positions.items():
                if not isinstance(positions, dict):
                    continue
                for pos_id, pos in positions.items():
                    if not isinstance(pos, dict):
                        continue
                    mode = str(pos.get("mode") or "").strip().lower()
                    ts_str = (
                        pos.get("opened_ts")
                        or pos.get("ts")
                        or pos.get("ts_utc")
                        or pos.get("entry_time_utc")
                    )
                    ts_epoch = _parse_ts_to_epoch(ts_str)
                    age_h = _age_hours(ts_epoch, now)
                    has_order_id = bool(pos.get("order_id"))
                    run_id = pos.get("run_id", "")
                    in_closes = run_id in closes_run_ids

                    entry: Dict[str, Any] = {
                        "pos_id": pos_id,
                        "strategy": strategy,
                        "symbol": pos.get("symbol", ""),
                        "run_id": run_id,
                        "mode": mode,
                        "ts": ts_str or "",
                        "age_hours": round(age_h, 2) if age_h is not None else None,
                        "order_id_present": has_order_id,
                        "in_closes": in_closes,
                    }
                    detail.append(entry)

                    # Stuck = live mode AND older than threshold
                    if mode == "live" and age_h is not None and age_h >= _STUCK_POSITION_HOURS:
                        stuck.append(entry)

            report["open_positions_count"] = len(detail)
            report["open_positions_detail"] = detail
            report["stuck_positions"] = stuck
    except Exception:
        pass

    # --- Queue depth ---
    try:
        queue_lines: List[str] = raw.get("queue_lines", [])
        report["queue_depth"] = len([ln for ln in queue_lines if ln.strip()])
    except Exception:
        pass

    # --- Processing stuck ---
    try:
        proc_mtime = raw.get("processing_mtime")
        proc_exists = raw.get("processing_exists", False)
        if proc_exists and proc_mtime is not None:
            age_min = _age_minutes(proc_mtime, now)
            report["processing_age_minutes"] = round(age_min, 1) if age_min is not None else None
            report["processing_stuck"] = age_min is not None and age_min >= _RUNNER_STUCK_MINUTES
        else:
            report["processing_stuck"] = False
    except Exception:
        pass

    # --- Lock stale ---
    # Uses _LOCK_STALE_MINUTES (30 min), not _RUNNER_STUCK_MINUTES (5 min).
    # trading_runner.lock is a long-lived process lock, not a per-signal lock.
    # A fresh lock (< 30 min) at a healthy system is perfectly normal.
    try:
        lock_mtime = raw.get("lock_mtime")
        lock_exists = raw.get("lock_exists", False)
        if lock_exists and lock_mtime is not None:
            age_min = _age_minutes(lock_mtime, now)
            report["lock_age_minutes"] = round(age_min, 1) if age_min is not None else None
            is_stale = age_min is not None and age_min >= _LOCK_STALE_MINUTES
            report["lock_stale"] = is_stale
            report["lock_stuck"] = is_stale  # backward compat alias
        else:
            report["lock_stale"] = False
            report["lock_stuck"] = False
    except Exception:
        pass

    return report
