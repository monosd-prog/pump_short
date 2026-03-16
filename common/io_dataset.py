from __future__ import annotations

import csv
import json
import logging
import os
from datetime import datetime
from typing import Any, Dict

from common.dataset_schema import (
    EVENT_FIELDS_V2,
    EVENT_FIELDS_V3,
    OUTCOME_FIELDS_V2,
    OUTCOME_FIELDS_V3,
    TRADE_FIELDS_V2,
    TRADE_FIELDS_V3,
    normalize_event_v2,
    normalize_event_v3,
    normalize_outcome_v2,
    normalize_outcome_v3,
    normalize_trade_v2,
    normalize_trade_v3,
)


def _inject_signal_source_mode(payload_json_str: str, signal_source_mode: str) -> str:
    """Add signal_source_mode to payload_json; keep existing keys (e.g. source, pump_webhook)."""
    try:
        payload = json.loads(payload_json_str) if payload_json_str else {}
    except json.JSONDecodeError:
        payload = {}
    if not isinstance(payload, dict):
        payload = {}
    payload["signal_source_mode"] = signal_source_mode
    return json.dumps(payload, ensure_ascii=False)


def _get_exec_mode() -> str:
    """Execution mode (paper|live) for dataset path. From env EXECUTION_MODE / AUTO_TRADING_MODE."""
    mode = (os.getenv("EXECUTION_MODE") or os.getenv("AUTO_TRADING_MODE") or "paper").strip().lower()
    return mode if mode in ("paper", "live") else "paper"


def _path_mode_from_caller(mode: str) -> str | None:
    """If caller's mode is paper|live, use it for path; else None (fallback to env)."""
    m = (mode or "").strip().lower()
    return m if m in ("paper", "live") else None


_logged_writers: set[tuple[str, str, str, str]] = set()


def _log_dataset_write_once(
    writer: str,
    strategy: str,
    path_mode: str,
    dir_path: str,
    base_dir: str | None,
    row_mode: str,
    row: Dict[str, Any] | None = None,
) -> None:
    """Log once per (writer, strategy, path_mode, run_id) for diagnostics."""
    run_id = (row.get("run_id") or "")[:16] if row else ""
    key = (writer, strategy, path_mode, run_id)
    if key in _logged_writers:
        return
    _logged_writers.add(key)
    root = base_dir or "datasets"
    event_id = row.get("event_id") or row.get("eventId") if row else ""
    trade_id = row.get("trade_id") or row.get("tradeId") if row else ""
    logging.getLogger(__name__).info(
        "DATASET_WRITE | writer=%s strategy=%s path_mode=%s row_mode=%s run_id=%s event_id=%s trade_id=%s dataset_dir=%s",
        writer, strategy, path_mode, row_mode, run_id or "-", event_id or "-", trade_id or "-", dir_path,
    )


def get_dataset_dir(
    strategy: str,
    wall_time_utc: str,
    base_dir: str | None = None,
    path_mode: str | None = None,
) -> str:
    """Public: dataset directory for strategy/date; path segment mode=<path_mode|exec_mode>. For tests and logging."""
    return _dataset_dir(strategy, wall_time_utc, base_dir=base_dir, path_mode=path_mode)


def _dataset_dir(
    strategy: str,
    wall_time_utc: str,
    base_dir: str | None = None,
    path_mode: str | None = None,
) -> str:
    """Build dataset dir: date=.../strategy=.../mode=<path_mode|exec_mode>.
    When path_mode is paper|live (from caller), use it; else use _get_exec_mode() from env.
    This ensures FAST0 events/trades in live runs write to mode=live even when API server
    has EXECUTION_MODE=paper (different process env).
    """
    try:
        dt = datetime.fromisoformat(wall_time_utc.replace("Z", "+00:00"))
        day = dt.strftime("%Y%m%d")
    except Exception:
        day = "unknown_date"
    mode_for_path = path_mode if path_mode in ("paper", "live") else _get_exec_mode()
    rel_parts = (f"date={day}", f"strategy={strategy}", f"mode={mode_for_path}")
    if base_dir:
        return os.path.join(base_dir, *rel_parts)
    return os.path.join("datasets", *rel_parts)


# Final outcomes: once written, no second final outcome for same trade (idempotency for paper).
PAPER_FINAL_OUTCOMES = ("TP_hit", "SL_hit", "TIMEOUT")


def paper_outcome_final_in_file(path: str, trade_id: str, event_id: str) -> bool:
    """
    Return True if outcomes_v3.csv at path already contains a row with this trade_id or event_id
    and outcome in (TP_hit, SL_hit, TIMEOUT). Used to avoid duplicate paper outcome writes.
    """
    if not path or not os.path.isfile(path):
        return False
    tid = (trade_id or "").strip()
    eid = (event_id or "").strip()
    if not tid and not eid:
        return False
    try:
        with open(path, "r", newline="", encoding="utf-8", errors="replace") as f:
            r = csv.DictReader(f)
            for existing in r:
                out = (existing.get("outcome") or "").strip()
                if out not in PAPER_FINAL_OUTCOMES:
                    continue
                et = (existing.get("trade_id") or existing.get("tradeId") or "").strip()
                ev = (existing.get("event_id") or "").strip()
                if (tid and et == tid) or (eid and ev == eid):
                    return True
    except Exception:
        pass
    return False


def _live_outcome_duplicate(path: str, row: Dict[str, Any]) -> bool:
    """
    Return True if an outcome row for the same trade_id or event_id already exists in outcomes_v3.csv.
    Used as idempotency guard for live outcomes (e.g. retries in outcome workers).
    """
    tid = (row.get("trade_id") or row.get("tradeId") or "").strip()
    eid = (row.get("event_id") or "").strip()
    if not tid and not eid:
        return False
    if not os.path.isfile(path):
        return False
    try:
        with open(path, "r", newline="", encoding="utf-8", errors="replace") as f:
            r = csv.DictReader(f)
            for existing in r:
                et = (existing.get("trade_id") or existing.get("tradeId") or "").strip()
                ev = (existing.get("event_id") or "").strip()
                if tid and et == tid:
                    return True
                if eid and ev and ev == eid:
                    return True
    except Exception:
        pass
    return False


def _write_row(path: str, row: Dict[str, Any], fieldnames: list[str] | None = None) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    file_exists = os.path.isfile(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        if fieldnames is None:
            fieldnames = list(row.keys())
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        if not file_exists:
            w.writeheader()
        w.writerow(row)


def ensure_dataset_files(
    strategy: str,
    mode: str,
    wall_time_utc: str,
    schema_version: int = 2,
    base_dir: str | None = None,
) -> None:
    if schema_version < 2:
        return
    path_mode = _path_mode_from_caller(mode)
    dir_path = _dataset_dir(strategy, wall_time_utc, base_dir=base_dir, path_mode=path_mode)
    os.makedirs(dir_path, exist_ok=True)
    if schema_version == 3:
        targets = [
            ("events_v3.csv", EVENT_FIELDS_V3),
            ("trades_v3.csv", TRADE_FIELDS_V3),
            ("outcomes_v3.csv", OUTCOME_FIELDS_V3),
        ]
    else:
        targets = [
            ("events_v2.csv", EVENT_FIELDS_V2),
            ("trades_v2.csv", TRADE_FIELDS_V2),
            ("outcomes_v2.csv", OUTCOME_FIELDS_V2),
        ]
    for filename, fieldnames in targets:
        path = os.path.join(dir_path, filename)
        if os.path.isfile(path):
            continue
        with open(path, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
    event_filename = "events_v3.csv" if schema_version == 3 else "events_v2.csv"
    fieldnames = EVENT_FIELDS_V3 if schema_version == 3 else EVENT_FIELDS_V2
    has_1m_fields = all(
        k in fieldnames
        for k in (
            "liq_short_count_1m",
            "liq_short_usd_1m",
            "liq_long_count_1m",
            "liq_long_usd_1m",
        )
    )
    mode_for_path = path_mode if path_mode in ("paper", "live") else _get_exec_mode()
    logging.getLogger(__name__).info(
        "DATASET_ENSURE_OK | schema_version=%s | strategy=%s | path_mode=%s | event_file=%s | has_1m_fields=%s | wall_time_utc=%s",
        schema_version,
        strategy,
        mode_for_path,
        event_filename,
        has_1m_fields,
        wall_time_utc,
    )


def write_event_row(
    row: Dict[str, Any],
    *,
    strategy: str,
    mode: str,
    wall_time_utc: str,
    schema_version: int = 2,
    base_dir: str | None = None,
    path_mode: str | None = None,
) -> None:
    """mode = signal source (live/FAST0/ARMED). path_mode overrides: when paper|live, use for path AND row. Else path uses caller mode when paper|live else env."""
    exec_mode = _get_exec_mode()
    pm = path_mode if path_mode in ("paper", "live") else _path_mode_from_caller(mode)
    row_mode = pm if pm in ("paper", "live") else exec_mode
    payload_raw = row.get("entry_payload") or row.get("payload_json") or "{}"
    payload_json = _inject_signal_source_mode(payload_raw, mode)
    row = {**row, "mode": row_mode, "source_mode": row_mode, "payload_json": payload_json}
    dir_path = _dataset_dir(strategy, wall_time_utc, base_dir=base_dir, path_mode=pm)
    _log_dataset_write_once("event", strategy, pm or exec_mode, dir_path, base_dir, row_mode, row)
    if schema_version == 3:
        path = os.path.join(dir_path, "events_v3.csv")
        row_v3 = normalize_event_v3(row)
        _write_row(path, row_v3, EVENT_FIELDS_V3)
        if os.getenv("DATASET_V1", "1") == "1":
            _write_row(os.path.join(dir_path, "events.csv"), row)
    elif schema_version >= 2:
        path = os.path.join(dir_path, "events_v2.csv")
        row_v2 = normalize_event_v2(row)
        _write_row(path, row_v2, EVENT_FIELDS_V2)
        if os.getenv("DATASET_V1", "1") == "1":
            _write_row(os.path.join(dir_path, "events.csv"), row)
    else:
        path = os.path.join(dir_path, "events.csv")
        _write_row(path, row)


def write_trade_row(
    row: Dict[str, Any],
    *,
    strategy: str,
    mode: str,
    wall_time_utc: str,
    schema_version: int = 2,
    base_dir: str | None = None,
    path_mode: str | None = None,
) -> None:
    """path_mode overrides: when paper|live, use for path AND row. Else path uses caller mode when paper|live."""
    exec_mode = _get_exec_mode()
    pm = path_mode if path_mode in ("paper", "live") else _path_mode_from_caller(mode)
    row_mode = pm if pm in ("paper", "live") else exec_mode
    row = {
        "trade_type": row.get("trade_type", ""),
        "paper_entry_time_utc": row.get("paper_entry_time_utc", ""),
        "paper_entry_price": row.get("paper_entry_price", ""),
        "paper_tp_price": row.get("paper_tp_price", ""),
        "paper_sl_price": row.get("paper_sl_price", ""),
        **row,
        "mode": row_mode,
        "source_mode": row_mode,
    }
    dir_path = _dataset_dir(strategy, wall_time_utc, base_dir=base_dir, path_mode=pm)
    _log_dataset_write_once("trade", strategy, pm or exec_mode, dir_path, base_dir, row_mode, row)
    if schema_version == 3:
        path = os.path.join(dir_path, "trades_v3.csv")
        row_v3 = normalize_trade_v3(row)
        _write_row(path, row_v3, TRADE_FIELDS_V3)
        if os.getenv("DATASET_V1", "1") == "1":
            _write_row(os.path.join(dir_path, "trades.csv"), row)
    elif schema_version >= 2:
        path = os.path.join(dir_path, "trades_v2.csv")
        row_v2 = normalize_trade_v2(row)
        _write_row(path, row_v2, TRADE_FIELDS_V2)
        if os.getenv("DATASET_V1", "1") == "1":
            _write_row(os.path.join(dir_path, "trades.csv"), row)
    else:
        path = os.path.join(dir_path, "trades.csv")
        _write_row(path, row)


def write_outcome_row(
    row: Dict[str, Any],
    *,
    strategy: str,
    mode: str,
    wall_time_utc: str,
    schema_version: int = 2,
    base_dir: str | None = None,
    path_mode: str | None = None,
) -> None:
    """path_mode overrides: when paper|live, use for path AND row. Else path uses caller mode when paper|live."""
    exec_mode = _get_exec_mode()
    pm = path_mode if path_mode in ("paper", "live") else _path_mode_from_caller(mode)
    row_mode = pm if pm in ("paper", "live") else exec_mode
    row = {
        "trade_type": row.get("trade_type", ""),
        "details_payload": row.get("details_payload", ""),
        **row,
        "mode": row_mode,
        "source_mode": row_mode,
    }
    if not row.get("outcome_time_utc"):
        row["outcome_time_utc"] = wall_time_utc
    dir_path = _dataset_dir(strategy, wall_time_utc, base_dir=base_dir, path_mode=pm)
    mode_for_path = pm if pm in ("paper", "live") else _get_exec_mode()
    _log_dataset_write_once("outcome", strategy, pm or exec_mode, dir_path, base_dir, row_mode, row)
    if schema_version == 3:
        path = os.path.join(dir_path, "outcomes_v3.csv")
        row_v3 = normalize_outcome_v3(row)
        if mode_for_path == "live" and _live_outcome_duplicate(path, row_v3):
            logging.getLogger(__name__).info(
                "OUTCOME_DUPLICATE_SKIPPED | strategy=%s trade_id=%s event_id=%s (already in outcomes_v3)",
                strategy,
                row_v3.get("trade_id", ""),
                row_v3.get("event_id", ""),
            )
            return
        if (row_mode == "paper" or pm == "paper") and (row_v3.get("outcome") or "").strip() in PAPER_FINAL_OUTCOMES:
            if paper_outcome_final_in_file(
                path,
                row_v3.get("trade_id") or row_v3.get("tradeId") or "",
                row_v3.get("event_id") or "",
            ):
                logging.getLogger(__name__).info(
                    "PAPER_OUTCOME_DUPLICATE_SKIPPED | strategy=%s trade_id=%s event_id=%s (final outcome already in file)",
                    strategy,
                    row_v3.get("trade_id", ""),
                    row_v3.get("event_id", ""),
                )
                return
        fieldnames = OUTCOME_FIELDS_V3
        if os.path.isfile(path):
            try:
                with open(path, "r", newline="", encoding="utf-8", errors="replace") as f:
                    header = next(csv.reader(f), [])
                    if "outcome_source" not in header:
                        fieldnames = [f for f in OUTCOME_FIELDS_V3 if f != "outcome_source"]
            except Exception:
                pass
        _write_row(path, row_v3, fieldnames)
        if os.getenv("DATASET_V1", "1") == "1":
            _write_row(os.path.join(dir_path, "outcomes.csv"), row)
    elif schema_version >= 2:
        path = os.path.join(dir_path, "outcomes_v2.csv")
        row_v2 = normalize_outcome_v2(row)
        _write_row(path, row_v2, OUTCOME_FIELDS_V2)
        if os.getenv("DATASET_V1", "1") == "1":
            _write_row(os.path.join(dir_path, "outcomes.csv"), row)
    else:
        path = os.path.join(dir_path, "outcomes.csv")
        _write_row(path, row)
