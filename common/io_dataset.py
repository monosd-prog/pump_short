from __future__ import annotations

import csv
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


def _get_exec_mode() -> str:
    """Execution mode (paper|live) for dataset path. From env EXECUTION_MODE / AUTO_TRADING_MODE."""
    mode = (os.getenv("EXECUTION_MODE") or os.getenv("AUTO_TRADING_MODE") or "paper").strip().lower()
    return mode if mode in ("paper", "live") else "paper"


def get_dataset_dir(strategy: str, wall_time_utc: str, base_dir: str | None = None) -> str:
    """Public: dataset directory for strategy/date; path segment mode=<exec_mode>. For tests and logging."""
    return _dataset_dir(strategy, wall_time_utc, base_dir=base_dir)


def _dataset_dir(
    strategy: str, wall_time_utc: str, base_dir: str | None = None
) -> str:
    """Build dataset dir: date=.../strategy=.../mode=<exec_mode>. Path reflects execution mode (paper/live)."""
    try:
        dt = datetime.fromisoformat(wall_time_utc.replace("Z", "+00:00"))
        day = dt.strftime("%Y%m%d")
    except Exception:
        day = "unknown_date"
    exec_mode = _get_exec_mode()
    rel_parts = (f"date={day}", f"strategy={strategy}", f"mode={exec_mode}")
    if base_dir:
        return os.path.join(base_dir, *rel_parts)
    return os.path.join("datasets", *rel_parts)


def _write_row(path: str, row: Dict[str, Any], fieldnames: list[str] | None = None) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    file_exists = os.path.isfile(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        if fieldnames is None:
            fieldnames = list(row.keys())
        w = csv.DictWriter(f, fieldnames=fieldnames)
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
    dir_path = _dataset_dir(strategy, wall_time_utc, base_dir=base_dir)
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
    logging.getLogger(__name__).info(
        "DATASET_ENSURE_OK | schema_version=%s | strategy=%s | exec_mode=%s | event_file=%s | has_1m_fields=%s | wall_time_utc=%s",
        schema_version,
        strategy,
        _get_exec_mode(),
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
) -> None:
    """mode = source_mode (live/FAST0/ARMED); path uses exec_mode (paper|live). Row gets mode=exec_mode, source_mode=passed mode."""
    exec_mode = _get_exec_mode()
    row = {**row, "mode": exec_mode, "source_mode": mode}
    dir_path = _dataset_dir(strategy, wall_time_utc, base_dir=base_dir)
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
) -> None:
    exec_mode = _get_exec_mode()
    row = {
        "trade_type": row.get("trade_type", ""),
        "paper_entry_time_utc": row.get("paper_entry_time_utc", ""),
        "paper_entry_price": row.get("paper_entry_price", ""),
        "paper_tp_price": row.get("paper_tp_price", ""),
        "paper_sl_price": row.get("paper_sl_price", ""),
        "mode": exec_mode,
        "source_mode": mode,
        **row,
    }
    dir_path = _dataset_dir(strategy, wall_time_utc, base_dir=base_dir)
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
) -> None:
    exec_mode = _get_exec_mode()
    row = {
        "trade_type": row.get("trade_type", ""),
        "details_payload": row.get("details_payload", ""),
        "mode": exec_mode,
        "source_mode": mode,
        **row,
    }
    if not row.get("outcome_time_utc"):
        row["outcome_time_utc"] = wall_time_utc
    dir_path = _dataset_dir(strategy, wall_time_utc, base_dir=base_dir)
    if schema_version == 3:
        path = os.path.join(dir_path, "outcomes_v3.csv")
        row_v3 = normalize_outcome_v3(row)
        _write_row(path, row_v3, OUTCOME_FIELDS_V3)
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
