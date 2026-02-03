from __future__ import annotations

import csv
import os
from datetime import datetime
from typing import Any, Dict


def _dataset_dir(strategy: str, mode: str, wall_time_utc: str) -> str:
    # date partition from wall_time_utc
    try:
        dt = datetime.fromisoformat(wall_time_utc.replace("Z", "+00:00"))
        day = dt.strftime("%Y%m%d")
    except Exception:
        day = "unknown_date"
    return os.path.join(
        "datasets",
        f"date={day}",
        f"strategy={strategy}",
        f"mode={mode}",
    )


def _write_row(path: str, row: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    file_exists = os.path.isfile(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(row.keys()))
        if not file_exists:
            w.writeheader()
        w.writerow(row)


def write_event_row(row: Dict[str, Any], *, strategy: str, mode: str, wall_time_utc: str) -> None:
    path = os.path.join(_dataset_dir(strategy, mode, wall_time_utc), "events.csv")
    _write_row(path, row)


def write_trade_row(row: Dict[str, Any], *, strategy: str, mode: str, wall_time_utc: str) -> None:
    row = {
        "trade_type": row.get("trade_type", ""),
        "paper_entry_time_utc": row.get("paper_entry_time_utc", ""),
        "paper_entry_price": row.get("paper_entry_price", ""),
        "paper_tp_price": row.get("paper_tp_price", ""),
        "paper_sl_price": row.get("paper_sl_price", ""),
        **row,
    }
    path = os.path.join(_dataset_dir(strategy, mode, wall_time_utc), "trades.csv")
    _write_row(path, row)


def write_outcome_row(row: Dict[str, Any], *, strategy: str, mode: str, wall_time_utc: str) -> None:
    row = {
        "trade_type": row.get("trade_type", ""),
        "details_payload": row.get("details_payload", ""),
        **row,
    }
    path = os.path.join(_dataset_dir(strategy, mode, wall_time_utc), "outcomes.csv")
    _write_row(path, row)
