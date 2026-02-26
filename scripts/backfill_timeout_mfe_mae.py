#!/usr/bin/env python3
"""Backfill empty mfe_pct/mae_pct/mfe_r/mae_r for old timeout rows in trading_closes.csv."""
from __future__ import annotations

import csv
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent

FILL_VALUE = "0.0000"


def _index_or_none(header: list[str], name: str) -> int | None:
    try:
        return header.index(name)
    except ValueError:
        return None


def _all_four_empty(row: list[str], idxs: tuple[int, int, int, int]) -> bool:
    mfe_pct_i, mae_pct_i, mfe_r_i, mae_r_i = idxs
    return (
        (row[mfe_pct_i].strip() == "" if mfe_pct_i < len(row) else True)
        and (row[mae_pct_i].strip() == "" if mae_pct_i < len(row) else True)
        and (row[mfe_r_i].strip() == "" if mfe_r_i < len(row) else True)
        and (row[mae_r_i].strip() == "" if mae_r_i < len(row) else True)
    )


def backfill_timeout_mfe_mae(csv_path: str) -> dict[str, Any]:
    """
    For rows with close_reason=timeout and all four mfe/mae fields empty, set them to "0.0000".
    Creates backup at <path>.bak_<YYYYmmdd_HHMMSS>, writes atomically via temp + replace.
    Returns dict with total_rows, timeout_rows, updated_rows, backup_path.
    """
    path = Path(csv_path)
    if not path.exists():
        return {"total_rows": 0, "timeout_rows": 0, "updated_rows": 0, "backup_path": None}

    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        rows = list(reader)

    close_reason_idx = _index_or_none(header, "close_reason")
    mfe_pct_idx = _index_or_none(header, "mfe_pct")
    mae_pct_idx = _index_or_none(header, "mae_pct")
    mfe_r_idx = _index_or_none(header, "mfe_r")
    mae_r_idx = _index_or_none(header, "mae_r")
    if close_reason_idx is None or mfe_pct_idx is None or mae_pct_idx is None or mfe_r_idx is None or mae_r_idx is None:
        return {
            "total_rows": len(rows),
            "timeout_rows": 0,
            "updated_rows": 0,
            "backup_path": None,
        }

    idxs = (mfe_pct_idx, mae_pct_idx, mfe_r_idx, mae_r_idx)
    total_rows = len(rows)
    timeout_rows = sum(1 for r in rows if close_reason_idx < len(r) and r[close_reason_idx].strip() == "timeout")
    updated_rows = 0

    for row in rows:
        if close_reason_idx >= len(row):
            continue
        if row[close_reason_idx].strip() != "timeout":
            continue
        if not _all_four_empty(row, idxs):
            continue
        while len(row) <= max(idxs):
            row.append("")
        row[mfe_pct_idx] = FILL_VALUE
        row[mae_pct_idx] = FILL_VALUE
        row[mfe_r_idx] = FILL_VALUE
        row[mae_r_idx] = FILL_VALUE
        updated_rows += 1

    if updated_rows == 0 and total_rows >= 0:
        return {
            "total_rows": total_rows,
            "timeout_rows": timeout_rows,
            "updated_rows": 0,
            "backup_path": None,
        }

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    backup_path = f"{csv_path}.bak_{stamp}"
    path_backup = Path(backup_path)
    path_backup.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "r", newline="", encoding="utf-8") as src:
        with open(backup_path, "w", newline="", encoding="utf-8") as dst:
            dst.write(src.read())

    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with open(tmp_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    os.replace(tmp_path, path)

    return {
        "total_rows": total_rows,
        "timeout_rows": timeout_rows,
        "updated_rows": updated_rows,
        "backup_path": backup_path,
    }


def main() -> int:
    default_path = str(_ROOT / "datasets" / "trading_closes.csv")
    path = sys.argv[1] if len(sys.argv) > 1 else default_path
    stats = backfill_timeout_mfe_mae(path)
    print(
        f"total_rows={stats['total_rows']} timeout_rows={stats['timeout_rows']} "
        f"updated_rows={stats['updated_rows']} backup_path={stats['backup_path']!r}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
