#!/usr/bin/env python3
"""
Find duplicate core outcomes (TP_hit/SL_hit/TIMEOUT) for short_pump live.

Analysis-only:
- Reads datasets/date=*/strategy=short_pump/mode=live/outcomes_v3.csv
- Filters core outcomes
- Reports duplicate groups by trade_id and event_id
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Dict, List, Tuple

CORE_OUTCOMES = ("TP_hit", "SL_hit", "TIMEOUT")


def _find_dataset_root(data_dir: Path) -> Path:
    data_dir = data_dir.resolve()
    if any((data_dir / d).is_dir() for d in ["date=20200101"]):
        return data_dir
    sub = data_dir / "datasets"
    if sub.is_dir():
        return sub
    return data_dir


def _collect_outcomes_paths(root: Path) -> List[Path]:
    paths: List[Path] = []
    for date_dir in sorted(root.glob("date=*")):
        if not date_dir.is_dir():
            continue
        base = date_dir / "strategy=short_pump" / "mode=live"
        p = base / "outcomes_v3.csv"
        if p.is_file():
            paths.append(p)
    return paths


def _read_rows(path: Path) -> List[Dict[str, str]]:
    with open(path, "r", newline="", encoding="utf-8", errors="replace") as f:
        r = csv.DictReader(f)
        return list(r)


def _is_core(row: Dict[str, str]) -> bool:
    o = (row.get("outcome") or "").strip()
    return o in CORE_OUTCOMES


def _print_group(title: str, key: str, rows: List[Tuple[Path, Dict[str, str]]]) -> None:
    print("")
    print(f"=== DUPLICATE {title}: {key} ===")
    for path, row in rows:
        print(f"file: {path}")
        print(
            "  run_id={run_id} symbol={symbol} outcome={outcome} "
            "outcome_time_utc={outcome_time_utc} outcome_source={outcome_source} "
            "pnl_r={pnl_r} pnl_pct={pnl_pct} exit_price={exit_price}"
            .format(
                run_id=row.get("run_id"),
                symbol=row.get("symbol"),
                outcome=row.get("outcome"),
                outcome_time_utc=row.get("outcome_time_utc"),
                outcome_source=row.get("outcome_source"),
                pnl_r=row.get("pnl_r"),
                pnl_pct=row.get("pnl_pct"),
                exit_price=row.get("exit_price"),
            )
        )
        print(f"  full_row={row}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Find duplicate core outcomes for short_pump live")
    ap.add_argument("--data-dir", default="/root/pump_short/datasets", help="Datasets root (default: /root/pump_short/datasets)")
    args = ap.parse_args()

    root = _find_dataset_root(Path(args.data_dir))
    print(f"DATASETS_ROOT: {root}")
    paths = _collect_outcomes_paths(root)
    print(f"FILES: outcomes_v3={len(paths)}")
    if not paths:
        print("No outcomes_v3.csv found for short_pump/mode=live.")
        return

    by_trade: Dict[str, List[Tuple[Path, Dict[str, str]]]] = {}
    by_event: Dict[str, List[Tuple[Path, Dict[str, str]]]] = {}

    for p in paths:
        rows = _read_rows(p)
        for row in rows:
            if not _is_core(row):
                continue
            tid = (row.get("trade_id") or row.get("tradeId") or "").strip()
            eid = (row.get("event_id") or "").strip()
            if tid:
                by_trade.setdefault(tid, []).append((p, row))
            if eid:
                by_event.setdefault(eid, []).append((p, row))

    dup_trade = {k: v for k, v in by_trade.items() if len(v) > 1}
    dup_event = {k: v for k, v in by_event.items() if len(v) > 1}

    print("")
    print(f"duplicate_by_trade_id_count: {len(dup_trade)}")
    print(f"duplicate_by_event_id_count: {len(dup_event)}")

    for k, rows in dup_trade.items():
        _print_group("TRADE_ID", k, rows)
    for k, rows in dup_event.items():
        _print_group("EVENT_ID", k, rows)


if __name__ == "__main__":
    main()

