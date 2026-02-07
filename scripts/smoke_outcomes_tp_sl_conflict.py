from __future__ import annotations

import csv
import json
import sys
from collections import deque
from datetime import datetime, timezone
from pathlib import Path


def _print_usage() -> None:
    print("Usage: python scripts/smoke_outcomes_tp_sl_conflict.py [YYYYMMDD] [--tail N] [--strategy STR] [--mode MODE]")


def main() -> int:
    date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
    tail = 200
    strategy = "long_pullback"
    mode = "live"

    args = list(sys.argv[1:])
    i = 0
    while i < len(args):
        if args[i] == "--tail":
            if i + 1 >= len(args):
                _print_usage()
                return 2
            try:
                tail = int(args[i + 1])
            except ValueError:
                _print_usage()
                return 2
            i += 2
        elif args[i] == "--strategy":
            if i + 1 >= len(args):
                _print_usage()
                return 2
            strategy = args[i + 1]
            i += 2
        elif args[i] == "--mode":
            if i + 1 >= len(args):
                _print_usage()
                return 2
            mode = args[i + 1]
            i += 2
        elif args[i].startswith("--"):
            _print_usage()
            return 2
        else:
            date_str = args[i]
            i += 1

    repo_root = Path(__file__).resolve().parents[1]
    rel_path = Path("datasets") / f"date={date_str}" / f"strategy={strategy}" / f"mode={mode}" / "outcomes_v2.csv"
    path = repo_root / rel_path
    if not path.exists():
        print(f"outcomes_v2.csv not found: {path}")
        return 2

    recent_rows: deque[dict[str, str]] = deque(maxlen=max(tail, 1))
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            recent_rows.append(row)

    rows = list(recent_rows)
    checked = rows[-tail:] if tail else rows

    bad_rows: list[dict[str, str]] = []
    for row in checked:
        details_raw = row.get("details_json") or ""
        try:
            details = json.loads(details_raw)
        except Exception:
            continue
        if details.get("tp_sl_same_candle") == 1:
            missing = []
            for key in (
                "alt_outcome_tp_first",
                "alt_pnl_tp_first",
                "alt_outcome_sl_first",
                "alt_pnl_sl_first",
                "candle_high",
                "candle_low",
            ):
                if key not in details:
                    missing.append(key)
            if missing:
                row_copy = dict(row)
                row_copy["_missing"] = ",".join(missing)
                bad_rows.append(row_copy)

    if bad_rows:
        for row in bad_rows[:20]:
            print(
                "BAD_ROW | time_utc={time_utc} | symbol={symbol} | outcome={outcome} | missing={missing}".format(
                    time_utc=row.get("outcome_time_utc", ""),
                    symbol=row.get("symbol", ""),
                    outcome=row.get("outcome", ""),
                    missing=row.get("_missing", ""),
                )
            )
        print(f"summary | rows_checked={len(checked)} | ok=0 | bad_count={len(bad_rows)}")
        return 1

    print(f"summary | rows_checked={len(checked)} | ok=1")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
