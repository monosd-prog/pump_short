from __future__ import annotations

import csv
import sys
from collections import deque
from datetime import datetime, timezone
from pathlib import Path


BAD_STRINGS = {"", "nan", "NaN", "None", "null"}


def _is_bad_value(value: object) -> bool:
    if value is None:
        return True
    s = str(value).strip()
    return s in BAD_STRINGS


def _parse_float(value: object) -> float | None:
    if _is_bad_value(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _print_usage() -> None:
    print("Usage: python scripts/smoke_events_v2_no_nan.py [YYYYMMDD] [--tail N]")


def main() -> int:
    date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
    tail = 200

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
        elif args[i].startswith("--"):
            _print_usage()
            return 2
        else:
            date_str = args[i]
            i += 1

    repo_root = Path(__file__).resolve().parents[1]
    rel_path = Path("datasets") / f"date={date_str}" / "strategy=short_pump" / "mode=live" / "events_v2.csv"
    path = repo_root / rel_path
    if not path.exists():
        print(f"events_v2.csv not found: {path}")
        return 2

    recent_rows: deque[dict[str, str]] = deque(maxlen=max(tail, 1))
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            recent_rows.append(row)

    recent_rows = list(recent_rows)
    checked = recent_rows[-tail:] if tail else recent_rows
    bad_rows: list[dict[str, str]] = []
    for row in checked:
        bad_fields: list[str] = []
        for field in ("dist_to_peak_pct", "price", "context_score"):
            value = row.get(field)
            parsed = _parse_float(value)
            if parsed is None:
                bad_fields.append(field)
        if bad_fields:
            row_copy = dict(row)
            row_copy["_bad_fields"] = ",".join(bad_fields)
            bad_rows.append(row_copy)

    if bad_rows:
        for row in bad_rows[:20]:
            print(
                "BAD_ROW | time_utc={time_utc} | symbol={symbol} | stage={stage} | skip_reasons={skip_reasons} | "
                "dist_to_peak_pct={dist_to_peak_pct} | price={price} | context_score={context_score} | bad={bad}".format(
                    time_utc=row.get("time_utc", ""),
                    symbol=row.get("symbol", ""),
                    stage=row.get("stage", ""),
                    skip_reasons=row.get("skip_reasons", ""),
                    dist_to_peak_pct=row.get("dist_to_peak_pct", ""),
                    price=row.get("price", ""),
                    context_score=row.get("context_score", ""),
                    bad=row.get("_bad_fields", ""),
                )
            )
        print(f"summary | rows_checked={len(recent_rows[-tail:])} | ok=0 | bad_count={len(bad_rows)}")
        return 1

    print(f"summary | rows_checked={len(checked)} | ok=1")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
