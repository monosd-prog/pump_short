from __future__ import annotations

import csv
import json
import sys
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
import os


def _parse_float(value: object) -> float | None:
    if value is None:
        return None
    s = str(value).strip()
    if s in ("", "nan", "NaN", "None", "null"):
        return None
    try:
        return float(s)
    except (TypeError, ValueError):
        return None


def _print_usage() -> None:
    print("Usage: python scripts/smoke_long_outcomes_no_zero_hold.py [YYYYMMDD] [--tail N]")


def resolve_root() -> Path:
    env_root = os.getenv("DATASETS_ROOT")
    if env_root:
        root = Path(env_root).expanduser()
    else:
        root = Path(__file__).resolve().parents[1] / "datasets"
    if (root / "datasets").exists():
        root = root / "datasets"
    if os.getenv("DEBUG_SMOKE") == "1":
        print(f"DEBUG_SMOKE | datasets_root={root}")
    return root


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

    root = resolve_root()
    rel_path = Path(f"date={date_str}") / "strategy=long_pullback" / "mode=live" / "outcomes_v2.csv"
    path = root / rel_path
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

    zero_rows: list[dict[str, str]] = []
    tp_sl_zero_rows: list[dict[str, str]] = []
    for row in checked:
        hold_val = _parse_float(row.get("hold_seconds"))
        if hold_val is None:
            continue
        if hold_val == 0.0:
            zero_rows.append(row)
            details = row.get("details_json") or ""
            tp_hit = False
            sl_hit = False
            if details:
                try:
                    details_obj = json.loads(details)
                    tp_hit = bool(details_obj.get("tp_hit"))
                    sl_hit = bool(details_obj.get("sl_hit"))
                except Exception:
                    tp_hit = False
                    sl_hit = False
            if tp_hit or sl_hit:
                tp_sl_zero_rows.append(row)

    fail = False
    if tp_sl_zero_rows:
        fail = True
    elif checked:
        zero_share = len(zero_rows) / len(checked)
        if zero_share >= 0.05:
            fail = True

    if fail:
        sample = tp_sl_zero_rows if tp_sl_zero_rows else zero_rows
        for row in sample[:20]:
            print(
                "BAD_ROW | time_utc={time_utc} | symbol={symbol} | stage={stage} | skip_reasons={skip_reasons} | "
                "hold_seconds={hold_seconds} | outcome={outcome} | details_json={details}".format(
                    time_utc=row.get("outcome_time_utc", ""),
                    symbol=row.get("symbol", ""),
                    stage=row.get("stage", ""),
                    skip_reasons=row.get("skip_reasons", ""),
                    hold_seconds=row.get("hold_seconds", ""),
                    outcome=row.get("outcome", ""),
                    details=row.get("details_json", ""),
                )
            )
        print(
            "summary | rows_checked={rows_checked} | ok=0 | zero_count={zero_count} | tp_sl_zero_count={tp_sl_zero_count}".format(
                rows_checked=len(checked),
                zero_count=len(zero_rows),
                tp_sl_zero_count=len(tp_sl_zero_rows),
            )
        )
        return 1

    print(
        "summary | rows_checked={rows_checked} | ok=1 | zero_count={zero_count} | tp_sl_zero_count={tp_sl_zero_count}".format(
            rows_checked=len(checked),
            zero_count=len(zero_rows),
            tp_sl_zero_count=len(tp_sl_zero_rows),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
