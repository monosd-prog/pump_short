#!/usr/bin/env python3
"""
Paper execution dry-run: read events from CSV (last N minutes), run gating only, print accepted/rejected counts.

Usage:
  python3 scripts/run_paper_dry_run.py --root /path/to/datasets [--minutes 60]
  python3 scripts/run_paper_dry_run.py --events /path/to/events_v3.csv --minutes 120

Finds events_v3.csv (or events.csv, events_v2.csv) under root/date=*/strategy=*/ if --events not given.
Uses time_utc / wall_time_utc / ts for filtering. No pandas in execution path.
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, List, Optional, Tuple

# Project root
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from trading.broker import allow_entry


def _parse_ts(val: str) -> Optional[datetime]:
    if not val or not str(val).strip():
        return None
    s = str(val).strip().replace("Z", "+00:00")
    for fmt in (
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
    ):
        try:
            dt = datetime.strptime(s, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


def _float_or_none(val: Any) -> Optional[float]:
    if val is None or (isinstance(val, str) and not val.strip()):
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _int_or_none(val: Any) -> Optional[int]:
    if val is None or (isinstance(val, str) and not val.strip()):
        return None
    try:
        return int(float(val))
    except (TypeError, ValueError):
        return None


class _MinimalSignal:
    """Minimal signal-like object for gating (no pandas)."""
    __slots__ = ("strategy", "symbol", "ts_utc", "stage", "dist_to_peak_pct", "liq_long_usd_30s")

    def __init__(
        self,
        strategy: str,
        symbol: str,
        ts_utc: str,
        stage: Optional[int],
        dist_to_peak_pct: Optional[float],
        liq_long_usd_30s: Optional[float],
    ):
        self.strategy = strategy
        self.symbol = symbol
        self.ts_utc = ts_utc
        self.stage = stage
        self.dist_to_peak_pct = dist_to_peak_pct
        self.liq_long_usd_30s = liq_long_usd_30s


def _find_events_paths(root: Path, limit: int = 50) -> List[Path]:
    """Find events_v3.csv, events.csv, events_v2.csv under root/date=*/strategy=*."""
    candidates = ["events_v3.csv", "events.csv", "events_v2.csv"]
    found: List[Path] = []
    for date_dir in sorted(root.glob("date=*"), reverse=True):
        if not date_dir.is_dir():
            continue
        for strat_dir in date_dir.glob("strategy=*"):
            if not strat_dir.is_dir():
                continue
            for name in candidates:
                p = strat_dir / name
                if p.exists():
                    found.append(p)
                    if len(found) >= limit:
                        return found
    return found


def _read_events_csv(path: Path, time_cols: Tuple[str, ...]) -> Tuple[List[dict], List[str]]:
    """Read CSV; return (list of row dicts, column names)."""
    rows: List[dict] = []
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        for row in reader:
            rows.append(dict(row))
    return rows, fieldnames


def _pick_time_col(row: dict, time_cols: Tuple[str, ...]) -> Optional[str]:
    for c in time_cols:
        if c in row and row.get(c):
            return str(row[c]).strip()
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description="Paper dry-run: gating on events file, print accepted/rejected counts")
    parser.add_argument("--root", type=str, default=None, help="Datasets root (with date=*/strategy=*)")
    parser.add_argument("--events", type=str, default=None, help="Path to events CSV (overrides --root scan)")
    parser.add_argument("--minutes", type=int, default=60, help="Use events from last N minutes (default 60)")
    args = parser.parse_args()

    paths: List[Path] = []
    if args.events:
        p = Path(args.events)
        if not p.exists():
            print(f"ERROR: events file not found: {p}", file=sys.stderr)
            return 2
        paths = [p]
    elif args.root:
        root = Path(args.root).resolve()
        if not root.exists():
            print(f"ERROR: root not found: {root}", file=sys.stderr)
            return 2
        paths = _find_events_paths(root)
        if not paths:
            print(f"WARNING: no events CSV under {root}", file=sys.stderr)
            return 0
    else:
        print("ERROR: pass --root or --events", file=sys.stderr)
        return 2

    time_cols = ("time_utc", "wall_time_utc", "ts", "time", "timestamp", "created_at")
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=args.minutes)
    accepted = 0
    rejected_by_reason: dict[str, int] = defaultdict(int)
    total = 0

    for path in paths:
        rows, _ = _read_events_csv(path, time_cols)
        strategy_from_path = ""
        for part in path.parts:
            if part.startswith("strategy="):
                strategy_from_path = part.split("=", 1)[1]
                break
        for row in rows:
            ts_val = _pick_time_col(row, time_cols)
            if not ts_val:
                continue
            dt = _parse_ts(ts_val)
            if dt is None or dt < cutoff:
                continue
            strategy = row.get("strategy", "") or strategy_from_path
            symbol = row.get("symbol", "") or row.get("ticker", "")
            if not strategy or not symbol:
                continue
            total += 1
            stage = _int_or_none(row.get("stage"))
            dist = _float_or_none(row.get("dist_to_peak_pct") or row.get("dist_to_peak"))
            liq = _float_or_none(row.get("liq_long_usd_30s"))
            sig = _MinimalSignal(
                strategy=strategy,
                symbol=symbol,
                ts_utc=ts_val or "",
                stage=stage,
                dist_to_peak_pct=dist,
                liq_long_usd_30s=liq,
            )
            ok, reason = allow_entry(sig)
            if ok:
                accepted += 1
            else:
                rejected_by_reason[reason or "unknown"] += 1

    print("PAPER DRY-RUN (gating only)")
    print(f"  Total events (last {args.minutes} min): {total}")
    print(f"  Accepted: {accepted}")
    print("  Rejected by reason:")
    for reason, count in sorted(rejected_by_reason.items(), key=lambda x: -x[1]):
        print(f"    {reason}: {count}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
