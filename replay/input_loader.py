from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, Optional

import pandas as pd


@dataclass(frozen=True)
class ReplaySignal:
    strategy: str
    symbol: str
    signal_time_utc: pd.Timestamp
    run_id: str
    event_id: str
    signal_type: str
    source: str
    meta: Dict[str, Any]


def _parse_ts(val: Any) -> pd.Timestamp:
    if isinstance(val, pd.Timestamp):
        ts = val
    else:
        ts = pd.Timestamp(str(val))
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return ts


def load_signals(path: str | Path) -> list[ReplaySignal]:
    """
    Load replay signals from CSV or JSONL.

    Required fields (case-insensitive):
      - strategy
      - symbol
      - signal_time_utc

    Optional:
      - run_id (default: generated per row)
      - event_id (default: generated)
      - signal_type (default: "entry")
      - source (default: "replay")
      - any extra columns -> meta
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(str(p))

    ext = p.suffix.lower()
    rows: list[dict] = []
    if ext in (".jsonl", ".ndjson"):
        for line in p.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            if not isinstance(obj, dict):
                continue
            rows.append(obj)
    else:
        with p.open("r", encoding="utf-8", newline="") as f:
            r = csv.DictReader(f)
            for row in r:
                rows.append(dict(row))

    out: list[ReplaySignal] = []
    for i, raw in enumerate(rows, 1):
        lower = {str(k).strip().lower(): v for k, v in raw.items()}
        strategy = (lower.get("strategy") or "").strip()
        symbol = (lower.get("symbol") or "").strip().upper()
        ts_raw = lower.get("signal_time_utc") or lower.get("time_utc") or lower.get("ts") or ""
        if not strategy or not symbol or not ts_raw:
            raise ValueError(f"Invalid signal row #{i}: missing strategy/symbol/signal_time_utc")
        ts = _parse_ts(ts_raw)

        run_id = str(lower.get("run_id") or lower.get("runid") or f"replay_{ts.strftime('%Y%m%d_%H%M%S')}_{i:06d}")
        event_id = str(lower.get("event_id") or lower.get("eventid") or f"{run_id}_{symbol}_{i:06d}")
        signal_type = str(lower.get("signal_type") or lower.get("type") or "entry")
        source = str(lower.get("source") or "replay")

        reserved = {"strategy", "symbol", "signal_time_utc", "time_utc", "ts", "run_id", "runid", "event_id", "eventid", "signal_type", "type", "source"}
        meta = {k: v for k, v in raw.items() if str(k).strip().lower() not in reserved}

        out.append(
            ReplaySignal(
                strategy=strategy,
                symbol=symbol,
                signal_time_utc=ts,
                run_id=run_id,
                event_id=event_id,
                signal_type=signal_type,
                source=source,
                meta=meta,
            )
        )
    return out

