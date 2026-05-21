"""Append pre-run Signal to CSV (no execution)."""
from __future__ import annotations

import csv
import pathlib
from datetime import datetime, timezone
from typing import Optional

from pump_v2.core.strategy_base import Signal

_CSV_PATH = pathlib.Path("datasets/prerun_signals_v2.csv")

_FIELDS = [
    "ts_utc",
    "symbol",
    "strategy",
    "side",
    "entry_price",
    "tp_price",
    "sl_price",
    "notional_usd",
    "leverage",
    "risk_profile",
    "stage",
    "dist_to_peak_pct",
    "context_score",
    "funding_rate_abs",
]


def log_prerun_signal(signal: Optional[Signal], symbol: str) -> None:
    """Append signal row to CSV. If signal is None, skip (no-signal not logged)."""
    if signal is None:
        return
    _CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    write_header = not _CSV_PATH.exists()
    row = {
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "symbol": symbol,
        "strategy": signal.strategy,
        "side": signal.side,
        "entry_price": signal.entry_price,
        "tp_price": signal.tp_price,
        "sl_price": signal.sl_price,
        "notional_usd": signal.notional_usd,
        "leverage": signal.leverage,
        **signal.metadata,
    }
    with _CSV_PATH.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=_FIELDS, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerow(row)
