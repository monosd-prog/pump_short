"""Signal queue: enqueue_signal writes one JSONL line per Signal. Option A: wire at ENTRY_OK send path."""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING

from typing import Any

from trading.config import SIGNALS_QUEUE_PATH
from trading.signal_io import signal_to_dict

if TYPE_CHECKING:
    from short_pump.signals import Signal

logger = logging.getLogger(__name__)


def _ensure_dir(path: str) -> None:
    p = Path(path)
    if p.suffix:
        p = p.parent
    p.mkdir(parents=True, exist_ok=True)


def enqueue_signal_dict(
    data: dict[str, Any],
    queue_path: str | None = None,
    *,
    fsync: bool = False,
) -> None:
    """Append one queue row from a pre-built dict (e.g. pump_v2 adapter)."""
    path = queue_path if queue_path is not None else SIGNALS_QUEUE_PATH
    _ensure_dir(path)
    line = json.dumps(data, ensure_ascii=False) + "\n"
    with open(path, "a", encoding="utf-8") as f:
        f.write(line)
        if fsync:
            f.flush()
            import os

            os.fsync(f.fileno())
    logger.debug(
        "enqueue_signal_dict | strategy=%s symbol=%s run_id=%s event_id=%s source=%s",
        data.get("strategy"),
        data.get("symbol"),
        data.get("run_id"),
        data.get("event_id"),
        data.get("source"),
    )


def enqueue_signal(signal: "Signal", queue_path: str | None = None, *, fsync: bool = False) -> None:
    """
    Append one Signal as a JSON line to the queue file.
    Includes stable dedupe key fields: strategy, symbol, side, run_id, event_id, ts_utc.
    UTF-8, append mode. If enqueue fails, caller should log and continue (do not crash loop).
    """
    path = queue_path if queue_path is not None else SIGNALS_QUEUE_PATH
    _ensure_dir(path)
    d = signal_to_dict(signal)
    line = json.dumps(d, ensure_ascii=False) + "\n"
    with open(path, "a", encoding="utf-8") as f:
        f.write(line)
        if fsync:
            f.flush()
            import os
            os.fsync(f.fileno())
    logger.debug(
        "enqueue_signal | strategy=%s symbol=%s side=%s run_id=%s event_id=%s ts_utc=%s",
        signal.strategy, signal.symbol, getattr(signal, "side", ""),
        getattr(signal, "run_id", ""), getattr(signal, "event_id", ""), getattr(signal, "ts_utc", ""),
    )
