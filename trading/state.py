"""Trading state: open positions, last signal ids. Load/save to JSON."""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from trading.config import STATE_PATH

logger = logging.getLogger(__name__)

# State schema: open_positions[strategy] = position dict; last_signal_ids[strategy] = str (dedupe key)


def _ensure_dir(path: str) -> None:
    p = Path(path)
    if p.suffix:
        p = p.parent
    p.mkdir(parents=True, exist_ok=True)


def load_state() -> dict[str, Any]:
    """Load state from STATE_PATH. Returns dict with open_positions, last_signal_ids."""
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        data = {"open_positions": {}, "last_signal_ids": {}}
    except Exception as e:
        logger.warning("trading state load failed: %s", e)
        data = {"open_positions": {}, "last_signal_ids": {}}
    if "open_positions" not in data:
        data["open_positions"] = {}
    if "last_signal_ids" not in data:
        data["last_signal_ids"] = {}
    return data


def save_state(data: dict[str, Any]) -> None:
    """Persist state to STATE_PATH."""
    _ensure_dir(STATE_PATH)
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def has_open_position(state: dict[str, Any], strategy: str) -> bool:
    """True if there is an open position for this strategy."""
    return strategy in (state.get("open_positions") or {})


def record_open(state: dict[str, Any], position: dict[str, Any]) -> None:
    """Add position to open_positions keyed by strategy. Overwrites if same strategy."""
    strategy = position.get("strategy")
    if not strategy:
        logger.warning("record_open: position missing strategy, skip")
        return
    state.setdefault("open_positions", {})[strategy] = position


def record_close(
    state: dict[str, Any],
    strategy: str,
    close_reason: str,
    exit_price: float,
    pnl_r: float,
    pnl_usd: float,
    ts_utc: str,
) -> None:
    """Remove open position for strategy and optionally record close metadata (e.g. for CSV)."""
    open_positions = state.get("open_positions") or {}
    if strategy in open_positions:
        del open_positions[strategy]
    # Close details are written to CSV by runner; state only drops the position
    logger.info(
        "record_close | strategy=%s reason=%s exit=%.4f pnl_r=%.2f pnl_usd=%.2f ts=%s",
        strategy, close_reason, exit_price, pnl_r, pnl_usd, ts_utc,
    )
