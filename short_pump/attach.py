from __future__ import annotations

import logging
import threading
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from short_pump.config import Config

from trading.state import load_state, save_state

logger = logging.getLogger(__name__)


def _find_position_in_state(state: dict[str, Any], position_id: str) -> Optional[tuple[str, dict[str, Any]]]:
    op = state.get("open_positions") or {}
    for strat, positions in (op or {}).items():
        if isinstance(positions, dict) and position_id in positions:
            return strat, positions[position_id]
    return None


def _run_tracker_background(cfg: Config, pos: dict[str, Any]) -> None:
    # intentionally left minimal: A3-lite avoids relying on background start as primary confirmation.
    # This helper is deprecated for A3-lite; do not use background start as guarantee.
    logger.debug("attach._run_tracker_background called for pos, but A3-lite avoids background guarantees")


def attach_outcome_monitor(position_id: str, metadata: Dict[str, Any], state: Optional[dict[str, Any]] = None) -> Optional[str]:
    """
    Idempotent: ensure open position has outcome_attached flag and persist durable ownership evidence.
    If `state` is provided, mutate it in-place and persist; otherwise load/save internally.
    Returns outcome_tracker_id or None.
    """
    internal_load = False
    if state is None:
        state = load_state()
        internal_load = True
    found = _find_position_in_state(state, position_id)
    if not found:
        logger.warning("attach_outcome_monitor: position not found in state: %s", position_id)
        return None
    strat, pos = found
    # guard: only paper positions for SP
    if pos.get("mode") != "paper" or (pos.get("strategy") or "") not in {"short_pump", "short_pump_filtered"}:
        logger.debug("attach_outcome_monitor: skip non-paper/other-strategy pos=%s", position_id)
        return None

    if pos.get("outcome_attached"):
        return pos.get("outcome_tracker_id") or "attached"

    # mark attached metadata
    tracker_id = f"runner:{datetime.now(timezone.utc).isoformat()}"
    pos["outcome_attached"] = True
    pos["outcome_tracker_id"] = tracker_id
    pos["attach_timestamp"] = datetime.now(timezone.utc).isoformat()
    pos["attach_by"] = metadata.get("attach_by", "runner")
    # propagate minimal tracker config if provided
    if "outcome_watch_minutes" in metadata:
        pos.setdefault("outcome_watch_minutes", metadata.get("outcome_watch_minutes"))
    if "outcome_poll_seconds" in metadata:
        pos.setdefault("outcome_poll_seconds", metadata.get("outcome_poll_seconds"))

    # persist atomically: save provided state or saved internal state
    try:
        save_state(state)
    except Exception:
        logger.exception("attach_outcome_monitor: save_state failed for position %s", position_id)
        return None

    # IMPORTANT: A3-lite does NOT rely on background best-effort start as the guarantee.
    # Return durable tracker_id as confirmation that ownership evidence is persisted.
    return tracker_id

