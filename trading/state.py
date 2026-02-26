"""Trading state: open positions, last signal ids. Load/save to JSON."""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from trading.config import STATE_PATH

logger = logging.getLogger(__name__)

# State schema: open_positions[strategy][position_id] = position; last_signal_ids[strategy] = list of position_ids


def make_position_id(strategy: str, run_id: str, event_id: str, symbol: str) -> str:
    """Stable unique id per opened trade: strategy:run_id:event_id:symbol."""
    return f"{strategy}:{run_id or ''}:{event_id or ''}:{symbol or ''}"


def _ensure_dir(path: str) -> None:
    p = Path(path)
    if p.suffix:
        p = p.parent
    p.mkdir(parents=True, exist_ok=True)


def _is_legacy_position(val: Any) -> bool:
    """True if val is a single position dict (old schema)."""
    return isinstance(val, dict) and "entry" in val and "symbol" in val


def _migrate_open_positions(data: dict[str, Any]) -> dict[str, Any]:
    """Convert old schema open_positions[strategy]=position to new nested dict."""
    op = data.get("open_positions") or {}
    if not op:
        return op
    migrated: dict[str, dict[str, Any]] = {}
    for strategy, val in op.items():
        if _is_legacy_position(val):
            pos = val
            run_id = pos.get("run_id", "")
            event_id = pos.get("event_id", "") or ""
            symbol = pos.get("symbol", "")
            opened_ts = pos.get("opened_ts", "")
            if run_id or event_id:
                pid = make_position_id(strategy, run_id, event_id, symbol)
            else:
                pid = f"{strategy}:legacy:{symbol}:{opened_ts}"
            migrated.setdefault(strategy, {})[pid] = pos
        elif isinstance(val, dict):
            for pid, p in val.items():
                if isinstance(p, dict) and "entry" in p:
                    migrated.setdefault(strategy, {})[pid] = p
        else:
            continue
    return migrated


def _migrate_last_signal_ids(data: dict[str, Any]) -> dict[str, Any]:
    """Convert last_signal_ids[strategy]=str to list of position_ids."""
    ls = data.get("last_signal_ids") or {}
    out: dict[str, list[str]] = {}
    for strategy, val in ls.items():
        if isinstance(val, list):
            out[strategy] = [str(x) for x in val]
        elif isinstance(val, str) and val:
            out[strategy] = [val]
        else:
            out[strategy] = []
    return out


def load_state() -> dict[str, Any]:
    """Load state from STATE_PATH. Returns dict with open_positions (nested), last_signal_ids (list per strategy)."""
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        data = {"open_positions": {}, "last_signal_ids": {}}
    except Exception as e:
        logger.warning("trading state load failed: %s", e)
        data = {"open_positions": {}, "last_signal_ids": {}}
    data["open_positions"] = _migrate_open_positions(data)
    data["last_signal_ids"] = _migrate_last_signal_ids(data)
    return data


def save_state(data: dict[str, Any]) -> None:
    """Persist state to STATE_PATH."""
    _ensure_dir(STATE_PATH)
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def has_open_position(state: dict[str, Any], strategy: str) -> bool:
    """True if there is at least one open position for this strategy."""
    op = state.get("open_positions") or {}
    strat_pos = op.get(strategy) or {}
    return bool(isinstance(strat_pos, dict) and strat_pos)


def count_open_positions(state: dict[str, Any], strategy: str | None = None) -> int:
    """Count open positions. If strategy given, count for that strategy only; else total."""
    op = state.get("open_positions") or {}
    if strategy:
        strat_pos = op.get(strategy) or {}
        return len(strat_pos) if isinstance(strat_pos, dict) else 0
    n = 0
    for strat_pos in (op or {}).values():
        if isinstance(strat_pos, dict):
            n += len(strat_pos)
    return n


def total_risk_usd(state: dict[str, Any]) -> float:
    """Sum risk_usd across all open positions."""
    op = state.get("open_positions") or {}
    total = 0.0
    for strat_pos in (op or {}).values():
        if isinstance(strat_pos, dict):
            for p in strat_pos.values():
                if isinstance(p, dict):
                    total += float(p.get("risk_usd", 0) or 0)
    return total


def record_open(state: dict[str, Any], position: dict[str, Any]) -> str | None:
    """Add position under open_positions[strategy][position_id]. Returns position_id or None."""
    strategy = position.get("strategy")
    if not strategy:
        logger.warning("record_open: position missing strategy, skip")
        return None
    run_id = position.get("run_id", "")
    event_id = position.get("event_id", "") or ""
    symbol = position.get("symbol", "")
    opened_ts = position.get("opened_ts", "")
    if run_id or event_id:
        pid = make_position_id(strategy, run_id, event_id, symbol)
    else:
        pid = f"{strategy}:legacy:{symbol}:{opened_ts}"
    state.setdefault("open_positions", {}).setdefault(strategy, {})[pid] = position
    return pid


def _date_from_ts_utc(ts_utc: str) -> str:
    """Extract YYYY-MM-DD from ts_utc (ISO or space-separated)."""
    if not ts_utc or not str(ts_utc).strip():
        return ""
    s = str(ts_utc).strip()
    if "T" in s:
        part = s.split("T")[0]
    elif " " in s:
        part = s.split(" ")[0]
    else:
        part = s[:10]
    if len(part) >= 10 and part[4] == "-" and part[7] == "-":
        return part[:10]
    return ""


def get_daily_realized_pnl_usd(state: dict[str, Any], date_utc: str) -> float:
    """Cumulative realized PnL in USD for the given UTC date (YYYY-MM-DD)."""
    daily = state.get("daily_realized_pnl_usd") or {}
    if not isinstance(daily, dict):
        return 0.0
    return float(daily.get(date_utc, 0) or 0)


def update_daily_pnl(state: dict[str, Any], ts_utc: str, pnl_usd: float) -> None:
    """Add pnl_usd to daily_realized_pnl_usd for the date in ts_utc."""
    date_key = _date_from_ts_utc(ts_utc)
    if not date_key:
        return
    state.setdefault("daily_realized_pnl_usd", {})
    if not isinstance(state["daily_realized_pnl_usd"], dict):
        state["daily_realized_pnl_usd"] = {}
    state["daily_realized_pnl_usd"][date_key] = (
        state["daily_realized_pnl_usd"].get(date_key, 0) + pnl_usd
    )


def record_close(
    state: dict[str, Any],
    strategy: str,
    position_id: str,
    close_reason: str,
    exit_price: float,
    pnl_r: float,
    pnl_usd: float,
    ts_utc: str,
) -> None:
    """Remove position_id from open_positions[strategy]. Remove strategy key if empty."""
    op = state.get("open_positions") or {}
    strat_pos = op.get(strategy) or {}
    if isinstance(strat_pos, dict) and position_id in strat_pos:
        del strat_pos[position_id]
        if not strat_pos:
            op.pop(strategy, None)
    update_daily_pnl(state, ts_utc, pnl_usd)
    logger.info(
        "record_close | strategy=%s position_id=%s reason=%s exit=%.4f pnl_r=%.2f pnl_usd=%.2f ts=%s",
        strategy, position_id, close_reason, exit_price, pnl_r, pnl_usd, ts_utc,
    )
