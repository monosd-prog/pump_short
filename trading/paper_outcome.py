"""Close paper positions from OUTCOME events (TP_hit/SL_hit) and TTL timeout."""
from __future__ import annotations

import csv
import logging
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal, Optional

from trading.config import CLOSES_PATH, POSITION_TTL_SECONDS, TIMEOUT_EXIT_MODE
from trading.paper import simulate_close
from trading.state import load_state, make_position_id, record_close, save_state

logger = logging.getLogger(__name__)

CLOSES_FULL_HEADER = [
    "ts", "strategy", "symbol", "side", "entry", "tp", "sl", "exit_price",
    "close_reason", "pnl_r", "pnl_usd", "run_id", "event_id",
    "mfe_pct", "mae_pct", "mfe_r", "mae_r",
]
CLOSES_EXTRA_COLUMNS = ["mfe_pct", "mae_pct", "mfe_r", "mae_r"]


def _ensure_dir(path: str) -> None:
    p = Path(path)
    if p.suffix:
        p = p.parent
    p.mkdir(parents=True, exist_ok=True)


def _ensure_closes_header(path: str) -> Literal["created", "migrated", "ok"]:
    """
    Ensure trading_closes CSV has full header including mfe_pct, mae_pct, mfe_r, mae_r.
    - If file does not exist → create with full header.
    - If exists and header lacks mfe_pct → backup to path.bak_TIMESTAMP, rewrite with extended header and rows.
    - If already correct → do nothing.
    Returns "created" | "migrated" | "ok".
    """
    p = Path(path)
    _ensure_dir(path)
    if not p.exists():
        with open(p, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(CLOSES_FULL_HEADER)
        return "created"
    with open(p, "r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        first = next(reader, None)
    if first is None:
        with open(p, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(CLOSES_FULL_HEADER)
        return "created"
    header = first
    if "mfe_pct" in header:
        return "ok"
    bak = f"{path}.bak_{int(time.time())}"
    shutil.copy2(p, bak)
    logger.info("trading_closes header migration: backup %s -> %s", path, bak)
    new_header = list(header) + list(CLOSES_EXTRA_COLUMNS)
    rows = [new_header]
    with open(p, "r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            while len(row) < len(new_header):
                row.append("")
            rows.append(row[: len(new_header)])
    with open(p, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerows(rows)
    return "migrated"


def _append_close_row(
    ts: str,
    strategy: str,
    symbol: str,
    side: str,
    entry: float,
    tp: float,
    sl: float,
    exit_price: float,
    close_reason: str,
    pnl_r: float,
    pnl_usd: float,
    run_id: str,
    event_id: str,
    log_path: str,
    *,
    mfe_pct: float | None = None,
    mae_pct: float | None = None,
    mfe_r: float | None = None,
    mae_r: float | None = None,
) -> None:
    _ensure_closes_header(log_path)
    file_exists = Path(log_path).exists()
    with open(log_path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if not file_exists:
            w.writerow(CLOSES_FULL_HEADER)
        mfe_pct_str = f"{float(mfe_pct):.4f}" if mfe_pct is not None else ""
        mae_pct_str = f"{float(mae_pct):.4f}" if mae_pct is not None else ""
        mfe_r_str = f"{float(mfe_r):.4f}" if mfe_r is not None else ""
        mae_r_str = f"{float(mae_r):.4f}" if mae_r is not None else ""
        w.writerow([
            ts,
            strategy,
            symbol,
            side,
            f"{entry:.6f}",
            f"{tp:.6f}",
            f"{sl:.6f}",
            f"{exit_price:.6f}",
            close_reason,
            f"{pnl_r:.4f}",
            f"{pnl_usd:.2f}",
            run_id,
            event_id or "",
            mfe_pct_str,
            mae_pct_str,
            mfe_r_str,
            mae_r_str,
        ])


def _find_position_for_outcome(
    open_positions: dict[str, Any],
    strategy: str,
    symbol: str,
    run_id: str,
    event_id: str,
) -> tuple[str, dict[str, Any]] | None:
    """
    Find matching open position for outcome.
    Prefer exact position_id match; fallback search by symbol+run_id/event_id, pick newest opened_ts.
    Returns (position_id, position) or None.
    """
    strat_pos = open_positions.get(strategy) or {}
    if not isinstance(strat_pos, dict):
        return None
    pid = make_position_id(strategy, run_id, event_id, symbol)
    if pid in strat_pos:
        return pid, strat_pos[pid]
    candidates: list[tuple[str, dict[str, Any], str]] = []
    for k, p in strat_pos.items():
        if not isinstance(p, dict):
            continue
        if p.get("symbol") != symbol:
            continue
        if run_id and p.get("run_id") != run_id:
            continue
        if event_id and (p.get("event_id") or "") != event_id:
            continue
        candidates.append((k, p, p.get("opened_ts", "")))
    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0][0], candidates[0][1]
    candidates.sort(key=lambda x: x[2], reverse=True)
    return candidates[0][0], candidates[0][1]


def _mfe_mae_r_from_pct(
    entry: float,
    sl: float,
    side: str,
    mfe_pct: float | None,
    mae_pct: float | None,
) -> tuple[float | None, float | None]:
    """Compute mfe_r, mae_r from mfe_pct/mae_pct using risk_per_unit = abs(entry - sl). R = pct / (risk_pct)."""
    if entry <= 0:
        return None, None
    risk_abs = abs(float(entry) - float(sl))
    if risk_abs <= 0:
        return None, None
    risk_pct = risk_abs / entry * 100.0
    mfe_r = (float(mfe_pct) / risk_pct) if mfe_pct is not None else None
    mae_r = (float(mae_pct) / risk_pct) if mae_pct is not None else None
    return mfe_r, mae_r


def close_from_outcome(
    strategy: str,
    symbol: str,
    run_id: str,
    event_id: str,
    res: str,
    pnl_pct: float | None,
    ts_utc: str,
    *,
    state_path: Optional[str] = None,
    log_path: Optional[str] = None,
    outcome_meta: Optional[dict[str, Any]] = None,
) -> bool:
    """
    Close paper position when OUTCOME arrives (TP_hit or SL_hit).
    outcome_meta may contain mfe_pct, mae_pct (and optionally mfe_r, mae_r). If only pct provided, R is computed from entry/sl.
    Returns True if closed, False if not found / wrong res.
    """
    if res not in ("TP_hit", "SL_hit"):
        logger.debug("close_from_outcome: skip res=%s (not TP_hit/SL_hit)", res)
        return False

    close_reason = "tp" if res == "TP_hit" else "sl"
    state = load_state()
    open_positions = state.get("open_positions") or {}
    found = _find_position_for_outcome(open_positions, strategy, symbol, run_id, event_id)
    if found is None:
        logger.warning("close_from_outcome: no matching position strategy=%s symbol=%s run_id=%s event_id=%s", strategy, symbol, run_id, event_id)
        return False

    position_id, position = found
    entry = float(position["entry"])
    sl = float(position["sl"])
    side = (position.get("side") or "SHORT").strip().upper()
    exit_price = float(position["tp"]) if res == "TP_hit" else sl
    pnl_r, pnl_usd = simulate_close(position, exit_price, close_reason, ts_utc)
    record_close(state, strategy, position_id, close_reason, exit_price, pnl_r, pnl_usd, ts_utc)
    save_state(state)

    meta = outcome_meta or {}
    mfe_pct = meta.get("mfe_pct")
    mae_pct = meta.get("mae_pct")
    mfe_r = meta.get("mfe_r")
    mae_r = meta.get("mae_r")
    if mfe_r is None or mae_r is None:
        _mfe_r, _mae_r = _mfe_mae_r_from_pct(entry, sl, side, mfe_pct, mae_pct)
        if mfe_r is None:
            mfe_r = _mfe_r
        if mae_r is None:
            mae_r = _mae_r

    path = log_path or CLOSES_PATH
    _append_close_row(
        ts=ts_utc,
        strategy=strategy,
        symbol=symbol,
        side=position.get("side", "SHORT"),
        entry=entry,
        tp=position["tp"],
        sl=position["sl"],
        exit_price=exit_price,
        close_reason=close_reason,
        pnl_r=pnl_r,
        pnl_usd=pnl_usd,
        run_id=run_id,
        event_id=event_id or "",
        log_path=path,
        mfe_pct=mfe_pct,
        mae_pct=mae_pct,
        mfe_r=mfe_r,
        mae_r=mae_r,
    )
    logger.info(
        "close_from_outcome: closed strategy=%s symbol=%s position_id=%s reason=%s exit=%.4f pnl_r=%.2f pnl_usd=%.2f",
        strategy, symbol, position_id, close_reason, exit_price, pnl_r, pnl_usd,
    )
    return True


def _parse_opened_ts(opened_ts: str) -> datetime | None:
    """Parse opened_ts (ISO or common str) to UTC-aware datetime. Return None on failure."""
    if not opened_ts or not opened_ts.strip():
        return None
    s = opened_ts.strip()
    for fmt in (
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S",
    ):
        try:
            s_parse = s.replace("Z", "+00:00") if "Z" in fmt else s
            dt = datetime.strptime(s_parse, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00").replace("+0000", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


def close_on_timeout(
    state: dict[str, Any],
    now_ts_utc: str,
    *,
    log_path: Optional[str] = None,
    ttl_seconds: Optional[int] = None,
) -> bool:
    """
    Close open positions older than POSITION_TTL_SECONDS (or ttl_seconds if given).
    Iterate all open_positions[strategy][position_id] safely.
    Returns True if any closed.
    """
    from trading.config import CLOSES_PATH as _closes_path

    ttl = POSITION_TTL_SECONDS if ttl_seconds is None else ttl_seconds
    now_dt = _parse_opened_ts(now_ts_utc) or datetime.now(timezone.utc)
    open_positions = state.get("open_positions") or {}
    path = log_path or _closes_path
    any_closed = False

    for strategy, strat_pos in list(open_positions.items()):
        if not isinstance(strat_pos, dict):
            continue
        for position_id, position in list(strat_pos.items()):
            if not isinstance(position, dict):
                continue
            opened_ts = position.get("opened_ts", "")
            opened_dt = _parse_opened_ts(opened_ts)
            if opened_dt is None:
                logger.debug("close_on_timeout: skip strategy=%s position_id=%s unparseable opened_ts=%s", strategy, position_id, opened_ts)
                continue
            age_sec = (now_dt - opened_dt).total_seconds()
            if age_sec < ttl:
                continue
            exit_mode = TIMEOUT_EXIT_MODE if TIMEOUT_EXIT_MODE in ("entry", "sl") else "entry"
            exit_price = float(position["entry"]) if exit_mode == "entry" else float(position["sl"])
            ts_str = now_dt.strftime("%Y-%m-%d %H:%M:%S+00:00")
            pnl_r, pnl_usd = simulate_close(position, exit_price, "timeout", ts_str)
            record_close(state, strategy, position_id, "timeout", exit_price, pnl_r, pnl_usd, ts_str)
            _append_close_row(
                ts=ts_str,
                strategy=strategy,
                symbol=position.get("symbol", ""),
                side=position.get("side", "SHORT"),
                entry=position["entry"],
                tp=position["tp"],
                sl=position["sl"],
                exit_price=exit_price,
                close_reason="timeout",
                pnl_r=pnl_r,
                pnl_usd=pnl_usd,
                run_id=position.get("run_id", ""),
                event_id=position.get("event_id", "") or "",
                log_path=path,
                mfe_pct=0.0,
                mae_pct=0.0,
                mfe_r=0.0,
                mae_r=0.0,
            )
            logger.info(
                "close_on_timeout: closed strategy=%s symbol=%s position_id=%s age_sec=%.0f exit_mode=%s pnl_r=%.2f",
                strategy, position.get("symbol", ""), position_id, age_sec, exit_mode, pnl_r,
            )
            any_closed = True

    return any_closed
