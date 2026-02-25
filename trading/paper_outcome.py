"""Close paper positions from OUTCOME events (TP_hit/SL_hit)."""
from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import Optional

from trading.config import CLOSES_PATH
from trading.paper import simulate_close
from trading.state import load_state, record_close, save_state

logger = logging.getLogger(__name__)


def _ensure_dir(path: str) -> None:
    p = Path(path)
    if p.suffix:
        p = p.parent
    p.mkdir(parents=True, exist_ok=True)


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
) -> None:
    _ensure_dir(log_path)
    file_exists = Path(log_path).exists()
    with open(log_path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if not file_exists:
            w.writerow([
                "ts", "strategy", "symbol", "side", "entry", "tp", "sl", "exit_price",
                "close_reason", "pnl_r", "pnl_usd", "run_id", "event_id",
            ])
        w.writerow([
            ts, strategy, symbol, side, f"{entry:.6f}", f"{tp:.6f}", f"{sl:.6f}",
            f"{exit_price:.6f}", close_reason, f"{pnl_r:.4f}", f"{pnl_usd:.2f}",
            run_id, event_id or "",
        ])


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
) -> bool:
    """
    Close paper position when OUTCOME arrives (TP_hit or SL_hit).
    res: end_reason from outcome (TP_hit -> tp, SL_hit -> sl)
    exit_price: TP_hit -> tp, SL_hit -> sl
    Returns True if closed, False if skipped (no match, wrong res, etc).
    """
    if res not in ("TP_hit", "SL_hit"):
        logger.debug("close_from_outcome: skip res=%s (not TP_hit/SL_hit)", res)
        return False

    close_reason = "tp" if res == "TP_hit" else "sl"
    state = load_state()
    open_positions = state.get("open_positions") or {}
    if strategy not in open_positions:
        logger.debug("close_from_outcome: no open position strategy=%s", strategy)
        return False

    position = open_positions[strategy]
    pos_symbol = position.get("symbol", "")
    pos_run_id = position.get("run_id", "")
    pos_event_id = position.get("event_id", "")
    if pos_symbol != symbol:
        logger.warning("close_from_outcome: symbol mismatch strategy=%s pos=%s outcome=%s", strategy, pos_symbol, symbol)
        return False
    # Optional: match run_id/event_id; allow loose match for robustness
    if pos_run_id and run_id and pos_run_id != run_id:
        logger.debug("close_from_outcome: run_id mismatch strategy=%s, skip", strategy)
        return False

    exit_price = float(position["tp"]) if res == "TP_hit" else float(position["sl"])
    pnl_r, pnl_usd = simulate_close(position, exit_price, close_reason, ts_utc)
    record_close(state, strategy, close_reason, exit_price, pnl_r, pnl_usd, ts_utc)
    save_state(state)

    path = log_path or CLOSES_PATH
    _append_close_row(
        ts=ts_utc,
        strategy=strategy,
        symbol=symbol,
        side=position.get("side", "SHORT"),
        entry=position["entry"],
        tp=position["tp"],
        sl=position["sl"],
        exit_price=exit_price,
        close_reason=close_reason,
        pnl_r=pnl_r,
        pnl_usd=pnl_usd,
        run_id=run_id,
        event_id=event_id or "",
        log_path=path,
    )
    logger.info(
        "close_from_outcome: closed strategy=%s symbol=%s reason=%s exit=%.4f pnl_r=%.2f pnl_usd=%.2f",
        strategy, symbol, close_reason, exit_price, pnl_r, pnl_usd,
    )
    return True
