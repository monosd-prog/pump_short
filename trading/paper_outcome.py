"""Close paper positions from OUTCOME events (TP_hit/SL_hit) and TTL timeout."""
from __future__ import annotations

import csv
import json
import logging
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal, Optional

from trading.config import CLOSES_PATH, DATASET_BASE_DIR, POSITION_TTL_SECONDS, TIMEOUT_EXIT_MODE
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


def close_from_live_outcome(
    strategy: str,
    symbol: str,
    run_id: str,
    event_id: str,
    res: str,
    exit_price: float,
    pnl_pct: float,
    ts_utc: str,
    *,
    log_path: Optional[str] = None,
    state: Optional[dict[str, Any]] = None,
) -> bool:
    """
    Close LIVE position from Bybit-resolved outcome. Uses actual exit_price and pnl_pct from exchange.
    If state is provided, modifies it in place (caller must save). Else loads and saves itself.
    Returns True if closed, False if position not found.
    """
    if res not in ("TP_hit", "SL_hit", "EARLY_EXIT"):
        logger.debug("close_from_live_outcome: skip res=%s", res)
        return False
    close_reason = "tp" if res == "TP_hit" else ("sl" if res == "SL_hit" else "early_exit")
    state = state if state is not None else load_state()
    open_positions = state.get("open_positions") or {}
    found = _find_position_for_outcome(open_positions, strategy, symbol, run_id, event_id)
    if found is None:
        logger.info(
            "LIVE_OUTCOME_ALREADY_FINALIZED | strategy=%s symbol=%s run_id=%s event_id=%s (no matching position)",
            strategy, symbol, run_id, event_id,
        )
        return False

    position_id, position = found
    entry = float(position["entry"])
    sl = float(position["sl"])
    side = (position.get("side") or "SHORT").strip().upper()
    risk_abs = abs(entry - sl)
    risk_pct = (risk_abs / entry * 100.0) if entry > 0 else 1.0
    pnl_r = (float(pnl_pct) / risk_pct) if risk_pct > 0 else 0.0
    notional = float(position.get("notional_usd") or 0)
    pnl_usd = notional * (float(pnl_pct) / 100.0) if notional else 0.0

    record_close(state, strategy, position_id, close_reason, exit_price, pnl_r, pnl_usd, ts_utc)
    save_state(state)
    logger.info(
        "STATE_FINALIZED_SAVED | strategy=%s symbol=%s position_id=%s reason=%s",
        strategy, symbol, position_id, close_reason,
    )

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
        mfe_pct=None,
        mae_pct=None,
        mfe_r=None,
        mae_r=None,
    )
    logger.info(
        "close_from_live_outcome: closed strategy=%s symbol=%s position_id=%s reason=%s exit=%.4f pnl_r=%.2f pnl_usd=%.2f",
        strategy, symbol, position_id, close_reason, exit_price, pnl_r, pnl_usd,
    )
    _write_live_outcome_to_datasets(position, close_reason, exit_price, pnl_r, pnl_pct, ts_utc)
    _send_live_outcome_telegram(
        position, strategy, symbol, run_id, event_id, res, exit_price, pnl_pct, pnl_r, ts_utc,
        state=state,
    )
    return True


def _send_live_outcome_telegram(
    position: dict[str, Any],
    strategy: str,
    symbol: str,
    run_id: str,
    event_id: str,
    res: str,
    exit_price: float,
    pnl_pct: float,
    pnl_r: float,
    ts_utc: str,
    *,
    state: Optional[dict[str, Any]] = None,
) -> None:
    """Send outcome TG for live trade when TG_SEND_OUTCOME=1. Idempotent: only once per trade_id."""
    try:
        if (__import__("os").getenv("TG_SEND_OUTCOME", "0") or "").strip() != "1":
            return
        from trading.state import load_state, save_state, outcome_tg_sent, add_outcome_tg_sent
        key = make_position_id(strategy, run_id or "", str(event_id or ""), symbol or "")
        state = state if state is not None else load_state()
        if outcome_tg_sent(state, key):
            logger.info(
                "LIVE_OUTCOME_DUPLICATE_BLOCKED | strategy=%s symbol=%s run_id=%s event_id=%s res=%s key=%s",
                strategy, symbol, run_id, event_id, res, key[:64],
            )
            return
        from short_pump.telegram import send_telegram
        from notifications.tg_format import format_outcome, format_fast0_outcome_message
        from trading.config import LIVE_LEVERAGE, LIVE_MARGIN_MODE

        entry = float(position.get("entry", 0) or 0)
        tp_price = float(position.get("tp", 0) or 0)
        sl_price = float(position.get("sl", 0) or 0)
        risk_profile = (position.get("risk_profile") or "").strip()
        notional_usd = float(position.get("notional_usd") or 0)
        leverage = int(position.get("leverage") or LIVE_LEVERAGE or 4)
        margin_mode = (position.get("margin_mode") or LIVE_MARGIN_MODE or "isolated").strip()
        hold_sec = _hold_seconds(position.get("opened_ts", ""), ts_utc)

        if strategy == "short_pump_fast0":
            msg = format_fast0_outcome_message(
                symbol=symbol,
                run_id=run_id,
                event_id=event_id,
                res=res,
                entry_price=entry,
                tp_price=tp_price,
                sl_price=sl_price,
                exit_price=exit_price,
                pnl_pct=pnl_pct,
                hold_seconds=hold_sec,
                risk_profile=risk_profile or None,
                notional_usd=notional_usd if notional_usd > 0 else None,
                leverage=leverage,
                margin_mode=margin_mode,
            )
        else:
            risk_pct = abs(entry - sl_price) / entry * 100.0 if entry > 0 else 1.0
            msg = format_outcome(
                strategy=strategy,
                side=position.get("side", "SHORT"),
                symbol=symbol,
                run_id=run_id,
                event_id=event_id,
                outcome=res,
                entry_price=entry,
                tp_price=tp_price,
                sl_price=sl_price,
                tp_pct=risk_pct * 1.5,
                sl_pct=risk_pct,
                pnl_pct=pnl_pct,
                hold_seconds=hold_sec,
                risk_profile=risk_profile or None,
                notional_usd=notional_usd if notional_usd > 0 else None,
                leverage=leverage,
                margin_mode=margin_mode,
            )
        send_telegram(
            msg,
            strategy=strategy,
            side=position.get("side", "SHORT"),
            mode="LIVE",
            event_id=event_id,
            context_score=None,
            entry_ok=True,
            formatted=True,
        )
        add_outcome_tg_sent(state, key)
        save_state(state)
        logger.info(
            "LIVE_OUTCOME_TG_SENT | strategy=%s symbol=%s run_id=%s event_id=%s res=%s",
            strategy, symbol, run_id, event_id, res,
        )
    except Exception as e:
        logger.debug("_send_live_outcome_telegram: %s", e)


def record_tpsl_failed_closed(
    position: dict[str, Any],
    signal: Any,
    *,
    log_path: Optional[str] = None,
    outcome_source: str = "tpsl_failed_closed",
) -> None:
    """
    Record a live case where entry filled but TPSL setup failed and position was force-closed.
    Writes to trading_closes, datasets/outcomes_v3.csv, and sends TG alert.
    """
    from datetime import datetime, timezone
    strategy = position.get("strategy", "") or getattr(signal, "strategy", "")
    symbol = position.get("symbol", "") or getattr(signal, "symbol", "")
    side = (position.get("side") or "SHORT").strip()
    entry = float(position.get("entry", 0))
    tp = float(position.get("tp", 0))
    sl = float(position.get("sl", 0))
    exit_price = float(position.get("exit_price", entry))
    pnl_pct = float(position.get("pnl_pct", 0))
    run_id = (position.get("run_id") or "") or getattr(signal, "run_id", "")
    event_id = str(position.get("event_id") or "") or str(getattr(signal, "event_id", "") or "")
    notional = float(position.get("notional_usd", 0) or 0)
    risk_abs = abs(entry - sl)
    risk_pct = (risk_abs / entry * 100.0) if entry > 0 else 1.0
    pnl_r = (pnl_pct / risk_pct) if risk_pct > 0 else 0.0
    pnl_usd = notional * (pnl_pct / 100.0) if notional else 0.0
    ts_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000000+00:00")
    close_reason = "tpsl_setup_failed"

    path = log_path or CLOSES_PATH
    _append_close_row(
        ts=ts_utc,
        strategy=strategy,
        symbol=symbol,
        side=side,
        entry=entry,
        tp=tp,
        sl=sl,
        exit_price=exit_price,
        close_reason=close_reason,
        pnl_r=pnl_r,
        pnl_usd=pnl_usd,
        run_id=run_id,
        event_id=event_id,
        log_path=path,
    )
    _write_live_outcome_to_datasets(
        position,
        close_reason=close_reason,
        exit_price=exit_price,
        pnl_r=pnl_r,
        pnl_pct=pnl_pct,
        outcome_ts_utc=ts_utc,
        outcome_source=outcome_source,
    )
    try:
        from notifications.tg_format import format_tpsl_failed_closed_message
        from short_pump.telegram import send_telegram
        msg = format_tpsl_failed_closed_message(position, pnl_pct=pnl_pct, exit_price=exit_price)
        send_telegram(
            msg,
            strategy=strategy,
            side=side,
            mode="LIVE",
            event_id=event_id,
            context_score=None,
            entry_ok=True,
            formatted=True,
        )
        logger.info(
            "TPSL_FAILED_CLOSED_RECORDED | strategy=%s symbol=%s run_id=%s event_id=%s exit=%.4f pnl_pct=%.2f",
            strategy, symbol, run_id, event_id, exit_price, pnl_pct,
        )
    except Exception as e:
        logger.warning("TPSL_FAILED_TG_SEND_FAILED | strategy=%s symbol=%s: %s", strategy, symbol, e)


def _write_live_outcome_to_datasets(
    position: dict[str, Any],
    close_reason: str,
    exit_price: float,
    pnl_r: float,
    pnl_pct: float,
    outcome_ts_utc: str,
    base_dir: Optional[str] = None,
    *,
    outcome_source: str = "live_outcome_worker",
) -> None:
    """Write live close to datasets/outcomes_v3.csv with trade_type=LIVE."""
    try:
        from common.outcome_tracker import build_outcome_row
        from common.io_dataset import write_outcome_row, _dataset_dir
        import os
        import pandas as pd
    except ImportError:
        logger.debug("_write_live_outcome_to_datasets: skip (no common)")
        return
    strategy = position.get("strategy", "")
    symbol = position.get("symbol", "")
    side = (position.get("side") or "SHORT").strip()
    entry = float(position.get("entry", 0))
    sl = float(position.get("sl", 0))
    run_id = position.get("run_id", "")
    event_id = (position.get("event_id") or "") or ""
    trade_id = make_position_id(strategy, run_id, event_id, symbol)
    mode = position.get("mode", "live")
    opened_ts = position.get("opened_ts", "")
    hold_sec = _hold_seconds(opened_ts, outcome_ts_utc)
    if close_reason == "tpsl_setup_failed":
        outcome_str = "TPSL_SETUP_FAILED_CLOSED"
    else:
        outcome_str = "TP_hit" if close_reason == "tp" else ("SL_hit" if close_reason == "sl" else "EARLY_EXIT")
    notional = float(position.get("notional_usd") or 0)
    pnl_usd = notional * (pnl_pct / 100.0) if notional else 0.0
    risk_abs = abs(entry - sl)
    risk_pct_val = (risk_abs / entry * 100.0) if entry > 0 else 1.0
    pnl_r = (pnl_pct / risk_pct_val) if risk_pct_val > 0 else 0.0
    summary = {
        "end_reason": outcome_str,
        "outcome": outcome_str,
        "pnl_pct": pnl_pct,
        "hold_seconds": hold_sec,
        "mae_pct": 0.0,
        "mfe_pct": 0.0,
        "entry_price": entry,
        "tp_price": position.get("tp"),
        "sl_price": sl,
        "exit_price": exit_price,
        "pnl_r": pnl_r,
        "pnl_usd": pnl_usd,
        "opened_ts": opened_ts,
        "order_id": position.get("order_id"),
        "position_idx": position.get("position_idx"),
        "notional_usd": position.get("notional_usd"),
        "leverage": position.get("leverage"),
        "margin_mode": position.get("margin_mode"),
        "risk_profile": position.get("risk_profile"),
        "trade_type": "LIVE",
        "details_payload": (
            '{"source":"bybit","tpsl_setup_failed":true}'
            if close_reason == "tpsl_setup_failed"
            else '{"source":"bybit","tp_hit":%s,"sl_hit":%s}' % ("true" if close_reason == "tp" else "false", "true" if close_reason == "sl" else "false")
        ),
    }
    # Dataset safety: skip duplicate outcomes for same event_id (idempotent by event_id).
    try:
        ds_base = base_dir or DATASET_BASE_DIR
        dir_path = _dataset_dir(strategy, outcome_ts_utc, base_dir=ds_base, path_mode=mode)
        outcomes_path = os.path.join(dir_path, "outcomes_v3.csv")
        if os.path.exists(outcomes_path):
            try:
                existing_df = pd.read_csv(outcomes_path)
                if "event_id" in existing_df.columns:
                    if (existing_df["event_id"].astype(str) == str(event_id)).any():
                        logger.info(
                            "OUTCOME_DUPLICATE_SKIPPED | strategy=%s symbol=%s run_id=%s event_id=%s",
                            strategy,
                            symbol,
                            run_id,
                            event_id,
                        )
                        return
            except Exception:
                # Best-effort guard; do not block dataset write on read error.
                pass
    except Exception:
        # If directory resolution fails, fall back to normal write (safety net is analytics dedup).
        pass

    orow = build_outcome_row(
        summary,
        trade_id=trade_id,
        event_id=event_id,
        run_id=run_id,
        symbol=symbol,
        strategy=strategy,
        mode=mode,
        side=side,
        outcome_time_utc=outcome_ts_utc,
    )
    if orow:
        orow["outcome_source"] = outcome_source
        write_outcome_row(
            orow,
            strategy=strategy,
            mode=mode,
            wall_time_utc=outcome_ts_utc,
            schema_version=3,
            base_dir=base_dir or DATASET_BASE_DIR,
        )


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
    _write_paper_outcome_to_datasets(
        position, close_reason, exit_price, pnl_r, ts_utc,
        mfe_pct=outcome_meta.get("mfe_pct") if outcome_meta else None,
        mae_pct=outcome_meta.get("mae_pct") if outcome_meta else None,
        outcome_source="paper_close",
    )
    return True


def _hold_seconds(opened_ts: str, outcome_ts_utc: str) -> float:
    """Compute hold duration in seconds. Returns 0 if parse fails."""
    open_dt = _parse_opened_ts(opened_ts)
    out_dt = _parse_opened_ts(outcome_ts_utc)
    if open_dt is None or out_dt is None:
        return 0.0
    return max(0.0, (out_dt - open_dt).total_seconds())


def _write_paper_outcome_to_datasets(
    position: dict[str, Any],
    close_reason: str,
    exit_price: float,
    pnl_r: float,
    outcome_ts_utc: str,
    mfe_pct: Optional[float] = None,
    mae_pct: Optional[float] = None,
    base_dir: Optional[str] = None,
    *,
    outcome_source: str = "runner_timeout",
) -> None:
    """Write paper close to datasets/outcomes_v3.csv so analytics stays identical to live."""
    try:
        from common.outcome_tracker import build_outcome_row
        from common.io_dataset import write_outcome_row
    except ImportError:
        logger.debug("write_paper_outcome_to_datasets: skip (no common.outcome_tracker/io_dataset)")
        return
    strategy = position.get("strategy", "")
    symbol = position.get("symbol", "")
    side = (position.get("side") or "SHORT").strip()
    entry = float(position.get("entry", 0))
    sl = float(position.get("sl", 0))
    run_id = position.get("run_id", "")
    event_id = (position.get("event_id") or "") or ""
    trade_id = make_position_id(strategy, run_id, event_id, symbol)
    mode = position.get("mode", "live")
    opened_ts = position.get("opened_ts", "")
    hold_sec = _hold_seconds(opened_ts, outcome_ts_utc)
    if entry <= 0:
        return
    if side.upper() == "SHORT":
        pnl_pct = (entry - exit_price) / entry * 100.0
    else:
        pnl_pct = (exit_price - entry) / entry * 100.0
    outcome_str = "TP_hit" if close_reason == "tp" else ("SL_hit" if close_reason == "sl" else "TIMEOUT")
    pos_mode = (position.get("mode") or "paper").strip().lower()
    trade_type = "LIVE" if pos_mode == "live" else "PAPER"
    notional = float(position.get("notional_usd") or 0)
    pnl_usd = notional * (pnl_pct / 100.0) if notional else 0.0
    risk_abs = abs(entry - sl)
    risk_pct_val = (risk_abs / entry * 100.0) if entry > 0 else 1.0
    pnl_r = (pnl_pct / risk_pct_val) if risk_pct_val > 0 else 0.0
    summary = {
        "end_reason": outcome_str,
        "outcome": outcome_str,
        "pnl_pct": pnl_pct,
        "hold_seconds": hold_sec,
        "mae_pct": float(mae_pct) if mae_pct is not None else 0.0,
        "mfe_pct": float(mfe_pct) if mfe_pct is not None else 0.0,
        "entry_price": entry,
        "tp_price": position.get("tp"),
        "sl_price": sl,
        "exit_price": exit_price,
        "pnl_r": pnl_r,
        "pnl_usd": pnl_usd,
        "opened_ts": opened_ts,
        "order_id": position.get("order_id"),
        "position_idx": position.get("position_idx"),
        "notional_usd": position.get("notional_usd"),
        "leverage": position.get("leverage"),
        "margin_mode": position.get("margin_mode"),
        "risk_profile": position.get("risk_profile"),
        "trade_type": trade_type,
        "details_payload": json.dumps({"timeout": close_reason == "timeout", "tp_hit": close_reason == "tp", "sl_hit": close_reason == "sl"}),
    }
    orow = build_outcome_row(
        summary,
        trade_id=trade_id,
        event_id=event_id,
        run_id=run_id,
        symbol=symbol,
        strategy=strategy,
        mode=mode,
        side=side,
        outcome_time_utc=outcome_ts_utc,
    )
    if orow:
        orow["outcome_source"] = outcome_source
        write_outcome_row(
            orow,
            strategy=strategy,
            mode=mode,
            wall_time_utc=outcome_ts_utc,
            schema_version=3,
            base_dir=base_dir or DATASET_BASE_DIR,
        )
        logger.debug("write_paper_outcome_to_datasets: wrote trade_id=%s outcome=%s", trade_id, outcome_str)


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
    broker: Optional[Any] = None,
) -> bool:
    """
    Close open positions older than POSITION_TTL_SECONDS (or ttl_seconds if given).
    Iterate all open_positions[strategy][position_id] safely.
    For live positions at TTL: if broker given, runs final reconciliation to avoid false TIMEOUT
    when position already closed on exchange. TIMEOUT used only as last fallback.
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
            mode_val = (position.get("mode") or "").strip().lower()
            if mode_val == "live":
                from trading.state import outcome_tg_sent, make_position_id
                key = make_position_id(
                    strategy,
                    position.get("run_id", "") or "",
                    str(position.get("event_id", "") or ""),
                    position.get("symbol", "") or "",
                )
                if outcome_tg_sent(state, key):
                    logger.info(
                        "LIVE_OUTCOME_ALREADY_FINALIZED_SKIP | strategy=%s symbol=%s run_id=%s event_id=%s (stale in open_positions)",
                        strategy, position.get("symbol", ""), position.get("run_id", ""), position.get("event_id", ""),
                    )
                    ts_str = now_dt.strftime("%Y-%m-%d %H:%M:%S+00:00")
                    record_close(state, strategy, position_id, "stale_cleanup", float(position.get("entry", 0)), 0.0, 0.0, ts_str)
                    any_closed = True
                    continue
            # Pre-timeout reconciliation for live: avoid false TIMEOUT when position already closed on exchange
            if mode_val == "live" and broker:
                try:
                    from trading.bybit_live_outcome import resolve_live_outcome_final_reconcile
                    outcome = resolve_live_outcome_final_reconcile(
                        broker=broker,
                        symbol=position.get("symbol", ""),
                        side=position.get("side", "SHORT"),
                        opened_ts=opened_ts,
                        entry=float(position.get("entry", 0) or 0),
                        tp=float(position.get("tp", 0) or 0),
                        sl=float(position.get("sl", 0) or 0),
                        raise_on_network_error=False,
                    )
                    if outcome and outcome.get("status") in ("TP_hit", "SL_hit", "EARLY_EXIT"):
                        res = outcome["status"]
                        exit_price = float(outcome.get("exit_price", 0) or 0)
                        pnl_pct = float(outcome.get("pnl_pct", 0) or 0)
                        exit_ts = outcome.get("exit_ts", now_dt.strftime("%Y-%m-%d %H:%M:%S+00:00"))
                        ok = close_from_live_outcome(
                            strategy=strategy,
                            symbol=position.get("symbol", ""),
                            run_id=position.get("run_id", "") or "",
                            event_id=position.get("event_id", "") or "",
                            res=res,
                            exit_price=exit_price,
                            pnl_pct=pnl_pct,
                            ts_utc=exit_ts,
                            log_path=path,
                            state=state,
                        )
                        if ok:
                            logger.info(
                                "LIVE_OUTCOME_FINAL_RECONCILE | closed strategy=%s symbol=%s position_id=%s as %s (not TIMEOUT)",
                                strategy, position.get("symbol", ""), position_id, res,
                            )
                            any_closed = True
                            continue
                except Exception as e:
                    logger.warning("LIVE_OUTCOME_FINAL_RECONCILE | failed strategy=%s symbol=%s: %s", strategy, position.get("symbol", ""), e)
                logger.info(
                    "LIVE_OUTCOME_TIMEOUT_FALLBACK | strategy=%s symbol=%s position_id=%s position_gone_or_unresolved",
                    strategy, position.get("symbol", ""), position_id,
                )
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
            _write_paper_outcome_to_datasets(
                position, "timeout", exit_price, pnl_r, ts_str,
                mfe_pct=0.0, mae_pct=0.0,
                outcome_source="runner_timeout",
            )
            # Live timeout: send TG outcome just like TP/SL live outcomes.
            # Use TIMEOUT as outcome label; reuse same position fields (risk_profile, notional, leverage, margin, entry/tp/sl).
            try:
                mode_val = (position.get("mode") or "").strip().lower()
                if mode_val == "live":
                    entry_val = float(position.get("entry", 0) or 0)
                    sl_val = float(position.get("sl", 0) or 0)
                    risk_abs = abs(entry_val - sl_val)
                    risk_pct = (risk_abs / entry_val * 100.0) if entry_val > 0 else 1.0
                    pnl_pct_timeout = float(pnl_r) * risk_pct if risk_pct > 0 else 0.0
                    _send_live_outcome_telegram(
                        position=position,
                        strategy=strategy,
                        symbol=position.get("symbol", ""),
                        run_id=position.get("run_id", "") or "",
                        event_id=position.get("event_id", "") or "",
                        res="TIMEOUT",
                        exit_price=exit_price,
                        pnl_pct=pnl_pct_timeout,
                        pnl_r=pnl_r,
                        ts_utc=ts_str,
                        state=state,
                    )
            except Exception as e:
                logger.debug("close_on_timeout: live outcome telegram failed: %s", e)
            any_closed = True

    return any_closed
