"""
Outcome finalization helpers for paper + live.

This module is imported by trading.runner / outcome_worker / watcher/fast0 paths.
It must be safe to import in live runner context.
"""

from __future__ import annotations

import csv
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from trading.config import CLOSES_PATH, POSITION_TTL_SECONDS, TIMEOUT_EXIT_MODE
from trading.state import (
    load_state,
    make_position_id,
    record_close,
    save_state,
)

logger = logging.getLogger(__name__)


_CLOSES_HEADER: list[str] = [
    "ts_utc",
    "strategy",
    "symbol",
    "run_id",
    "event_id",
    "trade_id",
    "mode",
    "side",
    "entry_price",
    "tp_price",
    "sl_price",
    "exit_price",
    "outcome",
    "close_reason",
    "pnl_pct",
    "pnl_r",
    "pnl_usd",
    "risk_usd",
    "notional_usd",
    "leverage",
    "risk_profile",
    "order_id",
    "position_idx",
    "outcome_source",
    "mfe_pct",
    "mae_pct",
    "mfe_r",
    "mae_r",
]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _parse_dt(ts: str) -> Optional[datetime]:
    if not ts or not str(ts).strip():
        return None
    s = str(ts).strip().replace("Z", "+00:00").replace("+0000", "+00:00")
    for fmt in (
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S",
    ):
        try:
            dt = datetime.strptime(s, fmt)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    try:
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _ensure_dir_for_file(path: str) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)


def _read_header(path: Path) -> list[str] | None:
    try:
        with open(path, "r", newline="", encoding="utf-8") as f:
            r = csv.reader(f)
            return next(r)
    except FileNotFoundError:
        return None
    except Exception:
        return None


def _write_header(path: Path, header: list[str]) -> None:
    _ensure_dir_for_file(str(path))
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)


def _needs_migration(existing: list[str]) -> bool:
    need = set(_CLOSES_HEADER)
    have = set([c.strip() for c in existing if c and str(c).strip()])
    return not need.issubset(have)


def _ensure_closes_header(path: str) -> str:
    """
    Ensure trading_closes.csv exists and contains extended columns (mfe/mae).
    Returns: "created" | "ok" | "migrated" | "error:<msg>".
    """
    p = Path(path)
    hdr = _read_header(p)
    if hdr is None:
        try:
            _write_header(p, _CLOSES_HEADER)
            return "created"
        except Exception as e:
            return f"error:{type(e).__name__}:{e}"
    if not _needs_migration(hdr):
        return "ok"
    # migrate: create backup, rewrite file with new header, keep existing rows by DictReader/DictWriter
    try:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        backup = p.with_suffix(p.suffix + f".bak_{stamp}")
        _ensure_dir_for_file(str(backup))
        backup.write_bytes(p.read_bytes())
        with open(p, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        with open(p, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=_CLOSES_HEADER)
            writer.writeheader()
            for row in rows:
                out = {k: row.get(k, "") for k in _CLOSES_HEADER}
                writer.writerow(out)
        return "migrated"
    except Exception as e:
        return f"error:{type(e).__name__}:{e}"


def _risk_pct_from_position(entry: float, sl: float, side: str) -> float:
    if entry <= 0 or sl <= 0:
        return 0.0
    # risk distance as percent of entry (always positive)
    return abs(sl - entry) / entry * 100.0


def _pnl_r_from_pct(pnl_pct: float, risk_pct: float) -> float:
    if risk_pct <= 1e-9:
        return 0.0
    return pnl_pct / risk_pct


def _append_close_row(row: dict[str, Any]) -> None:
    path = os.getenv("TRADING_CLOSES_PATH") or CLOSES_PATH
    ensure_res = _ensure_closes_header(path)
    if ensure_res.startswith("error:"):
        logger.warning("CLOSES_HEADER_ERROR | %s", ensure_res)
    file_exists = Path(path).exists()
    _ensure_dir_for_file(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=_CLOSES_HEADER)
        if not file_exists:
            w.writeheader()
        out = {k: row.get(k, "") for k in _CLOSES_HEADER}
        w.writerow(out)


def close_from_outcome(
    *,
    strategy: str,
    symbol: str,
    run_id: str,
    event_id: str,
    res: str,
    pnl_pct: Any,
    ts_utc: str,
    outcome_meta: Optional[Dict[str, Any]] = None,
) -> bool:
    """
    Finalize a PAPER outcome (from candle tracker):
    - remove open position from trading_state
    - append row to trading_closes.csv
    """
    state = load_state()
    pid = make_position_id(strategy, run_id, str(event_id or ""), symbol)
    pos = (state.get("open_positions") or {}).get(strategy, {}).get(pid)
    if not isinstance(pos, dict):
        logger.info("CLOSE_SKIP | reason=no_open_position strategy=%s pid=%s", strategy, pid)
        return False

    entry = float(pos.get("entry", 0) or 0)
    tp = float(pos.get("tp", 0) or 0)
    sl = float(pos.get("sl", 0) or 0)
    side = str(pos.get("side") or "SHORT")
    mode = str(pos.get("mode") or "paper")
    risk_usd = float(pos.get("risk_usd", 0) or 0)
    notional_usd = float(pos.get("notional_usd", 0) or 0)
    leverage = int(pos.get("leverage", 0) or 0)
    risk_profile = str(pos.get("risk_profile") or "")

    try:
        pnl_pct_f = float(pnl_pct) if pnl_pct is not None and pnl_pct != "" else 0.0
    except Exception:
        pnl_pct_f = 0.0

    # Best-effort exit_price for paper: TP/SL known; TIMEOUT uses config.
    exit_price = entry
    res_norm = str(res or "").strip()
    if res_norm == "TP_hit" and tp > 0:
        exit_price = tp
    elif res_norm == "SL_hit" and sl > 0:
        exit_price = sl
    elif res_norm == "TIMEOUT":
        exit_price = sl if TIMEOUT_EXIT_MODE == "sl" and sl > 0 else entry

    risk_pct = _risk_pct_from_position(entry, sl, side)
    pnl_r = _pnl_r_from_pct(pnl_pct_f, risk_pct)
    pnl_usd = float(pnl_r * risk_usd) if risk_usd > 0 else 0.0

    record_close(
        state,
        strategy=strategy,
        position_id=pid,
        close_reason=str(res_norm or "outcome"),
        exit_price=float(exit_price or 0),
        pnl_r=float(pnl_r),
        pnl_usd=float(pnl_usd),
        ts_utc=ts_utc or _now_iso(),
    )
    save_state(state)

    meta = outcome_meta or {}
    mfe_pct = meta.get("mfe_pct")
    mae_pct = meta.get("mae_pct")
    try:
        mfe_pct_f = float(mfe_pct) if mfe_pct is not None and mfe_pct != "" else ""
    except Exception:
        mfe_pct_f = ""
    try:
        mae_pct_f = float(mae_pct) if mae_pct is not None and mae_pct != "" else ""
    except Exception:
        mae_pct_f = ""
    mfe_r = _pnl_r_from_pct(float(mfe_pct_f), risk_pct) if mfe_pct_f != "" else ""
    mae_r = _pnl_r_from_pct(float(mae_pct_f), risk_pct) if mae_pct_f != "" else ""

    _append_close_row(
        {
            "ts_utc": ts_utc or _now_iso(),
            "strategy": strategy,
            "symbol": symbol,
            "run_id": run_id,
            "event_id": str(event_id or ""),
            "trade_id": str(pos.get("trade_id") or pid),
            "mode": mode,
            "side": side,
            "entry_price": entry,
            "tp_price": tp,
            "sl_price": sl,
            "exit_price": float(exit_price or 0),
            "outcome": res_norm,
            "close_reason": str(res_norm or "outcome"),
            "pnl_pct": pnl_pct_f,
            "pnl_r": float(pnl_r),
            "pnl_usd": float(pnl_usd),
            "risk_usd": risk_usd,
            "notional_usd": notional_usd,
            "leverage": leverage,
            "risk_profile": risk_profile,
            "order_id": str(pos.get("order_id") or ""),
            "position_idx": str(pos.get("position_idx") or ""),
            "outcome_source": "paper",
            "mfe_pct": (f"{mfe_pct_f:.4f}" if isinstance(mfe_pct_f, float) else mfe_pct_f),
            "mae_pct": (f"{mae_pct_f:.4f}" if isinstance(mae_pct_f, float) else mae_pct_f),
            "mfe_r": (f"{float(mfe_r):.4f}" if mfe_r != "" else ""),
            "mae_r": (f"{float(mae_r):.4f}" if mae_r != "" else ""),
        }
    )
    return True


def close_from_live_outcome(
    *,
    strategy: str,
    symbol: str,
    run_id: str,
    event_id: str,
    res: str,
    exit_price: Any,
    pnl_pct: Any,
    ts_utc: str,
    state: dict[str, Any],
) -> bool:
    """
    Finalize a LIVE outcome (from Bybit resolver):
    - remove open position from state
    - append to trading_closes.csv
    """
    pid = make_position_id(strategy, run_id, str(event_id or ""), symbol)
    pos = (state.get("open_positions") or {}).get(strategy, {}).get(pid)
    if not isinstance(pos, dict):
        return False

    entry = float(pos.get("entry", 0) or 0)
    sl = float(pos.get("sl", 0) or 0)
    side = str(pos.get("side") or "SHORT")
    risk_usd = float(pos.get("risk_usd", 0) or 0)

    try:
        exit_f = float(exit_price) if exit_price is not None and exit_price != "" else 0.0
    except Exception:
        exit_f = 0.0
    try:
        pnl_pct_f = float(pnl_pct) if pnl_pct is not None and pnl_pct != "" else 0.0
    except Exception:
        pnl_pct_f = 0.0

    risk_pct = _risk_pct_from_position(entry, sl, side)
    pnl_r = _pnl_r_from_pct(pnl_pct_f, risk_pct)
    pnl_usd = float(pnl_r * risk_usd) if risk_usd > 0 else 0.0

    record_close(
        state,
        strategy=strategy,
        position_id=pid,
        close_reason=str(res or "live_outcome"),
        exit_price=exit_f,
        pnl_r=float(pnl_r),
        pnl_usd=float(pnl_usd),
        ts_utc=ts_utc or _now_iso(),
    )
    _append_close_row(
        {
            "ts_utc": ts_utc or _now_iso(),
            "strategy": strategy,
            "symbol": symbol,
            "run_id": run_id,
            "event_id": str(event_id or ""),
            "trade_id": str(pos.get("trade_id") or pid),
            "mode": str(pos.get("mode") or "live"),
            "side": side,
            "entry_price": entry,
            "tp_price": float(pos.get("tp", 0) or 0),
            "sl_price": float(pos.get("sl", 0) or 0),
            "exit_price": exit_f,
            "outcome": str(res or ""),
            "close_reason": str(res or "live_outcome"),
            "pnl_pct": pnl_pct_f,
            "pnl_r": float(pnl_r),
            "pnl_usd": float(pnl_usd),
            "risk_usd": float(pos.get("risk_usd", 0) or 0),
            "notional_usd": float(pos.get("notional_usd", 0) or 0),
            "leverage": int(pos.get("leverage", 0) or 0),
            "risk_profile": str(pos.get("risk_profile") or ""),
            "order_id": str(pos.get("order_id") or ""),
            "position_idx": str(pos.get("position_idx") or ""),
            "outcome_source": "bybit",
        }
    )
    return True


def close_from_live_outcome_safe(*args: Any, **kwargs: Any) -> bool:
    """Backward-compatible alias (some callers may have older name)."""
    try:
        return bool(close_from_live_outcome(*args, **kwargs))
    except Exception:
        logger.exception("close_from_live_outcome failed")
        return False


def close_on_timeout(state: dict[str, Any], now_utc: str, *, broker: Any = None) -> bool:
    """
    TTL failsafe: close PAPER positions older than POSITION_TTL_SECONDS.
    Returns True if state changed.
    LIVE positions are not force-closed here.
    """
    now_dt = _parse_dt(now_utc) or datetime.now(timezone.utc)
    changed = False
    op = state.get("open_positions") or {}
    for strategy, strat_pos in list(op.items()):
        if not isinstance(strat_pos, dict):
            continue
        for pid, pos in list(strat_pos.items()):
            if not isinstance(pos, dict):
                continue
            mode = str(pos.get("mode") or "").strip().lower()
            if mode == "live":
                continue
            opened = _parse_dt(str(pos.get("opened_ts") or "")) or None
            if opened is None:
                continue
            age = (now_dt - opened).total_seconds()
            if age < POSITION_TTL_SECONDS:
                continue
            entry = float(pos.get("entry", 0) or 0)
            sl = float(pos.get("sl", 0) or 0)
            side = str(pos.get("side") or "SHORT")
            exit_price = sl if TIMEOUT_EXIT_MODE == "sl" and sl > 0 else entry
            pnl_pct = 0.0
            if entry > 0 and exit_price > 0 and TIMEOUT_EXIT_MODE == "sl" and sl > 0:
                if (side or "").strip().upper() in ("SHORT", "SELL"):
                    pnl_pct = (entry - exit_price) / entry * 100.0
                else:
                    pnl_pct = (exit_price - entry) / entry * 100.0
            ok = close_from_outcome(
                strategy=strategy,
                symbol=str(pos.get("symbol") or ""),
                run_id=str(pos.get("run_id") or ""),
                event_id=str(pos.get("event_id") or ""),
                res="TIMEOUT",
                pnl_pct=pnl_pct,
                ts_utc=now_dt.isoformat(),
                outcome_meta={"mfe_pct": 0.0, "mae_pct": 0.0},
            )
            if ok:
                changed = True
    return changed


def record_guard_blocked_paper(signal: Any, risk_profile_name: str, guard_reason: str) -> None:
    """Best-effort bookkeeping hook for guard-blocked paper paths. Safe no-op."""
    _ = (signal, risk_profile_name, guard_reason)
    return None


def record_tpsl_failed_closed(
    position: Dict[str, Any],
    signal: Any,
    *,
    outcome_source: str = "runner",
) -> None:
    """
    Persist a TPSL_SETUP_FAILED_CLOSED event to trading_closes.csv.
    Does NOT open/close state positions (position was already force-closed on exchange).
    """
    try:
        strategy = str(position.get("strategy") or getattr(signal, "strategy", "") or "")
        symbol = str(position.get("symbol") or getattr(signal, "symbol", "") or "")
        run_id = str(position.get("run_id") or getattr(signal, "run_id", "") or "")
        event_id = str(position.get("event_id") or getattr(signal, "event_id", "") or "")
        side = str(position.get("side") or getattr(signal, "side", "SHORT") or "SHORT")
        ts_utc = str(position.get("opened_ts") or getattr(signal, "ts_utc", "") or _now_iso())
        entry = float(position.get("entry", 0) or 0)
        tp = float(position.get("tp", 0) or 0)
        sl = float(position.get("sl", 0) or 0)
        exit_price = float(position.get("exit_price", entry) or entry)
        pnl_pct = float(position.get("pnl_pct", 0.0) or 0.0)
        risk_usd = float(position.get("risk_usd", 0) or 0)
        notional_usd = float(position.get("notional_usd", 0) or 0)
        leverage = int(position.get("leverage", 0) or 0)
        risk_profile = str(position.get("risk_profile") or "")
        order_id = str(position.get("order_id") or "")
        position_idx = str(position.get("position_idx") or "")
        close_reason = str(position.get("close_reason") or "tpsl_setup_failed")
        risk_pct = _risk_pct_from_position(entry, sl, side)
        pnl_r = _pnl_r_from_pct(pnl_pct, risk_pct)
        pnl_usd = float(pnl_r * risk_usd) if risk_usd > 0 else 0.0
        trade_id = str(position.get("trade_id") or make_position_id(strategy, run_id, event_id, symbol))
        _append_close_row(
            {
                "ts_utc": ts_utc,
                "strategy": strategy,
                "symbol": symbol,
                "run_id": run_id,
                "event_id": event_id,
                "trade_id": trade_id,
                "mode": "live",
                "side": side,
                "entry_price": entry,
                "tp_price": tp,
                "sl_price": sl,
                "exit_price": exit_price,
                "outcome": "TPSL_SETUP_FAILED_CLOSED",
                "close_reason": close_reason,
                "pnl_pct": pnl_pct,
                "pnl_r": float(pnl_r),
                "pnl_usd": float(pnl_usd),
                "risk_usd": risk_usd,
                "notional_usd": notional_usd,
                "leverage": leverage,
                "risk_profile": risk_profile,
                "order_id": order_id,
                "position_idx": position_idx,
                "outcome_source": outcome_source,
                "mfe_pct": "",
                "mae_pct": "",
                "mfe_r": "",
                "mae_r": "",
            }
        )
        logger.info(
            "TPSL_FAILED_CLOSED_RECORDED | strategy=%s symbol=%s run_id=%s event_id=%s exit=%.6f pnl_pct=%.3f",
            strategy, symbol, run_id, event_id, exit_price, pnl_pct,
        )
    except Exception:
        logger.exception("record_tpsl_failed_closed failed")

