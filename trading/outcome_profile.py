"""
Canonical outcome fields for outcomes_v3: read from open position, then trading_closes.
No get_risk_profile() recomputation on outcome paths (entry/sizing logic unchanged).
"""

from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)


def load_open_position(
    strategy: str,
    run_id: str,
    event_id: str,
    symbol: str,
    trade_id: str = "",
) -> Optional[dict[str, Any]]:
    try:
        from trading.state import load_state, make_position_id

        strat = (strategy or "").strip()
        state = load_state()
        op = (state.get("open_positions") or {}).get(strat) or {}
        if not isinstance(op, dict):
            return None
        pid = make_position_id(strat, run_id or "", str(event_id or ""), symbol or "")
        pos = op.get(pid)
        if isinstance(pos, dict):
            return pos
        tid = str(trade_id or "").strip()
        if tid:
            for _k, p in op.items():
                if isinstance(p, dict) and str(p.get("trade_id") or "").strip() == tid:
                    return p
        return None
    except Exception:
        return None


def last_closes_row_for_trade(trade_id: str) -> Optional[dict[str, str]]:
    """Last CSV row in trading_closes.csv matching trade_id (in file order)."""
    try:
        from trading.config import CLOSES_PATH

        p = Path(CLOSES_PATH)
        if not p.is_file():
            return None
        tid = str(trade_id or "").strip()
        if not tid:
            return None
        last: Optional[dict[str, str]] = None
        with open(p, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if str(row.get("trade_id") or "").strip() == tid:
                    last = row
        return last
    except Exception:
        return None


def canonical_risk_profile_for_outcome(
    *,
    strategy: str,
    run_id: str,
    event_id: str,
    symbol: str,
    trade_id: str,
) -> tuple[str, str]:
    """
    Returns (risk_profile, source) with source in position | close | fallback.
    """
    pos = load_open_position(strategy, run_id, event_id, symbol, trade_id)
    if pos is not None:
        rp = str(pos.get("risk_profile") or "").strip()
        if rp:
            return rp, "position"
    row = last_closes_row_for_trade(trade_id)
    if row:
        rp_close = str(row.get("risk_profile") or "").strip()
        if rp_close:
            return rp_close, "close"
    if pos is not None:
        return "", "position"
    return "", "fallback"


def apply_canonical_risk_profile_to_summary(
    summary: dict[str, Any],
    *,
    strategy: str,
    run_id: str,
    event_id: str,
    symbol: str,
    trade_id: str,
    log: Optional[logging.Logger] = None,
) -> str:
    """Sets summary['risk_profile']. Returns source tag."""
    rp, src = canonical_risk_profile_for_outcome(
        strategy=strategy,
        run_id=run_id,
        event_id=event_id,
        symbol=symbol,
        trade_id=trade_id,
    )
    summary["risk_profile"] = rp
    lg = log or logger
    lg.info(
        "OUTCOME_PROFILE_SOURCE | symbol=%s | risk_profile=%s | source=%s",
        symbol,
        rp if rp else "(empty)",
        src,
    )
    return src


def _snapshot_from_position_dict(pos: dict[str, Any]) -> dict[str, Any]:
    nu_f: Optional[float] = None
    nu_raw = pos.get("notional_usd")
    if nu_raw is not None and str(nu_raw).strip() != "":
        try:
            nu_f = float(nu_raw)
        except (TypeError, ValueError):
            nu_f = None
    lev_i: Optional[int] = None
    lev_raw = pos.get("leverage")
    if lev_raw is not None and str(lev_raw).strip() != "":
        try:
            lev_i = int(float(lev_raw))
        except (TypeError, ValueError):
            lev_i = None
    return {
        "risk_profile": str(pos.get("risk_profile") or "").strip(),
        "notional_usd": nu_f,
        "leverage": lev_i,
        "margin_mode": str(pos.get("margin_mode") or "").strip(),
    }


def _snapshot_from_closes_row(row: dict[str, str]) -> dict[str, Any]:
    nu_f: Optional[float] = None
    if str(row.get("notional_usd") or "").strip():
        try:
            nu_f = float(row["notional_usd"])
        except (TypeError, ValueError):
            nu_f = None
    lev_i: Optional[int] = None
    if str(row.get("leverage") or "").strip():
        try:
            lev_i = int(float(row["leverage"]))
        except (TypeError, ValueError):
            lev_i = None
    return {
        "risk_profile": str(row.get("risk_profile") or "").strip(),
        "notional_usd": nu_f,
        "leverage": lev_i,
        "margin_mode": str(row.get("margin_mode") or "").strip(),
    }


def outcome_position_or_closes_snapshot(
    strategy: str,
    run_id: str,
    event_id: str,
    symbol: str,
    trade_id: str,
) -> tuple[Optional[dict[str, Any]], str]:
    """
    Full snapshot for outcomes_v3: risk_profile, notional_usd, leverage, margin_mode.
    Prefer open position; else last closes row. Returns (dict or None, source).
    """
    pos = load_open_position(strategy, run_id, event_id, symbol, trade_id)
    if pos is not None:
        return _snapshot_from_position_dict(pos), "position"
    row = last_closes_row_for_trade(trade_id)
    if row is not None:
        return _snapshot_from_closes_row(row), "close"
    return None, "fallback"


def apply_outcome_snapshot_to_summary(
    summary: dict[str, Any],
    *,
    strategy: str,
    run_id: str,
    event_id: str,
    symbol: str,
    trade_id: str,
    log: Optional[logging.Logger] = None,
) -> str:
    """
    Sets summary risk_profile, notional_usd, leverage, margin_mode from position or closes.
    For fast0 candle outcomes. Logs OUTCOME_PROFILE_SOURCE. No get_risk_profile().
    """
    snap, src = outcome_position_or_closes_snapshot(strategy, run_id, event_id, symbol, trade_id)
    if snap is not None:
        summary["risk_profile"] = snap.get("risk_profile") or ""
        if snap.get("notional_usd") is not None:
            summary["notional_usd"] = snap["notional_usd"]
        if snap.get("leverage") is not None:
            summary["leverage"] = snap["leverage"]
        if snap.get("margin_mode"):
            summary["margin_mode"] = snap["margin_mode"]
    else:
        summary["risk_profile"] = summary.get("risk_profile") or ""
    lg = log or logger
    lg.info(
        "OUTCOME_PROFILE_SOURCE | symbol=%s | risk_profile=%s | notional=%s | source=%s",
        symbol,
        (summary.get("risk_profile") or "") or "(empty)",
        summary.get("notional_usd", ""),
        src,
    )
    return src
