"""Pipeline monitoring dashboard aggregator.

Reads partitioned datasets (events_v3, trades_v3, outcomes_v3) and the central
trading_closes.csv to build a single JSON-serializable dict consumed by the
FastAPI /api/dashboard/data endpoint.

All CSV reads use engine="python", on_bad_lines="skip" because event rows can
occasionally contain extra fields. Every branch is defensive: missing files,
empty frames, or unexpected columns return empty metrics instead of raising.
"""

from __future__ import annotations

import glob
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

import pandas as pd

_logger = logging.getLogger(__name__)

_DEFAULT_BASE_DIR = "/root/pump_short"
_DATASETS_SUBDIR = "datasets"
_TRADING_CLOSES = "trading_closes.csv"

_MAX_DAYS = 90
_MIN_DAYS = 1


# ---------------------------------------------------------------------------
# File discovery / reading
# ---------------------------------------------------------------------------
def _date_partition_dirs(base_dir: Path, days: int) -> List[Path]:
    """Return existing date=YYYYMMDD directories in datasets/ within the last N days."""
    dsets = base_dir / _DATASETS_SUBDIR
    if not dsets.is_dir():
        return []
    today = datetime.now(timezone.utc).date()
    keep: List[Path] = []
    for i in range(days):
        d = today - timedelta(days=i)
        p = dsets / f"date={d.strftime('%Y%m%d')}"
        if p.is_dir():
            keep.append(p)
    return keep


def _concat_csvs(paths: Iterable[str]) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    for p in paths:
        try:
            df = pd.read_csv(p, engine="python", on_bad_lines="skip")
            if not df.empty:
                frames.append(df)
        except Exception as exc:
            _logger.debug("dashboard: failed to read %s: %s", p, exc)
    if not frames:
        return pd.DataFrame()
    try:
        return pd.concat(frames, ignore_index=True, sort=False)
    except Exception as exc:
        _logger.debug("dashboard: concat failed: %s", exc)
        return pd.DataFrame()


def _load_partitioned(base_dir: Path, days: int, filename: str) -> pd.DataFrame:
    parts = _date_partition_dirs(base_dir, days)
    paths: List[str] = []
    for part in parts:
        paths.extend(
            glob.glob(str(part / "strategy=*" / "mode=*" / filename))
        )
    return _concat_csvs(paths)


def _load_trading_closes(base_dir: Path, days: int) -> pd.DataFrame:
    path = base_dir / _DATASETS_SUBDIR / _TRADING_CLOSES
    if not path.is_file():
        return pd.DataFrame()
    try:
        df = pd.read_csv(path, engine="python", on_bad_lines="skip")
    except Exception as exc:
        _logger.debug("dashboard: failed to read trading_closes: %s", exc)
        return pd.DataFrame()
    if df.empty or "ts_utc" not in df.columns:
        return df
    ts = pd.to_datetime(df["ts_utc"], utc=True, errors="coerce")
    # Fallback for mixed timestamp formats that may coerce to NaT.
    nat_mask = ts.isna()
    if nat_mask.any():
        ts2 = pd.to_datetime(
            df.loc[nat_mask, "ts_utc"], format="mixed", utc=True, errors="coerce"
        )
        ts = ts.copy()
        ts.loc[nat_mask] = ts2
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    df = df.loc[ts >= cutoff].copy()
    df["_ts_utc_parsed"] = ts.loc[df.index]
    return df


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _pct(numer: float, denom: float) -> float:
    if not denom:
        return 0.0
    return round(float(numer) * 100.0 / float(denom), 2)


def _safe_str_series(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series([], dtype=str)
    return df[col].fillna("").astype(str).str.strip()


def _to_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


# ---------------------------------------------------------------------------
# Metric builders
# ---------------------------------------------------------------------------
def _build_funnel(
    events: pd.DataFrame,
    trades: pd.DataFrame,
    closes: pd.DataFrame,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """Return per-strategy funnel rows + totals dict."""
    signals_by_strat: Dict[str, int] = {}
    if not events.empty and {"stage", "strategy", "run_id"}.issubset(events.columns):
        stage = pd.to_numeric(events["stage"], errors="coerce")
        m = stage == 0
        sub = events.loc[m, ["strategy", "run_id"]].copy()
        sub["strategy"] = _safe_str_series(sub, "strategy")
        sub["run_id"] = _safe_str_series(sub, "run_id")
        g = sub.groupby("strategy")["run_id"].nunique()
        signals_by_strat = {str(k): int(v) for k, v in g.items() if str(k)}

    entries_by_strat: Dict[str, int] = {}
    if not trades.empty and "strategy" in trades.columns:
        g = _safe_str_series(trades, "strategy").value_counts()
        entries_by_strat = {str(k): int(v) for k, v in g.items() if str(k)}

    outcomes_by_strat: Dict[str, int] = {}
    if not closes.empty and "strategy" in closes.columns:
        g = _safe_str_series(closes, "strategy").value_counts()
        outcomes_by_strat = {str(k): int(v) for k, v in g.items() if str(k)}

    all_strats = sorted(
        set(signals_by_strat) | set(entries_by_strat) | set(outcomes_by_strat)
    )
    rows: List[Dict[str, Any]] = []
    total_sig = total_ent = total_out = 0
    for strat in all_strats:
        sig = signals_by_strat.get(strat, 0)
        ent = entries_by_strat.get(strat, 0)
        out = outcomes_by_strat.get(strat, 0)
        total_sig += sig
        total_ent += ent
        total_out += out
        rows.append(
            {
                "strategy": strat,
                "signals": sig,
                "entries": ent,
                "outcomes": out,
                "sig_to_entry_pct": _pct(ent, sig),
                "entry_to_outcome_pct": _pct(out, ent),
            }
        )
    rows.append(
        {
            "strategy": "TOTAL",
            "signals": total_sig,
            "entries": total_ent,
            "outcomes": total_out,
            "sig_to_entry_pct": _pct(total_ent, total_sig),
            "entry_to_outcome_pct": _pct(total_out, total_ent),
        }
    )
    totals = {
        "signals": total_sig,
        "entries": total_ent,
        "outcomes": total_out,
    }
    return rows, totals


def _build_outcomes_breakdown(closes: pd.DataFrame) -> List[Dict[str, Any]]:
    if closes.empty or "strategy" not in closes.columns:
        return []
    df = closes.copy()
    df["_strategy"] = _safe_str_series(df, "strategy")
    df["_outcome"] = _safe_str_series(df, "outcome")
    for c in ("pnl_pct", "pnl_r", "mfe_pct", "mae_pct"):
        df[c] = _to_float(df[c]) if c in df.columns else float("nan")
    out: List[Dict[str, Any]] = []
    for strat, sub in df.groupby("_strategy"):
        if not strat:
            continue
        total = int(len(sub))
        tp = int((sub["_outcome"] == "TP_hit").sum())
        sl = int((sub["_outcome"] == "SL_hit").sum())
        to = int((sub["_outcome"] == "TIMEOUT").sum())
        out.append(
            {
                "strategy": strat,
                "total": total,
                "tp": tp,
                "sl": sl,
                "timeout": to,
                "tp_pct": _pct(tp, total),
                "sl_pct": _pct(sl, total),
                "timeout_pct": _pct(to, total),
                "avg_pnl_pct": round(float(sub["pnl_pct"].mean()), 3)
                if sub["pnl_pct"].notna().any()
                else 0.0,
                "sum_pnl_r": round(float(sub["pnl_r"].sum()), 3)
                if sub["pnl_r"].notna().any()
                else 0.0,
                "avg_mfe_pct": round(float(sub["mfe_pct"].mean()), 3)
                if sub["mfe_pct"].notna().any()
                else 0.0,
                "avg_mae_pct": round(float(sub["mae_pct"].mean()), 3)
                if sub["mae_pct"].notna().any()
                else 0.0,
            }
        )
    out.sort(key=lambda r: (-r["total"], r["strategy"]))
    return out


def _build_latency(
    events: pd.DataFrame,
    trades: pd.DataFrame,
    outcomes: pd.DataFrame,
) -> List[Dict[str, Any]]:
    if trades.empty or "event_id" not in trades.columns:
        return []

    trades_c = trades[["event_id", "strategy", "entry_time_utc"]].copy() \
        if {"strategy", "entry_time_utc"}.issubset(trades.columns) \
        else pd.DataFrame()
    if trades_c.empty:
        return []
    trades_c["event_id"] = _safe_str_series(trades_c, "event_id")
    trades_c["strategy"] = _safe_str_series(trades_c, "strategy")
    trades_c["entry_ts"] = pd.to_datetime(
        trades_c["entry_time_utc"], utc=True, errors="coerce"
    )
    trades_c = trades_c.dropna(subset=["entry_ts"])

    sig_to_entry: Dict[str, List[float]] = {}
    if (
        not events.empty
        and {"event_id", "wall_time_utc"}.issubset(events.columns)
    ):
        ev = events[["event_id", "wall_time_utc"]].copy()
        ev["event_id"] = _safe_str_series(ev, "event_id")
        ev["wall_ts"] = pd.to_datetime(
            ev["wall_time_utc"], utc=True, errors="coerce"
        )
        ev = ev.dropna(subset=["wall_ts"])
        ev = ev.sort_values("wall_ts").drop_duplicates(
            "event_id", keep="first"
        )
        merged = trades_c.merge(ev, on="event_id", how="inner")
        if not merged.empty:
            merged["delta_min"] = (
                merged["entry_ts"] - merged["wall_ts"]
            ).dt.total_seconds() / 60.0
            merged = merged[merged["delta_min"].between(0, 24 * 60)]
            for strat, sub in merged.groupby("strategy"):
                sig_to_entry.setdefault(str(strat), []).extend(
                    sub["delta_min"].tolist()
                )

    entry_to_outcome: Dict[str, List[float]] = {}
    if (
        not outcomes.empty
        and {"event_id", "outcome_time_utc"}.issubset(outcomes.columns)
    ):
        oc = outcomes[["event_id", "outcome_time_utc"]].copy()
        oc["event_id"] = _safe_str_series(oc, "event_id")
        oc["outcome_ts"] = pd.to_datetime(
            oc["outcome_time_utc"], utc=True, errors="coerce"
        )
        oc = oc.dropna(subset=["outcome_ts"])
        oc = oc.sort_values("outcome_ts").drop_duplicates(
            "event_id", keep="first"
        )
        merged = trades_c.merge(oc, on="event_id", how="inner")
        if not merged.empty:
            merged["delta_min"] = (
                merged["outcome_ts"] - merged["entry_ts"]
            ).dt.total_seconds() / 60.0
            merged = merged[merged["delta_min"] >= 0]
            for strat, sub in merged.groupby("strategy"):
                entry_to_outcome.setdefault(str(strat), []).extend(
                    sub["delta_min"].tolist()
                )

    all_strats = sorted(set(sig_to_entry) | set(entry_to_outcome))
    rows: List[Dict[str, Any]] = []
    for strat in all_strats:
        if not strat:
            continue
        se = sig_to_entry.get(strat, [])
        eo = entry_to_outcome.get(strat, [])
        rows.append(
            {
                "strategy": strat,
                "signal_to_entry_min": round(float(pd.Series(se).median()), 2)
                if se
                else None,
                "signal_to_entry_n": len(se),
                "entry_to_outcome_min": round(float(pd.Series(eo).median()), 2)
                if eo
                else None,
                "entry_to_outcome_n": len(eo),
            }
        )
    return rows


def _build_timeline(closes: pd.DataFrame, days: int) -> List[Dict[str, Any]]:
    today = datetime.now(timezone.utc).date()
    dates = [(today - timedelta(days=i)) for i in range(days - 1, -1, -1)]
    result = {d: {"entries": 0, "tp": 0, "sl": 0, "timeout": 0} for d in dates}
    if closes.empty or "_ts_utc_parsed" not in closes.columns:
        return [
            {
                "date": d.strftime("%Y-%m-%d"),
                "entries": 0,
                "tp": 0,
                "sl": 0,
                "timeout": 0,
            }
            for d in dates
        ]
    df = closes.copy()
    df["_date"] = df["_ts_utc_parsed"].dt.date
    df["_outcome"] = _safe_str_series(df, "outcome")
    for d, sub in df.groupby("_date"):
        if d not in result:
            continue
        result[d]["entries"] = int(len(sub))
        result[d]["tp"] = int((sub["_outcome"] == "TP_hit").sum())
        result[d]["sl"] = int((sub["_outcome"] == "SL_hit").sum())
        result[d]["timeout"] = int((sub["_outcome"] == "TIMEOUT").sum())
    return [
        {
            "date": d.strftime("%Y-%m-%d"),
            "entries": result[d]["entries"],
            "tp": result[d]["tp"],
            "sl": result[d]["sl"],
            "timeout": result[d]["timeout"],
        }
        for d in dates
    ]


def _build_skip_reasons(events: pd.DataFrame) -> List[Dict[str, Any]]:
    if events.empty or not {"strategy", "entry_ok", "skip_reasons"}.issubset(
        events.columns
    ):
        return []
    df = events.copy()
    df["_entry_ok"] = pd.to_numeric(df["entry_ok"], errors="coerce").fillna(0)
    df = df[df["_entry_ok"] == 0]
    if df.empty:
        return []
    df["_strategy"] = _safe_str_series(df, "strategy")
    df["_skip"] = _safe_str_series(df, "skip_reasons")
    rows: List[Dict[str, Any]] = []
    for strat, sub in df.groupby("_strategy"):
        if not strat:
            continue
        reasons: Dict[str, int] = {}
        for raw in sub["_skip"]:
            if not raw:
                continue
            for tok in raw.replace("|", ";").replace(",", ";").split(";"):
                tok = tok.strip()
                if not tok:
                    continue
                reasons[tok] = reasons.get(tok, 0) + 1
        top = sorted(reasons.items(), key=lambda kv: (-kv[1], kv[0]))[:5]
        if not top:
            continue
        rows.append(
            {
                "strategy": strat,
                "top": [{"reason": r, "count": c} for r, c in top],
            }
        )
    rows.sort(key=lambda r: (-sum(x["count"] for x in r["top"]), r["strategy"]))
    return rows


def _build_active_positions(
    active_short: Optional[Mapping[str, Any]],
    active_fast0: Optional[Mapping[str, Any]],
) -> Dict[str, Any]:
    import time

    now = time.time()
    short_list: List[Dict[str, Any]] = []
    for sym, meta in (active_short or {}).items():
        started = 0.0
        run_id = ""
        if isinstance(meta, Mapping):
            started = float(meta.get("started_ts", 0) or 0)
            run_id = str(meta.get("run_id", "") or "")
        age = int(max(0, now - started)) if started else None
        short_list.append({"symbol": sym, "run_id": run_id, "age_sec": age})
    fast0_list: List[Dict[str, Any]] = []
    for sym, ts in (active_fast0 or {}).items():
        try:
            started = float(ts)
        except Exception:
            started = 0.0
        age = int(max(0, now - started)) if started else None
        fast0_list.append({"symbol": sym, "age_sec": age})
    short_list.sort(key=lambda r: r["symbol"])
    fast0_list.sort(key=lambda r: r["symbol"])
    return {"short": short_list, "fast0": fast0_list}


def _extract_live_whitelist(base_dir: Path) -> set[str]:
    """Extract live profile whitelist literals from trading/runner.py."""
    runner_path = base_dir / "trading" / "runner.py"
    if not runner_path.is_file():
        return set()
    try:
        text = runner_path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return set()

    # Prefer explicit variable names used in this repo; fallback is empty set.
    block_match = re.search(
        r"(?s)(?:_LIVE_PROFILE_WHITELIST|allowed_live_profiles)\s*=\s*(?:frozenset|set)?\s*\((.*?)\)",
        text,
    )
    if not block_match:
        block_match = re.search(
            r"(?s)(?:_LIVE_PROFILE_WHITELIST|allowed_live_profiles)\s*=\s*(\{.*?\})",
            text,
        )
    if not block_match:
        return set()

    literal_block = block_match.group(1)
    items = re.findall(r'["\']([^"\']+)["\']', literal_block)
    return {s.strip() for s in items if s.strip()}


def build_mode_matrix(
    base_dir: str | os.PathLike,
    days: int,
    closes_df: Optional[pd.DataFrame] = None,
) -> List[Dict[str, Any]]:
    """Build execution mode matrix by strategy with whitelist status."""
    base = Path(base_dir)
    closes = closes_df if closes_df is not None else _load_trading_closes(base, days)
    whitelist = _extract_live_whitelist(base)

    events = _load_partitioned(base, days, "events_v3.csv")
    events_strats: set[str] = set()
    if not events.empty and "strategy" in events.columns:
        events_strats = {
            s for s in _safe_str_series(events, "strategy").tolist() if s
        }

    close_strats: set[str] = set()
    if not closes.empty and "strategy" in closes.columns:
        close_strats = {
            s for s in _safe_str_series(closes, "strategy").tolist() if s
        }

    strategy_profiles: Dict[str, set[str]] = {
        "short_pump": {"short_pump_mid", "short_pump_active_1R", "short_pump_deep", "short_pump_funding_1R"},
        "short_pump_filtered": {"short_pump_filtered_1R"},
        "short_pump_fast0": {"fast0_selective", "fast0_2R", "fast0_1p5R", "fast0_base_1R"},
        "short_pump_fast0_filtered": {"fast0_selective", "fast0_2R", "fast0_1p5R", "fast0_base_1R"},
        "short_pump_premium": {"short_pump_premium_1R"},
        "short_pump_wick": {"short_pump_wick_1R"},
        "false_pump": set(),
    }
    all_strats = set(close_strats) | set(strategy_profiles.keys())
    if "false_pump" in events_strats:
        all_strats.add("false_pump")

    mode_series = (
        _safe_str_series(closes, "mode") if "mode" in closes.columns else pd.Series([], dtype=str)
    )
    strategy_series = (
        _safe_str_series(closes, "strategy") if "strategy" in closes.columns else pd.Series([], dtype=str)
    )

    matrix: List[Dict[str, Any]] = []
    for strategy in sorted(all_strats):
        strat_mask = strategy_series == strategy
        paper_mask = strat_mask & (mode_series == "paper")
        live_mask = strat_mask & (mode_series == "live")
        paper_count = int(paper_mask.sum())
        live_count = int(live_mask.sum())
        has_paper = paper_count > 0
        has_live = live_count > 0

        mapped_profiles = strategy_profiles.get(strategy, set())
        in_whitelist = bool(mapped_profiles & whitelist) if mapped_profiles else False

        # STATUS is based on current whitelist policy, not historical live closes.
        if in_whitelist:
            status = "live"
        elif has_paper:
            status = "paper_only"
        else:
            status = "inactive"

        # LIVE column text policy:
        # - if in whitelist -> show "в whitelist"
        # - else if historical live exists -> show "<n> (историч.)"
        # - else -> "—"
        if in_whitelist:
            live_display = "в whitelist"
        elif has_live:
            live_display = f"{live_count} (историч.)"
        else:
            live_display = "—"

        matrix.append(
            {
                "strategy": strategy,
                "in_whitelist": in_whitelist,
                "has_paper": has_paper,
                "has_live": has_live,
                "paper_count": paper_count,
                "live_count": live_count,
                "live_display": live_display,
                "status": status,
            }
        )
    return matrix


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def build_dashboard_data(
    base_dir: str | os.PathLike = _DEFAULT_BASE_DIR,
    days: int = 7,
    active_short: Optional[Mapping[str, Any]] = None,
    active_fast0: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Build aggregated dashboard payload (JSON-serializable)."""
    try:
        days = int(days)
    except Exception:
        days = 7
    days = max(_MIN_DAYS, min(_MAX_DAYS, days))
    base = Path(base_dir)

    try:
        events = _load_partitioned(base, days, "events_v3.csv")
    except Exception as exc:
        _logger.warning("dashboard: events load failed: %s", exc)
        events = pd.DataFrame()
    try:
        trades = _load_partitioned(base, days, "trades_v3.csv")
    except Exception as exc:
        _logger.warning("dashboard: trades load failed: %s", exc)
        trades = pd.DataFrame()
    try:
        outcomes = _load_partitioned(base, days, "outcomes_v3.csv")
    except Exception as exc:
        _logger.warning("dashboard: outcomes load failed: %s", exc)
        outcomes = pd.DataFrame()
    try:
        closes = _load_trading_closes(base, days)
    except Exception as exc:
        _logger.warning("dashboard: closes load failed: %s", exc)
        closes = pd.DataFrame()

    try:
        funnel, totals = _build_funnel(events, trades, closes)
    except Exception as exc:
        _logger.warning("dashboard: funnel failed: %s", exc)
        funnel, totals = [], {"signals": 0, "entries": 0, "outcomes": 0}

    try:
        outcomes_breakdown = _build_outcomes_breakdown(closes)
    except Exception as exc:
        _logger.warning("dashboard: outcomes breakdown failed: %s", exc)
        outcomes_breakdown = []

    try:
        latency = _build_latency(events, trades, outcomes)
    except Exception as exc:
        _logger.warning("dashboard: latency failed: %s", exc)
        latency = []

    try:
        timeline = _build_timeline(closes, days)
    except Exception as exc:
        _logger.warning("dashboard: timeline failed: %s", exc)
        timeline = []

    try:
        skip_reasons = _build_skip_reasons(events)
    except Exception as exc:
        _logger.warning("dashboard: skip reasons failed: %s", exc)
        skip_reasons = []

    try:
        mode_matrix = build_mode_matrix(base, days, closes_df=closes)
    except Exception as exc:
        _logger.warning("dashboard: mode matrix failed: %s", exc)
        mode_matrix = []

    active_positions = _build_active_positions(active_short, active_fast0)

    tp_total = sum(row.get("tp", 0) for row in outcomes_breakdown)
    win_rate_pct = _pct(tp_total, totals["outcomes"])

    generated_at = datetime.now(timezone.utc)
    cutoff = generated_at - timedelta(days=days)

    return {
        "meta": {
            "generated_at_utc": generated_at.isoformat(timespec="seconds"),
            "days": days,
            "cutoff_utc": cutoff.isoformat(timespec="seconds"),
        },
        "summary": {
            "total_signals": totals["signals"],
            "total_entries": totals["entries"],
            "total_outcomes": totals["outcomes"],
            "win_rate_pct": win_rate_pct,
        },
        "funnel": funnel,
        "outcomes_breakdown": outcomes_breakdown,
        "latency": latency,
        "timeline": timeline,
        "skip_reasons": skip_reasons,
        "mode_matrix": mode_matrix,
        "active_positions": active_positions,
    }
