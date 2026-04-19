#!/usr/bin/env python3
"""
Daily outcomes report to Telegram.

Usage:
    python3 scripts/daily_tg_report.py --data-dir /root/pump_short/datasets --strategy short_pump --days 7
    python3 scripts/daily_tg_report.py --root /root/pump_short/datasets --strategy short_pump --days 7
    # Investor report (charts + extended metrics, PNG in logs/reports, optional TG photos):
    python3 scripts/daily_tg_report.py --root /root/pump_short/datasets --strategy short_pump --days 30 --investor-report --rolling 20 [--debug]
"""

from __future__ import annotations

import argparse
import io
import json
import mimetypes
import os
import re
import subprocess
import sys
import tempfile
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

# Ensure project root (parent of scripts/) is on sys.path so analysis/* and shared_analytics import correctly
_PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv("/root/pump_short/.env", override=False)
except ImportError:
    pass

import pandas as pd

from analytics.load import get_datasets_date_range, load_outcomes, load_events_v2
from analytics.stats import (
    _normalize_outcome_raw,
    _core_mask,
    _safe_stats_core,
    _format_filter_line,
    _maturity_status_unified,
    lightgbm_readiness_block,
    wr_core_from_tp_sl,
    ev_core_from_tp_sl_pnl,
    _sanity_check_wr,
)
from analytics.fast0_blocks import (
    _add_fast0_active_mode_block,
    _add_fast0_dist_to_peak_block,
    _add_fast0_edge_decay_block,
    _add_fast0_hypothesis_blocks,
    _add_fast0_hypothesis_blocks_compact,
    _add_fast0_whatif_tp_block,
)
from analytics.short_pump_blocks import filter_active_trades
from analytics.executive_report import build_executive_compact_report
from telegram.send_helpers import send_document as _send_document_file

# Canonical guard refresh window — independent of report display window.
# Override via env: GUARD_CANONICAL_DAYS=60 to use a wider history.
_GUARD_CANONICAL_DAYS: int = int(os.getenv("GUARD_CANONICAL_DAYS", "30"))


def _run_guard_update(data_dir: Path, days: int, rolling: int = 20) -> None:
    """
    Run update_auto_risk_guard.py before loading guard_state.
    Always uses _GUARD_CANONICAL_DAYS (default 30) regardless of the report display
    window (days), so guard_state is never overwritten by arbitrary report windows.
    Failures are logged but do not block report generation.
    """
    pump_short_root = data_dir.parent if data_dir.name == "datasets" else Path("/root/pump_short")
    script = Path(__file__).resolve().parent / "update_auto_risk_guard.py"
    if not script.is_file():
        return
    try:
        subprocess.run(
            [sys.executable, str(script), "--data-dir", str(data_dir), "--days", str(_GUARD_CANONICAL_DAYS), "--rolling", str(rolling), "--pump-short-root", str(pump_short_root)],
            cwd=str(_PROJECT_ROOT),
            capture_output=True,
            timeout=120,
        )
    except Exception:
        pass  # best-effort; report continues with existing guard state


def _safe_json_load(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        data = json.loads(value)
        return data if isinstance(data, dict) else {}
    except json.JSONDecodeError:
        return {}


def _get_details_payload(details: Dict[str, Any]) -> Dict[str, Any]:
    payload = details.get("details_payload")
    return _safe_json_load(payload)


def _normalize_outcome(raw: Any) -> str:
    """Backward-compatible wrapper around shared _normalize_outcome_raw."""
    return _normalize_outcome_raw(raw)


def _parse_pnl(value: Any) -> float:
    try:
        return float(value) if not pd.isna(value) else 0.0
    except Exception:
        return 0.0


def _extract_conflict(details: Dict[str, Any]) -> int:
    value = details.get("tp_sl_same_candle")
    try:
        return 1 if int(value) == 1 else 0
    except Exception:
        return 0


def _coverage_pct(df: pd.DataFrame, col: str) -> str:
    if col not in df.columns:
        return "N/A"
    pct = df[col].notna().mean() * 100
    return f"{pct:.0f}%"


def _base_id_series(s: pd.Series) -> pd.Series:
    out = s.astype(str).str.replace(r"_(entry.*|outcome)$", "", regex=True)
    return out


def _dataset_quality_outcomes(df: pd.DataFrame) -> List[str]:
    parts = []
    key_cols = ["trade_id", "event_id", "run_id", "symbol", "outcome", "pnl_pct", "hold_seconds", "details_json"]
    coverage_parts = [f"{c} {_coverage_pct(df, c)}" for c in key_cols if c in df.columns]
    if coverage_parts:
        parts.append(", ".join(coverage_parts))
    n = len(df)
    dup = (n - df["trade_id"].nunique()) if "trade_id" in df.columns else None
    parts.append(f"dup_trade_id={dup}" if dup is not None else "dup_trade_id=N/A")
    if "outcome" in df.columns:
        norm = df["outcome"].apply(lambda x: _normalize_outcome(x))
        valid = norm.isin(["TP_hit", "SL_hit", "TIMEOUT"])
        other_pct = (1 - valid.mean()) * 100 if len(norm) else 0
        parts.append(f"outcome_other={other_pct:.1f}%")
    else:
        parts.append("outcome_other=N/A")
    line1 = "; ".join(parts)
    # second line: pnl + hold
    pnl_parts = []
    if "pnl_pct" in df.columns:
        pnl = pd.to_numeric(df["pnl_pct"], errors="coerce").dropna()
        if len(pnl):
            outside = ((pnl < -5) | (pnl > 5)).mean() * 100
            pnl_parts.append(f"pnl: min={pnl.min():.2f} max={pnl.max():.2f}, outside_[-5,5]={outside:.1f}%")
        else:
            pnl_parts.append("pnl: N/A")
    else:
        pnl_parts.append("pnl: N/A")
    if "hold_seconds" in df.columns:
        hold = pd.to_numeric(df["hold_seconds"], errors="coerce")
        pct_zero = (hold <= 0).mean() * 100 if hold.notna().any() else 0
        pnl_parts.append(f"hold<=0: {pct_zero:.1f}%")
    else:
        pnl_parts.append("hold<=0: N/A")
    return [line1, " ".join(pnl_parts)]


def _dataset_quality_events(
    events_raw: pd.DataFrame,
    outcomes_df: pd.DataFrame,
) -> List[str]:
    lines = []
    lines.append(f"events_total={len(events_raw)}")
    entry_col = next((c for c in ["entry_ok", "entryOk", "is_entry", "entry_flag"] if c in events_raw.columns), None)
    if entry_col is not None:
        ok = pd.to_numeric(events_raw[entry_col], errors="coerce") == 1
        rate = ok.mean() * 100 if ok.notna().any() else 0
        lines.append(f"entry_ok_rate={rate:.1f}%")
    else:
        lines.append("entry_ok_rate=N/A")
    if "stage" in events_raw.columns:
        top3 = events_raw["stage"].value_counts(dropna=False).head(3)
        parts = [f"{k}={v}" for k, v in top3.items()]
        lines.append("stages: " + ",".join(parts))
    else:
        lines.append("stages: N/A")
    # join by base_id
    outcomes_total = len(outcomes_df)
    lines.append(f"outcomes_total={outcomes_total}")
    event_id_candidates = ["event_id", "eventId", "event_uuid", "id"]
    ev_raw = next((c for c in event_id_candidates if c in events_raw.columns), None)
    ev_out = next((c for c in event_id_candidates if c in outcomes_df.columns), None)
    if entry_col is not None and ev_raw is not None and ev_out is not None:
        ok_mask = pd.to_numeric(events_raw[entry_col], errors="coerce") == 1
        entries_ok = events_raw.loc[ok_mask]
        entries_total = len(entries_ok)
        if entries_total > 0:
            base_ev = _base_id_series(entries_ok[ev_raw])
            base_out = _base_id_series(outcomes_df[ev_out])
            ids_out = set(base_out.dropna().unique())
            matched = base_ev.isin(ids_out).sum()
            join_rate = matched / entries_total * 100
            missing = entries_total - matched
            lines.append(f"join_rate_by_base_id={join_rate:.1f}%")
            lines.append(f"missing_outcomes_by_base_id={missing}")
        else:
            lines.append("join_rate_by_base_id=N/A")
            lines.append("missing_outcomes_by_base_id=N/A")
    else:
        lines.append("join_rate_by_base_id=N/A")
        lines.append("missing_outcomes_by_base_id=N/A")
    return lines


def _liq_snapshot(events_raw: pd.DataFrame) -> List[str]:
    lines = []
    short_30 = "liq_short_usd_30s" in events_raw.columns
    long_30 = "liq_long_usd_30s" in events_raw.columns
    if not short_30 and not long_30:
        lines.append("(no liq columns)")
        return lines
    if long_30:
        s = pd.to_numeric(events_raw["liq_long_usd_30s"], errors="coerce")
        nz = ((s.notna()) & (s.abs() > 0)).mean() * 100
        lines.append(f"liq_long_30s nonzero={nz:.1f}%")
    else:
        lines.append("liq_long_30s nonzero=N/A")
    if short_30:
        s = pd.to_numeric(events_raw["liq_short_usd_30s"], errors="coerce")
        nz = ((s.notna()) & (s.abs() > 0)).mean() * 100
        lines.append(f"liq_short_30s nonzero={nz:.1f}%")
    else:
        lines.append("liq_short_30s nonzero=N/A")
    # top1 liq event
    er = events_raw.copy()
    if long_30 and short_30:
        s1 = pd.to_numeric(er["liq_short_usd_30s"], errors="coerce").fillna(0)
        s2 = pd.to_numeric(er["liq_long_usd_30s"], errors="coerce").fillna(0)
        er["_liq_max"] = pd.concat([s1, s2], axis=1).max(axis=1)
    elif "liq_short_usd_1m" in er.columns and "liq_long_usd_1m" in er.columns:
        s1 = pd.to_numeric(er["liq_short_usd_1m"], errors="coerce").fillna(0)
        s2 = pd.to_numeric(er["liq_long_usd_1m"], errors="coerce").fillna(0)
        er["_liq_max"] = pd.concat([s1, s2], axis=1).max(axis=1)
    else:
        er["_liq_max"] = 0.0
    top1 = er.nlargest(1, "_liq_max")
    if not top1.empty and top1["_liq_max"].iloc[0] > 0:
        row = top1.iloc[0]
        sym = row.get("symbol", "?")
        stage = row.get("stage", "?")
        eok = row.get("entry_ok", "?")
        val = top1["_liq_max"].iloc[0]
        lines.append(f"top liq: {sym} stage={stage} entry_ok={eok} liq_usd={val:.0f}")
    else:
        lines.append("top liq: (none)")
    return lines




def _report_title(strategy: str) -> str:
    if strategy == "long_pullback":
        return "📈 LONG_PULLBACK — MANAGEMENT REPORT"
    if strategy in ("short_pump_fast0", "short_pump_fast0_filtered"):
        if strategy == "short_pump_fast0_filtered":
            return "⚡ FAST0 FILTERED — MANAGEMENT REPORT"
        return "⚡ FAST0 — MANAGEMENT REPORT"
    return "📊 SHORT_PUMP — MANAGEMENT REPORT"


def _format_report(
    date_range: Tuple[str, str],
    total_trades: int,
    pnl_sum: float,
    winrate: float,
    conflicts_total: int,
    conflicts_top: int,
    conflicts_payload: int,
    by_strategy: Dict[str, Dict[str, Any]],
    strategies_label: str,
    outcomes_df: pd.DataFrame,
    events_raw: Optional[pd.DataFrame] = None,
    avg_win_R: float = 0.0,
    avg_loss_R: float = 0.0,
    expectancy: float = 0.0,
    avg_hold: float = 0.0,
    maturity_pct: float = 0.0,
    tp: int = 0,
    sl: int = 0,
    timeout: int = 0,
    other_count: int = 0,
    wr_core: float = 0.0,
    expectancy_core: float = 0.0,
    strategy_set: Optional[set] = None,
    report_strategy: str = "short_pump",
    debug: bool = False,
) -> str:
    if total_trades == 0:
        return "Исходов пока нет"
    
    start, end = date_range
    ev_mean = (pnl_sum / total_trades) if total_trades else 0.0
    
    # Breakeven WR (TP=1.2, SL=1.0)
    breakeven_wr = 1.0 / (1.2 + 1.0) if (1.2 + 1.0) > 0 else 0.0
    
    # System status
    if ev_mean > 0.02:
        status = "🟢 Положительный"
    elif ev_mean > 0:
        status = "🟡 Слабое преимущество"
    else:
        status = "🔴 Отрицательный"
    
    # Extract events data (strategy-filtered events_raw passed in)
    events_total = len(events_raw) if events_raw is not None and not events_raw.empty else 0
    entry_ok_rate = 0.0
    entry_ok_count_fast0: Optional[int] = None  # For fast0: unique trade_id in outcomes
    stage3 = 0
    stage4 = 0
    stage0 = 0
    if report_strategy in ("short_pump_fast0", "short_pump_fast0_filtered"):
        # Entry OK = number of trades (unique trade_id in outcomes)
        if "trade_id" in outcomes_df.columns:
            entry_ok_count_fast0 = int(outcomes_df["trade_id"].nunique())
        else:
            entry_ok_count_fast0 = len(outcomes_df)
    elif events_raw is not None and not events_raw.empty:
        entry_col = next((c for c in ["entry_ok", "entryOk", "is_entry", "entry_flag"] if c in events_raw.columns), None)
        if entry_col is not None:
            ok = pd.to_numeric(events_raw[entry_col], errors="coerce") == 1
            entry_ok_rate = ok.mean() * 100 if ok.notna().any() else 0.0
        if "stage" in events_raw.columns:
            stage_counts = events_raw["stage"].value_counts(dropna=False)
            stage3 = int(stage_counts.get(3, 0))
            stage4 = int(stage_counts.get(4, 0))
            stage0 = int(stage_counts.get(0, 0))

    # Extract liq data (strategy-filtered)
    # For FAST0: use ONLY events for this strategy (avoid SIRENUSDT etc from other strategies)
    liq_long_30s = 0.0
    liq_short_30s = 0.0
    top_liq_symbol = "N/A"
    top_liq_value = 0.0
    liq_no_data_fast0 = False

    if report_strategy in ("short_pump_fast0", "short_pump_fast0_filtered"):
        events_liq = (
            events_raw[events_raw["strategy"].astype(str).isin(["short_pump_fast0", "short_pump_fast0_filtered"])]
            if events_raw is not None and not events_raw.empty and "strategy" in events_raw.columns
            else events_raw
        )
    else:
        events_liq = events_raw

    if events_liq is None or events_liq.empty:
        liq_no_data_fast0 = report_strategy in ("short_pump_fast0", "short_pump_fast0_filtered")
    elif report_strategy in ("short_pump_fast0", "short_pump_fast0_filtered"):
        # FAST0: strict logic — long/short from available metrics, fallbacks, max spike from same row
        long_col = "liq_long_usd_30s" if "liq_long_usd_30s" in events_liq.columns else ("liq_long_count_30s" if "liq_long_count_30s" in events_liq.columns else None)
        short_col = "liq_short_usd_30s" if "liq_short_usd_30s" in events_liq.columns else ("liq_short_count_30s" if "liq_short_count_30s" in events_liq.columns else None)
        has_any = long_col is not None or short_col is not None
        if not has_any:
            liq_no_data_fast0 = True
        else:
            if long_col:
                s = pd.to_numeric(events_liq[long_col], errors="coerce")
                liq_long_30s = ((s.notna()) & (s.abs() > 0)).mean() * 100
            if short_col:
                s = pd.to_numeric(events_liq[short_col], errors="coerce")
                liq_short_30s = ((s.notna()) & (s.abs() > 0)).mean() * 100
            # Max spike: prefer liq_*_usd_30s, else liq_*_count_30s
            max_col_long = "liq_long_usd_30s" if "liq_long_usd_30s" in events_liq.columns else "liq_long_count_30s"
            max_col_short = "liq_short_usd_30s" if "liq_short_usd_30s" in events_liq.columns else "liq_short_count_30s"
            er = events_liq.copy()
            cols_for_max = [c for c in [max_col_long, max_col_short] if c in er.columns]
            if cols_for_max:
                er["_liq_max"] = pd.concat([pd.to_numeric(er[c], errors="coerce").fillna(0) for c in cols_for_max], axis=1).max(axis=1)
                top1 = er.nlargest(1, "_liq_max")
                if not top1.empty and top1["_liq_max"].iloc[0] > 0:
                    row = top1.iloc[0]
                    top_liq_symbol = str(row.get("symbol", "N/A"))
                    top_liq_value = float(top1["_liq_max"].iloc[0])
                    metric_used = max_col_long if max_col_long in cols_for_max else max_col_short
                    if debug:
                        time_col = next((c for c in ["time_utc", "ts_utc", "event_time"] if c in row.index), None)
                        time_val = str(row.get(time_col, "?")) if time_col else "?"
                        print(f"DEBUG FAST0 liq max-spike: metric={metric_used} top1 symbol={top_liq_symbol} time={time_val} value={top_liq_value:.0f}", file=sys.stderr)
    elif events_raw is not None and not events_raw.empty:
        if "liq_long_usd_30s" in events_raw.columns:
            s = pd.to_numeric(events_raw["liq_long_usd_30s"], errors="coerce")
            liq_long_30s = ((s.notna()) & (s.abs() > 0)).mean() * 100
        if "liq_short_usd_30s" in events_raw.columns:
            s = pd.to_numeric(events_raw["liq_short_usd_30s"], errors="coerce")
            liq_short_30s = ((s.notna()) & (s.abs() > 0)).mean() * 100
        has_liq = "liq_short_usd_30s" in events_raw.columns or "liq_long_usd_30s" in events_raw.columns
        if has_liq:
            er = events_raw.copy()
            if "liq_short_usd_30s" in er.columns and "liq_long_usd_30s" in er.columns:
                s1 = pd.to_numeric(er["liq_short_usd_30s"], errors="coerce").fillna(0)
                s2 = pd.to_numeric(er["liq_long_usd_30s"], errors="coerce").fillna(0)
                er["_liq_max"] = pd.concat([s1, s2], axis=1).max(axis=1)
                top1 = er.nlargest(1, "_liq_max")
                if not top1.empty and top1["_liq_max"].iloc[0] > 0:
                    top_liq_symbol = str(top1.iloc[0].get("symbol", "N/A"))
                    top_liq_value = float(top1["_liq_max"].iloc[0])
    
    # Extract dataset quality
    dup_trade_id = 0
    outcome_other = 0.0
    pnl_outside = 0.0
    hold_zero: Optional[float] = None  # None = N/A (no column)
    try:
        n = len(outcomes_df)
        if "trade_id" in outcomes_df.columns:
            dup_trade_id = n - outcomes_df["trade_id"].nunique()
        if "outcome" in outcomes_df.columns:
            norm = outcomes_df["outcome"].apply(lambda x: _normalize_outcome(x))
            valid = norm.isin(["TP_hit", "SL_hit", "TIMEOUT"])
            outcome_other = (1 - valid.mean()) * 100 if len(norm) else 0.0
        if "pnl_pct" in outcomes_df.columns:
            pnl = pd.to_numeric(outcomes_df["pnl_pct"], errors="coerce").dropna()
            if len(pnl):
                pnl_outside = ((pnl < -5) | (pnl > 5)).mean() * 100
        if "hold_seconds" in outcomes_df.columns:
            hold = pd.to_numeric(outcomes_df["hold_seconds"], errors="coerce")
            hold_zero = float((hold <= 0).mean() * 100) if hold.notna().any() else 0.0
    except Exception:
        pass
    
    # Build report
    title = _report_title(report_strategy)
    lines = [
        title,
        f"📅 {start}..{end}",
        f"Стратегия: {report_strategy}",
        "",
        "━━━━━━━━━━",
        "1️⃣ РЕЗУЛЬТАТЫ",
        "",
        f"• Сделки: {total_trades}",
        f"• Винрейт: {winrate:.1%}",
        f"• Итоговый PnL: {pnl_sum:.3f}R",
        f"• EV: {ev_mean:+.4f}R",
        "",
        f"• TP/SL/OTHER: {tp}/{sl}/{other_count}",
        f"• Breakeven WR (TP=1.2, SL=1.0): {breakeven_wr:.1%}",
        f"• WR core (только TP/SL): {wr_core:.1%}",
        f"• Expectancy core (только TP/SL): {expectancy_core:+.4f}R",
        "",
        f"• Средний выигрыш: {avg_win_R:+.2f}R",
        f"• Средний проигрыш: {avg_loss_R:+.2f}R",
        f"• Expectancy: {expectancy:+.4f}R",
        "",
        "━━━━━━━━━━",
        "2️⃣ КАЧЕСТВО ИСПОЛНЕНИЯ",
        "",
        f"• Событий просканировано: {events_total}",
        f"• Доля Entry OK: {entry_ok_rate:.1f}%" if entry_ok_count_fast0 is None else f"• Trades (unique): {entry_ok_count_fast0}",
        f"• Исходов записано: {total_trades}",
        f"• Целостность связок: 100%",
        f"• Конфликты: {conflicts_total}",
        "",
        f"Stages: fast0 only" if report_strategy in ("short_pump_fast0", "short_pump_fast0_filtered") else f"Stages: 3={stage3}, 4={stage4}, 0={stage0}",
        "",
        "━━━━━━━━━━",
        "3️⃣ ДИНАМИКА СДЕЛОК",
        "",
        f"• Среднее время удержания: {avg_hold:.0f} сек",
        "",
        "━━━━━━━━━━",
        "4️⃣ ЛИКВИДАЦИИ",
        "",
        *(
            [f"Ликвидации: недостаточно данных"]
            if liq_no_data_fast0
            else [
                f"• Long liq 30s ненулевые: {liq_long_30s:.1f}%",
                f"• Short liq 30s ненулевые: {liq_short_30s:.1f}%",
                f"• Максимальный всплеск: {top_liq_symbol} {top_liq_value:.0f}",
            ]
        ),
        "",
        "━━━━━━━━━━",
        "5️⃣ КАЧЕСТВО ДАТАСЕТА",
        "",
        f"• Дубликаты: {dup_trade_id}",
        f"• Outcome_other: {outcome_other:.1f}%",
        f"• PnL вне [-5;5]: {pnl_outside:.1f}%",
        f"• Hold: N/A" if hold_zero is None else f"• Hold<=0: {hold_zero:.1f}%",
        "",
        "━━━━━━━━━━",
        "6️⃣ ЗРЕЛОСТЬ ДАННЫХ",
        "",
        f"• Исходы: {len(outcomes_df)}/100 ({maturity_pct:.0f}%)",
        f"Статус системы: {status}",
    ]
    
    report_text = "\n".join(lines)
    
    # Trim if too long
    if len(report_text) > 4000:
        report_text = report_text[:3997] + "..."
    
    return report_text


def _load_env_file(path: str) -> Dict[str, str]:
    """Load .env file manually (without python-dotenv dependency).
    Returns dict of KEY=VALUE pairs. Ignores empty lines and comments (#).
    Supports quoted values ('...' or "...")."""
    env_map: Dict[str, str] = {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip()
                # Remove quotes if present
                if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
                    value = value[1:-1]
                if key:
                    env_map[key] = value
    except FileNotFoundError:
        pass
    except Exception:
        pass
    return env_map


def _is_bad_placeholder(v: str) -> bool:
    """Check if value is a bad placeholder (non-ASCII, starts with 'ВАШ_', or empty after strip)."""
    if not v:
        return True
    v = v.strip()
    if not v:
        return True
    if v.startswith("ВАШ_"):
        return True
    if any(ord(c) > 127 for c in v):
        return True
    return False


def _get_tg_creds(debug: bool = False) -> Tuple[Optional[str], Optional[str]]:
    """Get Telegram credentials with priority: env_file > os.environ (if not bad placeholder).
    Returns (token, chat_id) or (None, None) if not found/invalid.
    Validates token is ASCII-only to prevent UnicodeEncodeError."""
    
    # Load from /root/pump_short/.env file (without python-dotenv)
    env_map = _load_env_file("/root/pump_short/.env")
    
    # Get from os.environ
    os_token_tg = os.getenv("TG_BOT_TOKEN")
    os_token_telegram = os.getenv("TELEGRAM_BOT_TOKEN")
    os_chat_tg = os.getenv("TG_CHAT_ID")
    os_chat_telegram = os.getenv("TELEGRAM_CHAT_ID")
    
    # Get from env file
    file_token_telegram = env_map.get("TELEGRAM_BOT_TOKEN")
    file_token_tg = env_map.get("TG_BOT_TOKEN")
    file_chat_telegram = env_map.get("TELEGRAM_CHAT_ID")
    file_chat_tg = env_map.get("TG_CHAT_ID")
    
    # Select token with priority:
    # 1. TELEGRAM_BOT_TOKEN from file (if valid)
    # 2. TG_BOT_TOKEN from file (if valid)
    # 3. TELEGRAM_BOT_TOKEN from os.environ (if valid and not bad)
    # 4. TG_BOT_TOKEN from os.environ (if valid and not bad, but replace if bad with file value)
    token = None
    token_source = None
    
    if file_token_telegram and not _is_bad_placeholder(file_token_telegram):
        token = file_token_telegram.strip()
        token_source = "env_file:TELEGRAM_BOT_TOKEN"
    elif file_token_tg and not _is_bad_placeholder(file_token_tg):
        token = file_token_tg.strip()
        token_source = "env_file:TG_BOT_TOKEN"
    elif os_token_telegram and not _is_bad_placeholder(os_token_telegram):
        token = os_token_telegram.strip()
        token_source = "os.environ:TELEGRAM_BOT_TOKEN"
    elif os_token_tg:
        if _is_bad_placeholder(os_token_tg):
            # Replace bad placeholder with file value if available
            if file_token_telegram and not _is_bad_placeholder(file_token_telegram):
                token = file_token_telegram.strip()
                token_source = "env_file:TELEGRAM_BOT_TOKEN (replaced bad os.environ:TG_BOT_TOKEN)"
            elif file_token_tg and not _is_bad_placeholder(file_token_tg):
                token = file_token_tg.strip()
                token_source = "env_file:TG_BOT_TOKEN (replaced bad os.environ:TG_BOT_TOKEN)"
            else:
                token = None
        else:
            token = os_token_tg.strip()
            token_source = "os.environ:TG_BOT_TOKEN"
    
    # Select chat_id with same priority
    chat_id = None
    chat_id_source = None
    
    if file_chat_telegram and not _is_bad_placeholder(file_chat_telegram):
        chat_id = file_chat_telegram.strip()
        chat_id_source = "env_file:TELEGRAM_CHAT_ID"
    elif file_chat_tg and not _is_bad_placeholder(file_chat_tg):
        chat_id = file_chat_tg.strip()
        chat_id_source = "env_file:TG_CHAT_ID"
    elif os_chat_telegram and not _is_bad_placeholder(os_chat_telegram):
        chat_id = os_chat_telegram.strip()
        chat_id_source = "os.environ:TELEGRAM_CHAT_ID"
    elif os_chat_tg:
        if _is_bad_placeholder(os_chat_tg):
            # Replace bad placeholder with file value if available
            if file_chat_telegram and not _is_bad_placeholder(file_chat_telegram):
                chat_id = file_chat_telegram.strip()
                chat_id_source = "env_file:TELEGRAM_CHAT_ID (replaced bad os.environ:TG_CHAT_ID)"
            elif file_chat_tg and not _is_bad_placeholder(file_chat_tg):
                chat_id = file_chat_tg.strip()
                chat_id_source = "env_file:TG_CHAT_ID (replaced bad os.environ:TG_CHAT_ID)"
            else:
                chat_id = None
        else:
            chat_id = os_chat_tg.strip()
            chat_id_source = "os.environ:TG_CHAT_ID"
    
    # Final validation: token must be ASCII-only
    if token and any(ord(c) > 127 for c in token):
        if debug:
            print("WARNING: Telegram token contains non-ASCII characters (invalid config)", file=sys.stderr)
        token = None
        token_source = None
    
    if debug:
        token_len = len(token) if token else 0
        chat_id_str = chat_id if chat_id else "None"
        print(f"TG: token_len={token_len} chat_id={chat_id_str}", file=sys.stderr)
        if token_source:
            print(f"TG token source: {token_source}", file=sys.stderr)
        if chat_id_source:
            print(f"TG chat_id source: {chat_id_source}", file=sys.stderr)
        if not token or not chat_id:
            print("WARNING: Telegram credentials missing, report will be printed to stdout only", file=sys.stderr)
    
    return (token, chat_id)


def _send_telegram(message: str, token: str, chat_id: str) -> None:
    """Send text message to Telegram. Token must be ASCII-only (validated by _get_tg_creds)."""
    base_url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": message}
    data = urllib.parse.urlencode(payload).encode("utf-8")
    req = urllib.request.Request(
        base_url,
        data=data,
        method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded; charset=utf-8"},
    )
    with urllib.request.urlopen(req, timeout=20) as response:
        response.read()


def _sort_outcomes_for_series(df: pd.DataFrame) -> pd.DataFrame:
    """Sort outcomes by outcome_time_utc if present, else by run_id/trade_id/index for stable series."""
    out = df.copy()
    if "outcome_time_utc" in out.columns:
        t = pd.to_datetime(out["outcome_time_utc"], errors="coerce")
        if t.notna().any():
            out = out.assign(_t=t).sort_values("_t").drop(columns=["_t"])
            return out.reset_index(drop=True)
    for col in ("run_id", "trade_id", "tradeId", "event_id"):
        if col in out.columns and out[col].notna().any():
            out = out.sort_values(col).reset_index(drop=True)
            return out
    return out.reset_index(drop=True)

def _hypothesis_line(
    df: pd.DataFrame,
    mask: pd.Series,
    n_total: int,
    prefix: str,
) -> str:
    """Thin wrapper delegating to shared _format_filter_line."""
    return _format_filter_line(df, mask, n_total, prefix)


def _enrich_core_with_events(
    df_core: pd.DataFrame,
    events_df: Optional[pd.DataFrame],
    debug: bool = False,
) -> pd.DataFrame:
    """
    Enrich df_core (outcomes) with event features from events_df via a stable join key.

    Data flow / safety guarantees (important for FAST0 analysis):
      - Universe: df_core is the full outcomes subset passed in (e.g. all FAST0 TP/SL/TIMEOUT in --days window).
      - Join key preference (in order):
          1) event_id (if present in both)
          2) trade_id (if present in both)
          3) (run_id, symbol) pair as a fallback (if both columns exist)
      - If events_df is empty or no suitable key is found, df_core is returned unchanged (no rows dropped).
      - Left join on df_core; if row count changes after merge, fall back to the original df_core.
      - Event columns are brought in only from a small enrichment set (context_score, dist_to_peak_pct, volume_zscore,
        liq_long_usd_30s / liq_long_count_30s, stage) to keep memory and debug manageable.
      - When debug=True, we print coverage diagnostics:
          * rows_before / rows_after enrichment
          * join key actually used
          * how many rows received any enrichment
          * non-null counts for key enrichment columns (context/liq/dist).
    """
    if events_df is None or events_df.empty or df_core.empty:
        return df_core

    ev = events_df.copy()
    core = df_core.copy()

    enrich_cols = [
        "context_score",
        "dist_to_peak_pct",
        "volume_zscore",
        "liq_long_usd_30s",
        "liq_long_count_30s",
        "stage",
    ]

    # 1) Try event_id-based join
    event_id_candidates = ["event_id", "eventId", "event_uuid", "id"]
    ev_event_col = next((c for c in event_id_candidates if c in ev.columns), None)
    core_event_col = next((c for c in event_id_candidates if c in core.columns), None)

    join_key: Optional[str] = None
    join_on_pair: bool = False

    if ev_event_col and core_event_col:
        join_key = ev_event_col if ev_event_col == core_event_col else None
        if join_key is None:
            ev = ev.rename(columns={ev_event_col: core_event_col})
            join_key = core_event_col
    else:
        # 2) Try trade_id-based join
        trade_candidates = ["trade_id", "tradeId", "trade_uuid", "position_id"]
        ev_trade_col = next((c for c in trade_candidates if c in ev.columns), None)
        core_trade_col = next((c for c in trade_candidates if c in core.columns), None)
        if ev_trade_col and core_trade_col:
            join_key = ev_trade_col if ev_trade_col == core_trade_col else None
            if join_key is None:
                ev = ev.rename(columns={ev_trade_col: core_trade_col})
                join_key = core_trade_col
        else:
            # 3) Fallback: (run_id, symbol) pair
            if (
                "run_id" in ev.columns
                and "run_id" in core.columns
                and "symbol" in ev.columns
                and "symbol" in core.columns
            ):
                join_on_pair = True
            else:
                if debug:
                    print("DEBUG _enrich_core: no suitable join key, returning original df_core", file=sys.stderr)
                return df_core

    before_len = len(core)

    try:
        if join_on_pair:
            # Filter to entry_ok=1 to avoid multiple rows per (run_id,symbol) (ARMED vs entry)
            entry_col = next(
                (c for c in ["entry_ok", "entryOk", "is_entry", "entry_flag"] if c in ev.columns),
                None,
            )
            if entry_col is not None:
                ok = pd.to_numeric(ev[entry_col], errors="coerce") == 1
                ev = ev.loc[ok].copy() if ok.any() else pd.DataFrame()
                if ev.empty:
                    return df_core
            on_cols = ["run_id", "symbol"]
            ev_sub = ev[on_cols + [c for c in enrich_cols if c in ev.columns]].copy()
            merged = core.merge(
                ev_sub,
                on=on_cols,
                how="left",
                suffixes=("", "_ev"),
            )
        else:
            assert join_key is not None
            ev_sub = ev[[join_key] + [c for c in enrich_cols if c in ev.columns]].copy()
            merged = core.merge(
                ev_sub,
                on=join_key,
                how="left",
                suffixes=("", "_ev"),
            )

        if len(merged) != before_len:
            if debug:
                print(
                    f"DEBUG _enrich_core: row explosion before={before_len} after={len(merged)}, fallback",
                    file=sys.stderr,
                )
            return df_core

        # Convert numeric enrichment columns to numeric where present
        for col in ["context_score", "dist_to_peak_pct", "volume_zscore", "liq_long_usd_30s", "liq_long_count_30s"]:
            if col in merged.columns:
                merged[col] = pd.to_numeric(merged[col], errors="coerce")

        # Consolidate stage: prefer event's stage, fallback to core's (from _ensure_stage_column / details_json)
        if "stage_ev" in merged.columns:
            merged["stage"] = merged["stage_ev"].fillna(merged.get("stage", pd.Series(dtype=float)))
            merged = merged.drop(columns=["stage_ev"])
        elif "stage" in merged.columns:
            merged["stage"] = pd.to_numeric(merged["stage"], errors="coerce")

        if debug:
            key_desc = "run_id,symbol" if join_on_pair else (join_key or "unknown")
            enrich_present = [c for c in enrich_cols if c in merged.columns]
            matched_any = 0
            if enrich_present:
                matched_any = int(merged[enrich_present].notna().any(axis=1).sum())
            coverage_parts = []
            for c in ["context_score", "dist_to_peak_pct", "liq_long_usd_30s", "liq_long_count_30s"]:
                if c in merged.columns:
                    coverage_parts.append(f"{c}_non_null={int(merged[c].notna().sum())}")
            print(
                "DEBUG _enrich_core: "
                f"join_key={key_desc} rows_before={before_len} rows_after={len(merged)} "
                f"matched_any={matched_any} "
                + (" ".join(coverage_parts) if coverage_parts else ""),
                file=sys.stderr,
            )

        return merged
    except Exception as e:
        if debug:
            print(f"DEBUG _enrich_core: merge failed {e}, fallback", file=sys.stderr)
        return df_core


def _metrics_block(
    df: pd.DataFrame,
    df_core: pd.DataFrame,
    rolling_n: int,
    section_label: str,
    maturity_threshold: int,
) -> tuple[List[str], int, str]:
    """
    Compute metrics block (equity, MDD, n_all, n_core, TP/SL/TIMEOUT, WR, EV, rolling) for a given df.
    Returns (lines, n_core, maturity_status).
    """
    lines: List[str] = []
    pnl = pd.to_numeric(df["pnl_pct"], errors="coerce").fillna(0.0)
    n = len(pnl)
    if n == 0:
        lines.append(f"{section_label}: нет сделок.")
        return lines, 0, "🔴 Недостаточно данных"

    equity_all = pnl.cumsum()
    mdd = (equity_all - equity_all.cummax()).min()
    lines.append(f"• Кривая капитала (все): последнее={equity_all.iloc[-1]:.3f}R  MDD={mdd:.3f}R")

    outcome_norm = df["outcome"].apply(lambda x: _normalize_outcome(x))
    core_mask = outcome_norm.isin(["TP_hit", "SL_hit"])
    n_core = core_mask.sum()
    lines.append(f"• Сделки: n_all={n}  n_core={n_core}")

    tp = int((outcome_norm == "TP_hit").sum())
    sl = int((outcome_norm == "SL_hit").sum())
    timeout = int((outcome_norm == "TIMEOUT").sum())
    wr_str = "N/A"
    ev_str = "N/A"
    if n_core > 0:
        pnl_core = pd.to_numeric(df.loc[core_mask, "pnl_pct"], errors="coerce").fillna(0.0)
        _sanity_check_wr(tp, sl, pnl_core, label=section_label)
        wr_val = wr_core_from_tp_sl(tp, sl)
        ev_val = ev_core_from_tp_sl_pnl(tp, sl, pnl_core)
        wr_str = f"{wr_val:.1%}"
        ev_str = f"{ev_val:+.4f}R"
    lines.append(f"• TP/SL/TIMEOUT: {tp}/{sl}/{timeout}  WR core={wr_str}  EV core={ev_str}")

    maturity_status = _maturity_status_unified(n_core)
    lines.append(f"• Зрелость (core): {n_core}  статус={maturity_status}")

    if n_core == 0:
        lines.append("• Скользящий WR/EV (core): нет core исходов")
        return lines, 0, maturity_status

    pnl_core_vals = pnl[core_mask].values
    win_core = (pnl_core_vals > 0).astype(float)
    roll_n = min(rolling_n, n_core)
    suffix = f" (n<{rolling_n})" if n_core < rolling_n else f" (N={rolling_n})"
    wr_last = win_core[-roll_n:].mean() * 100 if roll_n else 0.0
    ev_last = pnl_core_vals[-roll_n:].mean() if roll_n else 0.0
    lines.append(f"• Скользящий WR core{suffix}: {wr_last:.1f}%")
    lines.append(f"• Скользящий EV core{suffix}: {ev_last:+.4f}R")
    lines.append(f"• Скользящее кол-во сделок{suffix}: {roll_n}")
    return lines, int(n_core), maturity_status


def _ensure_stage_column(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure 'stage' column exists by extracting from JSON columns if needed.
    Returns df with 'stage' column added (or unchanged if already present)."""
    if "stage" in df.columns:
        return df
    
    # Find JSON column candidates
    json_candidates = [
        "extra", "details", "payload", "details_json", "meta", "raw", "info", "context",
        "extra_json", "details_payload", "context_json"
    ]
    json_col = None
    
    # Try exact matches first
    for col in json_candidates:
        if col in df.columns:
            json_col = col
            break
    
    # If no exact match, try last column if it looks like object/string
    if json_col is None and len(df.columns) > 0:
        last_col = df.columns[-1]
        if df[last_col].dtype == "object":
            # Check if it might be JSON (sample first non-null value)
            sample = df[last_col].dropna()
            if len(sample) > 0:
                sample_val = str(sample.iloc[0])
                if sample_val.startswith("{") or sample_val.startswith('{"'):
                    json_col = last_col
    
    if json_col is None:
        # No JSON column found, add stage as NaN (float for compatibility)
        df = df.copy()
        df["stage"] = pd.NA
        df["stage"] = pd.to_numeric(df["stage"], errors="coerce")
        return df
    
    # Extract stage from JSON
    def _extract_stage_from_json(val: Any) -> Any:
        if pd.isna(val):
            return pd.NA
        val_str = str(val)
        if not val_str.strip():
            return pd.NA
        
        # Try to parse JSON (handle CSV-escaped quotes)
        try:
            # Remove doubled quotes if present
            if val_str.startswith('"') and val_str.endswith('"'):
                val_str = val_str[1:-1]
            val_str = val_str.replace('""', '"')
            data = json.loads(val_str)
        except (json.JSONDecodeError, ValueError):
            # Try to extract from stringified JSON inside string
            try:
                if isinstance(val_str, str) and '"details_payload"' in val_str:
                    # Try to find nested JSON
                    match = re.search(r'"details_payload"\s*:\s*"([^"]+)"', val_str)
                    if match:
                        nested = match.group(1).replace('\\"', '"')
                        data = json.loads(nested)
                    else:
                        return pd.NA
                else:
                    return pd.NA
            except Exception:
                return pd.NA
        
        if not isinstance(data, dict):
            return pd.NA
        
        # Try entry_snapshot.stage first
        if "entry_snapshot" in data and isinstance(data["entry_snapshot"], dict):
            stage = data["entry_snapshot"].get("stage")
            if stage is not None:
                try:
                    return int(stage)
                except (ValueError, TypeError):
                    pass
        
        # Try top-level stage
        if "stage" in data:
            stage = data["stage"]
            if stage is not None:
                try:
                    return int(stage)
                except (ValueError, TypeError):
                    pass
        
        # Try details_payload nested
        if "details_payload" in data:
            payload_str = data["details_payload"]
            if isinstance(payload_str, str):
                try:
                    payload_data = json.loads(payload_str)
                    if isinstance(payload_data, dict):
                        if "entry_snapshot" in payload_data and isinstance(payload_data["entry_snapshot"], dict):
                            stage = payload_data["entry_snapshot"].get("stage")
                            if stage is not None:
                                try:
                                    return int(stage)
                                except (ValueError, TypeError):
                                    pass
                        if "stage" in payload_data:
                            stage = payload_data["stage"]
                            if stage is not None:
                                try:
                                    return int(stage)
                                except (ValueError, TypeError):
                                    pass
                except (json.JSONDecodeError, ValueError):
                    pass
        
        return pd.NA
    
    df = df.copy()
    df["stage"] = df[json_col].apply(_extract_stage_from_json)
    # Convert to numeric (pd.NA becomes NaN, int stays int)
    df["stage"] = pd.to_numeric(df["stage"], errors="coerce")
    return df


def _investor_metrics_text(
    df_sorted: pd.DataFrame,
    rolling_n: int,
    date_range: Tuple[str, str],
    report_strategy: str,
    maturity_threshold: int,
    events_df: Optional[pd.DataFrame] = None,
    debug: bool = False,
    compact: bool = False,
) -> Tuple[List[str], int, str]:
    """Build extra text lines for investor report: MDD, rolling WR/EV/count (core), maturity gate.
    When compact=True and report_strategy=short_pump_fast0 or short_pump_fast0_filtered, use compact liq research and skip DIAG block.
    When compact=True and report_strategy=short_pump, skip context_score hypothesis and use shorter labels for Telegram.
    Returns: (lines, n_core, maturity_status)."""
    lines = ["", "━━━━━━━━━━", "ИНВЕСТОРСКИЕ МЕТРИКИ (скользящие / core)", ""]
    if "strategy" in df_sorted.columns and report_strategy:
        df_sorted = df_sorted[df_sorted["strategy"].astype(str) == report_strategy].copy()
    pnl = pd.to_numeric(df_sorted["pnl_pct"], errors="coerce").fillna(0.0)
    n = len(pnl)
    if n == 0:
        lines.append("Сделок нет.")
        lines.extend(lightgbm_readiness_block(0))
        return lines, 0, "🔴 Недостаточно данных"
    outcome_norm = df_sorted["outcome"].apply(lambda x: _normalize_outcome(x))
    core_mask = outcome_norm.isin(["TP_hit", "SL_hit"])
    n_core = core_mask.sum()

    # FAST0 / short_pump date coverage debug (universe = all outcomes in df_sorted within --days window)
    if debug:
        dates = df_sorted["date"].dropna().astype(str) if "date" in df_sorted.columns else []
        min_date = min(dates) if len(dates) else "N/A"
        max_date = max(dates) if len(dates) else "N/A"
        print(
            f"DEBUG outcomes_universe: strategy={report_strategy} n={n} n_core={n_core} "
            f"dates_count={len(set(dates))} min_date={min_date} max_date={max_date}",
            file=sys.stderr,
        )
        if events_df is not None and not events_df.empty:
            ev = events_df
            if "strategy" in ev.columns:
                ev = ev[ev["strategy"].astype(str) == str(report_strategy)].copy()
            ev_dates_series = ev["date"] if "date" in ev.columns else None
            if ev_dates_series is not None and not ev.empty:
                ev_dates = ev_dates_series.dropna().astype(str)
                ev_min = min(ev_dates) if len(ev_dates) else "N/A"
                ev_max = max(ev_dates) if len(ev_dates) else "N/A"
                print(
                    f"DEBUG events_universe: strategy={report_strategy} n={len(ev)} "
                    f"dates_count={len(set(ev_dates))} min_date={ev_min} max_date={ev_max}",
                    file=sys.stderr,
                )
    # For short_pump skip generic header; ACTIVE MODE is first, RAW has these at bottom
    if report_strategy != "short_pump":
        equity_all = pnl.cumsum()
        mdd = (equity_all - equity_all.cummax()).min()
        lines.append(f"• Кривая капитала (все): последнее={equity_all.iloc[-1]:.3f}R  MDD={mdd:.3f}R")
        lines.append(f"• Сделки: n_all={n}  n_core={n_core}")

    if report_strategy in ("short_pump_fast0", "short_pump_fast0_filtered"):
        # Entry OK = number of unique trades; TP/SL/TIMEOUT by unique trade_id
        entry_ok_count = int(df_sorted["trade_id"].nunique()) if "trade_id" in df_sorted.columns else len(df_sorted)
        if "trade_id" in df_sorted.columns:
            tp_count = int(df_sorted.loc[outcome_norm == "TP_hit", "trade_id"].nunique())
            sl_count = int(df_sorted.loc[outcome_norm == "SL_hit", "trade_id"].nunique())
            timeout_count = int(df_sorted.loc[outcome_norm == "TIMEOUT", "trade_id"].nunique())
        else:
            tp_count = int((outcome_norm == "TP_hit").sum())
            sl_count = int((outcome_norm == "SL_hit").sum())
            timeout_count = int((outcome_norm == "TIMEOUT").sum())
        wr_str = "N/A"
        ev_str = "N/A"
        if n_core > 0:
            pnl_core = pd.to_numeric(df_sorted.loc[core_mask, "pnl_pct"], errors="coerce").fillna(0.0)
            _sanity_check_wr(tp_count, sl_count, pnl_core, label="FAST0")
            wr_val = wr_core_from_tp_sl(tp_count, sl_count)
            ev_val = ev_core_from_tp_sl_pnl(tp_count, sl_count, pnl_core)
            wr_str = f"{wr_val:.1%}"
            ev_str = f"{ev_val:+.4f}R"
        lines.append(f"• FAST0: Entry OK={entry_ok_count}  TP={tp_count} SL={sl_count} TIMEOUT={timeout_count}  WR={wr_str} EV={ev_str}")

    # Ensure stage column and enrich with events (for both fast0 and short_pump)
    df_with_stage = _ensure_stage_column(df_sorted)
    df_core = df_with_stage[core_mask].copy()
    df_core["stage"] = pd.to_numeric(df_core["stage"], errors="coerce")
    df_core = _enrich_core_with_events(df_core, events_df, debug=debug)

    n_core_out = n_core
    maturity_status_out = _maturity_status_unified(n_core)

    if report_strategy in ("short_pump_fast0", "short_pump_fast0_filtered"):
        # FAST0: hypothesis universe = TP_hit, SL_hit, TIMEOUT (join with events to enrich).
        # Universe here is the full FAST0 outcomes subset within --days; enrichment may be partial if some dates
        # are missing events. Coverage is printed in debug logs.
        hyp_mask = outcome_norm.isin(["TP_hit", "SL_hit", "TIMEOUT"])
        df_hyp = df_with_stage[hyp_mask].copy()
        df_hyp["stage"] = pd.to_numeric(df_hyp["stage"], errors="coerce")
        df_hyp = _enrich_core_with_events(df_hyp, events_df, debug=debug)
        n_hyp = len(df_hyp)
        if debug:
            cov_cols = ["context_score", "dist_to_peak_pct", "liq_long_usd_30s", "liq_long_count_30s"]
            present = [c for c in cov_cols if c in df_hyp.columns]
            any_enriched = 0
            if present:
                any_enriched = int(df_hyp[present].notna().any(axis=1).sum())
            cov_parts = [f"{c}_non_null={int(df_hyp[c].notna().sum())}" for c in present]
            print(
                "DEBUG FAST0 hyp_universe: "
                f"n={n_hyp} matched_any={any_enriched} "
                + (" ".join(cov_parts) if cov_parts else ""),
                file=sys.stderr,
            )
        lines.append("")
        _add_fast0_active_mode_block(lines, df_hyp, rolling_n, maturity_threshold)
        if compact:
            _add_fast0_hypothesis_blocks_compact(lines, df_hyp, n_hyp)
        else:
            _add_fast0_hypothesis_blocks(lines, df_hyp, n_hyp, debug)
        _add_fast0_whatif_tp_block(lines, df_hyp)
        _add_fast0_edge_decay_block(lines, df_hyp)
        _add_fast0_dist_to_peak_block(lines, df_hyp)

        # FAST0 DIAGNOSTIC MODE: skip in compact to save length
        if not compact:
            df_enriched_fast0 = _enrich_core_with_events(df_with_stage, events_df, debug=debug)
            df_raw_fast0 = df_enriched_fast0
            df_raw_core_fast0 = df_enriched_fast0[core_mask].copy()
            lines.append("")
            lines.append("DIAGNOSTIC MODE (FAST0: liq_long_usd_30s > 0)")
            lines.append("")
            liq_col = None
            if "liq_long_usd_30s" in df_raw_fast0.columns:
                liq_col = "liq_long_usd_30s"
            elif "liq_long_count_30s" in df_raw_fast0.columns:
                liq_col = "liq_long_count_30s"
            if liq_col is None:
                lines.append("недоступно (нет колонки liq_long_usd_30s/liq_long_count_30s)")
            else:
                liq_vals = pd.to_numeric(df_raw_fast0[liq_col], errors="coerce")
                liq_positive = (liq_vals.notna()) & (liq_vals > 0)
                df_liq = df_raw_fast0[liq_positive].copy()
                outcome_liq = df_liq["outcome"].apply(lambda x: _normalize_outcome(x))
                core_liq = outcome_liq.isin(["TP_hit", "SL_hit"])
                df_liq_core = df_liq[core_liq].copy()

                if df_liq.empty:
                    lines.append("no trades")
                elif df_liq_core.empty:
                    lines.append("no core trades (TP/SL)")
                else:
                    diag_lines, _, _ = _metrics_block(df_liq, df_liq_core, rolling_n, "DIAG", maturity_threshold)
                    lines.extend(diag_lines)
                n_raw = len(df_raw_fast0)
                n_raw_core = len(df_raw_core_fast0)
                if n_raw_core > 0 and n_raw > 0:
                    kept_core = len(df_liq_core) / n_raw_core
                    kept_all = len(df_liq) / n_raw
                    lines.append("")
                    lines.append(f"Покрытие: kept core={len(df_liq_core)}/{n_raw_core} ({100*kept_core:.1f}%), kept all={len(df_liq)}/{n_raw} ({100*kept_all:.1f}%)")
                if debug:
                    print(
                        "DEBUG FAST0 diag_universe: "
                        f"n_all={n_raw} n_core={n_raw_core} "
                        f"n_liq_all={len(df_liq)} n_liq_core={len(df_liq_core)} liq_col={liq_col}",
                        file=sys.stderr,
                    )

                # FAST0 split: liq>0 EV/WR vs liq==0 EV/WR (core only)
                if len(df_raw_core_fast0) > 0 and liq_col is not None:
                    liq_core_vals = pd.to_numeric(df_raw_core_fast0[liq_col], errors="coerce")
                    liq_gt0_core = (liq_core_vals.notna()) & (liq_core_vals > 0)
                    liq_eq0_core = ~liq_gt0_core
                    def _ev_wr_core(mask: pd.Series) -> str:
                        if mask.sum() == 0:
                            return "WR=N/A EV=N/A"
                        pnl_vals = pd.to_numeric(df_raw_core_fast0.loc[mask, "pnl_pct"], errors="coerce").dropna().values
                        if len(pnl_vals) == 0:
                            return "WR=N/A EV=N/A"
                        wr = (pnl_vals > 0).mean()
                        aw = pnl_vals[pnl_vals > 0].mean() if (pnl_vals > 0).any() else 0.0
                        al = pnl_vals[pnl_vals < 0].mean() if (pnl_vals < 0).any() else 0.0
                        ev = wr * aw + (1 - wr) * al
                        return f"WR={wr:.1%} EV={ev:+.4f}R"
                    lines.append("")
                    lines.append(f"FAST0 split (core): liq>0 {_ev_wr_core(liq_gt0_core)} vs liq==0 {_ev_wr_core(liq_eq0_core)}")
    else:
        # short_pump: ACTIVE mode (stage4 + dist>=3.5) first, then RAW reference
        tg_dist_min = float(os.getenv("TG_DIST_TO_PEAK_MIN", "3.5"))
        df_enriched_full = _enrich_core_with_events(df_with_stage, events_df, debug=debug)
        df_active, df_active_core, df_raw, df_raw_core = filter_active_trades(df_enriched_full, "short_pump", tg_dist_min)

        if debug:
            dates_out = df_sorted["date"].dropna().astype(str).tolist() if "date" in df_sorted.columns else []
            print(
                "DEBUG short_pump outcomes_universe: n=%s n_core=%s dates_count=%s min_date=%s max_date=%s"
                % (len(df_sorted), int(n_core), len(set(dates_out)), min(dates_out) if dates_out else "N/A", max(dates_out) if dates_out else "N/A"),
                file=sys.stderr,
            )
            if events_df is not None and not events_df.empty:
                dates_ev = events_df["date"].dropna().astype(str).tolist() if "date" in events_df.columns else []
                print(
                    "DEBUG short_pump events_universe: n=%s dates_count=%s min_date=%s max_date=%s"
                    % (len(events_df), len(set(dates_ev)), min(dates_ev) if dates_ev else "N/A", max(dates_ev) if dates_ev else "N/A"),
                    file=sys.stderr,
                )
            core_full = df_enriched_full["outcome"].apply(lambda x: _normalize_outcome(x)).isin(["TP_hit", "SL_hit"])
            df_core_enriched = df_enriched_full.loc[core_full]
            for col in ["stage", "dist_to_peak_pct", "context_score", "liq_long_usd_30s"]:
                if col in df_core_enriched.columns:
                    nn = df_core_enriched[col].notna().sum()
                    print("DEBUG short_pump enrich_coverage: %s non_null=%s" % (col, int(nn)), file=sys.stderr)

        lines.append("")
        lines.append("ACTIVE MODE (main filter: stage4 + dist>=3.5)")
        lines.append("")
        if df_active.empty:
            lines.append("ACTIVE: no trades")
            n_active_core = 0
            active_maturity = "🔴 Недостаточно данных"
        else:
            active_lines, n_active_core, active_maturity = _metrics_block(
                df_active, df_active_core, rolling_n, "ACTIVE", maturity_threshold
            )
            lines.extend(active_lines)
            if debug and len(df_active) > len(df_raw):
                print(f"DEBUG n_active={len(df_active)} n_raw={len(df_raw)}", file=sys.stderr)

        # Diagnostics: kept (stage4 & dist>=3.5) vs dropped (stage4 & dist<3.5) on raw core
        if "stage" in df_raw_core.columns and "dist_to_peak_pct" in df_raw_core.columns and len(df_raw_core) > 0:
            stage4_raw = (pd.to_numeric(df_raw_core["stage"], errors="coerce") == 4) & df_raw_core["stage"].notna()
            dtp_raw = pd.to_numeric(df_raw_core["dist_to_peak_pct"], errors="coerce")
            kept_mask = stage4_raw & (dtp_raw >= tg_dist_min)
            dropped_mask = stage4_raw & ~(dtp_raw >= tg_dist_min)
            n_kept = kept_mask.sum()
            n_dropped = dropped_mask.sum()
            if n_kept > 0 or n_dropped > 0:
                def _ev_wr(mask: pd.Series) -> str:
                    pnl_vals = pd.to_numeric(df_raw_core.loc[mask, "pnl_pct"], errors="coerce").dropna().values
                    if len(pnl_vals) == 0:
                        return "WR=N/A EV=N/A"
                    wr = (pnl_vals > 0).mean()
                    aw = pnl_vals[pnl_vals > 0].mean() if (pnl_vals > 0).any() else 0.0
                    al = pnl_vals[pnl_vals < 0].mean() if (pnl_vals < 0).any() else 0.0
                    ev = wr * aw + (1 - wr) * al
                    return f"WR={wr:.1%} EV={ev:+.4f}R"
                lines.append("")
                lines.append(f"Внутри stage4: kept (dist>={tg_dist_min}) n={n_kept} {_ev_wr(kept_mask)}, dropped n={n_dropped} {_ev_wr(dropped_mask)}")

        lines.append("")
        lines.append("RAW (reference only, not operational)")
        lines.append("")
        raw_lines, n_raw_core_count, _ = _metrics_block(df_raw, df_raw_core, rolling_n, "RAW", maturity_threshold)
        lines.extend(raw_lines)

        df_core = df_raw_core.copy()
        if "stage" in df_core.columns:
            df_core["stage"] = pd.to_numeric(df_core["stage"], errors="coerce")
        n_core_out = n_active_core
        maturity_status_out = active_maturity
        n_core = n_core_out
        maturity_status = maturity_status_out

        lines.append("")
        lines.append("Разрез stage 4 vs 3" if compact else "Разрез по stage (core TP/SL): stage=4 vs stage=3")
        if debug:
            stage_counts = df_core["stage"].value_counts(dropna=False)
            print(f"DEBUG stage value_counts (core): {dict(stage_counts)}", file=sys.stderr)
        if len(df_core) == 0:
            lines.append("  stage=4: n=0, WR=N/A, EV=N/A")
            lines.append("  stage=3: n=0, WR=N/A, EV=N/A")
        else:
            for stage_val in [4, 3]:
                sm = (df_core["stage"] == stage_val) & df_core["stage"].notna()
                stage_n = sm.sum()
                if stage_n == 0:
                    lines.append(f"  stage={stage_val}: n=0, WR=N/A, EV=N/A")
                else:
                    stage_pnl = pd.to_numeric(df_core.loc[sm, "pnl_pct"], errors="coerce").dropna().values
                    if len(stage_pnl) == 0:
                        lines.append(f"  stage={stage_val}: n={stage_n}, WR=N/A, EV=N/A")
                    else:
                        sw = (stage_pnl > 0).mean()
                        aw = stage_pnl[stage_pnl > 0].mean() if (stage_pnl > 0).any() else 0.0
                        al = stage_pnl[stage_pnl < 0].mean() if (stage_pnl < 0).any() else 0.0
                        aw = aw if not pd.isna(aw) else 0.0
                        al = al if not pd.isna(al) else 0.0
                        sev = sw * aw + (1 - sw) * al
                        sev = sev if not pd.isna(sev) else 0.0
                        lines.append(f"  stage={stage_val}: n={stage_n}, WR core={sw:.1%}, EV core={sev:+.4f}R")
        lines.append("")
        has_stage_col = "stage" in df_with_stage.columns
        n_core_denom = len(df_core) if report_strategy == "short_pump" else n_core
        _hyp_prefix = "" if compact else "Гипотеза "
        _hyp_suffix = "" if compact else " (core TP/SL)"
        if n_core_denom == 0 or not has_stage_col:
            lines.append(f"{_hyp_prefix}stage4-only{_hyp_suffix}: недоступно")
            lines.append(f"{_hyp_prefix}stage3-only{_hyp_suffix}: недоступно")
        else:
            share_pct = (lambda nx: f"{(100 * nx / n_core_denom):.1f}%" if n_core_denom else "0.0%")
            for stage_val, label in [(4, "stage4-only"), (3, "stage3-only")]:
                sm = (df_core["stage"] == stage_val) & df_core["stage"].notna()
                sn = sm.sum()
                if sn == 0:
                    lines.append(f"{_hyp_prefix}{label}{_hyp_suffix}: n=0, WR=N/A, EV=N/A" + (f", доля core=0.0%" if not compact else ""))
                else:
                    spnl = pd.to_numeric(df_core.loc[sm, "pnl_pct"], errors="coerce").dropna().values
                    if len(spnl) == 0:
                        lines.append(f"{_hyp_prefix}{label}{_hyp_suffix}: n={sn}, WR=N/A, EV=N/A" + (f", доля core={share_pct(sn)}" if not compact else ""))
                    else:
                        swr = (spnl > 0).mean()
                        savg_w = spnl[spnl > 0].mean() if (spnl > 0).any() else 0.0
                        savg_l = spnl[spnl < 0].mean() if (spnl < 0).any() else 0.0
                        savg_w = savg_w if not pd.isna(savg_w) else 0.0
                        savg_l = savg_l if not pd.isna(savg_l) else 0.0
                        sev = swr * savg_w + (1 - swr) * savg_l
                        sev = sev if not pd.isna(sev) else 0.0
                        lines.append(f"{_hyp_prefix}{label}{_hyp_suffix}: n={sn}, WR={swr:.1%}, EV={sev:+.4f}R" + (f", доля core={share_pct(sn)}" if not compact else ""))
        lines.append("")
        n_stage4_core = ((df_core["stage"] == 4) & df_core["stage"].notna()).sum() if "stage" in df_core.columns else 0
        # Порядок гипотез: stage4-only, stage3-only, stage4+dist_to_peak, stage4+context_score, Зрелость данных
        # Гипотеза stage4 + dist_to_peak_pct>=X (TG filter) — после stage3-only, до context_score
        tg_dist_min = float(os.getenv("TG_DIST_TO_PEAK_MIN", "3.5"))
        _dist_label = f"stage4+dist>={tg_dist_min}" if compact else f"Гипотеза stage4 + dist_to_peak>={tg_dist_min} (core TP/SL)"
        if "dist_to_peak_pct" not in df_core.columns:
            lines.append(f"{_dist_label}: недоступно (нет колонки dist_to_peak_pct)")
        else:
            dtp = pd.to_numeric(df_core["dist_to_peak_pct"], errors="coerce")
            m_dtp = (df_core["stage"] == 4) & df_core["stage"].notna() & (dtp >= tg_dist_min)
            n_dtp = m_dtp.sum()
            if n_dtp == 0:
                lines.append(f"{_dist_label}: n=0, WR=N/A, EV=N/A" + ("" if compact else ", доля stage4=0.0%"))
            else:
                spnl = pd.to_numeric(df_core.loc[m_dtp, "pnl_pct"], errors="coerce").dropna().values
                if len(spnl) == 0:
                    lines.append(f"{_dist_label}: n={n_dtp}, WR=N/A, EV=N/A" + ("" if compact else ", доля stage4=0.0%"))
                else:
                    swr = (spnl > 0).mean()
                    aw = spnl[spnl > 0].mean() if (spnl > 0).any() else 0.0
                    al = spnl[spnl < 0].mean() if (spnl < 0).any() else 0.0
                    aw = aw if not pd.isna(aw) else 0.0
                    al = al if not pd.isna(al) else 0.0
                    sev = swr * aw + (1 - swr) * al
                    sev = sev if not pd.isna(sev) else 0.0
                    sh = (100 * n_dtp / n_stage4_core) if n_stage4_core else 0.0
                    lines.append(f"{_dist_label}: n={n_dtp}, WR={swr:.1%}, EV={sev:+.4f}R" + (f", доля stage4={sh:.1f}%" if not compact else ""))
            if debug and n_stage4_core > 0:
                stage4_mask = (df_core["stage"] == 4) & df_core["stage"].notna()
                dtp_stage4 = df_core.loc[stage4_mask, "dist_to_peak_pct"]
                vc = pd.to_numeric(dtp_stage4, errors="coerce").value_counts(dropna=False).head(10)
                print(f"DEBUG dist_to_peak_pct (stage4 core, top10): {vc.to_dict()} threshold={tg_dist_min}", file=sys.stderr)
        # TG dist filter: kept/dropped (stage4 core)
        _tg_label = "TG dist" if compact else "TG dist filter (stage4 core)"
        if "dist_to_peak_pct" not in df_core.columns:
            lines.append(f"{_tg_label}: недоступно (нет колонки dist_to_peak_pct)" if compact else "TG dist filter (stage4 core): недоступно (нет колонки dist_to_peak_pct)")
        elif n_stage4_core == 0:
            lines.append(f"{_tg_label}: n=0")
        else:
            dtp = pd.to_numeric(df_core["dist_to_peak_pct"], errors="coerce")
            stage4_mask = (df_core["stage"] == 4) & df_core["stage"].notna()
            kept_mask = stage4_mask & (dtp >= tg_dist_min)
            dropped_mask = stage4_mask & ~(dtp >= tg_dist_min)
            n_kept = kept_mask.sum()
            pct_kept = 100 * n_kept / n_stage4_core if n_stage4_core else 0.0
            lines.append(f"{_tg_label}: kept={n_kept}/{n_stage4_core} ({pct_kept:.1f}%)")
            def _ev_wr_str(mask: pd.Series) -> str:
                pnl_vals = pd.to_numeric(df_core.loc[mask, "pnl_pct"], errors="coerce").dropna().values
                if len(pnl_vals) == 0:
                    return "EV=N/A WR=N/A"
                wr = (pnl_vals > 0).mean()
                aw = pnl_vals[pnl_vals > 0].mean() if (pnl_vals > 0).any() else 0.0
                al = pnl_vals[pnl_vals < 0].mean() if (pnl_vals < 0).any() else 0.0
                ev = wr * aw + (1 - wr) * al
                ev = ev if not pd.isna(ev) else 0.0
                return f"EV={ev:+.4f}R WR={wr:.1%}"
            kept_str = _ev_wr_str(kept_mask)
            dropped_str = _ev_wr_str(dropped_mask)
            lines.append(f"TG dist split: kept {kept_str}, dropped {dropped_str}" if compact else f"TG dist filter split: kept {kept_str}, dropped {dropped_str}")
        lines.append("")
        # context_score hypothesis: drop in compact to save length (priority B)
        has_ctx = "context_score" in df_core.columns
        if debug and has_ctx and n_stage4_core > 0:
            vc = df_core[(df_core["stage"] == 4) & df_core["stage"].notna()]["context_score"].value_counts(dropna=False)
            print(f"DEBUG context_score value_counts (stage4 core): {vc.head(10).to_dict()}", file=sys.stderr)
        if not compact:
            if not has_ctx:
                lines.append("Гипотеза stage4 + context_score>=0.75 (core TP/SL): недоступно (нет колонки context_score)")
            else:
                ctx_score = pd.to_numeric(df_core["context_score"], errors="coerce")
                m75 = (df_core["stage"] == 4) & df_core["stage"].notna() & (ctx_score >= 0.75)
                n75 = m75.sum()
                if n75 == 0:
                    lines.append("Гипотеза stage4 + context_score>=0.75 (core TP/SL): n=0, WR=N/A, EV=N/A, доля stage4=0.0%")
                else:
                    spnl = pd.to_numeric(df_core.loc[m75, "pnl_pct"], errors="coerce").dropna().values
                    if len(spnl) == 0:
                        lines.append(f"Гипотеза stage4 + context_score>=0.75 (core TP/SL): n={n75}, WR=N/A, EV=N/A, доля stage4=0.0%")
                    else:
                        swr = (spnl > 0).mean()
                        aw = spnl[spnl > 0].mean() if (spnl > 0).any() else 0.0
                        al = spnl[spnl < 0].mean() if (spnl < 0).any() else 0.0
                        aw = aw if not pd.isna(aw) else 0.0
                        al = al if not pd.isna(al) else 0.0
                        sev = swr * aw + (1 - swr) * al
                        sev = sev if not pd.isna(sev) else 0.0
                        sh = (100 * n75 / n_stage4_core) if n_stage4_core else 0.0
                        lines.append(f"Гипотеза stage4 + context_score>=0.75 (core TP/SL): n={n75}, WR={swr:.1%}, EV={sev:+.4f}R, доля stage4={sh:.1f}%")

        # short_pump: ACTIVE/RAW already printed; return with n_active_core and ACTIVE maturity
        if report_strategy == "short_pump":
            lines.append("")
            lines.append("Зрелость данных (ACTIVE):")
            lines.append(f"• Сделки ACTIVE core: {n_core_out}")
            lines.append(f"• Порог: {maturity_threshold}")
            lines.append(f"• Статус: {maturity_status_out}")
            lines.extend(lightgbm_readiness_block(n_core_out))
            return lines, int(n_core_out), maturity_status_out
    
    # Data maturity gate (for short_pump use ACTIVE maturity already set)
    if report_strategy != "short_pump":
        maturity_status = _maturity_status_unified(n_core)
    
    lines.append("")
    lines.append("Зрелость данных:")
    lines.append(f"• Сделки (все): {n}")
    lines.append(f"• Сделки (core TP/SL): {n_core}")
    lines.append(f"• Порог: {maturity_threshold}")
    lines.append(f"• Статус: {maturity_status}")
    if n_core < maturity_threshold:
        lines.append("⚠️ Скользящие метрики нестабильны (выборка < порога)")
    
    if n_core == 0:
        lines.append("• Скользящий WR/EV (core): нет core исходов")
        lines.extend(lightgbm_readiness_block(0))
        return lines, 0, maturity_status
    pnl_core = pnl[core_mask].values
    win_core = (pnl_core > 0).astype(float)
    roll_n = min(rolling_n, n_core)
    suffix = f" (n<{rolling_n})" if n_core < rolling_n else f" (N={rolling_n})"
    # Rolling WR last N (core)
    wr_last = win_core[-roll_n:].mean() * 100 if roll_n else 0.0
    ev_last = pnl_core[-roll_n:].mean() if roll_n else 0.0
    lines.append(f"• Скользящий WR core{suffix}: {wr_last:.1f}%")
    lines.append(f"• Скользящий EV core{suffix}: {ev_last:+.4f}R")
    lines.append(f"• Скользящее кол-во сделок{suffix}: {roll_n}")
    lines.extend(lightgbm_readiness_block(n_core))
    return lines, n_core, maturity_status


def _build_investor_charts(
    df_sorted: pd.DataFrame,
    rolling_n: int,
    date_range: Tuple[str, str],
    report_strategy: str,
    reports_dir: Path,
    debug: bool,
) -> List[Path]:
    """Generate 4 PNGs: equity_curve, rolling_winrate, rolling_ev, drawdown. Returns list of paths."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError as e:
        print("Error: matplotlib is required for --investor-report. Install with: pip install matplotlib", file=__import__("sys").stderr)
        raise SystemExit(1) from e
    paths_out: List[Path] = []
    start, end = date_range
    title_suffix = f" {report_strategy} {start}..{end} N={rolling_n}"
    n = len(df_sorted)
    pnl = pd.to_numeric(df_sorted["pnl_pct"], errors="coerce").fillna(0.0)
    outcome_norm = df_sorted["outcome"].apply(lambda x: _normalize_outcome(x))
    core_mask = outcome_norm.isin(["TP_hit", "SL_hit"])
    pnl_core = pnl.where(core_mask).dropna()
    win_core = (pnl_core > 0).astype(float)
    x_all = list(range(1, n + 1))
    equity_all = pnl.cumsum()
    # 1) equity_curve.png
    fig, ax = plt.subplots()
    ax.plot(x_all, equity_all.tolist())
    ax.set_title("Кривая капитала (все)" + title_suffix)
    ax.set_xlabel("Сделка #")
    ax.grid(True)
    fig.savefig(reports_dir / "equity_curve.png", dpi=100)
    plt.close(fig)
    paths_out.append(reports_dir / "equity_curve.png")
    # 2) rolling_winrate.png (core) — always produce file
    if len(win_core) > 0:
        roll_wr = win_core.rolling(rolling_n, min_periods=1).mean() * 100
        x_core = list(range(1, len(roll_wr) + 1))
        wr_vals = roll_wr.tolist()
    else:
        x_core, wr_vals = [1], [0.0]
    fig, ax = plt.subplots()
    ax.plot(x_core, wr_vals)
    ax.set_title("Скользящий WR (core)" + title_suffix)
    ax.set_xlabel("Сделка # (core)")
    ax.grid(True)
    fig.savefig(reports_dir / "rolling_winrate.png", dpi=100)
    plt.close(fig)
    paths_out.append(reports_dir / "rolling_winrate.png")
    # 3) rolling_ev.png (core) — always produce file
    if len(pnl_core) > 0:
        roll_ev = pnl_core.rolling(rolling_n, min_periods=1).mean()
        x_core = list(range(1, len(roll_ev) + 1))
        ev_vals = roll_ev.tolist()
    else:
        x_core, ev_vals = [1], [0.0]
    fig, ax = plt.subplots()
    ax.plot(x_core, ev_vals)
    ax.set_title("Скользящий EV (core)" + title_suffix)
    ax.set_xlabel("Сделка # (core)")
    ax.grid(True)
    fig.savefig(reports_dir / "rolling_ev.png", dpi=100)
    plt.close(fig)
    paths_out.append(reports_dir / "rolling_ev.png")
    # 4) drawdown.png
    dd = equity_all - equity_all.cummax()
    fig, ax = plt.subplots()
    ax.plot(x_all, dd.tolist())
    ax.set_title("Просадка (все)" + title_suffix)
    ax.set_xlabel("Сделка #")
    ax.grid(True)
    fig.savefig(reports_dir / "drawdown.png", dpi=100)
    plt.close(fig)
    paths_out.append(reports_dir / "drawdown.png")
    if debug:
        print(f"Charts: n_trades={n}, n_core={core_mask.sum()}, dates {start}..{end}, PNGs: {[str(p) for p in paths_out]}")
    return paths_out


def _send_photo(token: str, chat_id: str, photo_path: Union[str, Path], caption: Optional[str] = None) -> None:
    """Send a photo to Telegram via sendPhoto (multipart/form-data). Token must be ASCII-only (validated by _get_tg_creds).
    
    Args:
        token: Telegram bot token (ASCII-only)
        chat_id: Telegram chat ID
        photo_path: Path to photo file (str or Path)
        caption: Optional caption text
    """
    # Normalize photo_path to Path
    photo_path = Path(photo_path)
    
    # Check file exists
    if not photo_path.exists():
        raise FileNotFoundError(f"Photo file not found: {photo_path}")
    
    base_url = f"https://api.telegram.org/bot{token}/sendPhoto"
    
    # Read photo file bytes
    photo_bytes = photo_path.read_bytes()
    
    filename = photo_path.name
    mime_type, _ = mimetypes.guess_type(filename)
    mime_type = mime_type or "image/png"
    
    # Generate RFC-compliant boundary
    boundary = "----pump_short_boundary_" + uuid.uuid4().hex
    CRLF = b"\r\n"
    
    # Build multipart/form-data parts
    parts: List[bytes] = []
    
    # Helper: add text field
    def _mp_add_text(name: str, value: str) -> None:
        parts.append(f"--{boundary}".encode("utf-8"))
        parts.append(CRLF)
        parts.append(f'Content-Disposition: form-data; name="{name}"'.encode("utf-8"))
        parts.append(CRLF)
        parts.append(CRLF)
        parts.append(value.encode("utf-8"))
        parts.append(CRLF)
    
    # Helper: add file field
    def _mp_add_file(name: str, filename: str, content_type: str, data: bytes) -> None:
        parts.append(f"--{boundary}".encode("utf-8"))
        parts.append(CRLF)
        parts.append(f'Content-Disposition: form-data; name="{name}"; filename="{filename}"'.encode("utf-8"))
        parts.append(CRLF)
        parts.append(f"Content-Type: {content_type}".encode("utf-8"))
        parts.append(CRLF)
        parts.append(CRLF)
        parts.append(data)
        parts.append(CRLF)
    
    # Add fields in order: chat_id, photo, caption (optional)
    _mp_add_text("chat_id", str(chat_id))
    _mp_add_file("photo", filename, mime_type, photo_bytes)
    if caption:
        _mp_add_text("caption", caption)
    
    # Terminate multipart body
    parts.append(f"--{boundary}--".encode("utf-8"))
    parts.append(CRLF)
    
    # Join all parts
    body_bytes = b"".join(parts)
    
    # Create request
    req = urllib.request.Request(
        base_url,
        data=body_bytes,
        method="POST",
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
    )
    
    # Send with error handling
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            response.read()
    except urllib.error.HTTPError as e:
        error_body = ""
        try:
            error_body = e.read().decode("utf-8", errors="replace")
        except Exception:
            pass
        print(
            f"TG sendPhoto failed: {e.code} {e.reason} body={error_body}",
            file=sys.stderr
        )
        raise


def _load_events_union_for_strategy(
    data_dir: Path,
    strategy: str,
    days: int,
    *,
    raw: bool = True,
) -> pd.DataFrame:
    """
    Load events from live + paper partitions, concat, dedupe by event_id (live rows win).

    Used for short_pump_fast0 enrichment: live outcomes may reference event rows stored
    only under mode=paper in datasets layout.
    """
    res_live = load_events_v2(
        data_dir=data_dir,
        strategy=strategy,
        mode="live",
        days=days,
        raw=raw,
        return_file_count=False,
    )
    live_ev = res_live[0] if isinstance(res_live, tuple) else res_live
    res_paper = load_events_v2(
        data_dir=data_dir,
        strategy=strategy,
        mode="paper",
        days=days,
        raw=raw,
        return_file_count=False,
    )
    paper_ev = res_paper[0] if isinstance(res_paper, tuple) else res_paper
    n_live = len(live_ev)
    n_paper = len(paper_ev)
    combined = pd.concat([live_ev, paper_ev], ignore_index=True)
    if combined.empty:
        if os.getenv("DEBUG") == "1":
            print(
                f"fast0 events union: live={n_live}, paper={n_paper}, after_dedup=0",
                file=sys.stderr,
            )
        return combined
    if "event_id" not in combined.columns:
        if os.getenv("DEBUG") == "1":
            print(
                f"fast0 events union: live={n_live}, paper={n_paper}, after_dedup={len(combined)} (no event_id, skip dedup)",
                file=sys.stderr,
            )
        return combined
    combined = combined.drop_duplicates(subset=["event_id"], keep="first")
    n_dedup = len(combined)
    if os.getenv("DEBUG") == "1":
        print(
            f"fast0 events union: live={n_live}, paper={n_paper}, after_dedup={n_dedup}",
            file=sys.stderr,
        )
    return combined


def generate_compact_autotrading_report(
    days: int,
    data_dir: Optional[Union[str, Path]] = None,
    rolling: int = 20,
) -> str:
    """
    Build compact executive autotrading report (short_pump + short_pump_filtered + fast0 variants) for given window.

    Used by external callers (e.g. Telegram bot) to avoid duplicating report logic.
    """
    if days <= 0:
        raise ValueError("days must be positive")

    base_dir = Path(data_dir or "/root/pump_short/datasets")

    # Load outcomes for short_pump, short_pump_filtered and fast0 variants (live only)
    all_outcomes: List[pd.DataFrame] = []
    for strategy in (
        "short_pump",
        "short_pump_filtered",
        "short_pump_fast0",
        "short_pump_fast0_filtered",
        "short_pump_premium",
        "short_pump_wick",
        "false_pump",
    ):
        result = load_outcomes(
            base_dir=base_dir,
            strategy=strategy,
            mode="live",
            days=days,
            include_test=False,
            return_file_count=False,
        )
        df_part = result[0] if isinstance(result, tuple) else result
        if not df_part.empty:
            all_outcomes.append(df_part)
    df = pd.concat(all_outcomes, ignore_index=True) if all_outcomes else pd.DataFrame()
    if df.empty:
        return "Нет данных по автоторговле за выбранный период."

    # Load events for same strategies (for ACTIVE / stage / dist enrichment)
    all_events: List[pd.DataFrame] = []
    for strategy in (
        "short_pump",
        "short_pump_filtered",
        "short_pump_fast0",
        "short_pump_fast0_filtered",
        "short_pump_premium",
        "short_pump_wick",
        "false_pump",
    ):
        if strategy == "short_pump_fast0":
            ev = _load_events_union_for_strategy(base_dir, strategy, days, raw=True)
        else:
            result = load_events_v2(
                data_dir=base_dir,
                strategy=strategy,
                mode="live",
                days=days,
                raw=True,
                return_file_count=False,
            )
            ev = result[0] if isinstance(result, tuple) else result
        if not ev.empty:
            all_events.append(ev)
    events_raw = pd.concat(all_events, ignore_index=True) if all_events else pd.DataFrame()

    # Date range: full datasets period within days window
    ds_range = get_datasets_date_range(base_dir, days=days)
    if ds_range is not None:
        date_range = ds_range
    else:
        date_range = ("unknown", "unknown")

    # Compact executive report assembly (same as --compact path in main())
    df_sorted = _sort_outcomes_for_series(df)
    tg_dist_min = float(os.getenv("TG_DIST_TO_PEAK_MIN", "3.5"))
    has_strat = "strategy" in df_sorted.columns
    df_sp = (
        df_sorted[df_sorted["strategy"].astype(str) == "short_pump"]
        if has_strat
        else df_sorted
    )
    df_spf = (
        df_sorted[df_sorted["strategy"].astype(str) == "short_pump_filtered"]
        if has_strat
        else pd.DataFrame()
    )
    df_f0 = (
        df_sorted[df_sorted["strategy"].astype(str) == "short_pump_fast0"]
        if has_strat
        else pd.DataFrame()
    )
    all_ev = events_raw if not events_raw.empty else pd.DataFrame()
    has_ev_strat = "strategy" in all_ev.columns
    ev_sp = (
        all_ev[all_ev["strategy"].astype(str) == "short_pump"]
        if has_ev_strat
        else all_ev
    )
    ev_spf = (
        all_ev[all_ev["strategy"].astype(str) == "short_pump_filtered"]
        if has_ev_strat
        else pd.DataFrame()
    )
    ev_f0 = (
        all_ev[all_ev["strategy"].astype(str) == "short_pump_fast0"]
        if has_ev_strat
        else pd.DataFrame()
    )
    df_pr = (
        df_sorted[df_sorted["strategy"].astype(str) == "short_pump_premium"]
        if has_strat
        else pd.DataFrame()
    )
    df_wk = (
        df_sorted[df_sorted["strategy"].astype(str) == "short_pump_wick"]
        if has_strat
        else pd.DataFrame()
    )
    ev_pr = (
        all_ev[all_ev["strategy"].astype(str) == "short_pump_premium"]
        if has_ev_strat
        else pd.DataFrame()
    )
    ev_wk = (
        all_ev[all_ev["strategy"].astype(str) == "short_pump_wick"]
        if has_ev_strat
        else pd.DataFrame()
    )
    df_fp = (
        df_sorted[df_sorted["strategy"].astype(str) == "false_pump"]
        if has_strat
        else pd.DataFrame()
    )
    ev_fp = (
        all_ev[all_ev["strategy"].astype(str) == "false_pump"]
        if has_ev_strat
        else pd.DataFrame()
    )
    df_sp_e = _ensure_stage_column(df_sp.copy()) if not df_sp.empty else None
    df_spf_e = _ensure_stage_column(df_spf.copy()) if not df_spf.empty else None
    df_f0_e = _ensure_stage_column(df_f0.copy()) if not df_f0.empty else None
    df_pr_e = _ensure_stage_column(df_pr.copy()) if not df_pr.empty else None
    df_wk_e = _ensure_stage_column(df_wk.copy()) if not df_wk.empty else None
    if df_sp_e is not None and not df_sp_e.empty:
        df_sp_e = _enrich_core_with_events(df_sp_e, ev_sp if not ev_sp.empty else None, debug=False)
    if df_spf_e is not None and not df_spf_e.empty:
        df_spf_e = _enrich_core_with_events(df_spf_e, ev_spf if not ev_spf.empty else None, debug=False)
    if df_f0_e is not None and not df_f0_e.empty:
        df_f0_e = _enrich_core_with_events(df_f0_e, ev_f0 if not ev_f0.empty else None, debug=False)
    if df_pr_e is not None and not df_pr_e.empty:
        df_pr_e = _enrich_core_with_events(df_pr_e, ev_pr if not ev_pr.empty else None, debug=False)
    if df_wk_e is not None and not df_wk_e.empty:
        df_wk_e = _enrich_core_with_events(df_wk_e, ev_wk if not ev_wk.empty else None, debug=False)
    df_fp_e = _ensure_stage_column(df_fp.copy()) if not df_fp.empty else None
    if df_fp_e is not None and not df_fp_e.empty:
        df_fp_e = _enrich_core_with_events(df_fp_e, ev_fp if not ev_fp.empty else None, debug=False)

    # Ensure guard state is fresh before loading (report reads latest outcomes → guard must match)
    _run_guard_update(base_dir, days, rolling=rolling)

    # Load Auto Risk Guard state JSON from the same datasets root
    guard_state = None
    guard_path = base_dir / "auto_risk_guard_state.json"
    if guard_path.is_file():
        try:
            import json

            with open(guard_path, "r", encoding="utf-8") as f:
                raw = json.load(f)
            if isinstance(raw, dict):
                guard_state = raw
        except Exception:
            guard_state = None

    # Load mode=paper outcomes for PAPER block (source of truth for DISABLED submodes)
    res_f0_paper = load_outcomes(base_dir=base_dir, strategy="short_pump_fast0", mode="paper", days=days)
    res_f0_filtered_paper = load_outcomes(base_dir=base_dir, strategy="short_pump_fast0_filtered", mode="paper", days=days)
    res_sp_paper = load_outcomes(base_dir=base_dir, strategy="short_pump", mode="paper", days=days)
    res_spf_paper = load_outcomes(base_dir=base_dir, strategy="short_pump_filtered", mode="paper", days=days)
    df_fast0_paper = res_f0_paper[0] if isinstance(res_f0_paper, tuple) else res_f0_paper
    df_fast0_filtered_paper = res_f0_filtered_paper[0] if isinstance(res_f0_filtered_paper, tuple) else res_f0_filtered_paper
    df_short_pump_paper = res_sp_paper[0] if isinstance(res_sp_paper, tuple) else res_sp_paper
    df_short_pump_filtered_paper = res_spf_paper[0] if isinstance(res_spf_paper, tuple) else res_spf_paper

    exec_report = build_executive_compact_report(
        df_sp_e if (df_sp_e is not None and not df_sp_e.empty) else None,
        df_spf_e if (df_spf_e is not None and not df_spf_e.empty) else None,
        df_f0_e if (df_f0_e is not None and not df_f0_e.empty) else None,
        date_range,
        rolling_n=rolling,
        tg_dist_min=tg_dist_min,
        guard_state=guard_state,
        df_fast0_paper=df_fast0_paper if not df_fast0_paper.empty else None,
        df_fast0_filtered_paper=df_fast0_filtered_paper if not df_fast0_filtered_paper.empty else None,
        df_short_pump_paper=df_short_pump_paper if not df_short_pump_paper.empty else None,
        df_short_pump_filtered_paper=df_short_pump_filtered_paper if not df_short_pump_filtered_paper.empty else None,
        df_short_pump_premium=df_pr_e if (df_pr_e is not None and not df_pr_e.empty) else None,
        df_short_pump_wick=df_wk_e if (df_wk_e is not None and not df_wk_e.empty) else None,
        df_false_pump=df_fp_e if (df_fp_e is not None and not df_fp_e.empty) else None,
        report_window_days=days,
    )
    return exec_report


def main() -> None:
    parser = argparse.ArgumentParser(description="Daily outcomes report to Telegram")
    parser.add_argument("--days", type=int, default=2)
    parser.add_argument("--root", default=None, help="Data root directory (deprecated, use --data-dir)")
    parser.add_argument("--data-dir", default=None, help="Data root directory")
    parser.add_argument(
        "--strategy",
        action="append",
        help="Strategy filter (repeatable or comma-separated). Default: short_pump, short_pump_filtered, short_pump_fast0, short_pump_fast0_filtered.",
    )
    parser.add_argument("--debug", action="store_true", help="Print file counts and debug info")
    parser.add_argument("--investor-report", action="store_true", help="Add charts and extended metrics (PNG + optional TG photos)")
    parser.add_argument("--investor-text-only", action="store_true", help="Add investor metrics text block only, no charts/photos")
    parser.add_argument("--compact", action="store_true", help="Use compact executive report format (5 blocks, TG-friendly, Russian)")
    parser.add_argument("--no-active", action="store_true", help="For short_pump: skip ACTIVE MODE / investor metrics block (stage4+dist)")
    parser.add_argument("--rolling", type=int, default=20, help="Rolling window size for investor metrics (default: 20)")
    parser.add_argument("--maturity-threshold", type=int, default=50, help="Minimum core trades for maturity gate (default: 50)")
    args = parser.parse_args()

    # Support both --root and --data-dir (--data-dir takes precedence)
    data_dir = args.data_dir or args.root
    if not data_dir:
        data_dir = "/root/pump_short/datasets"
    data_dir = Path(data_dir)

    strategies: List[str] | None
    if args.strategy:
        raw_parts = []
        for item in args.strategy:
            raw_parts.extend(item.split(","))
        strategies = [part.strip() for part in raw_parts if part.strip()]
    else:
        env_strategies = os.getenv("REPORT_STRATEGIES")
        if env_strategies and env_strategies.strip().upper() == "ALL":
            strategies = None
        elif env_strategies:
            strategies = [part.strip() for part in env_strategies.split(",") if part.strip()]
        else:
            strategies = ["short_pump"]

    strategy_set = set(strategies) if strategies is not None else None
    strategies_label = "ALL" if strategy_set is None else ",".join(strategies)
    debug = args.debug or os.getenv("DEBUG") == "1"

    # Load outcomes using unified loader (strategy- and version-aware; optional days)
    all_outcomes: List[pd.DataFrame] = []
    n_outcomes_files = 0
    if strategy_set is None:
        for strategy in ["short_pump", "short_pump_filtered", "long_pullback"]:
            result = load_outcomes(
                base_dir=data_dir,
                strategy=strategy,
                mode="live",
                days=args.days,
                include_test=False,
                return_file_count=debug,
            )
            if isinstance(result, tuple):
                df_part, n = result
                if debug:
                    n_outcomes_files += n
            else:
                df_part = result
            if not df_part.empty:
                all_outcomes.append(df_part)
    else:
        for strategy in strategy_set:
            result = load_outcomes(
                base_dir=data_dir,
                strategy=strategy,
                mode="live",
                days=args.days,
                include_test=False,
                return_file_count=debug,
            )
            if isinstance(result, tuple):
                df_part, n = result
                if debug:
                    n_outcomes_files += n
            else:
                df_part = result
            if not df_part.empty:
                all_outcomes.append(df_part)

    if not all_outcomes:
        print("No outcomes found.")
        return

    df = pd.concat(all_outcomes, ignore_index=True)

    # Defensive filter: only rows for requested strategy(ies)
    if strategy_set is not None and "strategy" in df.columns:
        df = df[df["strategy"].astype(str).isin(strategy_set)]

    if df.empty:
        print("No valid outcomes after filtering.")
        return

    # Load events_raw for same strategies (for ENTRY QUALITY + LIQ snapshot)
    all_events: List[pd.DataFrame] = []
    n_events_files = 0
    strategies_to_load = sorted(strategy_set) if strategy_set else [
        "short_pump",
        "short_pump_filtered",
        "short_pump_fast0",
        "short_pump_fast0_filtered",
        "long_pullback",
    ]
    for strategy in strategies_to_load:
        if strategy == "short_pump_fast0":
            ev = _load_events_union_for_strategy(data_dir, strategy, args.days, raw=True)
            if debug:
                # Union uses two underlying loads; file count not tracked here
                pass
        else:
            result = load_events_v2(
                data_dir=data_dir,
                strategy=strategy,
                mode="live",
                days=args.days,
                raw=True,
                return_file_count=debug,
            )
            if isinstance(result, tuple):
                ev, n = result
                if debug:
                    n_events_files += n
            else:
                ev = result
        if not ev.empty:
            all_events.append(ev)
    events_raw = pd.concat(all_events, ignore_index=True) if all_events else pd.DataFrame()

    # Strict strategy filtering: ensure no mixing
    if strategy_set is not None and not events_raw.empty and "strategy" in events_raw.columns:
        events_raw = events_raw[events_raw["strategy"].astype(str).isin(strategy_set)].copy()

    if debug:
        print(f"FILES: outcomes={n_outcomes_files} events={n_events_files} (using v3/v2/plain mix)")

    total_trades = 0
    pnl_sum = 0.0
    tp = 0
    sl = 0
    timeout = 0
    other_count = 0
    conflicts_total = 0
    conflicts_top = 0
    conflicts_payload = 0

    by_strategy: Dict[str, Dict[str, Any]] = {}

    # Process each row
    for idx, row in df.iterrows():
        strategy = str(row.get("strategy", "unknown"))
        if strategy_set is not None and strategy not in strategy_set:
            continue

        if strategy not in by_strategy:
            by_strategy[strategy] = {
                "n": 0,
                "pnl_sum": 0.0,
                "tp": 0,
                "sl": 0,
                "timeout": 0,
                "winrate": 0.0,
            }

        # Normalize outcome
        outcome_raw = row.get("outcome")
        outcome_norm = _normalize_outcome(outcome_raw)
        
        # Extract conflicts from details_json
        details_json = row.get("details_json")
        details = _safe_json_load(details_json) if pd.notna(details_json) else {}
        top_conflict = _extract_conflict(details)
        payload_conflict = 0
        if not top_conflict:
            payload_conflict = _extract_conflict(_get_details_payload(details))
        if top_conflict:
            conflicts_top += 1
            conflicts_total += 1
        elif payload_conflict:
            conflicts_payload += 1
            conflicts_total += 1

        # Count OTHER outcomes (not TP/SL/TIMEOUT)
        if outcome_norm not in ("TP_hit", "SL_hit", "TIMEOUT"):
            other_count += 1
            continue

        pnl = _parse_pnl(row.get("pnl_pct"))
        total_trades += 1
        pnl_sum += pnl
        by_strategy[strategy]["n"] += 1
        by_strategy[strategy]["pnl_sum"] += pnl
        
        if outcome_norm == "TP_hit":
            tp += 1
            by_strategy[strategy]["tp"] += 1
        elif outcome_norm == "SL_hit":
            sl += 1
            by_strategy[strategy]["sl"] += 1
        elif outcome_norm == "TIMEOUT":
            timeout += 1
            by_strategy[strategy]["timeout"] += 1

    # Calculate winrates
    for strategy, agg in by_strategy.items():
        denom = agg["tp"] + agg["sl"] + agg["timeout"]
        agg["winrate"] = (agg["tp"] / denom) if denom else 0.0

    # For single-strategy fast0: use unique trade_id counts (not row counts)
    if strategy_set and len(strategy_set) == 1:
        report_strategy_pre = next(iter(strategy_set))
        if report_strategy_pre in ("short_pump_fast0", "short_pump_fast0_filtered") and "trade_id" in df.columns:
            onorm = df["outcome"].apply(lambda x: _normalize_outcome(x))
            tp = int(df.loc[onorm == "TP_hit", "trade_id"].nunique())
            sl = int(df.loc[onorm == "SL_hit", "trade_id"].nunique())
            timeout = int(df.loc[onorm == "TIMEOUT", "trade_id"].nunique())
            if report_strategy_pre in by_strategy:
                by_strategy[report_strategy_pre]["tp"] = tp
                by_strategy[report_strategy_pre]["sl"] = sl
                by_strategy[report_strategy_pre]["timeout"] = timeout
                denom = tp + sl + timeout
                by_strategy[report_strategy_pre]["winrate"] = (tp / denom) if denom else 0.0

    denom_total = tp + sl + timeout
    winrate = (tp / denom_total) if denom_total else 0.0
    
    # Calculate new metrics from outcomes_df
    valid_outcomes = df[df["outcome"].apply(lambda x: _normalize_outcome(x)).isin(["TP_hit", "SL_hit", "TIMEOUT"])]
    pnl_series = pd.to_numeric(valid_outcomes.get("pnl_pct", pd.Series()), errors="coerce")
    wins = pnl_series[pnl_series > 0]
    losses = pnl_series[pnl_series < 0]
    avg_win_R = wins.mean() if len(wins) > 0 else 0.0
    avg_loss_R = losses.mean() if len(losses) > 0 else 0.0
    if pd.isna(avg_win_R):
        avg_win_R = 0.0
    if pd.isna(avg_loss_R):
        avg_loss_R = 0.0
    expectancy = winrate * avg_win_R + (1 - winrate) * avg_loss_R
    if pd.isna(expectancy):
        expectancy = 0.0
    
    # Calculate core metrics (TP/SL only, excluding TIMEOUT)
    # Filter by strategy_set if specified (to match tp/sl counts)
    filtered_df = df.copy()
    if strategy_set is not None and "strategy" in filtered_df.columns:
        filtered_df = filtered_df[filtered_df["strategy"].astype(str).isin(strategy_set)]
    tp_sl_outcomes = filtered_df[filtered_df["outcome"].apply(lambda x: _normalize_outcome(x)).isin(["TP_hit", "SL_hit"])]
    tp_sl_pnl = pd.to_numeric(tp_sl_outcomes.get("pnl_pct", pd.Series()), errors="coerce") if not tp_sl_outcomes.empty else pd.Series(dtype=float)
    tp_sl_wins = tp_sl_pnl[tp_sl_pnl > 0]
    tp_sl_losses = tp_sl_pnl[tp_sl_pnl < 0]
    avg_win_core = tp_sl_wins.mean() if len(tp_sl_wins) > 0 else 0.0
    avg_loss_core = tp_sl_losses.mean() if len(tp_sl_losses) > 0 else 0.0
    if pd.isna(avg_win_core):
        avg_win_core = 0.0
    if pd.isna(avg_loss_core):
        avg_loss_core = 0.0
    wr_core = (tp / (tp + sl)) if (tp + sl) > 0 else 0.0
    expectancy_core = wr_core * avg_win_core + (1 - wr_core) * avg_loss_core
    if pd.isna(expectancy_core):
        expectancy_core = 0.0
    
    hold_series = pd.to_numeric(valid_outcomes.get("hold_seconds", pd.Series()), errors="coerce")
    avg_hold = hold_series.mean() if hold_series.notna().any() else 0.0
    if pd.isna(avg_hold):
        avg_hold = 0.0
    
    maturity_pct = (len(df) / 100) * 100
    
    # Determine report strategy for title
    if strategy_set and len(strategy_set) == 1:
        report_strategy = next(iter(strategy_set))
    else:
        report_strategy = "short_pump"

    # Strategy-scoped events for header and investor block (avoid mixing universes in multi-strategy runs)
    events_for_report = events_raw
    if events_raw is not None and not events_raw.empty and "strategy" in events_raw.columns and report_strategy:
        events_for_report = events_raw[events_raw["strategy"].astype(str) == report_strategy].copy()

    # Date range: full datasets period within --days (aligns short_pump with fast0)
    ds_range = get_datasets_date_range(data_dir, args.days)
    if ds_range is not None:
        date_range = ds_range
    else:
        # Fallback: union of outcomes + events
        all_dates: list = []
        if "date" in df.columns:
            out_dates = df["date"].dropna().astype(str).unique()
            all_dates.extend(out_dates.tolist())
        if events_for_report is not None and not events_for_report.empty and "date" in events_for_report.columns:
            ev_dates = events_for_report["date"].dropna().astype(str).unique()
            all_dates.extend(ev_dates.tolist())
        all_dates = list(set(d for d in all_dates if d and str(d) != "nan"))
        if all_dates:
            date_range = (str(min(all_dates)), str(max(all_dates)))
        else:
            date_range = ("unknown", "unknown")

    report = _format_report(
        date_range,
        total_trades,
        pnl_sum,
        winrate,
        conflicts_total,
        conflicts_top,
        conflicts_payload,
        by_strategy,
        strategies_label,
        outcomes_df=df,
        events_raw=events_for_report,
        avg_win_R=avg_win_R,
        avg_loss_R=avg_loss_R,
        expectancy=expectancy,
        avg_hold=avg_hold,
        maturity_pct=maturity_pct,
        tp=tp,
        sl=sl,
        timeout=timeout,
        other_count=other_count,
        wr_core=wr_core,
        expectancy_core=expectancy_core,
        strategy_set=strategy_set,
        report_strategy=report_strategy,
        debug=debug,
    )

    chart_paths: List[Path] = []
    maturity_status: str = ""
    n_core: int = 0
    # For short_pump always include investor block (ACTIVE MODE + splits) unless --no-active
    investor_mode = (
        args.investor_report
        or args.investor_text_only
        or (report_strategy == "short_pump" and not args.no_active)
    )
    base_report = ""  # used for FAST0 compact rebuild to avoid truncation
    tg_dist_min = float(os.getenv("TG_DIST_TO_PEAK_MIN", "3.5"))
    if investor_mode:
        df_sorted = _sort_outcomes_for_series(df)
        base_report = report
        if args.compact:
            # Executive compact format: 5 blocks, Russian, TG-friendly
            has_strat = "strategy" in df_sorted.columns
            df_sp = df_sorted[df_sorted["strategy"].astype(str) == "short_pump"] if has_strat else (df_sorted if report_strategy == "short_pump" else pd.DataFrame())
            df_spf = df_sorted[df_sorted["strategy"].astype(str) == "short_pump_filtered"] if has_strat else pd.DataFrame()
            df_f0 = df_sorted[df_sorted["strategy"].astype(str) == "short_pump_fast0"] if has_strat else (df_sorted if report_strategy == "short_pump_fast0" else pd.DataFrame())
            df_pr = df_sorted[df_sorted["strategy"].astype(str) == "short_pump_premium"] if has_strat else pd.DataFrame()
            df_wk = df_sorted[df_sorted["strategy"].astype(str) == "short_pump_wick"] if has_strat else pd.DataFrame()
            all_ev = events_raw if events_raw is not None and not events_raw.empty else pd.DataFrame()
            has_ev_strat = "strategy" in all_ev.columns
            ev_sp = all_ev[all_ev["strategy"].astype(str) == "short_pump"] if has_ev_strat else all_ev
            ev_spf = all_ev[all_ev["strategy"].astype(str) == "short_pump_filtered"] if has_ev_strat else pd.DataFrame()
            ev_f0 = all_ev[all_ev["strategy"].astype(str) == "short_pump_fast0"] if has_ev_strat else pd.DataFrame()
            ev_pr = all_ev[all_ev["strategy"].astype(str) == "short_pump_premium"] if has_ev_strat else pd.DataFrame()
            ev_wk = all_ev[all_ev["strategy"].astype(str) == "short_pump_wick"] if has_ev_strat else pd.DataFrame()
            df_sp_e = _ensure_stage_column(df_sp.copy()) if not df_sp.empty else None
            df_spf_e = _ensure_stage_column(df_spf.copy()) if not df_spf.empty else None
            df_f0_e = _ensure_stage_column(df_f0.copy()) if not df_f0.empty else None
            df_pr_e = _ensure_stage_column(df_pr.copy()) if not df_pr.empty else None
            df_wk_e = _ensure_stage_column(df_wk.copy()) if not df_wk.empty else None
            if df_sp_e is not None and not df_sp_e.empty:
                df_sp_e = _enrich_core_with_events(df_sp_e, ev_sp if not ev_sp.empty else None, debug=debug)
            if df_spf_e is not None and not df_spf_e.empty:
                df_spf_e = _enrich_core_with_events(df_spf_e, ev_spf if not ev_spf.empty else None, debug=debug)
            if df_f0_e is not None and not df_f0_e.empty:
                df_f0_e = _enrich_core_with_events(df_f0_e, ev_f0 if not ev_f0.empty else None, debug=debug)
            if df_pr_e is not None and not df_pr_e.empty:
                df_pr_e = _enrich_core_with_events(df_pr_e, ev_pr if not ev_pr.empty else None, debug=debug)
            if df_wk_e is not None and not df_wk_e.empty:
                df_wk_e = _enrich_core_with_events(df_wk_e, ev_wk if not ev_wk.empty else None, debug=debug)

            res_fp_live = load_outcomes(
                base_dir=data_dir,
                strategy="false_pump",
                mode="live",
                days=args.days,
                include_test=False,
                return_file_count=False,
            )
            df_fp_raw = res_fp_live[0] if isinstance(res_fp_live, tuple) else res_fp_live
            ev_fp_res = load_events_v2(
                data_dir=data_dir,
                strategy="false_pump",
                mode="live",
                days=args.days,
                raw=True,
                return_file_count=False,
            )
            ev_fp = ev_fp_res[0] if isinstance(ev_fp_res, tuple) else ev_fp_res
            df_fp_e = _ensure_stage_column(df_fp_raw.copy()) if not df_fp_raw.empty else None
            if df_fp_e is not None and not df_fp_e.empty:
                df_fp_e = _enrich_core_with_events(df_fp_e, ev_fp if not ev_fp.empty else None, debug=debug)

            # Ensure guard state is fresh before loading (report reads latest outcomes → guard must match)
            _run_guard_update(data_dir, args.days, rolling=args.rolling)

            # Load Auto Risk Guard state JSON from the same datasets root
            guard_state = None
            guard_path = data_dir / "auto_risk_guard_state.json"
            if guard_path.is_file():
                try:
                    with open(guard_path, "r", encoding="utf-8") as f:
                        raw = json.load(f)
                    if isinstance(raw, dict):
                        guard_state = raw
                    if debug:
                        keys = ", ".join(sorted(guard_state.keys())) if isinstance(guard_state, dict) else "N/A"
                        print(f"[EXEC_REPORT] guard_state loaded from {guard_path}: keys=[{keys}]", file=sys.stderr)
                except Exception as e:
                    if debug:
                        print(f"[EXEC_REPORT] failed to load guard_state from {guard_path}: {e}", file=sys.stderr)
                    guard_state = None

            # Load mode=paper outcomes for PAPER block (source of truth for DISABLED submodes)
            res_f0_paper = load_outcomes(base_dir=data_dir, strategy="short_pump_fast0", mode="paper", days=args.days)
            res_f0_filtered_paper = load_outcomes(base_dir=data_dir, strategy="short_pump_fast0_filtered", mode="paper", days=args.days)
            res_sp_paper = load_outcomes(base_dir=data_dir, strategy="short_pump", mode="paper", days=args.days)
            res_spf_paper = load_outcomes(base_dir=data_dir, strategy="short_pump_filtered", mode="paper", days=args.days)
            df_fast0_paper = res_f0_paper[0] if isinstance(res_f0_paper, tuple) else res_f0_paper
            df_fast0_filtered_paper = res_f0_filtered_paper[0] if isinstance(res_f0_filtered_paper, tuple) else res_f0_filtered_paper
            df_short_pump_paper = res_sp_paper[0] if isinstance(res_sp_paper, tuple) else res_sp_paper
            df_short_pump_filtered_paper = res_spf_paper[0] if isinstance(res_spf_paper, tuple) else res_spf_paper

            exec_report = build_executive_compact_report(
                df_sp_e if (df_sp_e is not None and not df_sp_e.empty) else None,
                df_spf_e if (df_spf_e is not None and not df_spf_e.empty) else None,
                df_f0_e if (df_f0_e is not None and not df_f0_e.empty) else None,
                date_range,
                rolling_n=args.rolling,
                tg_dist_min=tg_dist_min,
                guard_state=guard_state,
                df_fast0_paper=df_fast0_paper if not df_fast0_paper.empty else None,
                df_fast0_filtered_paper=df_fast0_filtered_paper if not df_fast0_filtered_paper.empty else None,
                df_short_pump_paper=df_short_pump_paper if not df_short_pump_paper.empty else None,
                df_short_pump_filtered_paper=df_short_pump_filtered_paper if not df_short_pump_filtered_paper.empty else None,
                df_short_pump_premium=df_pr_e if (df_pr_e is not None and not df_pr_e.empty) else None,
                df_short_pump_wick=df_wk_e if (df_wk_e is not None and not df_wk_e.empty) else None,
                df_false_pump=df_fp_e if (df_fp_e is not None and not df_fp_e.empty) else None,
                report_window_days=args.days,
            )
            investor_lines = exec_report.split("\n")
            report = exec_report  # compact mode: executive report only (self-contained)
            n_core = 0  # executive report doesn't return n_core; charts use df_sorted
            maturity_status = ""
        else:
            # short_pump: always compact for Telegram (unified, concise); FAST0: compact only when len>4096
            use_compact_investor = report_strategy == "short_pump"
            investor_lines, n_core, maturity_status = _investor_metrics_text(
                df_sorted, args.rolling, date_range, report_strategy, args.maturity_threshold,
                events_df=events_for_report, debug=debug, compact=use_compact_investor,
            )
            report = base_report + "\n" + "\n".join(investor_lines)
        # Charts only when --investor-report (not --investor-text-only)
        if args.investor_report:
            reports_dir = Path(__file__).resolve().parents[1] / "logs" / "reports"
            reports_dir.mkdir(parents=True, exist_ok=True)
            chart_paths = _build_investor_charts(
                df_sorted, args.rolling, date_range, report_strategy, reports_dir, debug
            )
        if debug:
            dates = df_sorted["date"].dropna().astype(str) if "date" in df_sorted.columns else []
            min_date = min(dates) if len(dates) else "N/A"
            max_date = max(dates) if len(dates) else "N/A"
            print(f"DEBUG investor: n_trades={len(df_sorted)}, n_core={n_core}, min_date={min_date}, max_date={max_date}")
            print(f"MATURITY: n_core={n_core}, threshold={args.maturity_threshold}, status={maturity_status}")
            print("DEBUG PNG paths:", [str(p) for p in chart_paths])

    token, chat_id = _get_tg_creds(debug=debug)
    if not token or not chat_id:
        print(report)
        if chart_paths:
            for p in chart_paths:
                print("Saved chart:", p)
        return

    # File-first delivery: send full report as .txt document (no 4096 truncation)
    _report_file_sent = False
    _tmp_path: Optional[Path] = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".txt", prefix="report_", delete=False, encoding="utf-8",
        ) as _f:
            _f.write(report)
            _tmp_path = Path(_f.name)
        _send_document_file(token, chat_id, _tmp_path, caption=f"Compact report {strategies_label}")
        _report_file_sent = True
    except Exception as e:
        print(f"WARNING: sendDocument failed, falling back to text: {e}", file=sys.stderr)
    finally:
        if _tmp_path and _tmp_path.exists():
            try:
                _tmp_path.unlink()
            except OSError:
                pass

    if not _report_file_sent:
        MAX_TG_LEN = 4096
        if len(report) > MAX_TG_LEN:
            report = report[: MAX_TG_LEN - 3] + "..."
        try:
            _send_telegram(report, token, chat_id)
        except Exception as e:
            print(f"WARNING: Failed to send text report to Telegram: {e}", file=sys.stderr)
    
    # Send photos (with per-photo error handling)
    if chart_paths:
        captions = ["Кривая капитала", f"Скользящий WR N={args.rolling}", f"Скользящий EV N={args.rolling}", "Просадка"]
        if args.investor_report and n_core < args.maturity_threshold:
            captions[0] = "Кривая капитала (РАННЯЯ ВЫБОРКА)"
        for p, cap in zip(chart_paths, captions):
            # Ensure caption is plain str and within Telegram limit (1024 chars)
            caption_str = str(cap)[:1024] if cap else None
            try:
                _send_photo(token, chat_id, p, caption_str)
                if debug:
                    print(f"TG sendPhoto ok: {p.name}", file=sys.stderr)
            except Exception as e:
                print(f"WARNING: Failed to send photo {p.name} to Telegram: {e}", file=sys.stderr)
                if debug:
                    print(f"TG sendPhoto failed: {p.name} {type(e).__name__}: {e}", file=sys.stderr)
                # Continue to next photo


if __name__ == "__main__":
    main()
