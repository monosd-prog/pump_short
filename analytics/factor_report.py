from __future__ import annotations

"""
Factor report engine for short_pump / short_pump_fast0.

Responsibilities:
- load outcomes, trades, events from datasets/*
- join chain: outcomes → trades (on trade_id) → events (on event_id) so factors from trades_v3 are kept
- derive core metrics (TP/SL/TIMEOUT, WR, EV proxy, EV20 proxy)
- compute single-factor, delta, symbol, regime, combo and candidate stats (MIN_N_COMBO / MIN_N_CANDIDATE)
- render human-readable TXT report and structured JSON
"""

from dataclasses import dataclass, asdict
from datetime import datetime
import json
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

from analytics.load import load_events_v2, load_outcomes, load_trades_v3
from analytics.joins import join_outcomes_with_events
from analytics.stats import (
    _core_mask,
    _normalize_outcome_raw,
)
from analytics.utils import dprint, DEBUG_ENABLED


EV_PROXY = {
    "TP_hit": 1.2,
    "SL_hit": -1.0,
    "TIMEOUT": 0.0,
}

CORE_OUTCOMES = ("TP_hit", "SL_hit", "TIMEOUT")

# Sample-safety thresholds (do not draw strong conclusions on tiny samples)
MIN_N_BUCKET = 20
MIN_N_COMBO = 30   # combinations filtering: only combo buckets with n >= MIN_N_COMBO
MIN_N_SYMBOL = 20
MIN_N_CANDIDATE = 40  # candidate selection: only combos with n >= MIN_N_CANDIDATE + WR/EV criteria


@dataclass
class BucketStats:
    bucket: str
    n: int
    tp: int
    sl: int
    timeout: int
    wr: float
    ev: float
    ev20: float


@dataclass
class FactorBlock:
    factor: str
    tracked: bool
    buckets: List[BucketStats]


@dataclass
class StrategySummary:
    strategy: str
    n: int
    tp: int
    sl: int
    timeout: int
    wr: float
    ev: float
    ev20: float
    data_quality: List[str]


@dataclass
class ComboCandidate:
    factor_a: str
    bucket_a: str
    factor_b: str
    bucket_b: str
    n: int
    tp: int
    sl: int
    timeout: int
    wr: float
    ev: float
    ev20: float
    why_candidate: str


def _debug_factor_columns(df: pd.DataFrame, *, tag: str) -> None:
    if os.getenv("FACTOR_REPORT_DEBUG", "0").strip() != "1":
        return
    cols = sorted(list(df.columns))
    print(f"[FACTOR_DEBUG] {tag}: n={len(df)} cols={len(cols)}")
    print(f"[FACTOR_DEBUG] {tag}: columns={cols}")


def _fill_from_payload_json(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fallback: extract key factors from payload_json(_event) when they are not present as CSV columns.

    This makes factor_report resilient when older datasets were written with a schema that didn't include
    some feature columns (extrasaction=ignore in CSV writer).
    """
    if df.empty:
        return df

    payload_col = None
    for c in ("payload_json_event", "payload_json", "payload_json_x", "payload_json_y"):
        if c in df.columns:
            payload_col = c
            break
    if payload_col is None:
        return df

    def _safe_load(x: Any) -> dict:
        if not isinstance(x, str) or not x.strip():
            return {}
        try:
            val = json.loads(x)
            return val if isinstance(val, dict) else {}
        except Exception:
            return {}

    payloads = df[payload_col].apply(_safe_load)

    key_map: Dict[str, List[str]] = {
        "delta_ratio_30s": ["delta_ratio_30s"],
        "delta_ratio_1m": ["delta_ratio_1m"],
        "cvd_delta_ratio_30s": ["cvd_delta_ratio_30s"],
        "cvd_delta_ratio_1m": ["cvd_delta_ratio_1m"],
        "liq_long_usd_30s": ["liq_long_usd_30s"],
        "liq_short_usd_30s": ["liq_short_usd_30s"],
        "oi_change_fast_pct": ["oi_change_fast_pct"],
        "oi_change_1m_pct": ["oi_change_1m_pct"],
        "oi_change_5m_pct": ["oi_change_5m_pct"],
        "funding_rate_abs": ["funding_rate_abs"],
        "volume_1m": ["volume_1m"],
        "volume_5m": ["volume_5m"],
        "volume_sma_20": ["volume_sma_20"],
        "volume_zscore_20": ["volume_zscore_20", "volume_zscore"],
        "spread_bps": ["spread_bps"],
        "orderbook_imbalance_10": ["orderbook_imbalance_10"],
        "dist_to_peak_pct": ["dist_to_peak_pct"],
        "context_score": ["context_score"],
        "stage": ["stage"],
        "symbol": ["symbol"],
    }

    out = df.copy()
    for col, keys in key_map.items():
        # Only fill if missing entirely or mostly empty
        if col in out.columns and out[col].notna().any():
            continue
        extracted = payloads.apply(
            lambda d: next((d.get(k) for k in keys if k in d and d.get(k) is not None), None)
        )
        if col == "symbol":
            fill = extracted.astype(str).replace({"None": ""})
        else:
            fill = pd.to_numeric(extracted, errors="coerce")
        if col in out.columns:
            out[col] = out[col].combine_first(fill)
        else:
            out[col] = fill
    return out


def _join_outcomes_trades_events(
    outcomes: pd.DataFrame,
    trades: pd.DataFrame,
    events: pd.DataFrame,
) -> pd.DataFrame:
    """
    Join chain: outcomes → trades (on trade_id) → events (on event_id).
    Keeps all outcome rows; attaches trade-level and event-level columns so factors from trades_v3
    and events_v3 are available (no loss). Duplicate column names get suffixes _trade / _event;
    we coalesce into a single column per factor so downstream code sees one value per row.
    """
    if outcomes.empty:
        return outcomes
    out = outcomes.copy()
    # outcomes × trades on trade_id (left keep all outcomes)
    if not trades.empty and "trade_id" in out.columns and "trade_id" in trades.columns:
        out = out.merge(
            trades,
            on="trade_id",
            how="left",
            suffixes=("", "_trade"),
        )
    # × events on event_id (left keep all outcome rows; event columns get _event suffix when duplicated)
    out = join_outcomes_with_events(out, events)
    # Coalesce: for known factor names, fill from _event then _trade so events/trades factors are not lost
    factor_cols = [
        "dist_to_peak_pct", "context_score", "stage",
        "cvd_delta_ratio_30s", "cvd_delta_ratio_1m", "delta_ratio_30s", "delta_ratio_1m",
        "liq_long_usd_30s", "liq_short_usd_30s",
        "oi_change_fast_pct", "oi_change_1m_pct", "oi_change_5m_pct",
        "funding_rate_abs", "volume_1m", "volume_5m", "volume_sma_20", "volume_zscore_20",
        "spread_bps", "orderbook_imbalance_10", "symbol",
    ]
    for col in factor_cols:
        ev_col = f"{col}_event"
        tr_col = f"{col}_trade"
        for src in [ev_col, tr_col]:
            if src not in out.columns:
                continue
            s = out[src]
            if col == "symbol" or (s.dtype == object and not pd.api.types.is_numeric_dtype(s)):
                fill = s
            else:
                fill = pd.to_numeric(s, errors="coerce")
            if col in out.columns:
                out[col] = out[col].combine_first(fill)
            else:
                out[col] = fill
    return out


def _normalize_outcome_column(df: pd.DataFrame) -> pd.Series:
    col = df.get("outcome")
    if col is None:
        return pd.Series(["UNKNOWN"] * len(df), index=df.index)
    return col.apply(_normalize_outcome_raw)


def _add_ev_proxy(df: pd.DataFrame) -> pd.DataFrame:
    norm = _normalize_outcome_column(df)
    ev_vals = norm.map(EV_PROXY).fillna(0.0)
    df = df.copy()
    df["ev_proxy"] = pd.to_numeric(ev_vals, errors="coerce").fillna(0.0)
    return df


def _compute_ev20(series: pd.Series) -> float:
    series = pd.to_numeric(series, errors="coerce").dropna()
    if series.empty:
        return 0.0
    tail = series.tail(20)
    return float(tail.mean()) if len(tail) else 0.0


def _dataset_quality_notes(df: pd.DataFrame) -> List[str]:
    notes: List[str] = []
    if df.empty:
        return ["no data"]
    for col in ["trade_id", "event_id", "run_id", "symbol", "outcome", "pnl_pct", "hold_seconds"]:
        if col in df.columns:
            pct = df[col].notna().mean() * 100
            notes.append(f"{col} coverage={pct:.0f}%")
        else:
            notes.append(f"{col} missing")
    dup = None
    if "trade_id" in df.columns:
        dup = len(df) - df["trade_id"].nunique()
    notes.append(f"dup_trade_id={dup}" if dup is not None else "dup_trade_id=N/A")
    return notes


def _strategy_summary(df: pd.DataFrame, strategy: str) -> StrategySummary:
    if df.empty:
        return StrategySummary(
            strategy=strategy,
            n=0,
            tp=0,
            sl=0,
            timeout=0,
            wr=0.0,
            ev=0.0,
            ev20=0.0,
            data_quality=["no data"],
        )
    norm = _normalize_outcome_column(df)
    tp = int((norm == "TP_hit").sum())
    sl = int((norm == "SL_hit").sum())
    timeout = int((norm == "TIMEOUT").sum())
    n = len(df)
    core_mask = norm.isin(CORE_OUTCOMES)
    ev_series = pd.to_numeric(df.get("ev_proxy", 0.0), errors="coerce").fillna(0.0)
    core_ev = ev_series[core_mask]
    ev = float(core_ev.mean()) if not core_ev.empty else 0.0
    ev20 = _compute_ev20(core_ev)
    denom = tp + sl
    wr = (tp / denom) if denom else 0.0
    dq = _dataset_quality_notes(df)
    return StrategySummary(
        strategy=strategy,
        n=n,
        tp=tp,
        sl=sl,
        timeout=timeout,
        wr=wr,
        ev=ev,
        ev20=ev20,
        data_quality=dq,
    )


def _bucketize(series: pd.Series, factor: str) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce")
    # predefined bins for known factors
    bins_map: Dict[str, Sequence[float]] = {
        "dist_to_peak_pct": [-np.inf, 2.0, 3.5, 5.0, 7.5, 10.0, np.inf],
        "context_score": [-np.inf, 0.2, 0.4, 0.6, 0.8, 1.0, np.inf],
        "stage": [-np.inf, 2, 3, 4, 5, 6, np.inf],
        "cvd_delta_ratio_30s": [-np.inf, -1.0, -0.5, 0.0, 0.5, 1.0, 2.0, np.inf],
        "cvd_delta_ratio_1m": [-np.inf, -1.0, -0.5, 0.0, 0.5, 1.0, 2.0, np.inf],
        "delta_ratio_30s": [-np.inf, -1.0, -0.5, 0.0, 0.5, 1.0, 2.0, np.inf],
        "delta_ratio_1m": [-np.inf, -1.0, -0.5, 0.0, 0.5, 1.0, 2.0, np.inf],
        "liq_long_usd_30s": [0, 5_000, 10_000, 25_000, 50_000, 100_000, np.inf],
        "liq_short_usd_30s": [0, 5_000, 10_000, 25_000, 50_000, 100_000, np.inf],
        "oi_change_fast_pct": [-np.inf, -5, -2, 0, 2, 5, 10, np.inf],
        "oi_change_1m_pct": [-np.inf, -5, -2, 0, 2, 5, 10, np.inf],
        "oi_change_5m_pct": [-np.inf, -5, -2, 0, 2, 5, 10, np.inf],
        "funding_rate_abs": [0, 0.0005, 0.001, 0.002, 0.005, 0.01, np.inf],
        "volume_1m": [0, 50_000, 100_000, 250_000, 500_000, 1_000_000, np.inf],
        "volume_5m": [0, 100_000, 250_000, 500_000, 1_000_000, 2_000_000, np.inf],
        "volume_sma_20": [0, 50_000, 100_000, 250_000, 500_000, 1_000_000, np.inf],
        "volume_zscore_20": [-np.inf, -1.0, -0.5, 0.0, 0.5, 1.0, 2.0, np.inf],
        "spread_bps": [0, 1, 2, 3, 5, 10, np.inf],
        "orderbook_imbalance_10": [-np.inf, -0.5, -0.25, 0.0, 0.25, 0.5, np.inf],
    }
    if factor == "symbol":
        return series.astype(str).fillna("UNKNOWN")

    bins = bins_map.get(factor)
    if bins is None:
        # generic fallback: quantiles into up to 6 buckets
        try:
            quantiles = s.dropna().quantile([0.0, 0.2, 0.4, 0.6, 0.8, 1.0]).unique()
            if len(quantiles) < 3:
                return pd.cut(s, 4, duplicates="drop").astype(str)
            bins = [float(v) for v in sorted(set(quantiles))]
        except Exception:
            return pd.cut(s, 4, duplicates="drop").astype(str)
    labels = [f"[{bins[i]:.3g},{bins[i+1]:.3g})" for i in range(len(bins) - 1)]
    return pd.cut(s, bins=bins, labels=labels, include_lowest=True).astype(str)


def _factor_block(df: pd.DataFrame, factor: str) -> FactorBlock:
    if factor not in df.columns:
        return FactorBlock(factor=factor, tracked=False, buckets=[])
    binned = _bucketize(df[factor], factor)
    norm = _normalize_outcome_column(df)
    ev_series = pd.to_numeric(df["ev_proxy"], errors="coerce").fillna(0.0)
    rows: List[BucketStats] = []
    for bucket, sub in df.groupby(binned, dropna=False):
        bucket_name = str(bucket)
        if bucket_name in ("nan", "NaN"):
            bucket_name = "NaN"
        n = len(sub)
        if n < MIN_N_BUCKET:
            continue
        norm_b = norm.loc[sub.index]
        core_mask = norm_b.isin(CORE_OUTCOMES)
        ev_b = ev_series.loc[sub.index][core_mask]
        tp = int((norm_b == "TP_hit").sum())
        sl = int((norm_b == "SL_hit").sum())
        timeout = int((norm_b == "TIMEOUT").sum())
        denom = tp + sl
        wr = (tp / denom) if denom else 0.0
        ev = float(ev_b.mean()) if not ev_b.empty else 0.0
        ev20 = _compute_ev20(ev_b)
        rows.append(
            BucketStats(
                bucket=bucket_name,
                n=n,
                tp=tp,
                sl=sl,
                timeout=timeout,
                wr=wr,
                ev=ev,
                ev20=ev20,
            )
        )
    rows.sort(key=lambda r: r.ev, reverse=True)
    return FactorBlock(factor=factor, tracked=True, buckets=rows)


def _symbol_block(df: pd.DataFrame) -> List[BucketStats]:
    if "symbol" not in df.columns:
        return []
    ev_df = df.copy()
    ev_df["symbol"] = ev_df["symbol"].astype(str).fillna("UNKNOWN")
    norm = _normalize_outcome_column(ev_df)
    ev_series = pd.to_numeric(ev_df["ev_proxy"], errors="coerce").fillna(0.0)
    rows: List[BucketStats] = []
    for sym, sub in ev_df.groupby("symbol"):
        n = len(sub)
        if n < MIN_N_SYMBOL:
            continue
        norm_s = norm.loc[sub.index]
        core_mask = norm_s.isin(CORE_OUTCOMES)
        ev_s = ev_series.loc[sub.index][core_mask]
        tp = int((norm_s == "TP_hit").sum())
        sl = int((norm_s == "SL_hit").sum())
        timeout = int((norm_s == "TIMEOUT").sum())
        denom = tp + sl
        wr = (tp / denom) if denom else 0.0
        ev = float(ev_s.mean()) if not ev_s.empty else 0.0
        ev20 = _compute_ev20(ev_s)
        rows.append(
            BucketStats(
                bucket=str(sym),
                n=n,
                tp=tp,
                sl=sl,
                timeout=timeout,
                wr=wr,
                ev=ev,
                ev20=ev20,
            )
        )
    rows.sort(key=lambda r: r.ev, reverse=True)
    return rows


def _market_regime(df: pd.DataFrame) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    def _stat(col: str) -> Optional[Dict[str, float]]:
        if col not in df.columns:
            return None
        s = pd.to_numeric(df[col], errors="coerce")
        if not s.notna().any():
            return None
        return {
            "mean": float(s.mean()),
            "p25": float(s.quantile(0.25)),
            "p50": float(s.median()),
            "p75": float(s.quantile(0.75)),
        }

    out["funding_rate_abs"] = _stat("funding_rate_abs")
    out["oi_change_fast_pct"] = _stat("oi_change_fast_pct")
    out["liq_long_usd_30s"] = _stat("liq_long_usd_30s")
    out["liq_short_usd_30s"] = _stat("liq_short_usd_30s")
    out["volume_1m"] = _stat("volume_1m")
    return out


def _combo_candidates(df: pd.DataFrame, factors: Sequence[str]) -> List[ComboCandidate]:
    """
    Best 2-factor combinations and paper candidates.
    - Combinations filtering: only buckets with n >= MIN_N_COMBO.
    - Candidate selection: among those, only rows with n >= MIN_N_CANDIDATE and WR/EV thresholds.
    """
    norm = _normalize_outcome_column(df)
    ev_series = pd.to_numeric(df["ev_proxy"], errors="coerce").fillna(0.0)
    candidates: List[ComboCandidate] = []
    present = [f for f in factors if f in df.columns]
    if len(present) < 2:
        return []
    pairs: List[Tuple[str, str]] = []
    for a in present:
        for b in present:
            if a >= b:
                continue
            pairs.append((a, b))
    for fa, fb in pairs:
        ba = _bucketize(df[fa], fa)
        bb = _bucketize(df[fb], fb)
        combo_key = list(zip(ba, bb))
        key_series = pd.Series(combo_key, index=df.index)
        for (ba_val, bb_val), sub_idx in key_series.groupby(key_series).groups.items():
            sub = df.loc[sub_idx]
            n = len(sub)
            if n < MIN_N_COMBO:  # combinations filtering
                continue
            norm_c = norm.loc[sub.index]
            core_mask = norm_c.isin(CORE_OUTCOMES)
            ev_c = ev_series.loc[sub.index][core_mask]
            tp = int((norm_c == "TP_hit").sum())
            sl = int((norm_c == "SL_hit").sum())
            timeout = int((norm_c == "TIMEOUT").sum())
            denom = tp + sl
            wr = (tp / denom) if denom else 0.0
            ev = float(ev_c.mean()) if not ev_c.empty else 0.0
            ev20 = _compute_ev20(ev_c)
            if n >= MIN_N_CANDIDATE and ev > 0.1 and wr > 0.52:  # candidate selection
                why = []
                why.append(f"n={n}")
                why.append(f"WR={wr:.1%}")
                why.append(f"EV={ev:+.3f}")
                why.append(f"EV20={ev20:+.3f}")
                candidates.append(
                    ComboCandidate(
                        factor_a=fa,
                        bucket_a=str(ba_val),
                        factor_b=fb,
                        bucket_b=str(bb_val),
                        n=n,
                        tp=tp,
                        sl=sl,
                        timeout=timeout,
                        wr=wr,
                        ev=ev,
                        ev20=ev20,
                        why_candidate=", ".join(why),
                    )
                )
    candidates.sort(key=lambda c: c.ev, reverse=True)
    return candidates[:10]


def _render_summary_block(summary: StrategySummary) -> List[str]:
    lines = [
        f"Strategy: {summary.strategy}",
        f"N={summary.n}, TP={summary.tp}, SL={summary.sl}, TIMEOUT={summary.timeout}",
        f"WR={summary.wr:.1%}, EV={summary.ev:+.3f}, EV20={summary.ev20:+.3f}",
    ]
    if summary.data_quality:
        lines.append("Data quality:")
        for note in summary.data_quality:
            lines.append(f"  - {note}")
    return lines


def _render_factor_block(block: FactorBlock) -> List[str]:
    if not block.tracked:
        return [f"{block.factor}: not tracked"]
    lines = [f"Factor: {block.factor}"]
    if not block.buckets:
        lines.append("  no buckets passing sample safety threshold")
        return lines
    for b in block.buckets:
        lines.append(
            f"  {b.bucket}: n={b.n}, TP={b.tp}, SL={b.sl}, TIMEOUT={b.timeout}, "
            f"WR={b.wr:.1%}, EV={b.ev:+.3f}, EV20={b.ev20:+.3f}"
        )
    return lines


def _render_symbol_block(rows: List[BucketStats]) -> List[str]:
    if not rows:
        return ["symbol: not tracked"]
    lines: List[str] = []
    lines.append("Top EV symbols:")
    for b in rows[:5]:
        lines.append(
            f"  {b.bucket}: n={b.n}, WR={b.wr:.1%}, EV={b.ev:+.3f}, EV20={b.ev20:+.3f}"
        )
    lines.append("Worst EV symbols:")
    for b in sorted(rows, key=lambda r: r.ev)[:5]:
        lines.append(
            f"  {b.bucket}: n={b.n}, WR={b.wr:.1%}, EV={b.ev:+.3f}, EV20={b.ev20:+.3f}"
        )
    lines.append(f"Sample-safe threshold: n>={MIN_N_SYMBOL}")
    return lines


def _render_regime_block(reg: Mapping[str, Any]) -> List[str]:
    lines: List[str] = []
    lines.append("Market regime:")
    for key in ["funding_rate_abs", "oi_change_fast_pct", "liq_long_usd_30s", "liq_short_usd_30s", "volume_1m"]:
        stats = reg.get(key)
        if stats is None:
            lines.append(f"  {key}: not tracked")
        else:
            lines.append(
                f"  {key}: mean={stats['mean']:.4g}, p25={stats['p25']:.4g}, "
                f"p50={stats['p50']:.4g}, p75={stats['p75']:.4g}"
            )
    return lines


def _render_candidates_block(cands: Sequence[ComboCandidate]) -> List[str]:
    if not cands:
        return ["No paper candidates passing thresholds (n, WR, EV)."]
    lines: List[str] = []
    for c in cands:
        lines.append(
            f"- ({c.factor_a}={c.bucket_a}) & ({c.factor_b}={c.bucket_b}): "
            f"n={c.n}, WR={c.wr:.1%}, EV={c.ev:+.3f}, EV20={c.ev20:+.3f} :: {c.why_candidate}"
        )
    return lines


def build_factor_report_for_strategy(
    *,
    df: pd.DataFrame,
    strategy: str,
) -> Dict[str, Any]:
    """
    Build factor report blocks for a single strategy.
    Returns JSON-like dict with summary + blocks and also provides text blocks via renderer.
    """
    if df.empty:
        summary = _strategy_summary(df, strategy)
        return {
            "strategy": strategy,
            "summary": asdict(summary),
            "factors": {},
            "delta_analysis": [],
            "symbol_analysis": [],
            "market_regime": {},
            "combos": [],
            "candidates": [],
        }

    df = _fill_from_payload_json(df)
    _debug_factor_columns(df, tag=f"{strategy}:after_payload_fill")
    df = _add_ev_proxy(df)
    summary = _strategy_summary(df, strategy)

    factor_list = [
        "dist_to_peak_pct",
        "context_score",
        "stage",
        "cvd_delta_ratio_30s",
        "cvd_delta_ratio_1m",
        "delta_ratio_30s",
        "delta_ratio_1m",
        "liq_long_usd_30s",
        "liq_short_usd_30s",
        "oi_change_fast_pct",
        "oi_change_1m_pct",
        "oi_change_5m_pct",
        "funding_rate_abs",
        "volume_1m",
        "volume_5m",
        "volume_sma_20",
        "volume_zscore_20",
        "spread_bps",
        "orderbook_imbalance_10",
        "symbol",
    ]

    factors: Dict[str, Any] = {}
    factor_blocks: Dict[str, FactorBlock] = {}
    for f in factor_list:
        block = _factor_block(df, f)
        factor_blocks[f] = block
        factors[f] = {
            "tracked": block.tracked,
            "buckets": [asdict(b) for b in block.buckets],
        }

    delta_factors = [
        "delta_ratio_30s",
        "delta_ratio_1m",
        "cvd_delta_ratio_30s",
        "cvd_delta_ratio_1m",
    ]
    delta_blocks = [f for f in delta_factors if factor_blocks.get(f, FactorBlock("", False, [])).tracked]

    symbol_rows = _symbol_block(df)
    regime = _market_regime(df)
    combos = _combo_candidates(
        df,
        factors=[
            "dist_to_peak_pct",
            "context_score",
            "delta_ratio_30s",
            "cvd_delta_ratio_30s",
            "liq_long_usd_30s",
            "volume_zscore_20",
            "symbol",
        ],
    )

    return {
        "strategy": strategy,
        "summary": asdict(summary),
        "factors": factors,
        "delta_analysis": delta_blocks,
        "symbol_analysis": [asdict(b) for b in symbol_rows],
        "market_regime": regime,
        "combos": [asdict(c) for c in combos],
        "candidates": [asdict(c) for c in combos],
    }


def render_factor_report_txt(report: Dict[str, Any]) -> str:
    """
    Render multi-strategy factor report to human-readable TXT.
    """
    lines: List[str] = []
    meta = report.get("meta", {})
    header = f"FACTOR REPORT — days={meta.get('days')} strategies={','.join(meta.get('strategies', []))}"
    lines.append(header)
    lines.append(f"Generated at UTC: {meta.get('generated_at')}")
    lines.append("")

    for strat_block in report.get("strategies", []):
        strategy = strat_block["strategy"]
        lines.append("=" * 80)
        lines.append(f"STRATEGY: {strategy}")
        lines.append("=" * 80)
        # A. Strategy summary
        lines.append("")
        lines.append("A) Strategy summary")
        summary = StrategySummary(**strat_block["summary"])
        lines.extend(_render_summary_block(summary))

        # B. Single factors
        lines.append("")
        lines.append("B) Single factors")
        for fname, fdata in strat_block["factors"].items():
            block = FactorBlock(
                factor=fname,
                tracked=bool(fdata["tracked"]),
                buckets=[BucketStats(**b) for b in fdata["buckets"]],
            )
            lines.extend(_render_factor_block(block))

        # C. Delta analysis
        lines.append("")
        lines.append("C) Delta analysis")
        for fname in strat_block["delta_analysis"]:
            block_data = strat_block["factors"][fname]
            block = FactorBlock(
                factor=fname,
                tracked=bool(block_data["tracked"]),
                buckets=[BucketStats(**b) for b in block_data["buckets"]],
            )
            lines.extend(_render_factor_block(block))

        # D. Symbol analysis
        lines.append("")
        lines.append("D) Symbol analysis")
        sym_rows = [BucketStats(**b) for b in strat_block.get("symbol_analysis", [])]
        lines.extend(_render_symbol_block(sym_rows))

        # E. Market regime
        lines.append("")
        lines.append("E) Market regime")
        lines.extend(_render_regime_block(strat_block.get("market_regime", {})))

        # F. Best combinations
        lines.append("")
        lines.append("F) Best combinations")
        combos = [ComboCandidate(**c) for c in strat_block.get("combos", [])]
        lines.extend(_render_candidates_block(combos))

        # G. Candidates for paper filters
        lines.append("")
        lines.append("G) Candidates for paper filters")
        cands = [ComboCandidate(**c) for c in strat_block.get("candidates", [])]
        lines.extend(_render_candidates_block(cands))

        lines.append("")

    return "\n".join(lines)


def build_factor_report(
    *,
    base_dir: Path | str,
    days: int,
    strategies: Sequence[str] | None = None,
    mode: str = "live",
) -> Tuple[Dict[str, Any], str]:
    """
    High-level entrypoint.
    Returns (json_report, txt_report).
    """
    if strategies is None or not strategies:
        strategies = ["short_pump", "short_pump_fast0"]
    base_dir = Path(base_dir)

    strat_blocks: List[Dict[str, Any]] = []
    for strat in strategies:
        outcomes = load_outcomes(
            base_dir=base_dir,
            strategy=strat,
            mode=mode,
            days=days,
            include_test=False,
            return_file_count=False,
        )
        if isinstance(outcomes, tuple):
            outcomes = outcomes[0]
        trades = load_trades_v3(
            data_dir=base_dir,
            strategy=strat,
            mode=mode,
            days=days,
            include_test=False,
            return_file_count=False,
        )
        if isinstance(trades, tuple):
            trades = trades[0]
        events = load_events_v2(
            data_dir=base_dir,
            strategy=strat,
            mode=mode,
            days=days,
            include_test=False,
            raw=True,
            return_file_count=False,
        )
        if isinstance(events, tuple):
            events = events[0]
        # Join chain: outcomes → trades → events so factors from trades_v3 and events_v3 are preserved
        joined = _join_outcomes_trades_events(outcomes, trades, events)
        _debug_factor_columns(joined, tag=f"{strat}:joined_raw")
        dprint(DEBUG_ENABLED, f"[FACTOR] strategy={strat} rows={len(joined)}")
        strat_blocks.append(build_factor_report_for_strategy(df=joined, strategy=strat))

    meta = {
        "days": days,
        "strategies": list(strategies),
        "base_dir": str(base_dir),
        "generated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
    }
    full = {
        "meta": meta,
        "strategies": strat_blocks,
    }
    txt = render_factor_report_txt(full)
    return full, txt


def save_factor_report_files(
    *,
    base_dir: Path | str,
    days: int,
    strategies: Sequence[str] | None = None,
    reports_dir: Path | str | None = None,
) -> Tuple[Path, Path, str]:
    """
    Convenience entrypoint for CLI/Telegram.
    Returns (txt_path, json_path, summary_text).
    """
    json_report, txt = build_factor_report(base_dir=base_dir, days=days, strategies=strategies)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    if reports_dir is None:
        reports_dir = Path("reports")
    reports_dir = Path(reports_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)
    txt_path = reports_dir / f"factor_report_{days}d_{ts}.txt"
    json_path = reports_dir / f"factor_report_{days}d_{ts}.json"
    txt_path.write_text(txt, encoding="utf-8")
    import json

    with json_path.open("w", encoding="utf-8") as f:
        json.dump(json_report, f, ensure_ascii=False, indent=2)

    # Short summary for CLI / Telegram
    lines: List[str] = []
    lines.append(
        f"FACTOR REPORT SUMMARY — days={json_report['meta']['days']} "
        f"strategies={','.join(json_report['meta']['strategies'])}"
    )
    for strat in json_report["strategies"]:
        s = StrategySummary(**strat["summary"])
        lines.append(
            f"{s.strategy}: N={s.n}, TP={s.tp}, SL={s.sl}, TIMEOUT={s.timeout}, "
            f"WR={s.wr:.1%}, EV={s.ev:+.3f}, EV20={s.ev20:+.3f}"
        )
    summary = "\n".join(lines)
    return txt_path, json_path, summary

