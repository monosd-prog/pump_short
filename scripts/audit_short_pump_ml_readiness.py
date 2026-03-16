#!/usr/bin/env python3
"""
Audit: SHORT_PUMP ML readiness (analysis-only, no runtime changes).

Goals:
- Inspect live datasets for strategy=short_pump, mode=live.
- Join outcomes_v3 (core TP/SL/TIMEOUT) with trades_v3 and events_v3.
- Report coverage, feature availability, duplicates, and potential target leakage.
- Produce a short verdict: ML_READY or ML_NOT_READY with reasons.

Usage:
  PYTHONPATH=. python3 scripts/audit_short_pump_ml_readiness.py
  PYTHONPATH=. python3 scripts/audit_short_pump_ml_readiness.py --data-dir /root/pump_short/datasets
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import List, Tuple

from pathlib import Path

import pandas as pd


CORE_OUTCOMES = ("TP_hit", "SL_hit", "TIMEOUT")
STRATEGY = "short_pump"
MODE = "live"

CRITICAL_FEATURES = [
    "dist_to_peak_pct",
    "context_score",
    "stage",
    "delta_ratio_30s",
    "delta_ratio_1m",
    "cvd_delta_ratio_30s",
    "cvd_delta_ratio_1m",
    "oi_change_fast_pct",
    "funding_rate_abs",
    "liq_long_usd_30s",
    "liq_short_usd_30s",
    "volume_1m",
    "volume_5m",
    "volume_sma_20",
    "volume_zscore_20",
    "spread_bps",
    "orderbook_imbalance_10",
]


@dataclass
class JoinStats:
    total_outcomes_core: int
    matched_outcomes_trades: int
    matched_outcomes_events: int
    matched_full: int
    unmatched_outcomes_no_trade: int
    unmatched_outcomes_no_event: int


def _find_dataset_root(data_dir: Path) -> Path:
    """
    Resolve datasets root:
    - If data_dir/date=* exists, use data_dir
    - Else if data_dir/datasets/date=* exists, use data_dir/datasets
    """
    data_dir = data_dir.resolve()
    direct = list(data_dir.glob("date=*"))
    if any(p.is_dir() for p in direct):
        return data_dir
    sub = data_dir / "datasets"
    if sub.exists():
        direct = list(sub.glob("date=*"))
        if any(p.is_dir() for p in direct):
            return sub
    return data_dir


def _collect_paths(root: Path, strategy: str, mode: str) -> Tuple[List[Path], List[Path], List[Path]]:
    """Collect events_v3, trades_v3, outcomes_v3 for given strategy/mode."""
    events_paths: List[Path] = []
    trades_paths: List[Path] = []
    outcomes_paths: List[Path] = []
    for date_dir in sorted(root.glob("date=*")):
        if not date_dir.is_dir():
            continue
        base = date_dir / f"strategy={strategy}" / f"mode={mode}"
        if not base.is_dir():
            continue
        e = base / "events_v3.csv"
        t = base / "trades_v3.csv"
        o = base / "outcomes_v3.csv"
        if e.is_file():
            events_paths.append(e)
        if t.is_file():
            trades_paths.append(t)
        if o.is_file():
            outcomes_paths.append(o)
    return events_paths, trades_paths, outcomes_paths


def _read_concat_csv(paths: List[Path]) -> pd.DataFrame:
    if not paths:
        return pd.DataFrame()
    frames = []
    for p in paths:
        try:
            df = pd.read_csv(p)
            df["__source_file"] = str(p)
            frames.append(df)
        except Exception:
            continue
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _normalize_outcome(o: pd.Series) -> pd.Series:
    return o.astype(str).str.strip()


def _build_core_view(events: pd.DataFrame, trades: pd.DataFrame, outcomes: pd.DataFrame) -> Tuple[pd.DataFrame, JoinStats]:
    # Filter core outcomes: TP_hit/SL_hit/TIMEOUT
    if outcomes.empty:
        core = pd.DataFrame()
    else:
        out_norm = _normalize_outcome(outcomes.get("outcome", pd.Series([])))
        core_mask = out_norm.isin(CORE_OUTCOMES)
        core = outcomes.loc[core_mask].copy()
        core["outcome_norm"] = out_norm[core_mask]

    total_core = len(core)

    # Join keys: trade_id and event_id where possible
    trades_key = trades.get("trade_id") if not trades.empty and "trade_id" in trades.columns else None
    events_key = events.get("event_id") if not events.empty and "event_id" in events.columns else None

    core_trades = core
    if trades_key is not None and "trade_id" in core.columns:
        core_trades = core.merge(
            trades,
            how="left",
            on=["trade_id", "symbol", "strategy"],
            suffixes=("", "_trade"),
        )
    matched_trades = len(core_trades[core_trades.get("entry_time_utc").notna()]) if not trades.empty and "entry_time_utc" in core_trades.columns else 0

    core_full = core_trades
    if events_key is not None and "event_id" in core_trades.columns:
        core_full = core_trades.merge(
            events,
            how="left",
            on=["event_id", "symbol", "strategy"],
            suffixes=("", "_event"),
        )

    matched_events = len(core_full[core_full.get("time_utc").notna()]) if not events.empty and "time_utc" in core_full.columns else 0
    matched_full = len(core_full[(core_full.get("entry_time_utc").notna()) & (core_full.get("time_utc").notna())]) if not core_full.empty else 0

    unmatched_no_trade = total_core - matched_trades
    unmatched_no_event = total_core - matched_events

    stats = JoinStats(
        total_outcomes_core=total_core,
        matched_outcomes_trades=matched_trades,
        matched_outcomes_events=matched_events,
        matched_full=matched_full,
        unmatched_outcomes_no_trade=unmatched_no_trade,
        unmatched_outcomes_no_event=unmatched_no_event,
    )
    return core_full, stats


def _feature_coverage(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    rows = []
    total = len(df)
    for c in cols:
        if c not in df.columns:
            rows.append({"feature": c, "present": False, "filled_pct": 0.0})
            continue
        s = df[c]
        filled = s.notna().sum()
        filled_pct = (filled / total) * 100.0 if total else 0.0
        rows.append({"feature": c, "present": True, "filled_pct": filled_pct})
    return pd.DataFrame(rows)


def _target_distribution(df: pd.DataFrame) -> pd.Series:
    if df.empty or "outcome_norm" not in df.columns:
        return pd.Series(dtype=int)
    return df["outcome_norm"].value_counts().reindex(CORE_OUTCOMES, fill_value=0)


def _duplicate_checks(df: pd.DataFrame) -> Tuple[int, int, int]:
    """Return (#duplicate trade_id, #duplicate event_id, #duplicate run_id)."""
    dup_trade = 0
    dup_event = 0
    dup_run = 0
    if "trade_id" in df.columns:
        dup_trade = int(df["trade_id"].duplicated().sum())
    if "event_id" in df.columns:
        dup_event = int(df["event_id"].duplicated().sum())
    if "run_id" in df.columns:
        dup_run = int(df["run_id"].duplicated().sum())
    return dup_trade, dup_event, dup_run


def _detect_target_leakage(df: pd.DataFrame) -> Tuple[List[str], List[str]]:
    """
    Very conservative check:
    - suspicious: columns containing substrings like 'outcome', 'pnl', 'mae', 'mfe'
      (these are likely post-entry or label-like)
    - safe candidates: numeric features not in suspicious set and not in obvious IDs/timestamps
    """
    if df.empty:
        return [], []
    cols = list(df.columns)
    suspicious = []
    for c in cols:
        cn = c.lower()
        if any(tok in cn for tok in ("outcome", "pnl", "mae", "mfe", "tp_", "sl_", "exit_price", "closed_", "close_")):
            suspicious.append(c)
    id_like = {"trade_id", "event_id", "run_id", "symbol", "strategy", "side", "schema_version", "mode", "source_mode", "__source_file"}
    time_like = {c for c in cols if "time" in c.lower() or "ts" in c.lower() or c.endswith("_utc")}
    numeric = df.select_dtypes(include=["number", "bool"]).columns
    safe_candidates = [
        c for c in numeric
        if c not in suspicious and c not in id_like and c not in time_like
    ]
    return suspicious, safe_candidates


def _recommend_feature_sets(df: pd.DataFrame) -> Tuple[List[str], List[str], List[str]]:
    """
    Build recommended_feature_set:
    - safe_for_ml_now: intersection of safe numeric candidates with critical features that have good coverage
    - exclude_for_now: heavy-tailed or suspicious (from leakage list)
    - missing_but_desirable: critical features absent or with very low coverage
    """
    suspicious, safe_candidates = _detect_target_leakage(df)
    cov = _feature_coverage(df, CRITICAL_FEATURES)
    safe_now = []
    missing = []
    for _, row in cov.iterrows():
        feat = row["feature"]
        present = bool(row["present"])
        filled = float(row["filled_pct"])
        if not present or filled < 30.0:  # arbitrary threshold; data-poor
            missing.append(feat)
        elif feat in safe_candidates and filled >= 60.0:
            safe_now.append(feat)
    exclude_now = sorted(list(set(suspicious)))
    return sorted(safe_now), exclude_now, sorted(missing)


def main() -> None:
    ap = argparse.ArgumentParser(description="Audit SHORT_PUMP ML readiness (analysis-only)")
    ap.add_argument("--data-dir", default="/root/pump_short/datasets", help="Datasets root (default: /root/pump_short/datasets)")
    args = ap.parse_args()

    root = _find_dataset_root(Path(args.data_dir))
    print(f"DATASETS_ROOT: {root}")
    events_paths, trades_paths, outcomes_paths = _collect_paths(root, STRATEGY, MODE)
    print(f"FILES: events={len(events_paths)} trades={len(trades_paths)} outcomes={len(outcomes_paths)}")
    if not outcomes_paths:
        print("No outcomes_v3.csv found for short_pump/mode=live. ML_NOT_READY (no data).")
        return

    events = _read_concat_csv(events_paths)
    trades = _read_concat_csv(trades_paths)
    outcomes = _read_concat_csv(outcomes_paths)

    df_core, join_stats = _build_core_view(events, trades, outcomes)

    print("\n--- CORE OUTCOMES (TP/SL/TIMEOUT) ---")
    print(f"total_core_outcomes: {join_stats.total_outcomes_core}")
    print(f"matched_outcomes_trades: {join_stats.matched_outcomes_trades}")
    print(f"matched_outcomes_events: {join_stats.matched_outcomes_events}")
    print(f"matched_full (outcome+trade+event): {join_stats.matched_full}")
    print(f"unmatched_outcomes_no_trade: {join_stats.unmatched_outcomes_no_trade}")
    print(f"unmatched_outcomes_no_event: {join_stats.unmatched_outcomes_no_event}")

    print("\n--- TARGET DISTRIBUTION (core) ---")
    td = _target_distribution(df_core)
    if td.empty:
        print("no core outcomes")
    else:
        for outcome, cnt in td.items():
            print(f"{outcome}: {cnt}")

    print("\n--- FEATURE COVERAGE (critical features) ---")
    cov = _feature_coverage(df_core, CRITICAL_FEATURES)
    if cov.empty:
        print("no data for coverage (join failed or empty)")
    else:
        for _, row in cov.sort_values("feature").iterrows():
            present = "yes" if row["present"] else "no "
            print(f"{row['feature']}: present={present} filled={row['filled_pct']:.1f}%")

    print("\n--- DUPLICATE CHECKS (core outcomes) ---")
    dup_trade, dup_event, dup_run = _duplicate_checks(df_core)
    print(f"duplicate trade_id: {dup_trade}")
    print(f"duplicate event_id: {dup_event}")
    print(f"duplicate run_id: {dup_run}")

    print("\n--- TARGET LEAKAGE / FEATURE SET ---")
    suspicious, safe_candidates = _detect_target_leakage(df_core)
    print("suspicious_columns (potential leakage / post-entry):")
    if suspicious:
        for c in sorted(suspicious):
            print(f"  - {c}")
    else:
        print("  (none)")
    print("\nall_safe_numeric_candidates (raw):")
    if safe_candidates:
        for c in sorted(safe_candidates):
            print(f"  - {c}")
    else:
        print("  (none)")

    safe_now, exclude_now, missing = _recommend_feature_sets(df_core)
    print("\nrecommended_feature_set:")
    print("  safe_for_ml_now:")
    if safe_now:
        for c in safe_now:
            print(f"    - {c}")
    else:
        print("    (none)")
    print("  exclude_for_now:")
    if exclude_now:
        for c in exclude_now:
            print(f"    - {c}")
    else:
        print("    (none)")
    print("  missing_but_desirable:")
    if missing:
        for c in missing:
            print(f"    - {c}")
    else:
        print("    (none)")

    # Verdict
    reasons: list[str] = []
    if join_stats.total_outcomes_core < 200:
        reasons.append(f"low_core_samples={join_stats.total_outcomes_core}")
    if join_stats.matched_full < max(100, join_stats.total_outcomes_core * 0.6):
        reasons.append(f"weak_join_full={join_stats.matched_full}")
    if not safe_now:
        reasons.append("no_safe_features_with_good_coverage")
    if dup_trade > 0 or dup_event > 0:
        reasons.append(f"duplicates_trade={dup_trade}_event={dup_event}")

    if reasons:
        print("\nVERDICT: ML_NOT_READY")
        print("REASONS:")
        for r in reasons:
            print(f"  - {r}")
    else:
        print("\nVERDICT: ML_READY")
        print("REASONS:")
        print("  - enough core samples, good joins, and safe features with coverage")


if __name__ == "__main__":
    main()

