#!/usr/bin/env python3
"""
Temporary debug helper:
Print feature coverage (exists + % non-null) for the new ML factors inside the joined dataframe.

This script does NOT change trading logic. It is read-only.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd

from analytics.factor_report import _join_outcomes_trades_events
from analytics.load import load_events_v2, load_outcomes, load_trades_v3


STRATEGIES_DEFAULT = ("short_pump", "short_pump_fast0")

# Keep in sync with scripts/train_lgbm_entry_filter.py FEATURES
FEATURES = [
    "dist_to_peak_pct",
    "context_score",
    "liq_long_usd_30s",
    "liq_short_usd_30s",
    "funding_rate_abs",
    "oi_change_1m_pct",
    "oi_change_5m_pct",
    "delta_ratio_30s",
    "delta_ratio_1m",
    "volume_1m",
    "volume_zscore_20",
    "volume_ratio_1m_20",
    "volume_ratio_5m_20",
    "price_change_30s_pct",
    "price_change_1m_pct",
    "price_change_3m_pct",
    "accel_30s_vs_3m",
    "green_candles_5",
    "max_candle_body_pct_5",
    "wick_body_ratio_last",
    "time_since_peak_sec",
    "time_since_signal_sec",
    "pump_age_sec",
]

NEW_FEATURES = [
    "price_change_30s_pct",
    "price_change_1m_pct",
    "price_change_3m_pct",
    "accel_30s_vs_3m",
    "green_candles_5",
    "max_candle_body_pct_5",
    "wick_body_ratio_last",
    "volume_ratio_1m_20",
    "volume_ratio_5m_20",
    "time_since_peak_sec",
    "time_since_signal_sec",
    "pump_age_sec",
]


def _pct_non_null(df: pd.DataFrame, col: str) -> float:
    if col not in df.columns:
        return 0.0
    s = pd.to_numeric(df[col], errors="coerce")
    return float(s.notna().mean() * 100.0) if len(s) else 0.0


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", required=True, help="datasets root directory")
    ap.add_argument("--mode", required=True, choices=["live", "paper", "lab"], help="dataset mode")
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--strategies", type=str, default=",".join(STRATEGIES_DEFAULT))
    ap.add_argument("--max-nan-frac", type=float, default=0.7, help="Same default as train script")
    args = ap.parse_args()

    base_dir = Path(args.data_dir)
    strategies = [s.strip() for s in args.strategies.split(",") if s.strip()]

    rows_all: List[pd.DataFrame] = []
    for strat in strategies:
        outcomes = load_outcomes(base_dir=base_dir, strategy=strat, mode=args.mode, days=args.days, include_test=False)
        if isinstance(outcomes, tuple):
            outcomes = outcomes[0]
        trades = load_trades_v3(data_dir=base_dir, strategy=strat, mode=args.mode, days=args.days, include_test=False)
        if isinstance(trades, tuple):
            trades = trades[0]
        events = load_events_v2(
            data_dir=base_dir,
            strategy=strat,
            mode=args.mode,
            days=args.days,
            include_test=False,
            raw=True,
        )
        if isinstance(events, tuple):
            events = events[0]
        joined = _join_outcomes_trades_events(outcomes, trades, events)
        joined["strategy"] = strat
        rows_all.append(joined)

    df = pd.concat(rows_all, ignore_index=True) if rows_all else pd.DataFrame()
    rows_joined = int(len(df))

    print(f"[DEBUG_FEATURE_COVERAGE] mode={args.mode} days={args.days} strategies={strategies}")
    print(f"[DEBUG_FEATURE_COVERAGE] rows_joined={rows_joined}")

    outc = df.get("outcome", pd.Series([], dtype=str)).astype(str).str.strip()
    mask = outc.isin(["TP_hit", "SL_hit"])
    df = df.loc[mask].copy()
    rows_tpsl = int(len(df))
    print(f"[DEBUG_FEATURE_COVERAGE] rows_tpsl={rows_tpsl} (TP_hit/SL_hit only)")

    feat_present = [f for f in FEATURES if f in df.columns]
    print(f"[DEBUG_FEATURE_COVERAGE] features_present={len(feat_present)} => {feat_present}")
    print("")

    # Apply the same missingness filter strategy as train script uses (max-nan-frac across selected features)
    feat_present_all = [f for f in FEATURES if f in df.columns]
    if feat_present_all:
        X_raw = df[feat_present_all].copy()
        nan_frac = X_raw.apply(lambda r: pd.to_numeric(r, errors="coerce").isna().mean(), axis=1)
        df = df.loc[nan_frac <= float(args.max_nan_frac)].copy()
    print(f"[DEBUG_FEATURE_COVERAGE] rows_after_nan_filter={int(len(df))} (max-nan-frac={float(args.max_nan_frac)})")

    print("\nFEATURE_COVERAGE:")
    for f in NEW_FEATURES:
        exists = f in df.columns
        non_null = _pct_non_null(df, f) if exists else 0.0
        print(f"{f} exists={exists} non_null={non_null:.1f}%")


if __name__ == "__main__":
    main()

