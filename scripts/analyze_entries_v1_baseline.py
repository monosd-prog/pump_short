#!/usr/bin/env python3
"""
Baseline analysis for entries_v1 dataset.
Reads datasets/ml/entries_v1.parquet or .csv and produces summary reports.
"""
from __future__ import annotations

import math
import os
from pathlib import Path
from typing import Optional

import pandas as pd

ROOT = Path.cwd()
ML_PATH_PARQ = ROOT / "datasets" / "ml" / "entries_v1.parquet"
ML_PATH_CSV = ROOT / "datasets" / "ml" / "entries_v1.csv"
REPORT_DIR = ROOT / "reports"
REPORT_DIR.mkdir(parents=True, exist_ok=True)


def load_entries() -> pd.DataFrame:
    if ML_PATH_PARQ.exists():
        return pd.read_parquet(ML_PATH_PARQ)
    if ML_PATH_CSV.exists():
        return pd.read_csv(ML_PATH_CSV, low_memory=False)
    raise FileNotFoundError("No entries_v1 dataset found (parquet or csv)")


def safe_mean(series) -> float:
    try:
        return float(pd.to_numeric(series, errors="coerce").dropna().astype(float).mean())
    except Exception:
        return float("nan")


def bucket_series(series, bins=5):
    try:
        ser = pd.to_numeric(series, errors="coerce")
        return pd.qcut(ser.rank(method="first"), q=bins, labels=False, duplicates="drop")
    except Exception:
        return pd.Series([None] * len(series))


def main():
    df = load_entries()
    # basic normalization
    df["outcome_norm"] = df.get("outcome", "").astype(str)
    df["outcome_norm"] = df["outcome_norm"].str.strip()
    total = len(df)

    outcomes = df["outcome_norm"].value_counts(dropna=False).to_dict()
    tp_count = int(outcomes.get("TP_hit", outcomes.get("TP", 0) or 0))
    sl_count = int(outcomes.get("SL_hit", outcomes.get("SL", 0) or 0))
    timeout_count = int(outcomes.get("TIMEOUT", 0) or 0)
    unknown_no_data_count = int(outcomes.get("UNKNOWN_NO_DATA", 0) or 0)

    core_total = tp_count + sl_count
    core_winrate = tp_count / core_total if core_total > 0 else float("nan")
    avg_pnl = safe_mean(df.get("pnl_pct"))
    avg_r = safe_mean(df.get("r_multiple"))

    # by strategy
    # ensure r_multiple exists
    if "r_multiple" in df.columns:
        df["r_multiple"] = pd.to_numeric(df["r_multiple"], errors="coerce").fillna(0.0)
    else:
        df["r_multiple"] = 0.0
    by_strategy = df.groupby("strategy").agg(
        total=("strategy", "size"),
        tp=("outcome_norm", lambda s: (s == "TP_hit").sum()),
        sl=("outcome_norm", lambda s: (s == "SL_hit").sum()),
        timeout=("outcome_norm", lambda s: (s == "TIMEOUT").sum()),
        winrate=("outcome_norm", lambda s: ((s == "TP_hit").sum() / max(1, ((s == "TP_hit") | (s == "SL_hit")).sum())) if ((s == "TP_hit") | (s == "SL_hit")).sum() > 0 else float("nan")),
        avg_pnl=("pnl_pct", lambda x: safe_mean(x)),
        avg_r=("r_multiple", lambda x: safe_mean(x)),
    ).reset_index()

    # by risk_profile
    risk_col = "risk_profile" if "risk_profile" in df.columns else None
    if risk_col:
        by_risk = df.groupby(risk_col).agg(
            total=(risk_col, "size"),
            tp=("outcome_norm", lambda s: (s == "TP_hit").sum()),
            sl=("outcome_norm", lambda s: (s == "SL_hit").sum()),
            timeout=("outcome_norm", lambda s: (s == "TIMEOUT").sum()),
            avg_pnl=("pnl_pct", lambda x: safe_mean(x)),
        ).reset_index()
    else:
        by_risk = pd.DataFrame()

    # by symbol (top 20)
    by_symbol = df.groupby("symbol").agg(
        total=("symbol", "size"),
        tp=("outcome_norm", lambda s: (s == "TP_hit").sum()),
        sl=("outcome_norm", lambda s: (s == "SL_hit").sum()),
        timeout=("outcome_norm", lambda s: (s == "TIMEOUT").sum()),
    ).reset_index().sort_values("total", ascending=False).head(20)

    # TIMEOUT share and mfe/mae
    timeout_df = df[df["outcome_norm"] == "TIMEOUT"]
    timeout_share = len(timeout_df) / total if total > 0 else 0.0
    avg_mfe_by_outcome = df.groupby("outcome_norm")["mfe_pct"].apply(lambda s: safe_mean(s)).to_dict()
    avg_mae_by_outcome = df.groupby("outcome_norm")["mae_pct"].apply(lambda s: safe_mean(s)).to_dict()

    # suspicious symbols (many SL or TIMEOUT)
    sym_stats = by_symbol.copy()
    sym_stats["sl_rate"] = sym_stats["sl"] / sym_stats["total"]
    sym_stats["timeout_rate"] = sym_stats["timeout"] / sym_stats["total"]
    top_suspicious = sym_stats.sort_values(["timeout_rate", "sl_rate"], ascending=False).head(10)

    # context_score buckets
    if "context_score" in df.columns:
        df["context_bucket"] = pd.qcut(pd.to_numeric(df["context_score"], errors="coerce").fillna(0.0), q=5, duplicates="drop")
        by_context = df.groupby("context_bucket").agg(
            total=("context_bucket", "size"),
            tp=("outcome_norm", lambda s: (s == "TP_hit").sum()),
            sl=("outcome_norm", lambda s: (s == "SL_hit").sum()),
            avg_pnl=("pnl_pct", lambda x: safe_mean(x)),
        ).reset_index()
    else:
        by_context = pd.DataFrame()

    # dist buckets if exist
    dist_cols = []
    for c in ("dist_to_tp_pct", "dist_to_sl_pct"):
        if c in df.columns:
            dist_cols.append(c)
    by_dist = pd.DataFrame()
    if dist_cols:
        for c in dist_cols:
            df[f"{c}_bucket"] = pd.qcut(pd.to_numeric(df[c], errors="coerce").fillna(0.0).rank(method="first"), q=5, duplicates="drop")
        # aggregate for first dist column
        c0 = dist_cols[0]
        by_dist = df.groupby(f"{c0}_bucket").agg(
            total=(c0, "size"),
            tp=("outcome_norm", lambda s: (s == "TP_hit").sum()),
            sl=("outcome_norm", lambda s: (s == "SL_hit").sum()),
            avg_pnl=("pnl_pct", lambda x: safe_mean(x)),
        ).reset_index()

    # time of day
    if "opened_ts" in df.columns:
        df["opened_dt"] = pd.to_datetime(df["opened_ts"], errors="coerce")
        df["hour"] = df["opened_dt"].dt.hour
        by_hour = df.groupby("hour").agg(
            total=("hour", "size"),
            tp=("outcome_norm", lambda s: (s == "TP_hit").sum()),
            sl=("outcome_norm", lambda s: (s == "SL_hit").sum()),
        ).reset_index()
    else:
        by_hour = pd.DataFrame()

    # write reports
    rpt = REPORT_DIR / "entries_v1_baseline_summary.md"
    with open(rpt, "w", encoding="utf-8") as f:
        f.write("# entries_v1 baseline summary\n\n")
        f.write(f"Total rows: {total}\n\n")
        f.write("Outcome counts:\n\n")
        for k, v in outcomes.items():
            f.write(f"- {k}: {v}\n")
        f.write("\nOverall metrics:\n\n")
        f.write(f"- core winrate (TP/(TP+SL)): {core_winrate:.4f}\n")
        f.write(f"- avg pnl_pct: {avg_pnl:.4f}\n")
        f.write(f"- avg r_multiple: {avg_r:.4f}\n")
        f.write(f"- TIMEOUT share: {timeout_share:.4f}\n\n")
        f.write("Average mfe/mae by outcome:\n\n")
        for k, v in avg_mfe_by_outcome.items():
            f.write(f"- {k}: mfe_pct={v:.4f} mae_pct={avg_mae_by_outcome.get(k,'nan'):.4f}\n")

    by_symbol.to_csv(REPORT_DIR / "entries_v1_baseline_by_symbol.csv", index=False)
    if not by_risk.empty:
        by_risk.to_csv(REPORT_DIR / "entries_v1_baseline_by_risk_profile.csv", index=False)
    else:
        pd.DataFrame().to_csv(REPORT_DIR / "entries_v1_baseline_by_risk_profile.csv", index=False)

    print("Summary:")
    print(" total valid rows:", total)
    print(" TP:", tp_count, " SL:", sl_count, " TIMEOUT:", timeout_count)
    print(" core winrate:", core_winrate)
    print(" expectancy avg pnl_pct:", avg_pnl)
    # strongest bucket heuristic: pick context bucket with highest winrate if exists
    strongest = None
    if not by_context.empty:
        by_context["win"] = by_context["tp"] / by_context["total"]
        strongest = by_context.sort_values("win", ascending=False).iloc[0].to_dict()
        print(" strongest context bucket:", strongest.get("context_bucket"), "winrate:", strongest.get("win"))

if __name__ == "__main__":
    main()

