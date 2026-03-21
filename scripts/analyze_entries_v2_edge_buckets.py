#!/usr/bin/env python3
"""
Analyze profitable / loss-making buckets in entries_v2.
"""
from __future__ import annotations

import math
from pathlib import Path
from typing import Sequence

import pandas as pd

ROOT = Path.cwd()
DATASET_PARQUET = ROOT / "datasets" / "ml" / "entries_v2.parquet"
DATASET_CSV = ROOT / "datasets" / "ml" / "entries_v2.csv"
REPORT_DIR = ROOT / "reports"
REPORT_DIR.mkdir(parents=True, exist_ok=True)

OUT_MD = REPORT_DIR / "entries_v2_edge_buckets_summary.md"
OUT_CSV = REPORT_DIR / "entries_v2_edge_buckets.csv"
COUNT_THRESHOLD_DEFAULT = 20
Q_BUCKETS_DEFAULT = 5


def load_entries() -> pd.DataFrame:
    if DATASET_PARQUET.exists():
        return pd.read_parquet(DATASET_PARQUET)
    if DATASET_CSV.exists():
        return pd.read_csv(DATASET_CSV, low_memory=False)
    raise FileNotFoundError("No entries_v2 dataset found (parquet or csv)")


def safe_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def pick_outcome_label(df: pd.DataFrame) -> pd.Series:
    outcome = df.get("outcome", pd.Series([""] * len(df), index=df.index)).astype(str).str.strip()
    return outcome.replace({"TP": "TP_hit", "SL": "SL_hit"})


def core_expectancy(tp: int, sl: int) -> float:
    total = tp + sl
    return float("nan") if total <= 0 else (tp - sl) / total


def winrate(tp: int, sl: int) -> float:
    total = tp + sl
    return float("nan") if total <= 0 else tp / total


def bucket_qcut(series: pd.Series, q: int = Q_BUCKETS_DEFAULT) -> pd.Series:
    numeric = safe_numeric(series)
    if numeric.dropna().empty:
        return pd.Series(["NA"] * len(series), index=series.index)
    try:
        return pd.qcut(numeric.rank(method="first"), q=q, duplicates="drop").astype(str).fillna("NA")
    except Exception:
        return pd.Series(["NA"] * len(series), index=series.index)


def bucket_cut(series: pd.Series, bins: Sequence[float], labels: Sequence[str]) -> pd.Series:
    numeric = safe_numeric(series)
    try:
        return pd.cut(numeric, bins=bins, labels=labels, include_lowest=True, right=False).astype(str).fillna("NA")
    except Exception:
        return pd.Series(["NA"] * len(series), index=series.index)


def add_bucket_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "context_score" in out.columns:
        out["context_score_bucket"] = bucket_qcut(out["context_score"], q=5)
    if "opened_ts" in out.columns:
        opened = pd.to_datetime(out["opened_ts"], errors="coerce", utc=True)
        out["hour_of_day"] = opened.dt.hour.astype("Int64")
        out["hour_of_day_bucket"] = out["hour_of_day"].fillna(-1).astype(int).astype(str)
    range_specs = [
        ("dist_to_peak_pct", [-math.inf, -5, -2, -1, -0.5, 0, 0.5, 1, 2, 5, math.inf]),
        ("oi_change_1m_pct", [-math.inf, -10, -5, -2, -1, 0, 1, 2, 5, 10, math.inf]),
        ("oi_change_5m_pct", [-math.inf, -10, -5, -2, -1, 0, 1, 2, 5, 10, math.inf]),
        ("oi_change_fast_pct", [-math.inf, -10, -5, -2, -1, 0, 1, 2, 5, 10, math.inf]),
        ("funding_rate_abs", [0, 0.00001, 0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.002, math.inf]),
        ("liq_long_usd_30s", [-math.inf, 0, 1000, 5000, 10000, 25000, 50000, 100000, math.inf]),
        ("liq_short_usd_30s", [-math.inf, 0, 1000, 5000, 10000, 25000, 50000, 100000, math.inf]),
        ("cvd_delta_ratio_1m", [-math.inf, -1, -0.5, -0.2, 0, 0.2, 0.5, 1, math.inf]),
        ("cvd_delta_ratio_30s", [-math.inf, -1, -0.5, -0.2, 0, 0.2, 0.5, 1, math.inf]),
        ("delta_ratio_1m", [-math.inf, -1, -0.5, -0.2, 0, 0.2, 0.5, 1, math.inf]),
        ("delta_ratio_30s", [-math.inf, -1, -0.5, -0.2, 0, 0.2, 0.5, 1, math.inf]),
    ]
    for col, bins in range_specs:
        if col in out.columns:
            labels = []
            for left, right in zip(bins[:-1], bins[1:]):
                l = "-inf" if left == -math.inf else f"{left:g}"
                r = "inf" if right == math.inf else f"{right:g}"
                labels.append(f"[{l}, {r})")
            out[f"{col}_bucket"] = bucket_cut(out[col], bins=bins, labels=labels)
    return out


def aggregate_bucket(df: pd.DataFrame, group_cols: Sequence[str], bucket_name: str, threshold: int) -> pd.DataFrame:
    grouped = (
        df.groupby(list(group_cols), dropna=False)
        .agg(
            count=("outcome", "size"),
            tp=("outcome_norm", lambda s: (s == "TP_hit").sum()),
            sl=("outcome_norm", lambda s: (s == "SL_hit").sum()),
            timeout=("outcome_norm", lambda s: (s == "TIMEOUT").sum()),
            avg_pnl_pct=("pnl_pct", lambda s: pd.to_numeric(s, errors="coerce").mean()),
            avg_r_multiple=("r_multiple", lambda s: pd.to_numeric(s, errors="coerce").mean()),
        )
        .reset_index()
    )
    grouped["winrate_core"] = grouped.apply(lambda r: winrate(int(r["tp"]), int(r["sl"])), axis=1)
    grouped["expectancy_pnl_pct"] = grouped["avg_pnl_pct"]
    grouped["expectancy"] = grouped.apply(lambda r: core_expectancy(int(r["tp"]), int(r["sl"])), axis=1)
    grouped["bucket_family"] = bucket_name
    grouped["bucket_key"] = grouped[group_cols].astype(str).agg(" | ".join, axis=1)
    return grouped[grouped["count"] >= threshold].copy()


def table_md(frame: pd.DataFrame, cols: Sequence[str]) -> str:
    if frame.empty:
        return "- None\n"
    lines = ["| " + " | ".join(cols) + " |", "| " + " | ".join(["---"] * len(cols)) + " |"]
    for _, row in frame[cols].iterrows():
        vals = []
        for c in cols:
            v = row[c]
            if pd.isna(v):
                vals.append("")
            elif isinstance(v, float):
                vals.append(f"{v:.6f}")
            else:
                vals.append(str(v))
        lines.append("| " + " | ".join(vals) + " |")
    return "\n".join(lines) + "\n"


def main() -> None:
    df = load_entries().copy()
    df["outcome_norm"] = pick_outcome_label(df)
    df["pnl_pct"] = safe_numeric(df.get("pnl_pct", 0.0))
    df["r_multiple"] = safe_numeric(df.get("r_multiple", 0.0))
    df = add_bucket_columns(df)

    analyses: list[pd.DataFrame] = []
    missing: list[str] = []
    simple_specs = [
        ("risk_profile", ["risk_profile"]),
        ("symbol", ["symbol"]),
        ("context_score bucket", ["context_score_bucket"]),
        ("dist_to_peak_pct bucket", ["dist_to_peak_pct_bucket"]),
        ("oi_change_1m_pct bucket", ["oi_change_1m_pct_bucket"]),
        ("oi_change_5m_pct bucket", ["oi_change_5m_pct_bucket"]),
        ("oi_change_fast_pct bucket", ["oi_change_fast_pct_bucket"]),
        ("funding_rate_abs bucket", ["funding_rate_abs_bucket"]),
        ("liq_long_usd_30s bucket", ["liq_long_usd_30s_bucket"]),
        ("liq_short_usd_30s bucket", ["liq_short_usd_30s_bucket"]),
        ("cvd_delta_ratio_1m bucket", ["cvd_delta_ratio_1m_bucket"]),
        ("cvd_delta_ratio_30s bucket", ["cvd_delta_ratio_30s_bucket"]),
        ("delta_ratio_1m bucket", ["delta_ratio_1m_bucket"]),
        ("delta_ratio_30s bucket", ["delta_ratio_30s_bucket"]),
        ("hour_of_day", ["hour_of_day_bucket"]),
    ]
    for name, cols in simple_specs:
        if all(c in df.columns for c in cols):
            analyses.append(aggregate_bucket(df, cols, name, COUNT_THRESHOLD_DEFAULT))
        else:
            missing.append(name)

    combo_specs = [
        ("risk_profile + context_score bucket", ["risk_profile", "context_score_bucket"]),
        ("risk_profile + dist_to_peak_pct bucket", ["risk_profile", "dist_to_peak_pct_bucket"]),
        ("context_score + oi_change_1m_pct bucket", ["context_score_bucket", "oi_change_1m_pct_bucket"]),
        ("context_score + funding_rate_abs bucket", ["context_score_bucket", "funding_rate_abs_bucket"]),
        ("dist_to_peak_pct + oi_change_fast_pct bucket", ["dist_to_peak_pct_bucket", "oi_change_fast_pct_bucket"]),
    ]
    for name, cols in combo_specs:
        if all(c in df.columns for c in cols):
            analyses.append(aggregate_bucket(df, cols, name, COUNT_THRESHOLD_DEFAULT))
        else:
            missing.append(name)

    result = pd.concat(analyses, ignore_index=True, sort=False) if analyses else pd.DataFrame()
    if not result.empty:
        result = result.sort_values(["expectancy", "count"], ascending=[False, False], na_position="last").reset_index(drop=True)
    result.to_csv(OUT_CSV, index=False)

    top_positive = result[result["expectancy"] > 0].sort_values(["expectancy", "count"], ascending=[False, False]) if not result.empty else result
    top_negative = result[result["expectancy"] < 0].sort_values(["expectancy", "count"], ascending=[True, False]) if not result.empty else result
    stable_positive = result[(result["count"] >= COUNT_THRESHOLD_DEFAULT) & (result["expectancy"] > 0)] if not result.empty else result

    with open(OUT_MD, "w", encoding="utf-8") as f:
        f.write("# entries_v2 edge buckets summary\n\n")
        f.write(f"- Total rows analyzed: {len(df)}\n")
        f.write(f"- Rows with count threshold >= {COUNT_THRESHOLD_DEFAULT}: {len(result)}\n")
        f.write(f"- Distinct bucket slices: {result['bucket_key'].nunique() if not result.empty else 0}\n\n")
        f.write("## Missing columns\n\n")
        if missing:
            for item in missing:
                f.write(f"- {item}\n")
        else:
            f.write("- None\n")
        f.write("\n## Top profitable buckets\n\n")
        f.write(table_md(top_positive.head(10), ["bucket_family", "bucket_key", "count", "tp", "sl", "timeout", "winrate_core", "expectancy_pnl_pct", "avg_r_multiple"]))
        f.write("\n## Top loss-making buckets\n\n")
        f.write(table_md(top_negative.head(10), ["bucket_family", "bucket_key", "count", "tp", "sl", "timeout", "winrate_core", "expectancy_pnl_pct", "avg_r_multiple"]))
        f.write("\n## Stability note\n\n")
        if stable_positive.empty:
            f.write("No positive-expectancy segments met the minimum count threshold.\n")
        else:
            f.write(f"Positive-expectancy segments exist: {len(stable_positive)} slices have expectancy > 0 with count >= {COUNT_THRESHOLD_DEFAULT}.\n")

    print(f"Wrote: {OUT_CSV}")
    print(f"Wrote: {OUT_MD}")
    print("\nTop 10 profitable buckets with count >= 20:")
    if top_positive.empty:
        print("  None")
    else:
        print(top_positive.head(10)[["bucket_family", "bucket_key", "count", "winrate_core", "expectancy_pnl_pct", "avg_r_multiple"]].to_string(index=False))
    print("\nTop 10 worst buckets with count >= 20:")
    if top_negative.empty:
        print("  None")
    else:
        print(top_negative.head(10)[["bucket_family", "bucket_key", "count", "winrate_core", "expectancy_pnl_pct", "avg_r_multiple"]].to_string(index=False))
    print("\nStable positive-expectancy segments:", "yes" if not stable_positive.empty else "no")

    if not result.empty:
        top_features = []
        for feat in ["context_score bucket", "dist_to_peak_pct bucket", "oi_change_1m_pct bucket", "oi_change_fast_pct bucket", "funding_rate_abs bucket", "risk_profile"]:
            sub = result[result["bucket_family"] == feat]
            if sub.empty:
                continue
            best = sub.sort_values(["expectancy", "count"], ascending=[False, False]).iloc[0]
            top_features.append((feat, float(best["expectancy"]), int(best["count"])))
        top_features = sorted(top_features, key=lambda x: (x[1], x[2]), reverse=True)[:3]
        print("\nMost useful features for future ML/filter:")
        for feat, exp, cnt in top_features:
            print(f"  - {feat}: best_expectancy={exp:.6f}, count={cnt}")


if __name__ == "__main__":
    main()
