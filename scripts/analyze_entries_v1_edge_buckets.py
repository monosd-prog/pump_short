#!/usr/bin/env python3
"""
Analyze profitable / loss-making buckets in entries_v1.

The script is intentionally defensive: it analyzes only columns that are
available in the dataset and records skipped slices in the report.
"""
from __future__ import annotations

import math
from pathlib import Path
from typing import Iterable, List, Sequence

import pandas as pd

ROOT = Path.cwd()
DATASET_PARQUET = ROOT / "datasets" / "ml" / "entries_v1.parquet"
DATASET_CSV = ROOT / "datasets" / "ml" / "entries_v1.csv"
REPORT_DIR = ROOT / "reports"
REPORT_DIR.mkdir(parents=True, exist_ok=True)

OUT_MD = REPORT_DIR / "entries_v1_edge_buckets_summary.md"
OUT_CSV = REPORT_DIR / "entries_v1_edge_buckets.csv"

COUNT_THRESHOLD_DEFAULT = 20
Q_BUCKETS_DEFAULT = 5


def load_entries() -> pd.DataFrame:
    if DATASET_PARQUET.exists():
        return pd.read_parquet(DATASET_PARQUET)
    if DATASET_CSV.exists():
        return pd.read_csv(DATASET_CSV, low_memory=False)
    raise FileNotFoundError("No entries_v1 dataset found (parquet or csv)")


def safe_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def pick_outcome_label(df: pd.DataFrame) -> pd.Series:
    outcome = df.get("outcome", pd.Series([""] * len(df), index=df.index)).astype(str).str.strip()
    mapped = outcome.replace(
        {
            "TP": "TP_hit",
            "SL": "SL_hit",
            "TIMEOUT": "TIMEOUT",
            "UNKNOWN_NO_DATA": "UNKNOWN_NO_DATA",
        }
    )
    return mapped


def core_expectancy(tp: int, sl: int) -> float:
    total = tp + sl
    if total <= 0:
        return float("nan")
    return (tp - sl) / total


def winrate(tp: int, sl: int) -> float:
    total = tp + sl
    if total <= 0:
        return float("nan")
    return tp / total


def format_interval(series: pd.Series) -> pd.Series:
    if pd.api.types.is_interval_dtype(series):
        return series.astype(str)
    return series.astype(str)


def bucket_qcut(series: pd.Series, q: int = Q_BUCKETS_DEFAULT) -> pd.Series:
    numeric = safe_numeric(series)
    valid = numeric.dropna()
    if valid.empty:
        return pd.Series(["NA"] * len(series), index=series.index)
    try:
        buckets = pd.qcut(numeric.rank(method="first"), q=q, duplicates="drop")
        return buckets.astype(str).fillna("NA")
    except Exception:
        return pd.Series(["NA"] * len(series), index=series.index)


def bucket_cut(series: pd.Series, bins: Sequence[float], labels: Sequence[str]) -> pd.Series:
    numeric = safe_numeric(series)
    try:
        buckets = pd.cut(numeric, bins=bins, labels=labels, include_lowest=True, right=False)
        return buckets.astype(str).fillna("NA")
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
        ("dist_to_tp_pct", [-math.inf, -5, -2, -1, -0.5, 0, 0.5, 1, 2, 5, math.inf]),
        ("dist_to_sl_pct", [-math.inf, -5, -2, -1, -0.5, 0, 0.5, 1, 2, 5, math.inf]),
        ("oi_change_1m_pct", [-math.inf, -10, -5, -2, -1, 0, 1, 2, 5, 10, math.inf]),
        ("oi_change_5m_pct", [-math.inf, -10, -5, -2, -1, 0, 1, 2, 5, 10, math.inf]),
        ("funding_rate_abs", [0, 0.00001, 0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.002, math.inf]),
        ("liq_long_usd_30s", [-math.inf, 0, 1000, 5000, 10000, 25000, 50000, 100000, math.inf]),
        ("liq_short_usd_30s", [-math.inf, 0, 1000, 5000, 10000, 25000, 50000, 100000, math.inf]),
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


def aggregate_bucket(
    df: pd.DataFrame,
    group_cols: Sequence[str],
    bucket_name: str,
    source_cols: Sequence[str],
    threshold: int,
) -> pd.DataFrame:
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
    grouped["winrate"] = grouped.apply(lambda r: winrate(int(r["tp"]), int(r["sl"])), axis=1)
    grouped["expectancy"] = grouped.apply(lambda r: core_expectancy(int(r["tp"]), int(r["sl"])), axis=1)
    grouped["expectancy_per_trade_pnl_pct"] = grouped["avg_pnl_pct"]
    grouped["source_cols"] = ", ".join(source_cols)
    grouped["bucket_family"] = bucket_name
    grouped = grouped[grouped["count"] >= threshold].copy()
    return grouped


def write_markdown(
    df_all: pd.DataFrame,
    df_filtered: pd.DataFrame,
    unavailable: List[str],
    top_pos: pd.DataFrame,
    top_neg: pd.DataFrame,
) -> None:
    def table_md(frame: pd.DataFrame, cols: Sequence[str]) -> str:
        if frame.empty:
            return "- None\n"
        lines = []
        header = "| " + " | ".join(cols) + " |"
        sep = "| " + " | ".join(["---"] * len(cols)) + " |"
        lines.append(header)
        lines.append(sep)
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

    with open(OUT_MD, "w", encoding="utf-8") as f:
        f.write("# entries_v1 edge buckets summary\n\n")
        f.write(f"- Total rows analyzed: {len(df_all)}\n")
        f.write(f"- Rows with count threshold >= {COUNT_THRESHOLD_DEFAULT}: {len(df_filtered)}\n")
        f.write(f"- Distinct bucket slices: {df_filtered['bucket_key'].nunique() if not df_filtered.empty else 0}\n")
        f.write("\n## Missing columns\n\n")
        if unavailable:
            for item in unavailable:
                f.write(f"- {item}\n")
        else:
            f.write("- None\n")
        f.write("\n## Top profitable buckets\n\n")
        f.write(table_md(top_pos.head(10), ["bucket_family", "bucket_key", "count", "tp", "sl", "winrate", "expectancy", "avg_pnl_pct"]))
        f.write("\n## Top loss-making buckets\n\n")
        f.write(table_md(top_neg.head(10), ["bucket_family", "bucket_key", "count", "tp", "sl", "winrate", "expectancy", "avg_pnl_pct"]))

        f.write("\n## Stability note\n\n")
        stable_positive = df_filtered[(df_filtered["count"] >= COUNT_THRESHOLD_DEFAULT) & (df_filtered["expectancy"] > 0)].sort_values(
            ["expectancy", "count"], ascending=[False, False]
        )
        if stable_positive.empty:
            f.write("No positive-expectancy segments met the minimum count threshold.\n")
        else:
            f.write(
                f"Positive-expectancy segments exist: {len(stable_positive)} slices have expectancy > 0 with count >= {COUNT_THRESHOLD_DEFAULT}.\n"
            )
            f.write("The strongest ones are listed above; use the CSV for deeper inspection.\n")


def main() -> None:
    df = load_entries()
    df = df.copy()
    df["outcome_norm"] = pick_outcome_label(df)
    if "pnl_pct" in df.columns:
        df["pnl_pct"] = safe_numeric(df["pnl_pct"])
    else:
        df["pnl_pct"] = 0.0
    if "r_multiple" in df.columns:
        df["r_multiple"] = safe_numeric(df["r_multiple"])
    else:
        df["r_multiple"] = 0.0
    df = add_bucket_columns(df)

    analyses: list[pd.DataFrame] = []
    unavailable: list[str] = []

    simple_specs = [
        ("strategy", ["strategy"], ["strategy"]),
        ("risk_profile", ["risk_profile"], ["risk_profile"]),
        ("symbol", ["symbol"], ["symbol"]),
        ("context_score bucket", ["context_score_bucket"], ["context_score"]),
        ("hour-of-day", ["hour_of_day_bucket"], ["opened_ts"]),
        ("dist_to_tp_pct bucket", ["dist_to_tp_pct_bucket"], ["dist_to_tp_pct"]),
        ("dist_to_sl_pct bucket", ["dist_to_sl_pct_bucket"], ["dist_to_sl_pct"]),
        ("oi_change_1m_pct bucket", ["oi_change_1m_pct_bucket"], ["oi_change_1m_pct"]),
        ("oi_change_5m_pct bucket", ["oi_change_5m_pct_bucket"], ["oi_change_5m_pct"]),
        ("funding_rate_abs bucket", ["funding_rate_abs_bucket"], ["funding_rate_abs"]),
        ("liq_long_usd_30s bucket", ["liq_long_usd_30s_bucket"], ["liq_long_usd_30s"]),
        ("liq_short_usd_30s bucket", ["liq_short_usd_30s_bucket"], ["liq_short_usd_30s"]),
    ]

    for name, group_cols, source_cols in simple_specs:
        if all(col in df.columns for col in group_cols):
            agg = aggregate_bucket(df, group_cols, name, source_cols, COUNT_THRESHOLD_DEFAULT)
            if not agg.empty:
                agg["bucket_key"] = agg[group_cols].astype(str).agg(" | ".join, axis=1)
                analyses.append(agg)
        else:
            unavailable.append(name)

    combo_specs = [
        ("risk_profile + context_score bucket", ["risk_profile", "context_score_bucket"], ["risk_profile", "context_score"]),
        ("symbol + hour-of-day", ["symbol", "hour_of_day_bucket"], ["symbol", "opened_ts"]),
        ("context_score + oi_change_1m_pct bucket", ["context_score_bucket", "oi_change_1m_pct_bucket"], ["context_score", "oi_change_1m_pct"]),
    ]

    for name, group_cols, source_cols in combo_specs:
        if all(col in df.columns for col in group_cols):
            agg = aggregate_bucket(df, group_cols, name, source_cols, COUNT_THRESHOLD_DEFAULT)
            if not agg.empty:
                agg["bucket_key"] = agg[group_cols].astype(str).agg(" | ".join, axis=1)
                analyses.append(agg)
        else:
            unavailable.append(name)

    if analyses:
        result = pd.concat(analyses, ignore_index=True, sort=False)
    else:
        result = pd.DataFrame(
            columns=[
                "bucket_family",
                "bucket_key",
                "count",
                "tp",
                "sl",
                "timeout",
                "avg_pnl_pct",
                "avg_r_multiple",
                "winrate",
                "expectancy",
                "expectancy_per_trade_pnl_pct",
                "source_cols",
            ]
        )

    result = result.sort_values(["expectancy", "count"], ascending=[False, False], na_position="last").reset_index(drop=True)
    result.to_csv(OUT_CSV, index=False)

    top_positive = result[result["expectancy"] > 0].sort_values(["expectancy", "count"], ascending=[False, False])
    top_negative = result[result["expectancy"] < 0].sort_values(["expectancy", "count"], ascending=[True, False])

    write_markdown(df, result, unavailable, top_positive, top_negative)

    print(f"Wrote: {OUT_CSV}")
    print(f"Wrote: {OUT_MD}")
    print("\nTop 10 profitable buckets with count >= 20:")
    if top_positive.empty:
        print("  None")
    else:
        print(top_positive.head(10)[["bucket_family", "bucket_key", "count", "winrate", "expectancy"]].to_string(index=False))
    print("\nTop 10 worst buckets with count >= 20:")
    if top_negative.empty:
        print("  None")
    else:
        print(top_negative.head(10)[["bucket_family", "bucket_key", "count", "winrate", "expectancy"]].to_string(index=False))
    stable_positive = result[(result["count"] >= COUNT_THRESHOLD_DEFAULT) & (result["expectancy"] > 0)]
    print("\nStable positive-expectancy segments:", "yes" if not stable_positive.empty else "no")
    if not stable_positive.empty:
        print(f"Count: {len(stable_positive)}")


if __name__ == "__main__":
    main()
