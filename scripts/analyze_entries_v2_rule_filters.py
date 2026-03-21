#!/usr/bin/env python3
"""
Evaluate simple rule-based filters on entries_v2.
"""
from __future__ import annotations

import math
from pathlib import Path

import pandas as pd

ROOT = Path.cwd()
DATASET_PARQUET = ROOT / "datasets" / "ml" / "entries_v2.parquet"
DATASET_CSV = ROOT / "datasets" / "ml" / "entries_v2.csv"
REPORT_DIR = ROOT / "reports"
REPORT_DIR.mkdir(parents=True, exist_ok=True)

OUT_MD = REPORT_DIR / "entries_v2_rule_filters_summary.md"
OUT_CSV = REPORT_DIR / "entries_v2_rule_filters.csv"


def load_entries() -> pd.DataFrame:
    if DATASET_PARQUET.exists():
        return pd.read_parquet(DATASET_PARQUET)
    if DATASET_CSV.exists():
        return pd.read_csv(DATASET_CSV, low_memory=False)
    raise FileNotFoundError("No entries_v2 dataset found (parquet or csv)")


def safe_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def outcome_stats(df: pd.DataFrame) -> dict:
    o = df.get("outcome", pd.Series([], dtype=str)).astype(str).str.strip()
    tp = int((o == "TP_hit").sum())
    sl = int((o == "SL_hit").sum())
    timeout = int((o == "TIMEOUT").sum())
    total = len(df)
    core = tp + sl
    winrate = tp / core if core > 0 else float("nan")
    expectancy = (tp - sl) / core if core > 0 else float("nan")
    return {
        "rows_kept": total,
        "tp": tp,
        "sl": sl,
        "timeout": timeout,
        "core_winrate": winrate,
        "expectancy_pnl_pct": float(df["pnl_pct"].mean()) if "pnl_pct" in df.columns and total else float("nan"),
        "expectancy_core": expectancy,
    }


def bucket_qcut(series: pd.Series, q: int = 5) -> pd.Series:
    numeric = safe_numeric(series)
    if numeric.dropna().empty:
        return pd.Series(["NA"] * len(series), index=series.index)
    try:
        return pd.qcut(numeric.rank(method="first"), q=q, duplicates="drop").astype(str).fillna("NA")
    except Exception:
        return pd.Series(["NA"] * len(series), index=series.index)


def bucket_cut(series: pd.Series, bins, labels) -> pd.Series:
    numeric = safe_numeric(series)
    try:
        return pd.cut(numeric, bins=bins, labels=labels, include_lowest=True, right=False).astype(str).fillna("NA")
    except Exception:
        return pd.Series(["NA"] * len(series), index=series.index)


def baseline_metrics(df: pd.DataFrame) -> dict:
    stats = outcome_stats(df)
    stats["rule"] = "baseline"
    stats["keep_rate"] = 1.0
    stats["delta_vs_baseline"] = 0.0
    return stats


def rule_metrics(df: pd.DataFrame, rule_name: str, mask: pd.Series, baseline_expectancy: float) -> dict:
    kept = df[mask].copy()
    stats = outcome_stats(kept)
    stats["rule"] = rule_name
    stats["keep_rate"] = len(kept) / len(df) if len(df) else float("nan")
    stats["delta_vs_baseline"] = stats["expectancy_pnl_pct"] - baseline_expectancy
    return stats


def robustness_score(early: dict, late: dict) -> float:
    if any(math.isnan(x) for x in (early["expectancy_pnl_pct"], late["expectancy_pnl_pct"])):
        return float("nan")
    if early["rows_kept"] == 0 or late["rows_kept"] == 0:
        return float("nan")
    return min(early["expectancy_pnl_pct"], late["expectancy_pnl_pct"])


def main() -> None:
    df = load_entries().copy()
    df["pnl_pct"] = safe_numeric(df.get("pnl_pct", 0.0))
    df["opened_ts"] = pd.to_datetime(df.get("opened_ts"), errors="coerce", utc=True)
    df = df.sort_values("opened_ts", na_position="last").reset_index(drop=True)
    df["hour_of_day"] = df["opened_ts"].dt.hour

    df["context_score_bucket"] = bucket_qcut(df.get("context_score", pd.Series([pd.NA] * len(df), index=df.index)))
    df["dist_to_peak_pct_bucket"] = bucket_cut(
        df.get("dist_to_peak_pct", pd.Series([pd.NA] * len(df), index=df.index)),
        bins=[-math.inf, 0, 0.5, 1, 2, 5, math.inf],
        labels=["[-inf, 0)", "[0, 0.5)", "[0.5, 1)", "[1, 2)", "[2, 5)", "[5, inf)"],
    )
    df["oi_change_1m_pct_bucket"] = bucket_cut(
        df.get("oi_change_1m_pct", pd.Series([pd.NA] * len(df), index=df.index)),
        bins=[-math.inf, -5, -2, -1, 0, 1, 2, 5, math.inf],
        labels=["[-inf, -5)", "[-5, -2)", "[-2, -1)", "[-1, 0)", "[0, 1)", "[1, 2)", "[2, 5)", "[5, inf)"],
    )
    df["funding_rate_abs_bucket"] = bucket_cut(
        df.get("funding_rate_abs", pd.Series([pd.NA] * len(df), index=df.index)),
        bins=[0, 0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.002, math.inf],
        labels=["[0, 5e-05)", "[5e-05, 0.0001)", "[0.0001, 0.00025)", "[0.00025, 0.0005)", "[0.0005, 0.001)", "[0.001, 0.002)", "[0.002, inf)"],
    )

    baseline = baseline_metrics(df)
    baseline_expectancy = baseline["expectancy_pnl_pct"]

    # Best buckets from prior edge analysis.
    best_oi_bucket = "[1, 2)"
    best_funding_bucket = "[0.0005, 0.001)"
    worst_dist_bucket = "[5, inf)"

    rules = [
        ("A_dist_to_peak_lt_1", df["dist_to_peak_pct"].lt(1)),
        ("B_dist_to_peak_lt_2", df["dist_to_peak_pct"].lt(2)),
        ("C_exclude_dist_to_peak_ge_5", ~df["dist_to_peak_pct"].ge(5)),
        ("D_exclude_symbol_PYRUSDT", df["symbol"].ne("PYRUSDT")),
        (
            "E_dist_to_peak_lt_1_and_best_oi_bucket",
            df["dist_to_peak_pct"].lt(1) & (df["oi_change_1m_pct_bucket"] == best_oi_bucket),
        ),
        (
            "F_dist_to_peak_lt_1_and_best_funding_bucket",
            df["dist_to_peak_pct"].lt(1) & (df["funding_rate_abs_bucket"] == best_funding_bucket),
        ),
        (
            "G_exclude_worst_dist_bucket",
            df["dist_to_peak_pct_bucket"].ne(worst_dist_bucket),
        ),
    ]

    rows = [baseline]
    for rule_name, mask in rules:
        rows.append(rule_metrics(df, rule_name, mask.fillna(False), baseline_expectancy))

    result = pd.DataFrame(rows)

    # Early/late split robustness.
    split_idx = len(df) // 2
    early = df.iloc[:split_idx].copy()
    late = df.iloc[split_idx:].copy()

    robust_rows = []
    for rule_name, mask in [("baseline", pd.Series(True, index=df.index)), *rules]:
        if rule_name == "baseline":
            early_stats = baseline_metrics(early)
            late_stats = baseline_metrics(late)
        else:
            raw_mask = mask.fillna(False)
            early_stats = outcome_stats(early[raw_mask.iloc[:split_idx].values])
            late_stats = outcome_stats(late[raw_mask.iloc[split_idx:].values])
            early_stats["keep_rate"] = len(early[raw_mask.iloc[:split_idx].values]) / len(early) if len(early) else float("nan")
            late_stats["keep_rate"] = len(late[raw_mask.iloc[split_idx:].values]) / len(late) if len(late) else float("nan")
        robust_rows.append(
            {
                "rule": rule_name,
                "early_expectancy_pnl_pct": early_stats["expectancy_pnl_pct"],
                "late_expectancy_pnl_pct": late_stats["expectancy_pnl_pct"],
                "robustness_score": robustness_score(early_stats, late_stats),
            }
        )

    robustness = pd.DataFrame(robust_rows)
    result = result.merge(robustness, on="rule", how="left")
    result["delta_vs_baseline"] = result["expectancy_pnl_pct"] - baseline_expectancy

    result.to_csv(OUT_CSV, index=False)

    def md_table(frame: pd.DataFrame, cols: list[str]) -> str:
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

    with open(OUT_MD, "w", encoding="utf-8") as f:
        f.write("# entries_v2 rule filters summary\n\n")
        f.write(f"- Baseline expectancy_pnl_pct: {baseline_expectancy:.6f}\n")
        f.write(f"- Baseline rows: {len(df)}\n")
        f.write(f"- Early sample rows: {len(early)}\n")
        f.write(f"- Late sample rows: {len(late)}\n\n")
        f.write("## Rule Metrics\n\n")
        f.write(md_table(result, ["rule", "rows_kept", "keep_rate", "tp", "sl", "timeout", "core_winrate", "expectancy_pnl_pct", "delta_vs_baseline", "early_expectancy_pnl_pct", "late_expectancy_pnl_pct", "robustness_score"]))

    top_expectancy = result[result["rule"] != "baseline"].sort_values(["expectancy_pnl_pct", "keep_rate"], ascending=[False, False]).head(5)
    top_robustness = result[result["rule"] != "baseline"].sort_values(["robustness_score", "expectancy_pnl_pct"], ascending=[False, False]).head(5)

    print(f"Wrote: {OUT_CSV}")
    print(f"Wrote: {OUT_MD}")
    print("\nTop 5 rules by expectancy:")
    print(top_expectancy[["rule", "rows_kept", "keep_rate", "expectancy_pnl_pct", "delta_vs_baseline"]].to_string(index=False))
    print("\nTop 5 rules by robustness:")
    print(top_robustness[["rule", "rows_kept", "keep_rate", "early_expectancy_pnl_pct", "late_expectancy_pnl_pct", "robustness_score"]].to_string(index=False))
    shadow = result[(result["rule"] != "baseline") & (result["expectancy_pnl_pct"] > baseline_expectancy) & (result["robustness_score"].notna()) & (result["robustness_score"] > 0)]
    print("\nShadow-mode candidates:", "yes" if not shadow.empty else "no")
    if not shadow.empty:
        print(shadow.sort_values(["expectancy_pnl_pct", "robustness_score"], ascending=[False, False]).head(2)[["rule", "expectancy_pnl_pct", "robustness_score"]].to_string(index=False))


if __name__ == "__main__":
    main()
