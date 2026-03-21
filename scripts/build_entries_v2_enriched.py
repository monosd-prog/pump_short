#!/usr/bin/env python3
"""
Build entries_v2 enriched dataset for offline edge analysis.

Input:
  - datasets/ml/entries_v1.csv
  - datasets/**/events_v3.csv
  - datasets/**/trades_v3.csv

Output:
  - datasets/ml/entries_v2.csv
  - reports/entries_v2_enrichment_qc.md
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

import pandas as pd

ROOT = Path.cwd()
ML_DIR = ROOT / "datasets" / "ml"
DATASETS_DIR = ROOT / "datasets"
REPORT_DIR = ROOT / "reports"
REPORT_DIR.mkdir(parents=True, exist_ok=True)

INPUT_PATH = ML_DIR / "entries_v1.csv"
OUTPUT_PATH = ML_DIR / "entries_v2.csv"
QC_PATH = REPORT_DIR / "entries_v2_enrichment_qc.md"


def read_csvs(paths: Iterable[Path]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in paths:
        try:
            df = pd.read_csv(path, low_memory=False)
        except Exception:
            continue
        if df.empty:
            continue
        df["__source_file"] = str(path)
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True, sort=False)


def collect_paths(filename: str) -> list[Path]:
    return sorted(DATASETS_DIR.glob(f"**/{filename}"))


def normalize_key_cols(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in ("trade_id", "event_id", "run_id", "strategy", "symbol", "position_id"):
        if col in out.columns:
            out[col] = out[col].astype(str).str.strip()
    return out


def ensure_numeric(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for col in cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def first_non_null(series: pd.Series):
    for value in series:
        if pd.notna(value) and value != "":
            return value
    return pd.NA


def merge_columns(left: pd.DataFrame, right: pd.DataFrame, cols: list[str], suffix: str) -> pd.DataFrame:
    right_cols = [c for c in cols if c in right.columns]
    if not right_cols:
        return left
    renamed = right[right_cols].copy()
    rename_map = {c: f"{c}{suffix}" for c in right_cols if c in left.columns}
    renamed = renamed.rename(columns=rename_map)
    return left.join(renamed, how="left")


def build_aggregated_right(df: pd.DataFrame, key_cols: list[str], value_cols: list[str]) -> pd.DataFrame:
    existing = [c for c in key_cols + value_cols if c in df.columns]
    if not existing:
        return pd.DataFrame()
    use_keys = [c for c in key_cols if c in df.columns]
    use_values = [c for c in value_cols if c in df.columns]
    if not use_keys:
        return pd.DataFrame()
    agg = (
        df[use_keys + use_values]
        .groupby(use_keys, dropna=False)
        .agg({c: first_non_null for c in use_values})
        .reset_index()
    )
    return agg


def main() -> None:
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Missing input dataset: {INPUT_PATH}")

    entries = pd.read_csv(INPUT_PATH, low_memory=False)
    entries = normalize_key_cols(entries)
    entries = ensure_numeric(entries, ["entry", "tp", "sl", "exit_price", "pnl_pct", "mfe_pct", "mae_pct"])

    entries["hour_of_day"] = pd.to_datetime(entries.get("opened_ts"), errors="coerce", utc=True).dt.hour
    entries["dist_to_tp_pct"] = ((entries["tp"] - entries["entry"]) / entries["entry"]) * 100.0
    entries["dist_to_sl_pct"] = ((entries["sl"] - entries["entry"]) / entries["entry"]) * 100.0

    events_paths = collect_paths("events_v3.csv")
    trades_paths = collect_paths("trades_v3.csv")
    events = normalize_key_cols(read_csvs(events_paths))
    trades = normalize_key_cols(read_csvs(trades_paths))

    if not events.empty:
        events = ensure_numeric(
            events,
            [
                "context_score",
                "liq_long_usd_30s",
                "liq_short_usd_30s",
                "oi_change_1m_pct",
                "oi_change_5m_pct",
                "funding_rate_abs",
                "dist_to_peak_pct",
                "oi_change_fast_pct",
                "cvd_delta_ratio_1m",
                "cvd_delta_ratio_30s",
                "delta_ratio_1m",
                "delta_ratio_30s",
                "liq_long_count_30s",
                "liq_short_count_30s",
                "liq_long_count_1m",
                "liq_short_count_1m",
                "volume_1m",
                "volume_5m",
                "volume_sma_20",
                "volume_zscore_20",
                "price_change_30s_pct",
                "price_change_1m_pct",
                "price_change_3m_pct",
                "accel_30s_vs_3m",
                "green_candles_5",
                "max_candle_body_pct_5",
                "avg_candle_body_pct_5",
                "upper_wick_ratio_last",
                "lower_wick_ratio_last",
                "wick_body_ratio_last",
                "volume_ratio_1m_20",
                "volume_ratio_5m_20",
                "time_since_peak_sec",
                "time_since_signal_sec",
                "pump_age_sec",
                "spread_bps",
                "orderbook_imbalance_10",
            ],
        )
        if "funding_rate" in events.columns and "funding_rate_abs" not in events.columns:
            events["funding_rate_abs"] = pd.to_numeric(events["funding_rate"], errors="coerce").abs()
        if "funding_rate_abs" in events.columns and "funding_rate" in events.columns:
            events["funding_rate_abs"] = events["funding_rate_abs"].fillna(pd.to_numeric(events["funding_rate"], errors="coerce").abs())

    if not trades.empty:
        trades = ensure_numeric(
            trades,
            [
                "entry_price",
                "tp_price",
                "sl_price",
                "exit_price",
            ],
        )

    stable_event_cols = [
        "context_score",
        "liq_long_usd_30s",
        "liq_short_usd_30s",
        "oi_change_1m_pct",
        "oi_change_5m_pct",
        "funding_rate_abs",
        "risk_profile",
        "dist_to_peak_pct",
        "oi_change_fast_pct",
        "cvd_delta_ratio_1m",
        "cvd_delta_ratio_30s",
        "delta_ratio_1m",
        "delta_ratio_30s",
        "liq_long_count_30s",
        "liq_short_count_30s",
        "liq_long_count_1m",
        "liq_short_count_1m",
        "volume_1m",
        "volume_5m",
        "volume_sma_20",
        "volume_zscore_20",
        "price_change_30s_pct",
        "price_change_1m_pct",
        "price_change_3m_pct",
        "accel_30s_vs_3m",
        "green_candles_5",
        "max_candle_body_pct_5",
        "avg_candle_body_pct_5",
        "upper_wick_ratio_last",
        "lower_wick_ratio_last",
        "wick_body_ratio_last",
        "volume_ratio_1m_20",
        "volume_ratio_5m_20",
        "time_since_peak_sec",
        "time_since_signal_sec",
        "pump_age_sec",
        "spread_bps",
        "orderbook_imbalance_10",
        "stage",
        "side",
        "entry_ok",
        "skip_reasons",
        "time_utc",
        "wall_time_utc",
        "source_mode",
    ]
    stable_trade_cols = [
        "entry_price",
        "tp_price",
        "sl_price",
        "trade_type",
    ]

    merged = entries.copy()
    matched_primary = pd.Series(False, index=merged.index)
    matched_fallback = pd.Series(False, index=merged.index)

    # Primary join on trade_id when present.
    if not trades.empty and "trade_id" in merged.columns and "trade_id" in trades.columns:
        trade_keys = trades.drop_duplicates(subset=["trade_id"], keep="last").copy()
        trade_cols = [c for c in stable_trade_cols if c in trade_keys.columns]
        trade_cols = ["trade_id"] + trade_cols
        merged = merged.merge(
            trade_keys[trade_cols],
            how="left",
            on="trade_id",
            suffixes=("", "_trade"),
        )
        matched_primary |= merged["trade_type"].notna() if "trade_type" in merged.columns else False

    # Primary join on event_id from events.
    if not events.empty and "event_id" in merged.columns and "event_id" in events.columns:
        event_keys = events.drop_duplicates(subset=["event_id"], keep="last").copy()
        event_cols = [c for c in stable_event_cols if c in event_keys.columns]
        event_cols = ["event_id"] + [c for c in event_cols if c != "event_id"]
        merged = merged.merge(
            event_keys[event_cols],
            how="left",
            on="event_id",
            suffixes=("", "_event"),
        )
        matched_primary |= merged["context_score"].notna() if "context_score" in merged.columns else False

    # Fallback join only for rows still missing key features.
    fallback_needed = pd.Series(False, index=merged.index)
    for c in ("context_score", "risk_profile", "dist_to_peak_pct"):
        if c in merged.columns:
            fallback_needed |= merged[c].isna()

    fallback_cols = [c for c in stable_event_cols if c not in ("stage", "side", "entry_ok", "skip_reasons", "time_utc", "wall_time_utc", "source_mode")]
    if not events.empty and fallback_needed.any():
        fallback_keys = [c for c in ("run_id", "symbol", "strategy") if c in merged.columns and c in events.columns]
        if fallback_keys:
            event_fb = events.drop_duplicates(subset=fallback_keys, keep="last").copy()
            fb_cols = fallback_keys + [c for c in fallback_cols if c in event_fb.columns and c not in fallback_keys]
            fb_cols = list(dict.fromkeys(fb_cols))
            before = merged[fallback_needed].copy()
            tmp = merged.loc[fallback_needed, fallback_keys].merge(
                event_fb[fb_cols],
                how="left",
                on=fallback_keys,
                suffixes=("", "_fb"),
            )
            for col in [c for c in fb_cols if c not in fallback_keys]:
                if col not in merged.columns:
                    merged[col] = pd.NA
                merged.loc[fallback_needed, col] = merged.loc[fallback_needed, col].combine_first(tmp[col])
            matched_fallback |= fallback_needed & tmp[[c for c in fb_cols if c not in fallback_keys]].notna().any(axis=1).reindex(merged.index[fallback_needed], fill_value=False)

    if "risk_profile" not in merged.columns:
        merged["risk_profile"] = pd.NA

    missing_counts = {c: int(merged[c].isna().sum()) for c in merged.columns if c not in ("__source_file",)}
    total_rows = len(merged)
    enriched_rows = int(merged[[c for c in ["context_score", "risk_profile", "dist_to_peak_pct", "oi_change_1m_pct", "oi_change_5m_pct", "funding_rate_abs"] if c in merged.columns]].notna().any(axis=1).sum())
    primary_matches = int(matched_primary.sum())
    fallback_matches = int(matched_fallback.sum())

    merged.to_csv(OUTPUT_PATH, index=False)

    with open(QC_PATH, "w", encoding="utf-8") as f:
        f.write("# entries_v2 enrichment QC\n\n")
        f.write(f"- total_rows: {total_rows}\n")
        f.write(f"- matched_by_primary_key: {primary_matches}\n")
        f.write(f"- matched_by_fallback_key: {fallback_matches}\n")
        f.write(f"- enriched_rows_with_any_key_features: {enriched_rows}\n\n")
        f.write("## Source files\n\n")
        f.write(f"- events_v3 files: {len(events_paths)}\n")
        f.write(f"- trades_v3 files: {len(trades_paths)}\n\n")
        f.write("## Missing feature counts\n\n")
        for col, cnt in sorted(missing_counts.items(), key=lambda kv: kv[0]):
            f.write(f"- {col}: {cnt}\n")

    print("entries_v2 enrichment complete.")
    print("total rows:", total_rows)
    print("matched by primary key:", primary_matches)
    print("matched by fallback key:", fallback_matches)
    print("enriched rows:", enriched_rows)
    print("qc report:", QC_PATH)
    print("output:", OUTPUT_PATH)


if __name__ == "__main__":
    main()
