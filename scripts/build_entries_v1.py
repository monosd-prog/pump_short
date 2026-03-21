#!/usr/bin/env python3
"""
Build entries_v1 dataset for ML (QC + filter).

Rules:
- Exclude rows where data_available == False
- Exclude rows where mfe_pct == 0 AND mae_pct == 0 AND outcome == "TIMEOUT"
- Add column is_valid_for_ml (bool)
- Save final entries_v1.parquet (or CSV fallback)
- Write QC report reports/entries_v1_qc.md
"""
from __future__ import annotations

import sys
from pathlib import Path
import json
import os
from typing import List

import pandas as pd

ROOT = Path.cwd()
DATASET_GLOB = ROOT / "datasets"
REPORT_DIR = ROOT / "reports"
OUT_DIR = ROOT / "datasets" / "ml"

def find_outcome_files() -> List[Path]:
    return sorted(DATASET_GLOB.glob("**/outcomes_v3.csv"))

def load_outcomes(paths: List[Path]) -> pd.DataFrame:
    frames = []
    for p in paths:
        try:
            df = pd.read_csv(p, low_memory=False)
            df["__source_file"] = str(p)
            frames.append(df)
        except Exception:
            try:
                df = pd.read_csv(p, header=0, low_memory=False, dtype=str)
                df["__source_file"] = str(p)
                frames.append(df)
            except Exception as e:
                print(f"WARN: failed to read {p}: {e}", file=sys.stderr)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True, sort=False)
    return out

def main():
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    paths = find_outcome_files()
    if not paths:
        print("No outcomes_v3.csv files found under datasets/")
        sys.exit(0)
    df = load_outcomes(paths)
    if df.empty:
        print("No outcome rows loaded.")
        sys.exit(0)

    # ensure numeric mfe/mae
    for col in ("mfe_pct", "mae_pct"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
        else:
            df[col] = 0.0

    # data_available default True if missing
    df["data_available"] = df.get("data_available", True)
    df["data_available"] = df["data_available"].astype(bool)

    total_rows = len(df)
    filtered_no_data_mask = df["data_available"] == False
    filtered_no_data = int(filtered_no_data_mask.sum())

    timeout_mask = df.get("outcome", "").astype(str).str.upper() == "TIMEOUT"
    zero_mfe_mae_mask = (df["mfe_pct"] == 0.0) & (df["mae_pct"] == 0.0)
    filtered_zero_path_mask = timeout_mask & zero_mfe_mae_mask
    filtered_zero_path = int(filtered_zero_path_mask.sum())

    # is_valid_for_ml = not (no_data OR zero_path_timeout)
    df["is_valid_for_ml"] = (~filtered_no_data_mask) & (~filtered_zero_path_mask)

    final_rows_for_ml = int(df["is_valid_for_ml"].sum())

    # Save entries_v1 (parquet preferred)
    out_path_parquet = OUT_DIR / "entries_v1.parquet"
    out_path_csv = OUT_DIR / "entries_v1.csv"
    df_ml = df[df["is_valid_for_ml"]].copy()
    # select useful columns for entries_v1 (identity/timing/prices/outcome/features/meta)
    cols_keep = [
        "trade_id", "event_id", "run_id", "strategy", "symbol", "side",
        "opened_ts", "outcome_time_utc", "entry", "tp", "sl", "exit_price",
        "outcome", "pnl_pct", "r_multiple", "mfe_pct", "mae_pct",
        "is_valid_for_ml", "data_available", "__source_file"
    ]
    cols_exist = [c for c in cols_keep if c in df_ml.columns]
    df_out = df_ml[cols_exist].copy()

    wrote_parquet = False
    try:
        df_out.to_parquet(out_path_parquet, index=False)
        wrote_parquet = True
    except Exception as e:
        print(f"WARN: parquet write failed ({e}), falling back to CSV", file=sys.stderr)
        df_out.to_csv(out_path_csv, index=False)

    # QC report
    qc_path = REPORT_DIR / "entries_v1_qc.md"
    with open(qc_path, "w", encoding="utf-8") as f:
        f.write("# entries_v1 QC\n\n")
        f.write(f"- total_rows: {total_rows}\n")
        f.write(f"- filtered_no_data (data_available==False): {filtered_no_data}\n")
        f.write(f"- filtered_zero_path (mfe==0 && mae==0 && outcome==TIMEOUT): {filtered_zero_path}\n")
        f.write(f"- final_rows_for_ml: {final_rows_for_ml}\n\n")
        f.write("## Sample excluded (no_data)\n\n")
        sample_no_data = df[filtered_no_data_mask].head(10)
        if not sample_no_data.empty:
            f.write(sample_no_data[["trade_id", "event_id", "symbol", "outcome_time_utc"]].to_csv(index=False))
        f.write("\n## Sample excluded (zero_path_timeout)\n\n")
        sample_zero = df[filtered_zero_path_mask].head(10)
        if not sample_zero.empty:
            f.write(sample_zero[["trade_id", "event_id", "symbol", "outcome_time_utc"]].to_csv(index=False))
        f.write("\n")

    print("entries_v1 build complete.")
    print("total_rows:", total_rows)
    print("filtered_no_data:", filtered_no_data)
    print("filtered_zero_path:", filtered_zero_path)
    print("final_rows_for_ml:", final_rows_for_ml)
    print("qc report:", qc_path)
    if wrote_parquet:
        print("parquet:", out_path_parquet)
    else:
        print("csv:", out_path_csv)

if __name__ == "__main__":
    main()

