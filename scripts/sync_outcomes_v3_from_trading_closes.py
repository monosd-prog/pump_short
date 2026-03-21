#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shutil
from pathlib import Path
from typing import Any

import pandas as pd


TARGET_FIELDS = (
    "outcome",
    "exit_price",
    "mfe_pct",
    "mae_pct",
    "pnl_pct",
    "pnl_r",
    "outcome_source",
)


def _read_csv(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path, low_memory=False)
    except Exception:
        return pd.DataFrame()


def _normalize_text(df: pd.DataFrame, cols: tuple[str, ...]) -> pd.DataFrame:
    out = df.copy()
    for col in cols:
        if col not in out.columns:
            out[col] = ""
        out[col] = out[col].fillna("").astype(str).str.strip()
    if "symbol" in out.columns:
        out["symbol"] = out["symbol"].str.upper()
    return out


def _normalize_numeric(df: pd.DataFrame, cols: tuple[str, ...]) -> pd.DataFrame:
    out = df.copy()
    for col in cols:
        if col not in out.columns:
            out[col] = pd.NA
        out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def _same_value(left: Any, right: Any, digits: int = 8) -> bool:
    if pd.isna(left) and pd.isna(right):
        return True
    try:
        lf = float(left)
        rf = float(right)
        return round(lf, digits) == round(rf, digits)
    except Exception:
        return str(left or "").strip() == str(right or "").strip()


def _build_close_lookup(closes: pd.DataFrame) -> dict[str, dict[str, Any]]:
    if closes.empty:
        return {}
    closes = _normalize_text(
        closes,
        ("trade_id", "strategy", "run_id", "event_id", "symbol", "mode", "outcome", "outcome_source"),
    )
    closes = _normalize_numeric(closes, ("exit_price", "mfe_pct", "mae_pct", "pnl_pct", "pnl_r"))
    if "ts_utc" in closes.columns:
        closes["__ts_rank"] = pd.to_datetime(closes["ts_utc"], errors="coerce", utc=True)
        closes = closes.sort_values(["trade_id", "__ts_rank"], kind="stable")
    closes = closes[closes["trade_id"] != ""].drop_duplicates(subset=["trade_id"], keep="last")
    lookup: dict[str, dict[str, Any]] = {}
    for _, row in closes.iterrows():
        lookup[str(row["trade_id"])] = row.to_dict()
    return lookup


def _update_file(path: Path, close_lookup: dict[str, dict[str, Any]], dry_run: bool) -> tuple[int, bool]:
    df = _read_csv(path)
    if df.empty or "trade_id" not in df.columns:
        return 0, False

    df = _normalize_text(
        df,
        ("trade_id", "strategy", "run_id", "event_id", "symbol", "mode", "outcome", "outcome_source"),
    )
    df = _normalize_numeric(df, ("exit_price", "mfe_pct", "mae_pct", "pnl_pct", "pnl_r"))

    updates = 0
    changed = False

    for idx, trade_id in df["trade_id"].items():
        if not trade_id:
            continue
        close_row = close_lookup.get(trade_id)
        if not close_row:
            continue

        row_changed = False
        for field in TARGET_FIELDS:
            if field not in df.columns:
                continue
            new_value = close_row.get(field, pd.NA)
            old_value = df.at[idx, field]
            if not _same_value(old_value, new_value):
                df.at[idx, field] = new_value
                row_changed = True

        if "r_multiple" in df.columns:
            new_r = close_row.get("pnl_r", pd.NA)
            if not _same_value(df.at[idx, "r_multiple"], new_r):
                df.at[idx, "r_multiple"] = new_r
                row_changed = True

        if row_changed:
            updates += 1
            changed = True

    if not changed:
        return 0, False

    if dry_run:
        return updates, True

    backup_path = path.with_name(path.name + ".bak_sync")
    if not backup_path.exists():
        shutil.copy2(path, backup_path)
    df.to_csv(path, index=False)
    return updates, True


def main() -> None:
    ap = argparse.ArgumentParser(description="Sync outcomes_v3 partitions from trading_closes.csv")
    ap.add_argument("--data-dir", default="/root/pump_short/datasets")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    base_dir = Path(args.data_dir)
    closes_path = base_dir / "trading_closes.csv"
    if not closes_path.exists():
        raise SystemExit(f"missing closes file: {closes_path}")

    closes = _read_csv(closes_path)
    close_lookup = _build_close_lookup(closes)
    if not close_lookup:
        print("No close rows with trade_id found.")
        return

    total_updates = 0
    files_changed = 0
    for path in sorted(base_dir.glob("date=*/strategy=*/mode=*/outcomes_v3.csv")):
        updates, changed = _update_file(path, close_lookup, dry_run=args.dry_run)
        if not changed:
            continue
        total_updates += updates
        files_changed += 1
        print(f"UPDATED {path} rows={updates}{' (dry-run)' if args.dry_run else ''}")

    if total_updates == 0:
        print("No outcomes_v3 rows needed sync.")
        return

    print(
        f"OK updated_rows={total_updates} files_changed={files_changed}"
        f"{' dry_run=1' if args.dry_run else ''}"
    )


if __name__ == "__main__":
    main()
