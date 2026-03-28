#!/usr/bin/env python3
"""Sync outcomes_v3 partitions from trading_closes.csv.

Two passes:
  1. UPDATE: patch TARGET_FIELDS on existing rows whose partition mode matches the close row mode.
             Cross-mode updates (e.g. live close patching paper row) are skipped.
  2. INSERT: append rows from trading_closes.csv into the correct mode/date partition
             when the trade_id is not yet present there.
"""
from __future__ import annotations

import argparse
import shutil
from pathlib import Path
from typing import Any

import pandas as pd

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.dataset_schema import OUTCOME_FIELDS_V3, SCHEMA_VERSION


TARGET_FIELDS = (
    "outcome",
    "exit_price",
    "mfe_pct",
    "mae_pct",
    "pnl_pct",
    "pnl_r",
    "outcome_source",
)

# Renames when mapping trading_closes columns → outcomes_v3 columns
_CLOSE_TO_V3: dict[str, str] = {
    "ts_utc": "outcome_time_utc",
    "entry_price": "entry",
    "tp_price": "tp",
    "sl_price": "sl",
}


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
        return round(float(left), digits) == round(float(right), digits)
    except Exception:
        return str(left or "").strip() == str(right or "").strip()


def _mode_from_path(path: Path) -> str:
    """Extract mode from partition path component e.g. .../mode=live/... → 'live'."""
    for part in path.parts:
        if part.startswith("mode="):
            return part[5:].strip().lower()
    return ""


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


def _partition_path(base_dir: Path, close_row: dict[str, Any]) -> Path | None:
    """Compute target partition path from close row's ts_utc, strategy, mode."""
    ts_utc = str(close_row.get("ts_utc") or "").strip()
    strategy = str(close_row.get("strategy") or "").strip()
    mode = str(close_row.get("mode") or "").strip().lower()
    if not ts_utc or not strategy or not mode:
        return None
    date_part = ts_utc[:10].replace("-", "")  # "2026-03-23" → "20260323"
    if len(date_part) != 8 or not date_part.isdigit():
        return None
    return base_dir / f"date={date_part}" / f"strategy={strategy}" / f"mode={mode}" / "outcomes_v3.csv"


def _close_row_to_v3(close_row: dict[str, Any], columns: list[str]) -> dict[str, Any]:
    """Map a trading_closes row to the given column set."""
    mapped = dict(close_row)
    for src, dst in _CLOSE_TO_V3.items():
        if src in mapped:
            mapped[dst] = mapped[src]
    mapped["schema_version"] = SCHEMA_VERSION
    mapped["data_available"] = True
    mapped.setdefault("source_mode", mapped.get("mode", ""))
    mapped.setdefault("hold_seconds", "")
    mapped.setdefault("details_json", "")
    mapped.setdefault("opened_ts", "")
    mapped.setdefault("margin_mode", "")
    return {k: mapped.get(k, "") for k in columns}


def _update_file(path: Path, close_lookup: dict[str, dict[str, Any]], dry_run: bool) -> tuple[int, bool]:
    """Patch TARGET_FIELDS on existing rows. Skips rows whose close mode differs from partition mode."""
    df = _read_csv(path)
    if df.empty or "trade_id" not in df.columns:
        return 0, False

    partition_mode = _mode_from_path(path)
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

        # Mode guard: never let a live close patch a paper row (or vice versa).
        close_mode = str(close_row.get("mode") or "").strip().lower()
        if partition_mode and close_mode and partition_mode != close_mode:
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


def _insert_missing_rows(
    base_dir: Path,
    close_lookup: dict[str, dict[str, Any]],
    dry_run: bool,
) -> tuple[int, int]:
    """Insert close rows that are absent from their target mode/date partition."""
    # Group by target partition path
    inserts_by_path: dict[Path, list[dict[str, Any]]] = {}
    for trade_id, close_row in close_lookup.items():
        target = _partition_path(base_dir, close_row)
        if target is None:
            continue
        inserts_by_path.setdefault(target, []).append(close_row)

    total_inserted = 0
    files_changed = 0

    for target_path, rows_to_insert in sorted(inserts_by_path.items()):
        # Determine the column schema to use:
        # - if file exists (even header-only), use its columns for consistency
        # - otherwise use canonical OUTCOME_FIELDS_V3
        existing_df = _read_csv(target_path) if target_path.exists() else pd.DataFrame()
        if not existing_df.empty or (target_path.exists() and target_path.stat().st_size > 1):
            columns = list(existing_df.columns)
        else:
            # header-only or missing file — use canonical schema
            columns = list(OUTCOME_FIELDS_V3)

        # Find trade_ids already present
        existing_ids: set[str] = set()
        if "trade_id" in existing_df.columns:
            existing_ids = set(existing_df["trade_id"].dropna().astype(str).str.strip())

        new_rows = [r for r in rows_to_insert if str(r.get("trade_id", "")).strip() not in existing_ids]
        if not new_rows:
            continue

        if dry_run:
            print(f"INSERT (dry-run) {target_path} new_rows={len(new_rows)}")
            total_inserted += len(new_rows)
            files_changed += 1
            continue

        # Build new-row DataFrame with the file's own column schema
        v3_rows = [_close_row_to_v3(r, columns) for r in new_rows]
        new_df = pd.DataFrame(v3_rows, columns=columns)

        # Ensure directory exists
        target_path.parent.mkdir(parents=True, exist_ok=True)

        if not existing_df.empty:
            # File has existing rows — backup and append
            backup_path = target_path.with_name(target_path.name + ".bak_sync")
            if not backup_path.exists():
                shutil.copy2(target_path, backup_path)
            # Align existing df to same columns (add missing as empty)
            for col in columns:
                if col not in existing_df.columns:
                    existing_df[col] = ""
            combined = pd.concat([existing_df[columns], new_df], ignore_index=True)
            combined.to_csv(target_path, index=False)
        else:
            # Header-only or new file — just write new rows (no backup needed)
            new_df.to_csv(target_path, index=False)

        print(f"INSERTED {target_path} new_rows={len(new_rows)}")
        total_inserted += len(new_rows)
        files_changed += 1

    return total_inserted, files_changed


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

    # Pass 1: UPDATE existing rows (mode-guarded)
    total_updates = 0
    files_updated = 0
    for path in sorted(base_dir.glob("date=*/strategy=*/mode=*/outcomes_v3.csv")):
        updates, changed = _update_file(path, close_lookup, dry_run=args.dry_run)
        if not changed:
            continue
        total_updates += updates
        files_updated += 1
        print(f"UPDATED {path} rows={updates}{' (dry-run)' if args.dry_run else ''}")

    # Pass 2: INSERT missing rows into correct partitions
    total_inserted, files_inserted = _insert_missing_rows(base_dir, close_lookup, dry_run=args.dry_run)

    if total_updates == 0 and total_inserted == 0:
        print("No outcomes_v3 rows needed sync.")
        return

    print(
        f"OK updated_rows={total_updates} files_updated={files_updated}"
        f" inserted_rows={total_inserted} files_with_inserts={files_inserted}"
        f"{' dry_run=1' if args.dry_run else ''}"
    )


if __name__ == "__main__":
    main()
