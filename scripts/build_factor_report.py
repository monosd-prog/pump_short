#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import List

import pandas as pd


def _find_root_with_dates(base_dir: Path) -> Path | None:
    """
    Try base_dir, then base_dir/datasets, return first that contains date=* dirs.
    """
    candidates: List[Path] = [base_dir, base_dir / "datasets"]
    for c in candidates:
        if any((c / "date=*").parent.glob("date=*")):
            return c
    return None


def _load_outcomes_all_modes(base_dir: Path, mode: str) -> pd.DataFrame:
    """
    Load all outcomes_v3.csv under date=*/strategy=*/mode=<mode>/.
    Adds date,strategy,mode columns if missing.
    """
    root = _find_root_with_dates(base_dir)
    if root is None:
        return pd.DataFrame()

    frames: List[pd.DataFrame] = []
    for date_dir in sorted(root.glob("date=*")):
        if not date_dir.is_dir():
            continue
        date_str = date_dir.name.split("=", 1)[1]
        for strat_dir in date_dir.glob("strategy=*"):
            if not strat_dir.is_dir():
                continue
            strategy = strat_dir.name.split("=", 1)[1]
            mode_dir = strat_dir / f"mode={mode}"
            if not mode_dir.is_dir():
                continue
            path = mode_dir / "outcomes_v3.csv"
            if not path.exists():
                continue
            try:
                df = pd.read_csv(path)
            except Exception:
                continue
            if df.empty:
                continue
            if "date" not in df.columns:
                df["date"] = date_str
            if "strategy" not in df.columns:
                df["strategy"] = strategy
            if "mode" not in df.columns:
                df["mode"] = mode
            df["source_file"] = str(path)
            frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _safe_pnl(df: pd.DataFrame) -> pd.Series:
    return pd.to_numeric(df.get("pnl_pct", 0.0), errors="coerce").fillna(0.0)


def _group_stats(df: pd.DataFrame, by: str) -> pd.DataFrame:
    pnl = _safe_pnl(df)
    df = df.copy()
    df["_pnl"] = pnl
    grouped = df.groupby(by, dropna=False)
    stats = grouped["_pnl"].agg(
        trades_count="count",
        win_trades=lambda s: int((s > 0).sum()),
        avg_pnl="mean",
        median_pnl="median",
        sum_pnl="sum",
    ).reset_index()
    stats["winrate"] = stats["win_trades"] / stats["trades_count"].replace(0, pd.NA)
    return stats


def _bucket_hold_seconds(df: pd.DataFrame) -> pd.DataFrame:
    if "hold_seconds" not in df.columns:
        return pd.DataFrame()
    s = pd.to_numeric(df["hold_seconds"], errors="coerce")
    bins = [-1, 60, 300, float("inf")]
    labels = ["<60s", "60-300s", ">=300s"]
    bucket = pd.cut(s, bins=bins, labels=labels)
    tmp = df.copy()
    tmp["hold_bucket"] = bucket.astype(str)
    return _group_stats(tmp, "hold_bucket")


def _print_overall(df: pd.DataFrame) -> None:
    pnl = _safe_pnl(df)
    total = len(df)
    wins = int((pnl > 0).sum())
    winrate = (wins / total) if total else 0.0
    avg_pnl = float(pnl.mean()) if total else 0.0
    med_pnl = float(pnl.median()) if total else 0.0
    sum_pnl = float(pnl.sum()) if total else 0.0
    print("=== OVERALL ===")
    print(f"total_trades: {total}")
    print(f"winrate:      {winrate:.2%}")
    print(f"avg_pnl:      {avg_pnl:.3f}%")
    print(f"median_pnl:   {med_pnl:.3f}%")
    print(f"sum_pnl:      {sum_pnl:.3f}%")
    print("")


def _print_group_block(title: str, stats: pd.DataFrame, key_col: str, max_rows: int = 50) -> None:
    if stats.empty:
        print(f"=== {title} ===")
        print("no data")
        print("")
        return
    print(f"=== {title} ===")
    cols = [key_col, "trades_count", "win_trades", "winrate", "avg_pnl", "median_pnl", "sum_pnl"]
    head = stats.sort_values("trades_count", ascending=False).head(max_rows)
    for _, row in head[cols].iterrows():
        key = row[key_col]
        print(
            f"{key}: n={int(row['trades_count'])}, "
            f"WR={row['winrate']:.2% if pd.notna(row['winrate']) else 0.0:.2%}, "
            f"avg={row['avg_pnl']:.3f}%, med={row['median_pnl']:.3f}%, sum={row['sum_pnl']:.3f}%"
        )
    print("")


def _build_csv_rows(df: pd.DataFrame, mode: str) -> pd.DataFrame:
    rows: list[dict] = []

    # overall
    pnl = _safe_pnl(df)
    total = len(df)
    wins = int((pnl > 0).sum())
    row_overall = {
        "group_type": "overall",
        "group_value": "ALL",
        "mode": mode,
        "trades_count": total,
        "win_trades": wins,
        "winrate": (wins / total) if total else 0.0,
        "avg_pnl": float(pnl.mean()) if total else 0.0,
        "median_pnl": float(pnl.median()) if total else 0.0,
        "sum_pnl": float(pnl.sum()) if total else 0.0,
    }
    rows.append(row_overall)

    # by strategy
    strat_stats = _group_stats(df, "strategy")
    for _, r in strat_stats.iterrows():
        rows.append(
            {
                "group_type": "strategy",
                "group_value": r["strategy"],
                "mode": mode,
                "trades_count": int(r["trades_count"]),
                "win_trades": int(r["win_trades"]),
                "winrate": float(r["winrate"]) if pd.notna(r["winrate"]) else 0.0,
                "avg_pnl": float(r["avg_pnl"]),
                "median_pnl": float(r["median_pnl"]),
                "sum_pnl": float(r["sum_pnl"]),
            }
        )

    # by symbol
    if "symbol" in df.columns:
        sym_stats = _group_stats(df, "symbol")
        for _, r in sym_stats.iterrows():
            rows.append(
                {
                    "group_type": "symbol",
                    "group_value": r["symbol"],
                    "mode": mode,
                    "trades_count": int(r["trades_count"]),
                    "win_trades": int(r["win_trades"]),
                    "winrate": float(r["winrate"]) if pd.notna(r["winrate"]) else 0.0,
                    "avg_pnl": float(r["avg_pnl"]),
                    "median_pnl": float(r["median_pnl"]),
                    "sum_pnl": float(r["sum_pnl"]),
                }
            )

    # hold time buckets
    hold_stats = _bucket_hold_seconds(df)
    if not hold_stats.empty:
        for _, r in hold_stats.iterrows():
            rows.append(
                {
                    "group_type": "hold_bucket",
                    "group_value": r["hold_bucket"],
                    "mode": mode,
                    "trades_count": int(r["trades_count"]),
                    "win_trades": int(r["win_trades"]),
                    "winrate": float(r["winrate"]) if pd.notna(r["winrate"]) else 0.0,
                    "avg_pnl": float(r["avg_pnl"]),
                    "median_pnl": float(r["median_pnl"]),
                    "sum_pnl": float(r["sum_pnl"]),
                }
            )

    return pd.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build simple factor-style report from outcomes_v3 (by mode)")
    parser.add_argument(
        "--data-dir",
        type=str,
        default="/root/pump_short/datasets",
        help="Datasets root (directory containing date=* or its parent)",
    )
    parser.add_argument(
        "--mode",
        type=str,
        default="lab",
        help="Datasets mode to use (lab|live|paper). Default: lab",
    )
    parser.add_argument(
        "--out-dir",
        type=str,
        default="/root/pump_short/factor_reports",
        help="Directory to write CSV summary (default: /root/pump_short/factor_reports)",
    )
    args = parser.parse_args()

    base_dir = Path(args.data_dir)
    mode = str(args.mode).strip()
    out_dir = Path(args.out_dir)

    df = _load_outcomes_all_modes(base_dir, mode=mode)
    if df.empty:
        print(f"[FACTOR_REPORT] No outcomes_v3.csv found under {base_dir} for mode={mode}")
        sys.exit(0)

    # Basic overall stats
    _print_overall(df)

    # By strategy
    strat_stats = _group_stats(df, "strategy")
    _print_group_block("BY STRATEGY", strat_stats, "strategy")

    # By symbol
    if "symbol" in df.columns:
        sym_stats = _group_stats(df, "symbol")
        _print_group_block("BY SYMBOL", sym_stats, "symbol")

    # Hold time buckets
    hold_stats = _bucket_hold_seconds(df)
    if not hold_stats.empty:
        print("=== HOLD TIME BUCKETS ===")
        for _, r in hold_stats.iterrows():
            print(
                f"{r['hold_bucket']}: n={int(r['trades_count'])}, "
                f"WR={r['winrate']:.2% if pd.notna(r['winrate']) else 0.0:.2%}, "
                f"avg={r['avg_pnl']:.3f}%, med={r['median_pnl']:.3f}%, sum={r['sum_pnl']:.3f}%"
            )
        print("")

    # Save CSV summary
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_rows = _build_csv_rows(df, mode=mode)
    out_path = out_dir / f"factor_report_{mode}.csv"
    csv_rows.to_csv(out_path, index=False)
    print(f"[FACTOR_REPORT] CSV saved to: {out_path}")


if __name__ == "__main__":
    main()

