#!/usr/bin/env python3
"""Retro-analysis for liquidation_short hypothesis."""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

THRESHOLDS = [10_000, 25_000, 50_000, 100_000]
HORIZONS = [5, 15, 30, 60]
COOLDOWN_MINUTES = 5
TP_PCT = 0.006
SL_PCT = 0.004


def load_liquidations(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, parse_dates=["ts_utc"])
    expected = {"ts_utc", "ts_ms", "symbol", "side", "qty", "price", "value_usd"}
    missing = expected - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns in liquidations.csv: {sorted(missing)}")
    df["symbol"] = df["symbol"].astype(str).str.upper()
    df["side"] = df["side"].astype(str)
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["value_usd"] = pd.to_numeric(df["value_usd"], errors="coerce")
    df = df.dropna(subset=["ts_utc", "symbol", "side", "price", "value_usd"])
    return df.sort_values(["symbol", "ts_utc"]).reset_index(drop=True)


def add_rolling_liq_30s(liq_long: pd.DataFrame) -> pd.DataFrame:
    parts = []
    for sym, grp in liq_long.groupby("symbol", sort=False):
        g = grp.sort_values("ts_utc").copy()
        # Rolling sum in (t-30s, t] by event timestamp.
        roll = g.set_index("ts_utc")["value_usd"].rolling("30s").sum()
        g["liq_usd_30s"] = roll.to_numpy()
        parts.append(g)
    return pd.concat(parts, ignore_index=True)


def find_cascade_entries(df: pd.DataFrame, threshold: float, cooldown_minutes: int = 5) -> pd.DataFrame:
    cooldown_sec = cooldown_minutes * 60
    entries = []
    last_entry: dict[str, pd.Timestamp] = {}
    crossed = df[df["liq_usd_30s"] >= threshold]

    for row in crossed.itertuples(index=False):
        sym = str(row.symbol)
        ts = pd.Timestamp(row.ts_utc)
        prev = last_entry.get(sym)
        if prev is None or (ts - prev).total_seconds() > cooldown_sec:
            entries.append(
                {
                    "symbol": sym,
                    "ts": ts,
                    "liq_usd_30s": float(row.liq_usd_30s),
                    "entry_price": float(row.price),
                }
            )
            last_entry[sym] = ts
    return pd.DataFrame(entries)


def measure_outcome(entry_row: pd.Series, liq_df: pd.DataFrame, horizons: list[int]) -> dict[str, float | str | pd.Timestamp]:
    sym = entry_row["symbol"]
    ts = entry_row["ts"]
    entry_price = float(entry_row["entry_price"])
    max_h = max(horizons)

    sym_data = liq_df[
        (liq_df["symbol"] == sym)
        & (liq_df["ts_utc"] > ts)
        & (liq_df["ts_utc"] <= ts + pd.Timedelta(minutes=max_h))
    ]

    result: dict[str, float | str | pd.Timestamp] = {
        "symbol": sym,
        "ts": ts,
        "liq_usd_30s": float(entry_row["liq_usd_30s"]),
        "entry_price": entry_price,
    }
    for h in horizons:
        window = sym_data[sym_data["ts_utc"] <= ts + pd.Timedelta(minutes=h)]
        if not window.empty:
            low_price = float(window["price"].min())
            high_price = float(window["price"].max())
            result[f"max_profit_{h}m"] = (entry_price - low_price) / entry_price
            result[f"max_loss_{h}m"] = (high_price - entry_price) / entry_price
        else:
            result[f"max_profit_{h}m"] = np.nan
            result[f"max_loss_{h}m"] = np.nan
    return result


def print_threshold_report(threshold: int, entries: pd.DataFrame, outcomes: pd.DataFrame) -> None:
    print(f"\n--- Threshold: ${threshold:,} ---")
    print(f"N entries: {len(entries)}")
    print(f"Unique symbols: {entries['symbol'].nunique()}")

    rows = []
    for h in [5, 15, 30]:
        col_profit = f"max_profit_{h}m"
        col_loss = f"max_loss_{h}m"
        valid = outcomes[[col_profit, col_loss]].dropna()
        if valid.empty:
            rows.append((h, 0, np.nan, np.nan, np.nan))
            continue
        tp_hit = (valid[col_profit] >= TP_PCT).mean()
        sl_hit = (valid[col_loss] >= SL_PCT).mean()
        avg_max_profit = valid[col_profit].mean()
        rows.append((h, len(valid), tp_hit, sl_hit, avg_max_profit))

    table = pd.DataFrame(
        rows,
        columns=["horizon_m", "n_valid", "tp_hit_0.6pct", "sl_hit_0.4pct", "avg_max_profit"],
    )
    print(table.to_string(index=False, justify="left", float_format=lambda x: f"{x:.4f}"))


def main() -> None:
    liq_path = Path("datasets/liquidations.csv")
    liq = load_liquidations(liq_path)

    # Bybit: Sell = long liquidations.
    liq_long = liq[liq["side"].str.lower() == "sell"].copy()
    liq_long = add_rolling_liq_30s(liq_long)

    print("=== LIQUIDATION SHORT RETRO ANALYSIS ===")
    print(f"source={liq_path} rows_total={len(liq):,} rows_long_liq={len(liq_long):,}")
    print(f"thresholds={THRESHOLDS} horizons={HORIZONS} cooldown_min={COOLDOWN_MINUTES}")

    best = None
    all_stats = []

    for threshold in THRESHOLDS:
        entries = find_cascade_entries(liq_long, threshold, cooldown_minutes=COOLDOWN_MINUTES)
        if entries.empty:
            print(f"\n--- Threshold: ${threshold:,} ---")
            print("N entries: 0")
            print("Unique symbols: 0")
            continue

        outcomes = pd.DataFrame(
            [measure_outcome(row, liq_long, horizons=HORIZONS) for _, row in entries.iterrows()]
        )
        print_threshold_report(threshold, entries, outcomes)

        for h in [5, 15, 30]:
            col_profit = f"max_profit_{h}m"
            col_loss = f"max_loss_{h}m"
            valid = outcomes[[col_profit, col_loss]].dropna()
            if valid.empty:
                continue
            tp_hit = float((valid[col_profit] >= TP_PCT).mean())
            sl_hit = float((valid[col_loss] >= SL_PCT).mean())
            edge = tp_hit - sl_hit
            row = {
                "threshold": threshold,
                "horizon_m": h,
                "n_valid": int(len(valid)),
                "tp_hit": tp_hit,
                "sl_hit": sl_hit,
                "edge": edge,
            }
            all_stats.append(row)
            if best is None or edge > best["edge"]:
                best = row

    if all_stats:
        stats_df = pd.DataFrame(all_stats).sort_values(["edge", "tp_hit", "n_valid"], ascending=False)
        print("\n=== SUMMARY (sorted by TP-SL edge) ===")
        print(stats_df.to_string(index=False, float_format=lambda x: f"{x:.4f}"))
    if best is not None:
        print(
            "\nBEST_COMBO "
            f"threshold=${best['threshold']:,} horizon={best['horizon_m']}m "
            f"n={best['n_valid']} tp_hit={best['tp_hit']:.2%} "
            f"sl_hit={best['sl_hit']:.2%} edge={best['edge']:.2%}"
        )


if __name__ == "__main__":
    main()
