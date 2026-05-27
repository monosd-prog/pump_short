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
JOIN_TOLERANCE_MINUTES = 5
MIN_FILTER_SAMPLE = 20


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


def load_events_v3() -> pd.DataFrame:
    files = sorted(Path("datasets").rglob("events_v3.csv"))
    if not files:
        print("WARNING: no events_v3.csv files found; filtered analysis skipped.")
        return pd.DataFrame()

    sample = pd.read_csv(files[0], nrows=1)
    ts_col = "time_utc" if "time_utc" in sample.columns else "wall_time_utc" if "wall_time_utc" in sample.columns else None
    required = ["symbol", "stage", "dist_to_peak_pct", "context_score", "funding_rate", "oi_change_5m_pct", "liq_long_usd_30s"]
    if ts_col is None:
        print("WARNING: events_v3 missing time column (time_utc/wall_time_utc); filtered analysis skipped.")
        return pd.DataFrame()
    missing = [c for c in required if c not in sample.columns]
    if missing:
        print(f"WARNING: events_v3 missing required columns {missing}; filtered analysis skipped.")
        return pd.DataFrame()

    cols = [ts_col, *required]
    chunks = []
    for f in files:
        try:
            d = pd.read_csv(f, usecols=lambda c: c in cols)
            d["event_ts"] = pd.to_datetime(d[ts_col], utc=True, errors="coerce")
            d["symbol"] = d["symbol"].astype(str).str.upper()
            chunks.append(d.drop(columns=[ts_col]))
        except Exception:
            continue
    if not chunks:
        print("WARNING: events_v3 load failed for all files; filtered analysis skipped.")
        return pd.DataFrame()
    out = pd.concat(chunks, ignore_index=True).dropna(subset=["event_ts", "symbol"])
    return out.sort_values(["symbol", "event_ts"]).reset_index(drop=True)


def join_entries_with_events(entries: pd.DataFrame, events: pd.DataFrame) -> pd.DataFrame:
    if entries.empty or events.empty:
        return pd.DataFrame()
    left = entries.copy().sort_values(["ts", "symbol"]).reset_index(drop=True)
    right = events.copy().sort_values(["event_ts", "symbol"]).reset_index(drop=True)
    joined = pd.merge_asof(
        left,
        right,
        left_on="ts",
        right_on="event_ts",
        by="symbol",
        direction="nearest",
        tolerance=pd.Timedelta(minutes=JOIN_TOLERANCE_MINUTES),
    )
    joined["join_matched"] = joined["event_ts"].notna()
    return joined


def print_filtered_analysis(entries_50k: pd.DataFrame, outcomes_50k: pd.DataFrame) -> None:
    events = load_events_v3()
    if events.empty:
        return

    joined = join_entries_with_events(entries_50k, events)
    if joined.empty:
        print("WARNING: no joined rows for filtered analysis.")
        return

    base = joined.merge(outcomes_50k[["symbol", "ts", "max_profit_5m", "max_loss_5m"]], on=["symbol", "ts"], how="left")
    base = base.dropna(subset=["max_profit_5m", "max_loss_5m"])
    base = base[base["join_matched"]]
    if base.empty:
        print("WARNING: filtered analysis has no matched rows with 5m outcomes.")
        return

    filters = {
        "no_filter": lambda df: df,
        "stage_4": lambda df: df[df["stage"] == 4],
        "dist_gt5": lambda df: df[df["dist_to_peak_pct"] > 5.0],
        "dist_gt10": lambda df: df[df["dist_to_peak_pct"] > 10.0],
        "funding_pos": lambda df: df[df["funding_rate"] > 0],
        "ctx_lt05": lambda df: df[df["context_score"] < 0.5],
        "stage4_dist5_funding": lambda df: df[
            (df["stage"] == 4) & (df["dist_to_peak_pct"] > 5.0) & (df["funding_rate"] > 0)
        ],
        "stage4_dist10": lambda df: df[(df["stage"] == 4) & (df["dist_to_peak_pct"] > 10.0)],
    }

    print("\n=== FILTERED ANALYSIS (threshold=$50k, horizon=5m, tp=0.6%, sl=0.4%) ===")
    print(f"joined_rows={len(base)} tolerance={JOIN_TOLERANCE_MINUTES}m min_sample={MIN_FILTER_SAMPLE}")

    rows = []
    for name, fn in filters.items():
        try:
            d = fn(base).dropna(subset=["max_profit_5m", "max_loss_5m"])
        except Exception:
            continue
        n = len(d)
        if n < MIN_FILTER_SAMPLE:
            print(f"skip {name}: n={n} < {MIN_FILTER_SAMPLE}")
            continue
        tp = float((d["max_profit_5m"] >= TP_PCT).mean())
        sl = float((d["max_loss_5m"] >= SL_PCT).mean())
        edge = tp - sl
        rows.append({"filter": name, "N": n, "TP_hit": tp, "SL_hit": sl, "edge": edge})

    if not rows:
        print("No filters passed minimum sample.")
        return

    out = pd.DataFrame(rows).sort_values(["edge", "TP_hit", "N"], ascending=[False, False, False])
    print(out.to_string(index=False, float_format=lambda x: f"{x:.4f}"))
    best = out.iloc[0]
    print(
        f"BEST_FILTER {best['filter']} N={int(best['N'])} "
        f"TP={best['TP_hit']:.2%} SL={best['SL_hit']:.2%} edge={best['edge']:.2%}"
    )


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

    entries_50k = pd.DataFrame()
    outcomes_50k = pd.DataFrame()

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
        if threshold == 50_000:
            entries_50k = entries.copy()
            outcomes_50k = outcomes.copy()
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
    if not entries_50k.empty and not outcomes_50k.empty:
        print_filtered_analysis(entries_50k, outcomes_50k)


if __name__ == "__main__":
    main()
