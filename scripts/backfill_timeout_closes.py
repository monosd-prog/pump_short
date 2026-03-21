#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import shutil
from pathlib import Path
from typing import Any

import pandas as pd

from short_pump.bybit_api import get_klines_1m_range
from trading.state import make_position_id


def _read_csv_robust(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path, engine="python", on_bad_lines="skip")
    except Exception:
        return pd.DataFrame()


def _load_outcome_files(base_dir: Path) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for p in base_dir.glob("date=*/strategy=*/mode=*/outcomes_v3.csv"):
        df = _read_csv_robust(p)
        if df.empty:
            continue
        df = df.copy()
        df["source_file"] = str(p)
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _parse_ts(value: Any) -> pd.Timestamp | None:
    try:
        ts = pd.to_datetime(value, utc=True)
        if pd.isna(ts):
            return None
        return ts
    except Exception:
        return None


def _replay_outcome(
    candles: pd.DataFrame,
    *,
    side: str,
    entry: float,
    tp: float,
    sl: float,
) -> tuple[str, float, float, float]:
    """Return (outcome, exit_price, mfe_pct, mae_pct)."""
    side_up = (side or "").strip().upper()
    mfe = 0.0
    mae = 0.0
    outcome = "TIMEOUT"
    exit_price = entry
    if candles is None or candles.empty or entry <= 0 or tp <= 0 or sl <= 0:
        return outcome, exit_price, mfe, mae

    for _, row in candles.sort_values("ts").iterrows():
        hi = float(row["high"])
        lo = float(row["low"])
        close = float(row["close"])
        if side_up in ("SHORT", "SELL"):
            mfe = max(mfe, max(0.0, (entry - lo) / entry * 100.0))
            mae = max(mae, max(0.0, (hi - entry) / entry * 100.0))
            tp_hit = lo <= tp
            sl_hit = hi >= sl
        else:
            mfe = max(mfe, max(0.0, (hi - entry) / entry * 100.0))
            mae = max(mae, max(0.0, (entry - lo) / entry * 100.0))
            tp_hit = hi >= tp
            sl_hit = lo <= sl

        if tp_hit and sl_hit:
            outcome = "SL_hit"
            exit_price = sl
            break
        if sl_hit:
            outcome = "SL_hit"
            exit_price = sl
            break
        if tp_hit:
            outcome = "TP_hit"
            exit_price = tp
            break

        exit_price = close

    return outcome, exit_price, mfe, mae


def main() -> None:
    ap = argparse.ArgumentParser(description="Backfill TIMEOUT closes from historical klines")
    ap.add_argument("--data-dir", default="/root/pump_short/datasets", help="Datasets root")
    ap.add_argument("--strategy", default="short_pump", help="Strategy filter")
    ap.add_argument("--mode", default="paper", help="Mode filter")
    ap.add_argument("--dry-run", action="store_true", help="Do not write changes")
    args = ap.parse_args()

    base_dir = Path(args.data_dir)
    closes_path = base_dir / "trading_closes.csv"
    trades_path = base_dir / "trading_trades.csv"
    if not closes_path.exists():
        raise SystemExit(f"missing closes file: {closes_path}")

    closes = pd.read_csv(closes_path)
    trades = _read_csv_robust(trades_path) if trades_path.exists() else pd.DataFrame()
    if closes.empty:
        print("No closes found.")
        return

    for c in ("mfe_pct", "mae_pct"):
        if c not in closes.columns:
            closes[c] = ""

    close_outcome = closes.get("outcome", pd.Series(dtype=str)).astype(str)
    zero_mfe = pd.to_numeric(closes["mfe_pct"], errors="coerce").fillna(0) == 0
    zero_mae = pd.to_numeric(closes["mae_pct"], errors="coerce").fillna(0) == 0
    mask = (
        (closes.get("mode", pd.Series(dtype=str)).astype(str) == args.mode)
        & (closes.get("strategy", pd.Series(dtype=str)).astype(str) == args.strategy)
        & (close_outcome == "TIMEOUT")
        & zero_mfe
        & zero_mae
    )
    candidates = closes.loc[mask].copy()
    if candidates.empty:
        print("No matching TIMEOUT rows found.")
        return

    if trades.empty:
        print("No trades file found or it was unreadable.")
        return

    outcomes = _load_outcome_files(base_dir)

    trade_keys = ["strategy", "run_id", "event_id", "symbol"]
    for col in trade_keys + ["ts"]:
        if col not in trades.columns:
            raise SystemExit(f"trades file missing required column: {col}")

    if "trade_id" not in closes.columns:
        closes["trade_id"] = [
            make_position_id(str(r.get("strategy") or ""), str(r.get("run_id") or ""), str(r.get("event_id") or ""), str(r.get("symbol") or ""))
            for _, r in closes.iterrows()
        ]

    updated_rows = 0
    updated_trades: list[dict[str, Any]] = []
    unique_candidates = candidates.drop_duplicates(subset=["trade_id"], keep="first")

    for _, crow in unique_candidates.iterrows():
        trade_id = str(crow.get("trade_id") or "")
        if not trade_id:
            continue
        key_mask = (
            (closes["trade_id"].astype(str) == trade_id)
            & (closes.get("mode", pd.Series(dtype=str)).astype(str) == args.mode)
            & (closes.get("strategy", pd.Series(dtype=str)).astype(str) == args.strategy)
            & (closes.get("outcome", pd.Series(dtype=str)).astype(str) == "TIMEOUT")
        )
        tmatch = trades[
            (trades["strategy"].astype(str) == str(crow.get("strategy") or ""))
            & (trades["run_id"].astype(str) == str(crow.get("run_id") or ""))
            & (trades["event_id"].astype(str) == str(crow.get("event_id") or ""))
            & (trades["symbol"].astype(str).str.upper() == str(crow.get("symbol") or "").upper())
        ]
        entry_ts = _parse_ts(tmatch.iloc[0]["ts"]) if not tmatch.empty else None
        if entry_ts is None and not outcomes.empty:
            omatch = outcomes[
                (outcomes.get("trade_id", pd.Series(dtype=str)).astype(str) == trade_id)
                | (
                    (outcomes.get("run_id", pd.Series(dtype=str)).astype(str) == str(crow.get("run_id") or ""))
                    & (outcomes.get("event_id", pd.Series(dtype=str)).astype(str) == str(crow.get("event_id") or ""))
                    & (outcomes.get("symbol", pd.Series(dtype=str)).astype(str).str.upper() == str(crow.get("symbol") or "").upper())
                )
            ]
            if not omatch.empty and "details_json" in omatch.columns:
                details_raw = str(omatch.iloc[0].get("details_json") or "")
                try:
                    import json

                    details = json.loads(details_raw) if details_raw else {}
                except Exception:
                    details = {}
                entry_ts = _parse_ts(
                    details.get("entry_time_utc")
                    or details.get("entry_ts_utc")
                    or omatch.iloc[0].get("opened_ts")
                    or omatch.iloc[0].get("entry_time_utc")
                )
        close_ts = _parse_ts(crow.get("ts_utc"))
        if entry_ts is None or close_ts is None:
            continue

        try:
            candles = get_klines_1m_range(
                "linear",
                str(crow.get("symbol") or ""),
                start_ms=int(entry_ts.timestamp() * 1000),
                end_ms=int(close_ts.timestamp() * 1000),
                limit=1000,
            )
        except Exception as e:
            print(f"SKIP {trade_id} | kline_fetch_failed={type(e).__name__}")
            continue

        outcome, exit_price, mfe_pct, mae_pct = _replay_outcome(
            candles,
            side=str(crow.get("side") or "SHORT"),
            entry=float(crow.get("entry_price") or 0),
            tp=float(crow.get("tp_price") or 0),
            sl=float(crow.get("sl_price") or 0),
        )

        if outcome == "TIMEOUT":
            # Keep timeout semantics, but backfill the path metrics so it is no longer blind.
            outcome = "TIMEOUT"

        update_cols = {
            "outcome": outcome,
            "close_reason": outcome,
            "exit_price": float(exit_price),
            "mfe_pct": round(float(mfe_pct), 4),
            "mae_pct": round(float(mae_pct), 4),
            "outcome_source": "backfill_klines",
        }
        for col, value in update_cols.items():
            closes.loc[key_mask, col] = value
        if outcome == "TP_hit":
            side_up = str(crow.get("side") or "SHORT").upper()
            entry = float(crow.get("entry_price") or 0)
            tp = float(crow.get("tp_price") or 0)
            sl = float(crow.get("sl_price") or 0)
            if entry > 0:
                if side_up in ("SHORT", "SELL"):
                    closes.loc[key_mask, "pnl_pct"] = (entry - float(exit_price)) / entry * 100.0
                else:
                    closes.loc[key_mask, "pnl_pct"] = (float(exit_price) - entry) / entry * 100.0
        elif outcome == "SL_hit":
            side_up = str(crow.get("side") or "SHORT").upper()
            entry = float(crow.get("entry_price") or 0)
            if entry > 0:
                if side_up in ("SHORT", "SELL"):
                    closes.loc[key_mask, "pnl_pct"] = (entry - float(exit_price)) / entry * 100.0
                else:
                    closes.loc[key_mask, "pnl_pct"] = (float(exit_price) - entry) / entry * 100.0
        else:
            # For remaining TIMEOUT rows, keep the close price semantics but the metrics are now populated.
            pass

        updated_trades.append(
            {
                "trade_id": trade_id,
                "symbol": str(crow.get("symbol") or ""),
                "outcome": outcome,
                "mfe_pct": round(float(mfe_pct), 4),
                "mae_pct": round(float(mae_pct), 4),
            }
        )
        updated_rows += int(key_mask.sum())
        print(
            f"UPDATED {trade_id} | symbol={crow.get('symbol')} outcome={outcome} rows={int(key_mask.sum())} mfe={mfe_pct:.4f} mae={mae_pct:.4f}"
        )

    if updated_rows == 0:
        print("No rows updated.")
        return

    if args.dry_run:
        print(f"DRY RUN complete: would update {updated_rows} rows across {len(unique_candidates)} trades")
        return

    backup = closes_path.with_suffix(".csv.bak")
    shutil.copy2(closes_path, backup)
    closes.to_csv(closes_path, index=False)
    print(f"OK updated_rows={updated_rows} unique_trades={len(updated_trades)} backup={backup}")


if __name__ == "__main__":
    main()
