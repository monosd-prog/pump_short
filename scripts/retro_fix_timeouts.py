#!/usr/bin/env python3
"""
Retro-fix TIMEOUT rows in trading_closes.csv where mfe_pct and mae_pct are both 0.
Uses the same candle logic as common/outcome_tracker.py with OUTCOME_USE_CANDLE_HILO=True
and OUTCOME_TP_SL_CONFLICT=SL_FIRST (hard-coded for reproducibility).
"""
from __future__ import annotations

import argparse
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from short_pump.bybit_api import get_klines_1m_range

# Mirror outcome_tracker defaults for this retro pass (do not depend on process env).
OUTCOME_USE_CANDLE_HILO = True
OUTCOME_TP_SL_CONFLICT = "SL_FIRST"
WATCH_MINUTES = 120
KL_LIMIT = 200


def _normalize_conflict_policy(raw: str | None) -> str:
    val = (raw or "").strip().upper()
    if val in ("SL_FIRST", "TP_FIRST", "NEUTRAL"):
        return val
    return "SL_FIRST"


def _read_csv_robust(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path, engine="python", on_bad_lines="skip")
    except Exception:
        return pd.DataFrame()


def _load_all_events_v3(datasets_root: Path) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for p in sorted(datasets_root.rglob("events_v3.csv")):
        df = _read_csv_robust(p)
        if df.empty:
            continue
        df = df.copy()
        df["__source_file"] = str(p)
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _find_entry_wall_time_utc(
    row: pd.Series,
    events_df: pd.DataFrame,
) -> pd.Timestamp | None:
    if events_df.empty:
        return None
    rid = str(row.get("run_id", "") or "")
    sym = str(row.get("symbol", "") or "").strip().upper()
    strat = str(row.get("strategy", "") or "")
    mode = str(row.get("mode", "") or "")
    if not rid or not sym:
        return None

    m = (
        (events_df["run_id"].astype(str) == rid)
        & (events_df["symbol"].astype(str).str.upper() == sym)
        & (events_df["event_id"].astype(str).str.contains("entry", case=False, na=False))
    )
    if "strategy" in events_df.columns and strat:
        m &= events_df["strategy"].astype(str) == strat

    sub = events_df.loc[m].copy()
    if sub.empty:
        m2 = (
            (events_df["run_id"].astype(str) == rid)
            & (events_df["symbol"].astype(str).str.upper() == sym)
            & (events_df["event_id"].astype(str).str.contains("entry", case=False, na=False))
        )
        sub = events_df.loc[m2].copy()
    if sub.empty or "price" not in sub.columns:
        return None

    sub = sub.head(1)

    w = sub.iloc[0].get("wall_time_utc")
    ts = pd.to_datetime(w, utc=True, errors="coerce")
    if pd.isna(ts):
        return None
    return ts


def _replay_short_outcome_tracker_style(
    future: pd.DataFrame,
    *,
    entry_price: float,
    tp_price: float,
    sl_price: float,
    end_ts: pd.Timestamp,
) -> tuple[str, float, float, float]:
    """
    Same rules as outcome_tracker.track_outcome for a fixed historical slice:
    - SHORT, candle HILO, SL_FIRST on same candle
    - mfe/mae from full-window min low / max high vs entry (percent, not fraction)
    - TIMEOUT exit: last close among candles with ts <= end_ts
    """
    if future is None or future.empty or entry_price <= 0 or tp_price <= 0 or sl_price <= 0:
        return "TIMEOUT", float(entry_price), 0.0, 0.0

    fut = future.sort_values("ts").copy()
    fut = fut[fut["ts"] <= end_ts]
    if fut.empty:
        return "TIMEOUT", float(entry_price), 0.0, 0.0

    min_low = float(fut["low"].astype(float).min())
    max_high = float(fut["high"].astype(float).max())
    mfe_pct = (entry_price - min_low) / entry_price * 100.0
    mae_pct = (max_high - entry_price) / entry_price * 100.0

    end_reason = "TIMEOUT"

    policy = _normalize_conflict_policy(OUTCOME_TP_SL_CONFLICT)

    for _, crow in fut.iterrows():
        hi = float(crow["high"])
        lo = float(crow["low"])
        close = float(crow["close"])

        if OUTCOME_USE_CANDLE_HILO:
            tp_hit = lo <= tp_price
            sl_hit = hi >= sl_price
        else:
            tp_hit = close <= tp_price
            sl_hit = close >= sl_price

        if tp_hit and sl_hit:
            if policy == "NEUTRAL":
                end_reason = "CONFLICT"
            elif policy == "TP_FIRST":
                end_reason = "TP_hit"
            else:
                end_reason = "SL_hit"
            break
        if sl_hit:
            end_reason = "SL_hit"
            break
        if tp_hit:
            end_reason = "TP_hit"
            break

    if end_reason == "CONFLICT":
        return "TIMEOUT", float(entry_price), mfe_pct, mae_pct

    if end_reason in ("TP_hit", "SL_hit"):
        if end_reason == "TP_hit":
            exit_price = float(tp_price)
        else:
            exit_price = float(sl_price)
        return end_reason, exit_price, mfe_pct, mae_pct

    last_close = float(fut["close"].iloc[-1])
    exit_price = last_close
    return "TIMEOUT", exit_price, mfe_pct, mae_pct


def main() -> None:
    ap = argparse.ArgumentParser(description="Retro-fix bogus TIMEOUT rows in trading_closes.csv")
    ap.add_argument(
        "--data-dir",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "datasets",
        help="Datasets root (default: repo/datasets)",
    )
    ap.add_argument("--dry-run", action="store_true", help="Do not write backup or CSV")
    args = ap.parse_args()

    datasets_root: Path = args.data_dir
    closes_path = datasets_root / "trading_closes.csv"
    if not closes_path.exists():
        raise SystemExit(f"missing {closes_path}")

    events_df = _load_all_events_v3(datasets_root)
    print(f"Loaded events_v3 rows: {len(events_df)} from **/events_v3.csv")

    df = pd.read_csv(closes_path, engine="python", on_bad_lines="skip")
    for col in ("outcome", "mfe_pct", "mae_pct"):
        if col not in df.columns:
            raise SystemExit(f"missing column {col} in trading_closes.csv")

    mfe_num = pd.to_numeric(df["mfe_pct"], errors="coerce").fillna(0.0)
    mae_num = pd.to_numeric(df["mae_pct"], errors="coerce").fillna(0.0)
    mask = (df["outcome"].astype(str) == "TIMEOUT") & (mfe_num == 0.0) & (mae_num == 0.0)
    candidates = df.loc[mask].copy()
    n_checked = len(candidates)

    changed_tp = 0
    changed_sl = 0
    still_timeout = 0
    skipped = 0
    changed_run_ids: list[str] = []

    idx_updates: dict[int, dict[str, Any]] = {}

    for idx, row in candidates.iterrows():
        symbol = str(row.get("symbol", "") or "").strip().upper()
        entry_ts = _find_entry_wall_time_utc(row, events_df)
        if entry_ts is None:
            skipped += 1
            print(f"SKIP no entry event | run_id={row.get('run_id')} symbol={symbol}")
            continue

        try:
            ep = float(row.get("entry_price", 0.0))
            tp = float(row.get("tp_price", 0.0))
            sl = float(row.get("sl_price", 0.0))
        except (TypeError, ValueError):
            skipped += 1
            continue

        start_ms = int(entry_ts.timestamp() * 1000)
        end_ms = start_ms + WATCH_MINUTES * 60 * 1000

        try:
            candles = get_klines_1m_range(
                "linear",
                symbol,
                start_ms=start_ms,
                end_ms=end_ms,
                limit=KL_LIMIT,
            )
        except Exception as e:
            skipped += 1
            print(f"SKIP klines API | run_id={row.get('run_id')} symbol={symbol} err={e}")
            continue

        if candles is None or candles.empty:
            skipped += 1
            print(f"SKIP empty klines | run_id={row.get('run_id')} symbol={symbol}")
            continue

        candles = candles.copy()
        candles["ts"] = pd.to_datetime(candles["ts"], utc=True, errors="coerce")
        candles = candles.dropna(subset=["ts"])
        end_ts = entry_ts + pd.Timedelta(minutes=WATCH_MINUTES)
        future = candles[candles["ts"] >= entry_ts].copy()

        outcome, exit_price, mfe_pct, mae_pct = _replay_short_outcome_tracker_style(
            future,
            entry_price=ep,
            tp_price=tp,
            sl_price=sl,
            end_ts=end_ts,
        )

        if outcome == "TIMEOUT":
            still_timeout += 1
            continue

        pnl_pct = (ep - exit_price) / ep * 100.0 if ep > 0 else 0.0

        idx_updates[int(idx)] = {
            "outcome": outcome,
            "close_reason": outcome,
            "exit_price": exit_price,
            "pnl_pct": pnl_pct,
            "mfe_pct": mfe_pct,
            "mae_pct": mae_pct,
        }
        changed_run_ids.append(str(row.get("run_id", "") or ""))
        if outcome == "TP_hit":
            changed_tp += 1
        elif outcome == "SL_hit":
            changed_sl += 1

    n_changed = changed_tp + changed_sl

    if not args.dry_run and idx_updates:
        backup_name = f"trading_closes_backup_{datetime.now(timezone.utc).strftime('%Y%m%d')}.csv"
        backup_path = datasets_root / backup_name
        shutil.copy2(closes_path, backup_path)
        print(f"Backup written: {backup_path}")

        for i, upd in idx_updates.items():
            for k, v in upd.items():
                df.at[i, k] = v

        df.to_csv(closes_path, index=False)
        print(f"Updated: {closes_path}")
    elif args.dry_run:
        print("DRY-RUN: no files written")

    print("--- REPORT ---")
    print(f"rows checked (TIMEOUT + mfe=0 + mae=0): {n_checked}")
    print(f"skipped (no event / API / empty): {skipped}")
    print(f"changed total: {n_changed}  (TP_hit={changed_tp}, SL_hit={changed_sl}, still TIMEOUT={still_timeout})")
    print(f"changed run_id list ({len(changed_run_ids)}):")
    for r in changed_run_ids:
        print(f"  {r}")


if __name__ == "__main__":
    main()
