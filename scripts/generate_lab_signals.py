#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

import pandas as pd

# Ensure pump_short root on path
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from short_pump.bybit_api import get_klines_1m_range


@dataclass(frozen=True)
class LabSignal:
    strategy: str
    symbol: str
    signal_time_utc: str
    run_id: str
    event_id: str
    source: str


def _utc_now() -> pd.Timestamp:
    return pd.Timestamp.now(tz="UTC")


def _ms(ts: pd.Timestamp) -> int:
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return int(ts.timestamp() * 1000)


def _fetch_1m_klines_days(
    *,
    category: str,
    symbol: str,
    days: int,
    limit: int = 1000,
    sleep_ms: int = 0,
    debug: bool = False,
) -> pd.DataFrame:
    """
    Fetch ~days of 1m candles with simple pagination (Bybit limit is typically 1000 rows).
    """
    end = _utc_now()
    start = end - pd.Timedelta(days=int(days))
    start_ms = _ms(start)
    end_ms = _ms(end)

    frames: list[pd.DataFrame] = []
    # Page backwards from end_ms. Bybit often returns the most recent rows in-range,
    # so forward-pagination from start can get stuck on the newest 1000 candles only.
    end_cursor = end_ms
    guard = 0
    while end_cursor > start_ms and guard < 2000:
        guard += 1
        df = get_klines_1m_range(category, symbol, start_ms=None, end_ms=end_cursor, limit=limit)
        if df is None or df.empty:
            break
        df = df.sort_values("ts").reset_index(drop=True)
        frames.append(df)
        first_ts = pd.Timestamp(df["ts"].iloc[0])
        next_end = int(first_ts.timestamp() * 1000) - 1
        if debug:
            last_ts = pd.Timestamp(df["ts"].iloc[-1])
            print(
                f"[DEBUG_KLINES] symbol={symbol} chunk={guard} rows={len(df)} "
                f"first={first_ts.isoformat()} last={last_ts.isoformat()} next_end_ms={next_end}"
            )
        if next_end >= end_cursor:
            break
        end_cursor = next_end
        if sleep_ms > 0:
            time.sleep(sleep_ms / 1000.0)

        # If we got fewer than limit rows, likely no more older data
        if len(df) < limit:
            break

    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["ts"]).sort_values("ts").reset_index(drop=True)
    return out[(out["ts"] >= start) & (out["ts"] <= end)].reset_index(drop=True)

def _top_moves(
    df: pd.DataFrame, *, lookback_min: int, top_n: int
) -> list[tuple[str, float, float, float]]:
    """
    Returns list of (ts_iso, pct, close_now, close_prev) sorted by pct desc.
    """
    close = pd.to_numeric(df["close"], errors="coerce")
    prev = close.shift(lookback_min)
    pct = (close / prev - 1.0) * 100.0
    tmp = df.assign(_pct=pct, _prev=prev, _close=close).dropna(subset=["_pct", "_prev", "_close"])
    tmp = tmp.sort_values("_pct", ascending=False).head(int(top_n))
    out: list[tuple[str, float, float, float]] = []
    for _, r in tmp.iterrows():
        t = pd.Timestamp(r["ts"])
        if t.tzinfo is None:
            t = t.tz_localize("UTC")
        else:
            t = t.tz_convert("UTC")
        out.append(
            (
                t.isoformat().replace("+00:00", "Z"),
                float(r["_pct"]),
                float(r["_close"]),
                float(r["_prev"]),
            )
        )
    return out


def generate_signals_for_symbol(
    *,
    symbol: str,
    strategies: Iterable[str],
    days: int,
    lookback_min: int,
    threshold_pct: float,
    source: str,
    category: str = "linear",
    edge_trigger: bool = True,
    debug: bool = False,
    mode: str = "prodlike",
    top_k_explore: int = 50,
) -> list[LabSignal]:
    df = _fetch_1m_klines_days(category=category, symbol=symbol, days=days, debug=debug)
    if df is None or df.empty or len(df) <= lookback_min:
        if debug:
            print(f"[DEBUG] symbol={symbol} candles=0 (or <=lookback) days={days}")
        return []

    close = pd.to_numeric(df["close"], errors="coerce")
    prev = close.shift(lookback_min)
    pct = (close / prev - 1.0) * 100.0
    cond = pct >= float(threshold_pct)
    n_cond = int(cond.fillna(False).sum())
    if edge_trigger:
        fire = cond & (~cond.shift(1).fillna(False))
    else:
        fire = cond

    n_fire = int(fire.fillna(False).sum())
    if debug:
        min_ts = pd.Timestamp(df["ts"].iloc[0]).isoformat()
        max_ts = pd.Timestamp(df["ts"].iloc[-1]).isoformat()
        max_move = float(pd.to_numeric(pct, errors="coerce").max()) if pd.to_numeric(pct, errors="coerce").notna().any() else float("nan")
        print(f"[DEBUG] symbol={symbol} candles={len(df)} ts_min={min_ts} ts_max={max_ts}")
        print(f"[DEBUG] symbol={symbol} max_move_pct_over_{lookback_min}m={max_move:.2f}")
        top5 = _top_moves(df, lookback_min=lookback_min, top_n=5)
        print(f"[DEBUG] symbol={symbol} top5_moves_over_{lookback_min}m:")
        for t_iso, p, c_now, c_prev in top5:
            print(f"  - {t_iso}: {p:.2f}% (close {c_prev:.6g} -> {c_now:.6g})")
        print(f"[DEBUG] symbol={symbol} candidates_before_edge={n_cond} after_edge={n_fire} edge_trigger={edge_trigger}")

    # Explore mode: if prodlike yields nothing, emit top-K moves anyway to create a lab dataset.
    mode_norm = (mode or "prodlike").strip().lower()
    if mode_norm == "explore" and n_fire == 0:
        topk = _top_moves(df, lookback_min=lookback_min, top_n=top_k_explore)
        times = [pd.Timestamp(t) for t, *_ in topk]
    else:
        times = df.loc[fire.fillna(False), "ts"]

    if times is None or len(times) == 0:
        return []

    out: list[LabSignal] = []
    for i, ts in enumerate(times.tolist(), 1):
        t = pd.Timestamp(ts)
        if t.tzinfo is None:
            t = t.tz_localize("UTC")
        else:
            t = t.tz_convert("UTC")
        t_iso = t.isoformat().replace("+00:00", "Z")

        base_run = f"lab_{t.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
        for strat in strategies:
            run_id = f"{base_run}_{strat}"
            event_id = f"{run_id}_{symbol}_{i:06d}"
            out.append(
                LabSignal(
                    strategy=strat,
                    symbol=symbol,
                    signal_time_utc=t_iso,
                    run_id=run_id,
                    event_id=event_id,
                    source=source,
                )
            )
    return out


def write_signals_csv(path: str | Path, signals: List[LabSignal]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["strategy", "symbol", "signal_time_utc", "run_id", "event_id", "source"])
        for s in signals:
            w.writerow([s.strategy, s.symbol, s.signal_time_utc, s.run_id, s.event_id, s.source])


def main() -> None:
    ap = argparse.ArgumentParser(description="Generate lab replay signals from historical 1m candles (candidate generator)")
    ap.add_argument("--symbols", nargs="+", required=True, help="Symbols like BTCUSDT ETHUSDT SOLUSDT")
    ap.add_argument("--days", type=int, default=7, help="Lookback window in days (default: 7)")
    ap.add_argument("--threshold_pct", type=float, default=2.5, help="Pump threshold in % over lookback_min (default: 2.5)")
    ap.add_argument("--lookback_min", type=int, default=5, help="Lookback in minutes (default: 5)")
    ap.add_argument("--out-file", required=True, help="Output CSV path")
    ap.add_argument("--strategies", nargs="*", default=["short_pump", "short_pump_fast0"], help="Strategies to emit")
    ap.add_argument("--source", default="lab_generated", help="Source field in output (default: lab_generated)")
    ap.add_argument("--edge-trigger", action="store_true", help="Emit only on rising edge (default: off)")
    ap.add_argument("--debug", action="store_true", help="Print debug stats per symbol")
    ap.add_argument("--mode", default="prodlike", choices=["prodlike", "explore"], help="prodlike: threshold-based; explore: emit top moves when none")
    ap.add_argument("--top-k", type=int, default=50, help="Explore mode: emit top-K moves per symbol (default: 50)")
    args = ap.parse_args()

    symbols = [s.strip().upper() for s in args.symbols if s.strip()]
    strategies = [s.strip() for s in args.strategies if s.strip()]
    days = int(args.days)
    lookback_min = int(args.lookback_min)
    threshold_pct = float(args.threshold_pct)
    source = str(args.source)

    all_signals: list[LabSignal] = []
    per_symbol_counts: dict[str, int] = {}

    for sym in symbols:
        sigs = generate_signals_for_symbol(
            symbol=sym,
            strategies=strategies,
            days=days,
            lookback_min=lookback_min,
            threshold_pct=threshold_pct,
            source=source,
            edge_trigger=bool(args.edge_trigger),
            debug=bool(args.debug),
            mode=str(args.mode),
            top_k_explore=int(args.top_k),
        )
        per_symbol_counts[sym] = len(sigs)
        all_signals.extend(sigs)
        print(f"[LAB_SIGNALS] symbol={sym} signals={len(sigs)} days={days} lookback_min={lookback_min} threshold_pct={threshold_pct}")

    all_signals.sort(key=lambda s: (s.symbol, s.signal_time_utc, s.strategy))
    write_signals_csv(args.out_file, all_signals)

    print("")
    print(f"[LAB_SIGNALS] total_signals={len(all_signals)} out_file={args.out_file}")
    for sym in symbols:
        print(f"[LAB_SIGNALS] {sym}: {per_symbol_counts.get(sym, 0)}")


if __name__ == "__main__":
    main()

