#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional

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
) -> pd.DataFrame:
    """
    Fetch ~days of 1m candles with simple pagination (Bybit limit is typically 1000 rows).
    """
    end = _utc_now()
    start = end - pd.Timedelta(days=int(days))
    start_ms = _ms(start)
    end_ms = _ms(end)

    frames: list[pd.DataFrame] = []
    cursor = start_ms
    guard = 0
    while cursor < end_ms and guard < 1000:
        guard += 1
        df = get_klines_1m_range(category, symbol, start_ms=cursor, end_ms=end_ms, limit=limit)
        if df is None or df.empty:
            break
        df = df.sort_values("ts").reset_index(drop=True)
        frames.append(df)
        last_ts = pd.Timestamp(df["ts"].iloc[-1])
        next_cursor = int(last_ts.timestamp() * 1000) + 1
        if next_cursor <= cursor:
            break
        cursor = next_cursor
        if sleep_ms > 0:
            time.sleep(sleep_ms / 1000.0)

        # Stop early if we reached end
        if last_ts >= end:
            break

        # If we got fewer than limit rows, likely no more data in range
        if len(df) < limit:
            break

    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["ts"]).sort_values("ts").reset_index(drop=True)
    return out[(out["ts"] >= start) & (out["ts"] <= end)].reset_index(drop=True)


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
) -> list[LabSignal]:
    df = _fetch_1m_klines_days(category=category, symbol=symbol, days=days)
    if df is None or df.empty or len(df) <= lookback_min:
        return []

    close = pd.to_numeric(df["close"], errors="coerce")
    prev = close.shift(lookback_min)
    pct = (close / prev - 1.0) * 100.0
    cond = pct >= float(threshold_pct)
    if edge_trigger:
        fire = cond & (~cond.shift(1).fillna(False))
    else:
        fire = cond

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

