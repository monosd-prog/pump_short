#!/usr/bin/env python3
"""
Live Bybit WS + REST collector for pump_v2 raw fixtures.

Writes under pump_v2/tests/fixtures/live_collected/{symbol_lower}/.
Does not modify v1 code or stop pump-liquidations-collector.
"""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import logging
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, DefaultDict, Dict, List, Optional, Set

import pandas as pd

try:
    import websockets
    from websockets.exceptions import ConnectionClosed, WebSocketException
except ImportError as e:  # pragma: no cover
    print("Missing dependency: pip install websockets", file=sys.stderr)
    raise SystemExit(1) from e

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from short_pump.bybit_api import (  # noqa: E402
    get_funding_rate,
    get_klines_1m_range,
    get_klines_5m_range,
    get_open_interest,
)

COLLECT_DURATION_MIN = 45
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT", "LINKUSDT"]
CATEGORY = "linear"
WS_URL = "wss://stream.bybit.com/v5/public/linear"
OUT_ROOT = Path(__file__).resolve().parent / "live_collected"
LIQ_CSV = ROOT / "datasets" / "liquidations.csv"

TRADE_FLUSH_SEC = 60.0
OI_POLL_SEC = 30.0
KLINE_POLL_SEC = 300.0
PROGRESS_SEC = 300.0
REST_SLEEP = 0.12

TRADE_COLS = ["ts_utc", "ts_ms", "side", "price", "qty", "trade_id"]
OI_COLS = ["ts_utc", "ts_ms", "oi"]
KLINE_COLS = ["ts_utc", "open", "high", "low", "close", "volume"]

log = logging.getLogger("live_collect")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _sym_dir(symbol: str) -> Path:
    d = OUT_ROOT / symbol.lower()
    d.mkdir(parents=True, exist_ok=True)
    return d


def _parse_trade_row(symbol: str, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    ts_ms = item.get("T") or item.get("time") or item.get("timestamp")
    if ts_ms is None:
        return None
    ts_ms = int(ts_ms)
    side = str(item.get("S") or item.get("side") or "")
    price = float(item.get("p") or item.get("price") or 0)
    qty = float(item.get("v") or item.get("size") or item.get("qty") or 0)
    trade_id = str(item.get("i") or item.get("tradeId") or f"{ts_ms}_{side}_{price}_{qty}")
    return {
        "ts_utc": pd.Timestamp(ts_ms, unit="ms", tz="UTC").isoformat(),
        "ts_ms": ts_ms,
        "side": side,
        "price": price,
        "qty": qty,
        "trade_id": trade_id,
    }


def _append_parquet(path: Path, rows: List[Dict[str, Any]], cols: List[str], dedupe: Optional[str] = None) -> int:
    if not rows:
        return 0
    new_df = pd.DataFrame(rows)
    for c in cols:
        if c not in new_df.columns:
            new_df[c] = pd.NA
    new_df = new_df[cols]
    if path.is_file():
        try:
            old = pd.read_parquet(path)
            merged = pd.concat([old, new_df], ignore_index=True)
        except Exception:
            merged = new_df
    else:
        merged = new_df
    if dedupe and dedupe in merged.columns:
        merged = merged.drop_duplicates(subset=[dedupe], keep="last")
    if "ts_ms" in merged.columns:
        merged = merged.sort_values("ts_ms").reset_index(drop=True)
    elif "ts_utc" in merged.columns:
        merged["ts_utc"] = pd.to_datetime(merged["ts_utc"], utc=True)
        merged = merged.sort_values("ts_utc").reset_index(drop=True)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        merged.to_parquet(path, index=False, engine="pyarrow")
    except Exception:
        merged.to_parquet(path, index=False)
    return len(merged)


class CollectorState:
    def __init__(self, symbols: List[str]) -> None:
        self.symbols = [s.upper() for s in symbols]
        self.trade_bufs: DefaultDict[str, List[Dict[str, Any]]] = defaultdict(list)
        self.oi_bufs: DefaultDict[str, List[Dict[str, Any]]] = defaultdict(list)
        self.kl1_bufs: DefaultDict[str, List[Dict[str, Any]]] = defaultdict(list)
        self.kl5_bufs: DefaultDict[str, List[Dict[str, Any]]] = defaultdict(list)
        self.funding: Dict[str, Dict[str, Any]] = {}
        self.seen_trade_ids: DefaultDict[str, Set[str]] = defaultdict(set)
        self.stats: Dict[str, Dict[str, int]] = {s: {"n_trades": 0, "n_oi_snapshots": 0, "n_liquidations": 0} for s in self.symbols}
        self.start_utc: Optional[datetime] = None
        self.end_utc: Optional[datetime] = None
        self._lock = asyncio.Lock()

    async def add_trades(self, symbol: str, items: List[Dict[str, Any]]) -> None:
        sym = symbol.upper()
        added = 0
        async with self._lock:
            for it in items:
                row = _parse_trade_row(sym, it)
                if not row:
                    continue
                tid = row["trade_id"]
                if tid in self.seen_trade_ids[sym]:
                    continue
                self.seen_trade_ids[sym].add(tid)
                self.trade_bufs[sym].append(row)
                added += 1
        if added:
            async with self._lock:
                self.stats[sym]["n_trades"] = len(self.seen_trade_ids[sym])

    async def add_ticker(self, symbol: str, data: Dict[str, Any]) -> None:
        sym = symbol.upper()
        async with self._lock:
            self.funding[sym] = dict(data)

    async def flush_trades(self) -> None:
        async with self._lock:
            pending = {s: list(self.trade_bufs[s]) for s in self.symbols}
            for s in self.symbols:
                self.trade_bufs[s].clear()
        for sym, rows in pending.items():
            if not rows:
                continue
            path = _sym_dir(sym) / "trades.parquet"
            n = _append_parquet(path, rows, TRADE_COLS, dedupe="trade_id")
            async with self._lock:
                self.stats[sym]["n_trades"] = n

    async def poll_oi(self) -> None:
        now = _utcnow()
        ts_ms = int(now.timestamp() * 1000)
        for sym in self.symbols:
            try:
                df = get_open_interest(CATEGORY, sym, limit=1)
                await asyncio.sleep(REST_SLEEP)
            except Exception as e:
                log.warning("OI poll failed %s: %s", sym, e)
                continue
            if df is None or df.empty:
                continue
            row = df.iloc[-1]
            oi_val = float(row.get("openInterest", 0))
            snap = {
                "ts_utc": now.isoformat(),
                "ts_ms": ts_ms,
                "oi": oi_val,
            }
            async with self._lock:
                self.oi_bufs[sym].append(snap)
                self.stats[sym]["n_oi_snapshots"] += 1

    async def flush_oi(self) -> None:
        async with self._lock:
            pending = {s: list(self.oi_bufs[s]) for s in self.symbols}
            for s in self.symbols:
                self.oi_bufs[s].clear()
        for sym, rows in pending.items():
            if not rows:
                continue
            path = _sym_dir(sym) / "oi_history.parquet"
            _append_parquet(path, rows, OI_COLS, dedupe="ts_ms")

    def _klines_to_rows(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        if df is None or df.empty:
            return []
        out: List[Dict[str, Any]] = []
        for _, r in df.iterrows():
            ts = pd.Timestamp(r["ts"])
            if ts.tzinfo is None:
                ts = ts.tz_localize("UTC")
            out.append(
                {
                    "ts_utc": ts.isoformat(),
                    "open": float(r["open"]),
                    "high": float(r["high"]),
                    "low": float(r["low"]),
                    "close": float(r["close"]),
                    "volume": float(r["volume"]),
                }
            )
        return out

    async def poll_klines(self) -> None:
        end_ms = int(_utcnow().timestamp() * 1000)
        start_ms = end_ms - 3 * 3600 * 1000
        for sym in self.symbols:
            try:
                k1 = get_klines_1m_range(CATEGORY, sym, start_ms=start_ms, end_ms=end_ms, limit=200)
                await asyncio.sleep(REST_SLEEP)
                k5 = get_klines_5m_range(CATEGORY, sym, start_ms=start_ms, end_ms=end_ms, limit=200)
                await asyncio.sleep(REST_SLEEP)
            except Exception as e:
                log.warning("Kline poll failed %s: %s", sym, e)
                continue
            async with self._lock:
                self.kl1_bufs[sym].extend(self._klines_to_rows(k1))
                self.kl5_bufs[sym].extend(self._klines_to_rows(k5))

    async def flush_klines(self) -> None:
        async with self._lock:
            k1p = {s: list(self.kl1_bufs[s]) for s in self.symbols}
            k5p = {s: list(self.kl5_bufs[s]) for s in self.symbols}
            for s in self.symbols:
                self.kl1_bufs[s].clear()
                self.kl5_bufs[s].clear()
        for sym in self.symbols:
            if k1p[sym]:
                _append_parquet(_sym_dir(sym) / "klines_1m.parquet", k1p[sym], KLINE_COLS, dedupe="ts_utc")
            if k5p[sym]:
                _append_parquet(_sym_dir(sym) / "klines_5m.parquet", k5p[sym], KLINE_COLS, dedupe="ts_utc")

    async def save_funding_rest(self) -> None:
        for sym in self.symbols:
            try:
                payload = get_funding_rate(CATEGORY, sym)
                await asyncio.sleep(REST_SLEEP)
            except Exception as e:
                log.warning("Funding REST failed %s: %s", sym, e)
                payload = None
            async with self._lock:
                ws_snap = self.funding.get(sym) or {}
            out = {
                "ws_ticker_last": ws_snap,
                "rest_tickers": payload,
                "fetched_at_utc": _utcnow().isoformat(),
            }
            (_sym_dir(sym) / "funding.json").write_text(json.dumps(out, indent=2), encoding="utf-8")

    def progress_line(self, elapsed_min: float) -> str:
        parts = [f"[T+{elapsed_min:.0f}min]"]
        for sym in self.symbols:
            st = self.stats[sym]
            parts.append(
                f"{sym}: {st['n_trades']} trades, {st['n_oi_snapshots']} OI snapshots, "
                f"{st['n_liquidations']} liquidations"
            )
        return "\n  ".join(parts)

    async def slice_liquidations(self) -> None:
        if not LIQ_CSV.is_file():
            for sym in self.symbols:
                pd.DataFrame(columns=["ts_utc", "ts_ms", "symbol", "side", "qty", "price", "value_usd"]).to_csv(
                    _sym_dir(sym) / "liquidations.csv", index=False
                )
            return
        df = pd.read_csv(LIQ_CSV)
        if df.empty or self.start_utc is None or self.end_utc is None:
            return
        df["ts"] = pd.to_datetime(df["ts_utc"], utc=True)
        ws = pd.Timestamp(self.start_utc)
        we = pd.Timestamp(self.end_utc)
        for sym in self.symbols:
            w = df[
                (df["symbol"].astype(str).str.upper() == sym)
                & (df["ts"] >= ws)
                & (df["ts"] <= we)
            ]
            w.to_csv(_sym_dir(sym) / "liquidations.csv", index=False)
            async with self._lock:
                self.stats[sym]["n_liquidations"] = len(w)


async def ws_consumer(state: CollectorState, stop_event: asyncio.Event) -> None:
    args: List[str] = []
    for sym in state.symbols:
        args.append(f"publicTrade.{sym}")
        args.append(f"tickers.{sym}")
    assert len(args) <= 10, f"subscribe args={len(args)} exceeds Bybit limit 10"

    backoff = 2.0
    while not stop_event.is_set():
        try:
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=60, close_timeout=10) as ws:
                await ws.send(json.dumps({"op": "subscribe", "args": args}))
                log.info("WS subscribed: %s", args)
                backoff = 2.0
                while not stop_event.is_set():
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=30.0)
                    except asyncio.TimeoutError:
                        await ws.send(json.dumps({"op": "ping"}))
                        continue
                    msg = json.loads(raw)
                    if msg.get("op") == "pong" or msg.get("success") is True:
                        continue
                    topic = str(msg.get("topic", ""))
                    data = msg.get("data")
                    if not topic or data is None:
                        continue
                    if topic.startswith("publicTrade."):
                        sym = topic.split(".", 1)[1].upper()
                        items = data if isinstance(data, list) else [data]
                        await state.add_trades(sym, items)
                    elif topic.startswith("tickers."):
                        sym = topic.split(".", 1)[1].upper()
                        payload = data if isinstance(data, dict) else (data[0] if isinstance(data, list) and data else {})
                        if payload:
                            await state.add_ticker(sym, payload)
        except (ConnectionClosed, WebSocketException, OSError, asyncio.TimeoutError) as e:
            if stop_event.is_set():
                break
            log.warning("WS reconnect after: %s", e)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 1.5, 30.0)


async def periodic_flush(state: CollectorState, stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        await asyncio.sleep(TRADE_FLUSH_SEC)
        await state.flush_trades()
        await state.flush_oi()


async def periodic_oi(state: CollectorState, stop_event: asyncio.Event) -> None:
    await state.poll_oi()
    while not stop_event.is_set():
        await asyncio.sleep(OI_POLL_SEC)
        await state.poll_oi()


async def periodic_klines(state: CollectorState, stop_event: asyncio.Event) -> None:
    await state.poll_klines()
    await state.flush_klines()
    while not stop_event.is_set():
        await asyncio.sleep(KLINE_POLL_SEC)
        await state.poll_klines()
        await state.flush_klines()


async def periodic_progress(
    state: CollectorState, stop_event: asyncio.Event, duration_sec: float, eta_utc: str
) -> None:
    t0 = time.monotonic()
    await asyncio.sleep(PROGRESS_SEC)
    while not stop_event.is_set():
        elapsed = (time.monotonic() - t0) / 60.0
        line = state.progress_line(elapsed)
        log.info("%s (ETA %s)", line, eta_utc)
        print(line, flush=True)
        await asyncio.sleep(PROGRESS_SEC)


async def run_collection(duration_sec: float, dry_run: bool) -> None:
    state = CollectorState(SYMBOLS)
    state.start_utc = _utcnow()
    eta = state.start_utc + pd.Timedelta(seconds=duration_sec)
    eta_str = eta.strftime("%H:%M UTC")

    if OUT_ROOT.exists() and any(OUT_ROOT.iterdir()):
        log.warning("Output dir non-empty: %s (will merge/overwrite parquet)", OUT_ROOT)

    mode = "DRY-RUN 60s" if dry_run else f"{int(duration_sec / 60)}-minute"
    banner = (
        f"Starting {mode} live collection. ETA: {eta_str}. "
        f"Will report progress every {int(PROGRESS_SEC / 60)} minutes."
    )
    log.info(banner)
    print(banner, flush=True)

    stop_event = asyncio.Event()
    tasks = [
        asyncio.create_task(ws_consumer(state, stop_event), name="ws"),
        asyncio.create_task(periodic_flush(state, stop_event), name="flush"),
        asyncio.create_task(periodic_oi(state, stop_event), name="oi"),
        asyncio.create_task(periodic_klines(state, stop_event), name="klines"),
        asyncio.create_task(periodic_progress(state, stop_event, duration_sec, eta_str), name="progress"),
    ]
    await asyncio.sleep(duration_sec)
    stop_event.set()
    for t in tasks:
        t.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await t

    await state.flush_trades()
    await state.flush_oi()
    await state.poll_klines()
    await state.flush_klines()
    await state.save_funding_rest()

    state.end_utc = _utcnow()
    await state.slice_liquidations()

    meta = {
        "collection_start_utc": state.start_utc.isoformat(),
        "collection_end_utc": state.end_utc.isoformat(),
        "duration_minutes": round(duration_sec / 60.0, 2),
        "dry_run": dry_run,
        "symbols": SYMBOLS,
        "stats": state.stats,
    }
    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    (OUT_ROOT / "meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
    log.info("Collection complete. meta.json written.")
    print(f"Collection complete → {OUT_ROOT / 'meta.json'}", flush=True)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    ap = argparse.ArgumentParser(description="Live Bybit fixture collector")
    ap.add_argument("--dry-run", action="store_true", help="Collect 60 seconds only (connectivity check)")
    ap.add_argument("--duration-min", type=float, default=None, help="Override COLLECT_DURATION_MIN")
    args = ap.parse_args()
    if args.dry_run:
        duration_sec = 60.0
    else:
        dm = args.duration_min if args.duration_min is not None else COLLECT_DURATION_MIN
        duration_sec = float(dm) * 60.0
    asyncio.run(run_collection(duration_sec, dry_run=args.dry_run))


if __name__ == "__main__":
    main()
