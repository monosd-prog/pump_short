# short_pump/watcher.py
from __future__ import annotations

import json
import time
from typing import Any, Dict, Optional

import pandas as pd

from short_pump.bybit_api import (
    get_klines_5m,
    get_klines_1m,
    get_open_interest,
    get_recent_trades,
)
from short_pump.config import Config
from short_pump.context5m import (
    StructureState,
    build_dbg5,
    compute_context_score_5m,
    update_structure,
)
from short_pump.entry import decide_entry_fast, decide_entry_1m
from short_pump.io_csv import append_csv
from short_pump.outcome import track_outcome_short
from short_pump.telegram import TG_SEND_OUTCOME, send_telegram


def _utc_now_str() -> str:
    return pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%d %H:%M:%S%z")


def run_watch_for_symbol(
    symbol: str,
    run_id: Optional[str] = None,
    meta: Optional[Dict[str, Any]] = None,
    cfg: Optional[Config] = None,
) -> Dict[str, Any]:
    cfg = cfg or Config()
    cfg.symbol = symbol.strip().upper()
    meta = meta or {}
    run_id = run_id or time.strftime("%Y%m%d_%H%M%S")

    print(f"=== WATCH START: {cfg.symbol} | run_id={run_id} | meta={meta} ===")

    log_5m = f"logs/{run_id}_{cfg.symbol}_5m.csv"
    log_1m = f"logs/{run_id}_{cfg.symbol}_1m.csv"
    log_fast = f"logs/{run_id}_{cfg.symbol}_fast.csv"
    log_summary = f"logs/{run_id}_{cfg.symbol}_summary.csv"

    st = StructureState()
    start_ts = pd.Timestamp.now(tz="UTC")
    end_ts = start_ts + pd.Timedelta(minutes=cfg.watch_minutes)

    pump_start_ts = None
    if meta.get("pump_ts"):
        try:
            pump_start_ts = pd.Timestamp(meta["pump_ts"])
            if pump_start_ts.tzinfo is None:
                pump_start_ts = pump_start_ts.tz_localize("UTC")
            else:
                pump_start_ts = pump_start_ts.tz_convert("UTC")
        except Exception:
            pump_start_ts = None

    last_5m_wall_write = 0.0
    last_1m_wall_write = 0.0

    entry_ok = False
    entry_payload: Dict[str, Any] = {}

    try:
        while pd.Timestamp.now(tz="UTC") < end_ts:
            # =====================
            # 5m CONTEXT LOOP
            # =====================
            try:
                candles_5m = get_klines_5m(cfg.category, cfg.symbol, limit=250)
                if candles_5m.empty:
                    time.sleep(10)
                    continue

                if pump_start_ts is not None:
                    after = candles_5m[candles_5m["ts"] >= pump_start_ts]
                    peak_price = float(after["high"].max()) if not after.empty else float(
                        candles_5m["high"].tail(20).max()
                    )
                else:
                    peak_price = float(candles_5m["high"].tail(20).max())

                last_price = float(candles_5m.iloc[-1]["close"])
                st = update_structure(cfg, st, last_price, peak_price)

                oi = get_open_interest(cfg.category, cfg.symbol, limit=80)
                trades = get_recent_trades(cfg.category, cfg.symbol, limit=1000)

                dbg5 = build_dbg5(cfg, candles_5m, oi, trades, st)
                context_score, ctx_parts = compute_context_score_5m(dbg5)

                candle_ts = pd.Timestamp(dbg5["time_utc"])
                if candle_ts.tzinfo is None:
                    candle_ts = candle_ts.tz_localize("UTC")
                lag_sec = (pd.Timestamp.now(tz="UTC") - candle_ts).total_seconds()

                # write 5m log ~ once per 5m candle
                if time.time() - last_5m_wall_write >= 5:
                    append_csv(
                        log_5m,
                        {
                            "run_id": run_id,
                            "symbol": cfg.symbol,
                            "time_utc": dbg5["time_utc"],
                            "stage": dbg5["stage"],
                            "price": dbg5["price"],
                            "peak_price": dbg5["peak_price"],
                            "dist_to_peak_pct": dbg5["dist_to_peak_pct"],
                            "oi_change_15m_pct": dbg5.get("oi_change_15m_pct"),
                            "oi_divergence": dbg5.get("oi_divergence"),
                            "vol_z": dbg5.get("vol_z"),
                            "atr_14_5m_pct": dbg5.get("atr_14_5m_pct"),
                            "context_score": context_score,
                            "wall_time_utc": _utc_now_str(),
                            "candle_lag_sec": lag_sec,
                            "context_parts": json.dumps(ctx_parts, ensure_ascii=False),
                        },
                    )
                    last_5m_wall_write = time.time()

            except Exception as e:
                print(f"Error: {repr(e)}")
                time.sleep(5)
                continue

            # =====================
            # ARM / 1m / FAST
            # =====================
            if st.stage >= 3 and not entry_ok:
                # ARM notify once
                if not st.armed_notified:
                    st.armed_notified = True
                    send_telegram(
                        f"🟡 ARMED: {cfg.symbol}\n"
                        f"run_id={run_id}\n"
                        f"stage={st.stage}\n"
                        f"context_score={context_score:.2f}\n"
                        f"dist_to_peak={dbg5.get('dist_to_peak_pct'):.2f}%"
                    )

                # 1m polling
                try:
                    candles_1m = get_klines_1m(cfg.category, cfg.symbol, limit=250)
                    if not candles_1m.empty and (time.time() - last_1m_wall_write >= 3):
                        entry_ok, entry_payload = decide_entry_1m(cfg, candles_1m)
                        append_csv(
                            log_1m,
                            {
                                "run_id": run_id,
                                "symbol": cfg.symbol,
                                "time_utc": str(candles_1m.iloc[-1]["ts"]),
                                "price": float(candles_1m.iloc[-1]["close"]),
                                "entry_ok": bool(entry_ok),
                                "entry_payload": json.dumps(entry_payload, ensure_ascii=False),
                            },
                        )
                        last_1m_wall_write = time.time()
                except Exception as e:
                    print(f"Error: {repr(e)}")

                # fast polling inside ARMED
                try:
                    trades_fast = get_recent_trades(cfg.category, cfg.symbol, limit=1000)
                    ok_fast, payload_fast = decide_entry_fast(cfg, trades_fast)
                    append_csv(
                        log_fast,
                        {
                            "run_id": run_id,
                            "symbol": cfg.symbol,
                            "wall_time_utc": _utc_now_str(),
                            "entry_ok": bool(ok_fast),
                            "entry_payload": json.dumps(payload_fast, ensure_ascii=False),
                        },
                    )
                    if ok_fast:
                        entry_ok = True
                        entry_payload = payload_fast
                except Exception as e:
                    print(f"Error: {repr(e)}")

            # =====================
            # ENTRY OK → OUTCOME
            # =====================
            if entry_ok:
                send_telegram(
                    f"✅ ENTRY OK: {cfg.symbol}\n"
                    f"run_id={run_id}\n"
                    f"{json.dumps(entry_payload, ensure_ascii=False)}"
                )

                summary = track_outcome_short(
                    cfg=cfg,
                    symbol=cfg.symbol,
                    run_id=run_id,
                    entry_payload=entry_payload,
                    meta=meta,
                )
                append_csv(log_summary, summary)

                if TG_SEND_OUTCOME:
                    send_telegram(
                        f"🏁 OUTCOME: {cfg.symbol}\n"
                        f"run_id={run_id}\n"
                        f"{summary.get('outcome')} | {summary.get('end_reason')}"
                    )

                return summary

            time.sleep(cfg.poll_seconds)

        # timeout
        summary = {
            "run_id": run_id,
            "symbol": cfg.symbol,
            "end_reason": f"TIMEOUT_STAGE_{st.stage}",
        }
        append_csv(log_summary, summary)
        return summary

    except KeyboardInterrupt:
        summary = {
            "run_id": run_id,
            "symbol": cfg.symbol,
            "end_reason": "INTERRUPTED",
        }
        append_csv(log_summary, summary)
        return summary