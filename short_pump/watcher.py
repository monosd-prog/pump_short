import time
import json
from typing import Optional, Dict, Any

import pandas as pd
from short_pump.features import atr_pct
from short_pump.config import Config
from short_pump.telegram import send_telegram, TG_SEND_OUTCOME
from short_pump.io_csv import append_csv
from short_pump.bybit_api import (
    get_klines_5m,
    get_klines_1m,
    get_open_interest,
    get_recent_trades,
)
from short_pump.context5m import (
    StructureState,
    update_structure,
    build_dbg5,
    compute_context_score_5m,
)
from short_pump.entry import decide_entry_1m, decide_entry_fast
from short_pump.outcome import track_outcome_short

# =========================
# Main runner (adapted)
# =========================
def run_watch_for_symbol(symbol: str, run_id: str, meta: Dict[str, Any]) -> Dict[str, Any]:
    cfg = Config(symbol=symbol)
    summary_log = "mvp_runs_summary.csv"

    pump_start_ts = pd.Timestamp.now(tz="UTC")
    end_ts = pump_start_ts + pd.Timedelta(minutes=cfg.watch_minutes)

    st = StructureState()
    armed_once = False
    cached_1m: Optional[pd.DataFrame] = None
    last_1m_fetch: float = 0.0

    # ✅ уникальные имена файлов
    log_5m = f"mvp_log_{cfg.symbol}_{run_id}.csv"
    log_1m = f"mvp_log_1m_{cfg.symbol}_{run_id}.csv"

    print(f"=== WATCH START: {cfg.symbol} | run_id={run_id} | meta={meta} ===")

    while pd.Timestamp.now(tz="UTC") < end_ts:
        try:
            candles_5m = get_klines_5m(cfg.category, cfg.symbol, limit=250)
            if candles_5m.empty:
                time.sleep(10)
                continue

            after = candles_5m[candles_5m["ts"] >= pump_start_ts]
            peak_price = float(after["high"].max()) if not after.empty else float(candles_5m["high"].tail(20).max())

            last_price = float(candles_5m.iloc[-1]["close"])
            st = update_structure(cfg, st, last_price, peak_price)

            oi = get_open_interest(cfg.category, cfg.symbol, limit=80)
            trades = get_recent_trades(cfg.category, cfg.symbol, limit=1000)

            dbg5 = build_dbg5(cfg, candles_5m, oi, trades, st)
            context_score, ctx_parts = compute_context_score_5m(dbg5)

            # Lag diagnostics: how old is the latest 5m candle vs wall clock
            candle_ts = pd.Timestamp(dbg5["time_utc"])
            if candle_ts.tzinfo is None:
                candle_ts = candle_ts.tz_localize("UTC")
            lag_sec = (pd.Timestamp.now(tz="UTC") - candle_ts).total_seconds()

            # --- wall-clock lag for debugging (how stale is the last 5m candle) ---
            now_utc = pd.Timestamp.now(tz="UTC")

            candle_ts = None
            lag_sec = None
            try:
                candle_ts = pd.Timestamp(dbg5.get("time_utc"))
                if pd.isna(candle_ts):
                    candle_ts = None
                else:
                    if candle_ts.tzinfo is None:
                        candle_ts = candle_ts.tz_localize("UTC")
                    else:
                        candle_ts = candle_ts.tz_convert("UTC")
                    lag_sec = float((now_utc - candle_ts).total_seconds())
            except Exception:
                candle_ts = None
                lag_sec = None

            append_csv(log_5m, {
                "run_id": run_id,
                "symbol": cfg.symbol,
                "time_utc": dbg5["time_utc"],
                "stage": dbg5["stage"],
                "price": dbg5["price"],
                "peak_price": dbg5["peak_price"],
                "dist_to_peak_pct": dbg5["dist_to_peak_pct"],
                "oi_change_15m_pct": dbg5["oi_change_15m_pct"],
                "oi_divergence": dbg5["oi_divergence"],
                "delta_ratio_15m": dbg5["delta_ratio_15m"],
                "vol_z": dbg5["vol_z"],
                "atr_14_5m_pct": dbg5.get("atr_14_5m_pct"),
                "context_score": context_score,
                "wall_time_utc": pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%d %H:%M:%S%z"),
                "candle_lag_sec": lag_sec,
                "context_parts": json.dumps(ctx_parts, ensure_ascii=False),
            })

            if (not armed_once) and (dbg5["stage"] >= 3):
                armed_once = True

            if armed_once:
                # fast loop: fetch trades often, klines_1m only once per minute (reduces API load)
                trades_1m = get_recent_trades(cfg.category, cfg.symbol, limit=1000)

                now_epoch = time.time()
                if (cached_1m is None) or (now_epoch - last_1m_fetch >= cfg.poll_seconds_1m):
                    cached_1m = get_klines_1m(cfg.category, cfg.symbol, limit=300)
                    last_1m_fetch = now_epoch

                candles_1m = cached_1m

                entry_ok, dbg1 = decide_entry_1m(
                    cfg=cfg,
                    atr_14_1m=atr_pct(candles_1m, period=14),
                    candles_1m=candles_1m,
                    trades=trades_1m,
                    context_score=context_score,
                    ctx_parts=ctx_parts,
                    peak_price_5m=float(dbg5["peak_price"])
                )

                # fast path: allow earlier signal on strong selling pressure before 1m candle closes
                if not entry_ok:
                    entry_ok_fast, dbg_fast = decide_entry_fast(
                        cfg=cfg,
                        trades=trades_1m,
                        context_score=context_score,
                        ctx_parts=ctx_parts,
                        peak_price_5m=float(dbg5["peak_price"])
                    )
                    if entry_ok_fast and "reason" not in dbg_fast:
                        entry_ok = True
                        dbg1 = dbg_fast

                if "reason" not in dbg1:
                    entry_type = None
                    if entry_ok:
                        entry_type = "CONFIRM" if dbg1["flags"]["break_low"] else "EARLY"

                    append_csv(log_1m, {
                        "run_id": run_id,
                        "symbol": cfg.symbol,
                        "time_utc": dbg1["time_utc"],
                        "decision": "ENTRY_OK" if entry_ok else "WAIT",
                        "entry_type": entry_type,
                        "price": dbg1["price"],

                        "dist_to_peak_pct": dbg1["dist_to_peak_pct"],
                        "delta_ratio_1m": dbg1.get("delta_ratio_1m"),
                        "delta_ratio_3m": dbg1.get("delta_ratio_3m"),
                        "delta_ratio_30s": dbg1.get("delta_ratio_30s"),
                        "entry_source": dbg1.get("entry_source", "1m"),
                        "need": dbg1.get("need"),
                        "hit": dbg1.get("hit"),
                        "break_low": dbg1["flags"]["break_low"],
                        "delta_ok": dbg1["flags"]["delta_ok"],
                        "no_new_high": dbg1["flags"]["no_new_high"],
                        "near_top": dbg1["flags"]["near_top"],
                        "context_score": dbg1["context_score"],
                        "stage_5m": dbg5["stage"],
                        "late_mode": dbg1.get("late_mode"),
                        "context_parts": json.dumps(ctx_parts, ensure_ascii=False),
                    })

                    if entry_ok and (cfg.stop_on == "ANY" or (cfg.stop_on == "CONFIRM" and entry_type == "CONFIRM")):
                        if entry_type == "CONFIRM":
                            tp = cfg.tp_pct_confirm
                            sl = cfg.sl_pct_confirm
                        else:
                            tp = cfg.tp_pct_early
                            sl = cfg.sl_pct_early

                        entry_price = dbg1["price"]
                        tp_price = entry_price * (1 - tp)
                        sl_price = entry_price * (1 + sl)

                        # сразу сигнал в TG (ENTRY_OK)
                        send_telegram(
                            f"✅ SHORT SIGNAL {cfg.symbol} ({entry_type})\n"
                            f"time(UTC): {dbg1['time_utc']}\n"
                            f"entry: {entry_price}\n"
                            f"TP: {tp_price} (-{tp*100:.2f}%)\n"
                            f"SL: {sl_price} (+{sl*100:.2f}%)\n"
                            f"context: {context_score:.2f} | stage={dbg5['stage']} | dist={dbg1['dist_to_peak_pct']:.2f}%\n"
                            f"late={dbg1.get('late_mode')}"
                        )

                        entry_ts_utc = pd.Timestamp(dbg1["time_utc"])
                        if entry_ts_utc.tzinfo is None:
                            entry_ts_utc = entry_ts_utc.tz_localize("UTC")
                        else:
                            entry_ts_utc = entry_ts_utc.tz_convert("UTC")

                        # Import here to avoid circular import during refactor
                        from short_pump.outcome import track_outcome_short
                        out = track_outcome_short(cfg, entry_ts_utc, entry_price, tp_price, sl_price)

                        append_csv(summary_log, {
                            "run_id": run_id,
                            "symbol": cfg.symbol,
                            "end_reason": "ENTRY_OK",
                            "entry_time_utc": dbg1["time_utc"],
                            "entry_price": entry_price,
                            "entry_type": entry_type,
                            "tp_price": tp_price,
                            "sl_price": sl_price,
                            "tp_pct": tp,
                            "sl_pct": sl,
                            "context_score": context_score,
                            "stage_5m": dbg5["stage"],
                            "dist_to_peak_pct": dbg1["dist_to_peak_pct"],
                            "late_mode": dbg1.get("late_mode"),
                            "outcome": out["outcome"],
                            "hit_time_utc": out["hit_time_utc"],
                            "minutes_to_hit": out["minutes_to_hit"],
                            "mfe_pct": out["mfe_pct"],
                            "mae_pct": out["mae_pct"],
                            "timeout_exit_price": out.get("timeout_exit_price"),
                            "timeout_pnl_pct": out.get("timeout_pnl_pct"),
                            "entry_source": dbg1.get("entry_source", "1m"),
                            "delta_ratio_30s": dbg1.get("delta_ratio_30s"),
                            "context_parts": json.dumps(ctx_parts, ensure_ascii=False),
                        })

                        if TG_SEND_OUTCOME:
                            lines = [
                                f"📌 OUTCOME {cfg.symbol}",
                                f"result: {out.get('outcome')}",
                            ]

                            mins = out.get("minutes_to_hit")
                            if isinstance(mins, (int, float)):
                                lines.append(f"minutes_to_hit: {mins:.1f}")

                            mfe = out.get("mfe_pct")
                            mae = out.get("mae_pct")
                            if isinstance(mfe, (int, float)) and isinstance(mae, (int, float)):
                                lines.append(f"MFE={mfe:.2f}% | MAE={mae:.2f}%")

                            if out.get("outcome") == "TIMEOUT":
                                exit_px = out.get("timeout_exit_price")
                                if isinstance(exit_px, (int, float)):
                                    lines.append(f"timeout_exit_price: {exit_px:.6g}")

                                tpnl = out.get("timeout_pnl_pct")
                                if isinstance(tpnl, (int, float)):
                                    lines.append(f"timeout_pnl_pct: {tpnl:.2f}%")

                            send_telegram("\n".join(lines))

                        return {
                            "run_id": run_id,
                            "symbol": cfg.symbol,
                            "end_reason": "ENTRY_OK",
                            "entry_time_utc": dbg1["time_utc"],
                            "entry_price": entry_price,
                            "entry_type": entry_type,
                            "tp_price": tp_price,
                            "sl_price": sl_price,
                            "context_score": context_score,
                            "stage_5m": dbg5["stage"],
                            "dist_to_peak_pct": dbg1["dist_to_peak_pct"],
                            "late_mode": dbg1.get("late_mode"),
                            "outcome": out["outcome"],
                            "hit_time_utc": out["hit_time_utc"],
                            "minutes_to_hit": out["minutes_to_hit"],
                            "mfe_pct": out["mfe_pct"],
                            "mae_pct": out["mae_pct"],
                            "timeout_exit_price": out.get("timeout_exit_price"),
                            "timeout_pnl_pct": out.get("timeout_pnl_pct"),
                            "entry_source": dbg1.get("entry_source", "1m"),
                            "delta_ratio_30s": dbg1.get("delta_ratio_30s"),
                            "log_5m": log_5m,
                            "log_1m": log_1m,
                        }

                time.sleep(cfg.poll_seconds_fast)
            else:
                time.sleep(cfg.poll_seconds)

        except Exception as e:
            print("Error:", repr(e))
            time.sleep(10)

    append_csv(summary_log, {
        "run_id": run_id,
        "symbol": cfg.symbol,
        "end_reason": f"TIMEOUT_STAGE_{st.stage}",
        "entry_time_utc": None,
        "entry_price": None,
        "entry_type": None,
        "tp_price": None,
        "sl_price": None,
        "tp_pct": None,
        "sl_pct": None,
        "context_score": None,
        "stage_5m": st.stage,
        "dist_to_peak_pct": None,
        "late_mode": None,
        "outcome": None,
        "hit_time_utc": None,
        "minutes_to_hit": None,
        "mfe_pct": None,
        "mae_pct": None,
        "timeout_exit_price": None,
        "timeout_pnl_pct": None,
        "entry_source": None,
        "delta_ratio_30s": None,
        "context_parts": None,
    })

    return {
        "run_id": run_id,
        "symbol": cfg.symbol,
        "end_reason": f"TIMEOUT_STAGE_{st.stage}",
        "log_5m": log_5m,
        "log_1m": log_1m,
    }


# локальный запуск (для дебага)
def run_watch_cli():
    sym = input("Symbol (e.g., FFUSDT): ").strip().upper() or "BTCUSDT"
    run_id = pd.Timestamp.now(tz="UTC").strftime("%Y%m%d_%H%M%S")
    return run_watch_for_symbol(sym, run_id, meta={"source": "cli"})


if __name__ == "__main__":
    res = run_watch_cli()
    print(res)