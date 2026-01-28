import time
import math
import os
import csv
import json
import requests
import pandas as pd
import numpy as np
from dataclasses import dataclass
from typing import Optional, Tuple, Dict, Any

BYBIT_REST = "https://api.bybit.com"

TG_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TG_SEND_OUTCOME = os.getenv("TG_SEND_OUTCOME", "0") == "1"  # если хочешь 2-е сообщение с outcome


def send_telegram(text: str):
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TG_CHAT_ID, "text": text, "disable_web_page_preview": True}
    r = requests.post(url, json=payload, timeout=15)
    r.raise_for_status()


# =========================
# Config
# =========================
@dataclass
class Config:
    symbol: str = "BTCUSDT"
    category: str = "linear"

    watch_minutes: int = 90
    poll_seconds: int = 300  # 5m polling (used before ARMED)

    vol_z_lookback: int = 50

    # 5m structure thresholds (percent)
    drop1_min_pct: float = 0.03
    bounce1_min_pct: float = 0.012
    drop2_min_pct: float = 0.010
    bounce2_min_pct: float = 0.006

    # Allow deeper pullback from peak
    dist_to_peak_max_pct: float = 0.12

    # 1m cadence once ARMED
    poll_seconds_1m: int = 60

    # 1m entry thresholds
    delta_ratio_1m_max: float = -0.05
    break_low_lookback: int = 3
    no_new_high_lookback: int = 5

    # stop policy: "ANY" or "CONFIRM"
    stop_on: str = "ANY"

    # Outcome tracking after ENTRY_OK
    outcome_watch_minutes: int = 60
    outcome_poll_seconds: int = 60

    # Different TP/SL
    tp_pct_confirm: float = 0.006
    sl_pct_confirm: float = 0.004
    tp_pct_early: float = 0.006
    sl_pct_early: float = 0.008

    # Late-entry penalty
    late_dist_pct: float = 8.0
    delta_ratio_early_late_max: float = -0.12


# =========================
# CSV helpers
# =========================
def append_csv(path: str, row: dict):
    file_exists = os.path.isfile(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(row.keys()))
        if not file_exists:
            w.writeheader()
        w.writerow(row)


# =========================
# Bybit REST
# =========================
def _get_json(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    r = requests.get(BYBIT_REST + path, params=params, timeout=15)
    r.raise_for_status()
    return r.json()


def _klines(cfg: Config, interval: str, limit: int) -> pd.DataFrame:
    j = _get_json("/v5/market/kline", {
        "category": cfg.category,
        "symbol": cfg.symbol,
        "interval": interval,
        "limit": str(limit)
    })
    if j.get("retCode") != 0:
        raise RuntimeError(f"Bybit kline({interval}) error: {j}")
    lst = j["result"]["list"]
    df = pd.DataFrame(lst, columns=["ts", "open", "high", "low", "close", "volume", "turnover"])
    df["ts"] = pd.to_datetime(df["ts"].astype(np.int64), unit="ms", utc=True)
    for c in ["open", "high", "low", "close", "volume", "turnover"]:
        df[c] = df[c].astype(float)
    return df.sort_values("ts").reset_index(drop=True)


def get_klines_5m(cfg: Config, limit: int = 250) -> pd.DataFrame:
    return _klines(cfg, "5", limit)


def get_klines_1m(cfg: Config, limit: int = 300) -> pd.DataFrame:
    return _klines(cfg, "1", limit)


def get_open_interest(cfg: Config, limit: int = 80) -> pd.DataFrame:
    j = _get_json("/v5/market/open-interest", {
        "category": cfg.category,
        "symbol": cfg.symbol,
        "intervalTime": "5min",
        "limit": str(limit)
    })
    if j.get("retCode") != 0:
        raise RuntimeError(f"Bybit OI error: {j}")
    df = pd.DataFrame(j["result"]["list"])
    if df.empty:
        return df
    if "timestamp" in df.columns:
        df["ts"] = pd.to_datetime(df["timestamp"].astype(np.int64), unit="ms", utc=True)
    elif "time" in df.columns:
        df["ts"] = pd.to_datetime(df["time"].astype(np.int64), unit="ms", utc=True)
    else:
        df["ts"] = pd.NaT
    if "openInterest" in df.columns:
        df["openInterest"] = df["openInterest"].astype(float)
    return df.sort_values("ts").reset_index(drop=True)


def get_recent_trades(cfg: Config, limit: int = 1000) -> pd.DataFrame:
    j = _get_json("/v5/market/recent-trade", {
        "category": cfg.category,
        "symbol": cfg.symbol,
        "limit": str(limit)
    })
    if j.get("retCode") != 0:
        raise RuntimeError(f"Bybit recent-trade error: {j}")
    df = pd.DataFrame(j["result"]["list"])
    if df.empty:
        return df

    tcol = "time" if "time" in df.columns else ("timestamp" if "timestamp" in df.columns else None)
    if not tcol:
        raise RuntimeError(f"Unexpected trades schema: {df.columns.tolist()}")
    df["ts"] = pd.to_datetime(df[tcol].astype(np.int64), unit="ms", utc=True)

    qcol = "size" if "size" in df.columns else ("qty" if "qty" in df.columns else None)
    if not qcol:
        raise RuntimeError(f"Unexpected trades schema (no size/qty): {df.columns.tolist()}")
    df["qty"] = df[qcol].astype(float)

    df["side"] = df["side"].astype(str)
    return df.sort_values("ts").reset_index(drop=True)


# =========================
# Features
# =========================
def volume_zscore(df: pd.DataFrame, lookback: int = 50) -> float:
    v = df["volume"].tail(lookback)
    if len(v) < max(10, lookback // 3):
        return float("nan")
    return float((v.iloc[-1] - v.mean()) / (v.std(ddof=0) + 1e-9))


def delta_ratio(trades: pd.DataFrame, since_ts: pd.Timestamp) -> float:
    if trades.empty:
        return 0.0
    x = trades[trades["ts"] >= since_ts]
    if x.empty:
        return 0.0
    buy = x.loc[x["side"].str.lower() == "buy", "qty"].sum()
    sell = x.loc[x["side"].str.lower() == "sell", "qty"].sum()
    total = buy + sell
    if total <= 0:
        return 0.0
    return float((buy - sell) / total)


# =========================
# 5m structure state
# =========================
@dataclass
class StructureState:
    peak_price: float = 0.0
    stage: int = 0
    drop1_low: Optional[float] = None
    bounce1_high: Optional[float] = None
    drop2_low: Optional[float] = None


def update_structure(cfg: Config, st: StructureState, last_price: float, peak_price: float) -> StructureState:
    st.peak_price = peak_price

    if st.stage == 0:
        if peak_price > 0 and last_price <= peak_price * (1 - cfg.drop1_min_pct):
            st.stage = 1
            st.drop1_low = last_price
        return st

    if st.stage == 1:
        st.drop1_low = min(st.drop1_low or last_price, last_price)
        if last_price >= (st.drop1_low * (1 + cfg.bounce1_min_pct)):
            st.stage = 2
            st.bounce1_high = last_price
        return st

    if st.stage == 2:
        st.bounce1_high = max(st.bounce1_high or last_price, last_price)
        if last_price <= (st.bounce1_high * (1 - cfg.drop2_min_pct)):
            st.stage = 3
            st.drop2_low = last_price
        return st

    if st.stage == 3:
        st.drop2_low = min(st.drop2_low or last_price, last_price)
        if last_price >= (st.drop2_low * (1 + cfg.bounce2_min_pct)):
            st.stage = 4
        return st

    return st


# =========================
# 5m debug + context score
# =========================
def build_dbg5(cfg: Config, candles_5m: pd.DataFrame, oi: pd.DataFrame, trades: pd.DataFrame, st: StructureState) -> Dict[str, Any]:
    last = candles_5m.iloc[-1]
    price = float(last["close"])
    now_ts = candles_5m["ts"].iloc[-1]

    peak = st.peak_price if st.peak_price > 0 else float(candles_5m["high"].tail(20).max())
    dist_to_peak = (peak - price) / peak if peak > 0 else 0.0

    volz = volume_zscore(candles_5m, cfg.vol_z_lookback)

    oi_change_15m = 0.0
    oi_div = False
    if not oi.empty and "openInterest" in oi.columns:
        oi_series = oi["openInterest"].astype(float)
        if len(oi_series) >= 4:
            oi_change_15m = float((oi_series.iloc[-1] - oi_series.iloc[-4]) / (oi_series.iloc[-4] + 1e-9))
        if len(candles_5m) >= 4:
            price_15m_ago = float(candles_5m.iloc[-4]["close"])
            oi_div = bool(price > price_15m_ago and oi_change_15m <= 0)

    dr_15m = delta_ratio(trades, now_ts - pd.Timedelta(minutes=15))

    return {
        "time_utc": str(now_ts),
        "stage": st.stage,
        "price": price,
        "peak_price": peak,
        "dist_to_peak_pct": dist_to_peak * 100,
        "vol_z": volz,
        "oi_change_15m_pct": oi_change_15m * 100,
        "oi_divergence": oi_div,
        "delta_ratio_15m": dr_15m,
    }


def compute_context_score_5m(dbg5: Dict[str, Any]) -> Tuple[float, Dict[str, Any]]:
    parts: Dict[str, float] = {}
    stage = int(dbg5.get("stage", 0))
    dist = float(dbg5.get("dist_to_peak_pct", 999.0))
    oi15 = float(dbg5.get("oi_change_15m_pct", 0.0))
    oi_div = bool(dbg5.get("oi_divergence", False))
    dr15 = float(dbg5.get("delta_ratio_15m", 0.0))
    volz = float(dbg5.get("vol_z", float("nan")))

    if stage >= 4:
        parts["stage"] = 0.30
    elif stage >= 3:
        parts["stage"] = 0.20
    else:
        parts["stage"] = 0.0

    if dist <= 2.0:
        parts["near_top"] = 0.25
    elif dist <= 5.0:
        parts["near_top"] = 0.18
    elif dist <= 8.0:
        parts["near_top"] = 0.10
    else:
        parts["near_top"] = 0.0

    if oi_div or oi15 <= -0.2:
        parts["oi"] = 0.20
    elif oi15 <= 0.0:
        parts["oi"] = 0.12
    else:
        parts["oi"] = 0.0

    if dr15 <= -0.08:
        parts["cvd15"] = 0.20
    elif dr15 <= -0.03:
        parts["cvd15"] = 0.12
    else:
        parts["cvd15"] = 0.0

    if not math.isnan(volz) and volz <= 0.8:
        parts["vol"] = 0.10
    elif not math.isnan(volz) and volz <= 1.2:
        parts["vol"] = 0.05
    else:
        parts["vol"] = 0.0

    score = sum(parts.values())
    score = max(0.0, min(1.0, score))
    return score, parts


# =========================
# 1m entry decision (+ late penalty + delta_ok gate)
# =========================
def decide_entry_1m(cfg: Config,
                    candles_1m: pd.DataFrame,
                    trades: pd.DataFrame,
                    context_score: float,
                    ctx_parts: Dict[str, Any],
                    peak_price_5m: float) -> Tuple[bool, Dict[str, Any]]:
    if len(candles_1m) < max(cfg.break_low_lookback + 2, cfg.no_new_high_lookback + 2):
        return False, {"reason": "not_enough_1m_data"}

    last = candles_1m.iloc[-1]
    now_ts = candles_1m["ts"].iloc[-1]
    price = float(last["close"])

    n = cfg.break_low_lookback
    prev_lows = candles_1m["low"].iloc[-(n+1):-1]
    break_low = price < float(prev_lows.min())

    dr1 = delta_ratio(trades, now_ts - pd.Timedelta(minutes=1))
    dr3 = delta_ratio(trades, now_ts - pd.Timedelta(minutes=3))

    delta_ok = (dr1 <= cfg.delta_ratio_1m_max) or (dr3 <= cfg.delta_ratio_1m_max)

    m = cfg.no_new_high_lookback
    recent_highs = candles_1m["high"].iloc[-m:]
    max_idx = int(np.argmax(recent_highs.values))
    no_new_high = (max_idx < len(recent_highs) - 1)

    dist_to_peak = (peak_price_5m - price) / peak_price_5m if peak_price_5m > 0 else 0.0
    dist_pct = dist_to_peak * 100.0
    near_top = dist_to_peak <= cfg.dist_to_peak_max_pct

    if dist_pct > cfg.late_dist_pct:
        strong_delta = (dr1 <= cfg.delta_ratio_early_late_max) or (dr3 <= cfg.delta_ratio_early_late_max)
        if not strong_delta:
            delta_ok = False

    if context_score >= 0.65:
        need = 2
    elif context_score >= 0.45:
        need = 3
    else:
        need = 4

    flags = {"break_low": break_low, "delta_ok": delta_ok, "no_new_high": no_new_high, "near_top": near_top}
    hit = sum(1 for v in flags.values() if v)

    # ✅ gate
    entry_ok = (flags["delta_ok"] is True) and (hit >= need)

    dbg = {
        "time_utc": str(now_ts),
        "price": price,
        "dist_to_peak_pct": dist_pct,
        "delta_ratio_1m": dr1,
        "delta_ratio_3m": dr3,
        "need": need,
        "hit": hit,
        "flags": flags,
        "context_score": context_score,
        "context_parts": ctx_parts,
        "late_mode": dist_pct > cfg.late_dist_pct,
    }
    return entry_ok, dbg


# =========================
# Outcome tracking
# =========================
def track_outcome_short(cfg: Config,
                        entry_ts_utc: pd.Timestamp,
                        entry_price: float,
                        tp_price: float,
                        sl_price: float) -> Dict[str, Any]:
    end_ts = entry_ts_utc + pd.Timedelta(minutes=cfg.outcome_watch_minutes)

    mfe = 0.0
    mae = 0.0
    outcome = "TIMEOUT"
    hit_ts = None

    while pd.Timestamp.now(tz="UTC") < end_ts:
        candles_1m = get_klines_1m(cfg, limit=300)
        if candles_1m.empty:
            time.sleep(5)
            continue

        future = candles_1m[candles_1m["ts"] >= entry_ts_utc].copy()
        if future.empty:
            time.sleep(cfg.outcome_poll_seconds)
            continue

        min_low = float(future["low"].min())
        max_high = float(future["high"].max())
        mfe = max(mfe, (entry_price - min_low) / entry_price)
        mae = max(mae, (max_high - entry_price) / entry_price)

        for _, row in future.iterrows():
            hi = float(row["high"])
            lo = float(row["low"])
            ts = row["ts"]

            tp_hit = (lo <= tp_price)
            sl_hit = (hi >= sl_price)

            if tp_hit and sl_hit:
                outcome = "BOTH_SAME_CANDLE"
                hit_ts = ts
                break
            if sl_hit:
                outcome = "SL_hit"
                hit_ts = ts
                break
            if tp_hit:
                outcome = "TP_hit"
                hit_ts = ts
                break

        if outcome != "TIMEOUT":
            break

        time.sleep(cfg.outcome_poll_seconds)

    minutes_to_hit = None
    if hit_ts is not None:
        minutes_to_hit = (pd.Timestamp(hit_ts).to_pydatetime() - entry_ts_utc.to_pydatetime()).total_seconds() / 60.0

    return {
        "outcome": outcome,
        "hit_time_utc": str(hit_ts) if hit_ts is not None else None,
        "minutes_to_hit": minutes_to_hit,
        "mfe_pct": mfe * 100.0,
        "mae_pct": mae * 100.0,
    }


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

    # ✅ уникальные имена файлов
    log_5m = f"mvp_log_{cfg.symbol}_{run_id}.csv"
    log_1m = f"mvp_log_1m_{cfg.symbol}_{run_id}.csv"

    print(f"=== WATCH START: {cfg.symbol} | run_id={run_id} | meta={meta} ===")

    while pd.Timestamp.now(tz="UTC") < end_ts:
        try:
            candles_5m = get_klines_5m(cfg, limit=250)
            if candles_5m.empty:
                time.sleep(10)
                continue

            after = candles_5m[candles_5m["ts"] >= pump_start_ts]
            peak_price = float(after["high"].max()) if not after.empty else float(candles_5m["high"].tail(20).max())

            last_price = float(candles_5m.iloc[-1]["close"])
            st = update_structure(cfg, st, last_price, peak_price)

            oi = get_open_interest(cfg, limit=80)
            trades = get_recent_trades(cfg, limit=1000)

            dbg5 = build_dbg5(cfg, candles_5m, oi, trades, st)
            context_score, ctx_parts = compute_context_score_5m(dbg5)

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
                "context_score": context_score,
                "context_parts": json.dumps(ctx_parts, ensure_ascii=False),
            })

            if (not armed_once) and (dbg5["stage"] >= 3):
                armed_once = True

            if armed_once:
                candles_1m = get_klines_1m(cfg, limit=300)
                trades_1m = get_recent_trades(cfg, limit=1000)

                entry_ok, dbg1 = decide_entry_1m(
                    cfg=cfg,
                    candles_1m=candles_1m,
                    trades=trades_1m,
                    context_score=context_score,
                    ctx_parts=ctx_parts,
                    peak_price_5m=float(dbg5["peak_price"])
                )

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
                        "delta_ratio_1m": dbg1["delta_ratio_1m"],
                        "delta_ratio_3m": dbg1["delta_ratio_3m"],
                        "need": dbg1["need"],
                        "hit": dbg1["hit"],
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
                            "context_parts": json.dumps(ctx_parts, ensure_ascii=False),
                        })

                        if TG_SEND_OUTCOME:
                            send_telegram(
                                f"📌 OUTCOME {cfg.symbol}\n"
                                f"{out['outcome']} | min_to_hit={out['minutes_to_hit']}\n"
                                f"MFE={out['mfe_pct']:.2f}% MAE={out['mae_pct']:.2f}%"
                            )

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
                            "log_5m": log_5m,
                            "log_1m": log_1m,
                        }

                time.sleep(cfg.poll_seconds_1m)
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