import time
from typing import Dict, Any
import pandas as pd

from short_pump.bybit_api import get_klines_1m
from short_pump.config import Config
from short_pump.logging_utils import get_logger, log_exception

logger = get_logger(__name__)


def track_outcome_short(
    cfg: Config,
    entry_ts_utc: pd.Timestamp,
    entry_price: float,
    tp_price: float,
    sl_price: float,
) -> Dict[str, Any]:
    end_ts = entry_ts_utc + pd.Timedelta(minutes=cfg.outcome_watch_minutes)

    mfe = 0.0
    mae = 0.0
    outcome = "TIMEOUT"
    hit_ts = None

    while pd.Timestamp.now(tz="UTC") < end_ts:
        try:
            candles_1m = get_klines_1m(cfg.category, cfg.symbol, limit=300)
            if candles_1m is None or candles_1m.empty:
                time.sleep(5)
                continue

            future = candles_1m[candles_1m["ts"] >= entry_ts_utc].copy()
            if future is None or future.empty:
                time.sleep(cfg.outcome_poll_seconds)
                continue
        except Exception as e:
            log_exception(logger, "Error fetching candles in outcome tracking", symbol=cfg.symbol, step="OUTCOME_FETCH", extra={"outcome": outcome})
            time.sleep(5)
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

    timeout_exit_price = None
    timeout_pnl_pct = None
    if outcome == "TIMEOUT":
        try:
            candles_1m = get_klines_1m(cfg.category, cfg.symbol, limit=300)
            if candles_1m is not None and not candles_1m.empty:
                sub = candles_1m[candles_1m["ts"] <= end_ts].copy()
                if sub is not None and not sub.empty:
                    last_close = float(sub["close"].iloc[-1])
                    timeout_exit_price = last_close
                    timeout_pnl_pct = (entry_price - last_close) / entry_price * 100.0
        except Exception as e:
            log_exception(logger, "Error calculating timeout exit price", symbol=cfg.symbol, step="OUTCOME_TIMEOUT", extra={"outcome": outcome})

    return {
        "outcome": outcome,
        "hit_time_utc": str(hit_ts) if hit_ts is not None else None,
        "minutes_to_hit": minutes_to_hit,
        "mfe_pct": mfe * 100.0,
        "mae_pct": mae * 100.0,
        "timeout_exit_price": timeout_exit_price,
        "timeout_pnl_pct": timeout_pnl_pct,
    }