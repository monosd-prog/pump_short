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
    # Validate inputs
    if entry_price <= 0 or tp_price <= 0 or sl_price <= 0:
        logger.error(f"Invalid outcome parameters: entry_price={entry_price}, tp_price={tp_price}, sl_price={sl_price}")
        return {
            "outcome": "ERROR",
            "end_reason": "INVALID_PARAMS",
            "hit_time_utc": None,
            "minutes_to_hit": None,
            "mfe_pct": 0.0,
            "mae_pct": 0.0,
            "timeout_exit_price": None,
            "timeout_pnl_pct": None,
        }
    
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

            # Deterministic rule for BOTH_SAME_CANDLE: for SHORT, SL is more critical (conservative)
            # If both hit in same candle, prioritize SL (risk management)
            if tp_hit and sl_hit:
                outcome = "SL_hit"  # Conservative: SL takes precedence for SHORT positions
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

    # Ensure outcome is never None
    if outcome is None or outcome == "":
        outcome = "TIMEOUT"
    
    # Ensure all numeric fields are valid
    result = {
        "outcome": str(outcome),
        "end_reason": str(outcome),  # For compatibility
        "hit_time_utc": str(hit_ts) if hit_ts is not None else None,
        "minutes_to_hit": float(minutes_to_hit) if minutes_to_hit is not None else None,
        "mfe_pct": float(mfe * 100.0),
        "mae_pct": float(mae * 100.0),
        "timeout_exit_price": float(timeout_exit_price) if timeout_exit_price is not None else None,
        "timeout_pnl_pct": float(timeout_pnl_pct) if timeout_pnl_pct is not None else None,
    }
    
    return result