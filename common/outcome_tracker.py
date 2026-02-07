from __future__ import annotations

import json
import os
import threading
import time
from typing import Any, Callable, Dict, Optional

import pandas as pd

from common.logging_utils import get_logger
from short_pump.logging_utils import log_exception

PAPER_MODE = os.getenv("PAPER_MODE", "1") == "1"
OUTCOME_USE_CANDLE_HILO = os.getenv("OUTCOME_USE_CANDLE_HILO", "1") == "1"
OUTCOME_TP_SL_CONFLICT = os.getenv("OUTCOME_TP_SL_CONFLICT", "SL_FIRST").strip().upper()

_finalized_trade_ids: set[str] = set()
_finalized_lock = threading.Lock()


def mark_outcome_finalized(trade_id: str) -> bool:
    if not trade_id:
        return True
    with _finalized_lock:
        if trade_id in _finalized_trade_ids:
            return False
        _finalized_trade_ids.add(trade_id)
        return True


def track_outcome(
    cfg: Any,
    *,
    side: str,
    entry_ts_utc: pd.Timestamp,
    entry_price: float,
    tp_price: float,
    sl_price: float,
    entry_source: str = "unknown",
    entry_type: str = "unknown",
    run_id: str = "",
    symbol: str = "",
    category: Optional[str] = None,
    fetch_klines_1m: Optional[Callable[..., pd.DataFrame]] = None,
    strategy_name: Optional[str] = None,
) -> Dict[str, Any]:
    logger = get_logger(__name__, strategy=strategy_name, symbol=symbol)
    side_norm = (side or "").strip().lower()
    if side_norm not in ("short", "long"):
        side_norm = "short"

    if entry_price <= 0 or tp_price <= 0 or sl_price <= 0:
        log_exception(
            logger,
            "Invalid outcome parameters",
            symbol=symbol,
            run_id=run_id,
            step="OUTCOME_VALIDATE",
            extra={"entry_price": entry_price, "tp_price": tp_price, "sl_price": sl_price},
        )
        return _create_error_outcome(
            entry_price,
            tp_price,
            sl_price,
            entry_source,
            entry_type,
            run_id,
            symbol,
            entry_ts_utc,
            "INVALID_PARAMS",
            side_norm,
        )

    if entry_ts_utc is None:
        log_exception(logger, "entry_ts_utc is None", symbol=symbol, run_id=run_id, step="OUTCOME_VALIDATE")
        return _create_error_outcome(
            entry_price,
            tp_price,
            sl_price,
            entry_source,
            entry_type,
            run_id,
            symbol,
            pd.Timestamp.now(tz="UTC"),
            "INVALID_ENTRY_TS",
            side_norm,
        )

    if fetch_klines_1m is None:
        from common.bybit_api import get_klines_1m as fetch_klines_1m  # local import

    cat = category or getattr(cfg, "category", "linear")

    if side_norm == "short":
        tp_pct = ((entry_price - tp_price) / entry_price) * 100.0 if entry_price > 0 else 0.0
        sl_pct = ((sl_price - entry_price) / entry_price) * 100.0 if entry_price > 0 else 0.0
    else:
        tp_pct = ((tp_price - entry_price) / entry_price) * 100.0 if entry_price > 0 else 0.0
        sl_pct = ((entry_price - sl_price) / entry_price) * 100.0 if entry_price > 0 else 0.0

    end_ts = entry_ts_utc + pd.Timedelta(minutes=getattr(cfg, "outcome_watch_minutes", 120))
    poll_seconds = getattr(cfg, "outcome_poll_seconds", 60)

    mfe = 0.0
    mae = 0.0
    end_reason = "TIMEOUT"
    hit_ts = None
    tp_sl_same_candle = False
    conflict_candle_high: float | None = None
    conflict_candle_low: float | None = None

    while pd.Timestamp.now(tz="UTC") < end_ts:
        try:
            candles_1m = fetch_klines_1m(cat, symbol, limit=300)
            if candles_1m is None or candles_1m.empty:
                time.sleep(5)
                continue
            future = candles_1m[candles_1m["ts"] >= entry_ts_utc].copy()
            if future is None or future.empty:
                time.sleep(poll_seconds)
                continue
        except Exception:
            log_exception(
                logger,
                "Error fetching candles in outcome tracking",
                symbol=symbol,
                run_id=run_id,
                step="OUTCOME_FETCH",
                extra={"end_reason": end_reason},
            )
            time.sleep(5)
            continue

        min_low = float(future["low"].min())
        max_high = float(future["high"].max())
        if side_norm == "short":
            mfe = max(mfe, (entry_price - min_low) / entry_price)
            mae = max(mae, (max_high - entry_price) / entry_price)
        else:
            mfe = max(mfe, (max_high - entry_price) / entry_price)
            mae = min(mae, (min_low - entry_price) / entry_price)

        for _, row in future.iterrows():
            hi = float(row["high"])
            lo = float(row["low"])
            close = float(row["close"])
            ts = row["ts"]

            if OUTCOME_USE_CANDLE_HILO:
                if side_norm == "short":
                    tp_hit = lo <= tp_price
                    sl_hit = hi >= sl_price
                else:
                    tp_hit = hi >= tp_price
                    sl_hit = lo <= sl_price
            else:
                if side_norm == "short":
                    tp_hit = close <= tp_price
                    sl_hit = close >= sl_price
                else:
                    tp_hit = close >= tp_price
                    sl_hit = close <= sl_price

            if tp_hit and sl_hit:
                tp_sl_same_candle = True
                conflict_candle_high = hi
                conflict_candle_low = lo
                if OUTCOME_TP_SL_CONFLICT == "TP_FIRST":
                    end_reason = "TP_hit"
                else:
                    end_reason = "SL_hit"
                hit_ts = ts
                break
            if sl_hit:
                end_reason = "SL_hit"
                hit_ts = ts
                break
            if tp_hit:
                end_reason = "TP_hit"
                hit_ts = ts
                break

        if end_reason != "TIMEOUT":
            break

        time.sleep(poll_seconds)

    minutes_to_hit = None
    if hit_ts is not None:
        minutes_to_hit = (pd.Timestamp(hit_ts).to_pydatetime() - entry_ts_utc.to_pydatetime()).total_seconds() / 60.0

    timeout_exit_price = 0.0
    timeout_pnl_pct = 0.0
    if end_reason == "TIMEOUT":
        try:
            candles_1m = fetch_klines_1m(cat, symbol, limit=300)
            if candles_1m is not None and not candles_1m.empty:
                sub = candles_1m[candles_1m["ts"] <= end_ts].copy()
                if sub is not None and not sub.empty:
                    last_close = float(sub["close"].iloc[-1])
                    timeout_exit_price = last_close
                    if side_norm == "short":
                        timeout_pnl_pct = (entry_price - last_close) / entry_price * 100.0
                    else:
                        timeout_pnl_pct = (last_close - entry_price) / entry_price * 100.0
        except Exception:
            log_exception(
                logger,
                "Error calculating timeout exit price",
                symbol=symbol,
                run_id=run_id,
                step="OUTCOME_TIMEOUT",
                extra={"end_reason": end_reason},
            )
            timeout_exit_price = entry_price
            timeout_pnl_pct = 0.0

    if end_reason is None or end_reason == "":
        end_reason = "TIMEOUT"

    entry_time_utc_str = entry_ts_utc.isoformat() if hasattr(entry_ts_utc, "isoformat") else str(entry_ts_utc)
    hit_time_utc_str = ""
    if hit_ts is not None:
        if isinstance(hit_ts, pd.Timestamp):
            hit_time_utc_str = hit_ts.isoformat()
        else:
            hit_time_utc_str = str(hit_ts)

    if end_reason in ("TP_hit", "SL_hit"):
        exit_time_utc = hit_time_utc_str
        exit_price = tp_price if end_reason == "TP_hit" else sl_price
    else:
        exit_time_utc = entry_ts_utc.isoformat()
        exit_price = timeout_exit_price

    if side_norm == "short":
        pnl_pct = ((entry_price - exit_price) / entry_price) * 100.0 if entry_price > 0 else 0.0
        risk_pct = ((sl_price - entry_price) / entry_price) * 100.0 if entry_price > 0 else 0.0
    else:
        pnl_pct = ((exit_price - entry_price) / entry_price) * 100.0 if entry_price > 0 else 0.0
        risk_pct = ((entry_price - sl_price) / entry_price) * 100.0 if entry_price > 0 else 0.0
    r_multiple = (pnl_pct / risk_pct) if risk_pct > 0 else 0.0

    details_payload = {
        "tp_hit": end_reason == "TP_hit",
        "sl_hit": end_reason == "SL_hit",
        "tp_sl_same_candle": 1 if tp_sl_same_candle else 0,
        "conflict_policy": OUTCOME_TP_SL_CONFLICT,
        "use_candle_hilo": OUTCOME_USE_CANDLE_HILO,
        "side": side_norm,
    }
    if tp_sl_same_candle:
        if side_norm == "short":
            tp_pnl = ((entry_price - tp_price) / entry_price) * 100.0 if entry_price > 0 else 0.0
            sl_pnl = ((entry_price - sl_price) / entry_price) * 100.0 if entry_price > 0 else 0.0
        else:
            tp_pnl = ((tp_price - entry_price) / entry_price) * 100.0 if entry_price > 0 else 0.0
            sl_pnl = ((sl_price - entry_price) / entry_price) * 100.0 if entry_price > 0 else 0.0
        details_payload.update(
            {
                "candle_high": conflict_candle_high,
                "candle_low": conflict_candle_low,
                "alt_outcome_tp_first": "TP_hit",
                "alt_pnl_tp_first": float(tp_pnl),
                "alt_outcome_sl_first": "SL_hit",
                "alt_pnl_sl_first": float(sl_pnl),
            }
        )

    result = {
        "run_id": str(run_id) if run_id else "",
        "symbol": str(symbol) if symbol else "",
        "entry_time_utc": entry_time_utc_str,
        "entry_price": float(entry_price),
        "entry_source": str(entry_source) if entry_source else "unknown",
        "entry_type": str(entry_type) if entry_type else "unknown",
        "tp_price": float(tp_price),
        "sl_price": float(sl_price),
        "tp_pct": float(tp_pct),
        "sl_pct": float(sl_pct),
        "end_reason": str(end_reason),
        "outcome": str(end_reason),
        "hit_time_utc": hit_time_utc_str,
        "minutes_to_hit": float(minutes_to_hit) if minutes_to_hit is not None else 0.0,
        "mfe_pct": float(mfe * 100.0),
        "mae_pct": float(mae * 100.0),
        "timeout_exit_price": float(timeout_exit_price),
        "timeout_pnl_pct": float(timeout_pnl_pct),
        "trade_type": "PAPER" if PAPER_MODE else "LIVE",
        "exit_time_utc": exit_time_utc,
        "exit_price": float(exit_price),
        "pnl_pct": float(pnl_pct),
        "r_multiple": float(r_multiple),
        "details_payload": json.dumps(details_payload, ensure_ascii=False),
    }

    return result


def build_outcome_row(
    summary: Dict[str, Any],
    *,
    trade_id: str,
    event_id: str,
    run_id: str,
    symbol: str,
    strategy: str,
    mode: str,
    side: str,
    outcome_time_utc: str,
    entry_snapshot: Optional[Dict[str, Any]] = None,
    extra_details: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    if not mark_outcome_finalized(trade_id):
        return None
    details_payload = summary.get("details_payload")
    details_payload_obj: Dict[str, Any] = {}
    if isinstance(details_payload, dict):
        details_payload_obj = details_payload
    elif isinstance(details_payload, str) and details_payload:
        try:
            details_payload_obj = json.loads(details_payload)
        except Exception:
            details_payload_obj = {}

    end_reason = summary.get("end_reason") or summary.get("outcome") or ""
    details_json_obj: Dict[str, Any] = {
        "timeout": end_reason == "TIMEOUT",
        "tp_hit": bool(details_payload_obj.get("tp_hit")) if "tp_hit" in details_payload_obj else end_reason == "TP_hit",
        "sl_hit": bool(details_payload_obj.get("sl_hit")) if "sl_hit" in details_payload_obj else end_reason == "SL_hit",
        "tp_sl_same_candle": 1 if details_payload_obj.get("tp_sl_same_candle") else 0,
        "conflict_policy": details_payload_obj.get("conflict_policy"),
        "use_candle_hilo": details_payload_obj.get("use_candle_hilo"),
        "candle_high": details_payload_obj.get("candle_high"),
        "candle_low": details_payload_obj.get("candle_low"),
        "alt_outcome_tp_first": details_payload_obj.get("alt_outcome_tp_first"),
        "alt_pnl_tp_first": details_payload_obj.get("alt_pnl_tp_first"),
        "alt_outcome_sl_first": details_payload_obj.get("alt_outcome_sl_first"),
        "alt_pnl_sl_first": details_payload_obj.get("alt_pnl_sl_first"),
        "entry_price": summary.get("entry_price"),
        "tp": summary.get("tp_price"),
        "sl": summary.get("sl_price"),
        "entry_time_utc": summary.get("entry_time_utc"),
        "exit_price": summary.get("exit_price"),
        "mae_pct": summary.get("mae_pct"),
        "mfe_pct": summary.get("mfe_pct"),
        "hold_seconds": summary.get("hold_seconds"),
    }
    if details_payload:
        details_json_obj["details_payload"] = details_payload
    if entry_snapshot is not None:
        details_json_obj["entry_snapshot"] = entry_snapshot
    if extra_details:
        details_json_obj.update(extra_details)

    row = {
        "trade_id": trade_id,
        "event_id": event_id,
        "run_id": run_id,
        "symbol": symbol,
        "strategy": strategy,
        "mode": mode,
        "side": side,
        "outcome_time_utc": outcome_time_utc,
        "outcome": summary.get("outcome") or end_reason or "UNKNOWN",
        "pnl_pct": summary.get("pnl_pct") or 0.0,
        "hold_seconds": summary.get("hold_seconds") or 0.0,
        "mae_pct": summary.get("mae_pct") or 0.0,
        "mfe_pct": summary.get("mfe_pct") or 0.0,
        "details_json": json.dumps(details_json_obj, ensure_ascii=False),
        "trade_type": summary.get("trade_type", ""),
    }
    return row


def _create_error_outcome(
    entry_price: float,
    tp_price: float,
    sl_price: float,
    entry_source: str,
    entry_type: str,
    run_id: str,
    symbol: str,
    entry_ts_utc: pd.Timestamp,
    error_reason: str,
    side: str,
) -> Dict[str, Any]:
    if side == "long":
        tp_pct = ((tp_price - entry_price) / entry_price) * 100.0 if entry_price > 0 else 0.0
        sl_pct = ((entry_price - sl_price) / entry_price) * 100.0 if entry_price > 0 else 0.0
    else:
        tp_pct = ((entry_price - tp_price) / entry_price) * 100.0 if entry_price > 0 else 0.0
        sl_pct = ((sl_price - entry_price) / entry_price) * 100.0 if entry_price > 0 else 0.0

    entry_time_utc_str = entry_ts_utc.isoformat() if hasattr(entry_ts_utc, "isoformat") else str(entry_ts_utc)

    return {
        "run_id": str(run_id) if run_id else "",
        "symbol": str(symbol) if symbol else "",
        "entry_time_utc": entry_time_utc_str,
        "entry_price": float(entry_price) if entry_price > 0 else 0.0,
        "entry_source": str(entry_source) if entry_source else "unknown",
        "entry_type": str(entry_type) if entry_type else "unknown",
        "tp_price": float(tp_price) if tp_price > 0 else 0.0,
        "sl_price": float(sl_price) if sl_price > 0 else 0.0,
        "tp_pct": float(tp_pct),
        "sl_pct": float(sl_pct),
        "end_reason": f"ERROR_{error_reason}",
        "outcome": f"ERROR_{error_reason}",
        "hit_time_utc": "",
        "minutes_to_hit": 0.0,
        "mfe_pct": 0.0,
        "mae_pct": 0.0,
        "timeout_exit_price": 0.0,
        "timeout_pnl_pct": 0.0,
        "trade_type": "PAPER" if PAPER_MODE else "LIVE",
        "exit_time_utc": "",
        "exit_price": 0.0,
        "pnl_pct": 0.0,
        "r_multiple": 0.0,
        "details_payload": json.dumps({"error": error_reason}, ensure_ascii=False),
    }
