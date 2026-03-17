from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Dict, Optional

import pandas as pd

from common.outcome_tracker import OUTCOME_TP_SL_CONFLICT, OUTCOME_USE_CANDLE_HILO, _normalize_conflict_policy


@dataclass(frozen=True)
class OutcomeResult:
    end_reason: str
    hit_time_utc: str
    exit_time_utc: str
    exit_price: float
    pnl_pct: float
    hold_seconds: float
    mae_pct: float
    mfe_pct: float
    details_payload_json: str


def resolve_outcome_from_candles(
    *,
    candles_1m: pd.DataFrame,
    side: str,
    entry_ts_utc: pd.Timestamp,
    entry_price: float,
    tp_price: float,
    sl_price: float,
    watch_minutes: int,
    conflict_policy: Optional[str] = None,
) -> OutcomeResult:
    """
    Pure historical outcome resolver. No sleeps, no network.
    Mirrors the candle-based logic used in common.outcome_tracker.track_outcome.
    """
    side_norm = (side or "").strip().lower()
    if side_norm not in ("short", "long"):
        side_norm = "short"

    if entry_ts_utc.tzinfo is None:
        entry_ts_utc = entry_ts_utc.tz_localize("UTC")
    else:
        entry_ts_utc = entry_ts_utc.tz_convert("UTC")

    end_ts = entry_ts_utc + pd.Timedelta(minutes=int(watch_minutes))
    df = candles_1m.copy() if candles_1m is not None else pd.DataFrame()
    if df.empty or "ts" not in df.columns:
        details = {"error": "no_candles"}
        return OutcomeResult(
            end_reason="TIMEOUT",
            hit_time_utc="",
            exit_time_utc=end_ts.isoformat(),
            exit_price=float(entry_price),
            pnl_pct=0.0,
            hold_seconds=float(watch_minutes) * 60.0,
            mae_pct=0.0,
            mfe_pct=0.0,
            details_payload_json=json.dumps(details, ensure_ascii=False),
        )

    ts = pd.to_datetime(df["ts"], utc=True, errors="coerce")
    df = df.assign(ts=ts).dropna(subset=["ts"]).sort_values("ts").reset_index(drop=True)
    future = df[(df["ts"] >= entry_ts_utc) & (df["ts"] <= end_ts)].copy()
    if future.empty:
        details = {"error": "no_future_candles"}
        return OutcomeResult(
            end_reason="TIMEOUT",
            hit_time_utc="",
            exit_time_utc=end_ts.isoformat(),
            exit_price=float(entry_price),
            pnl_pct=0.0,
            hold_seconds=float(watch_minutes) * 60.0,
            mae_pct=0.0,
            mfe_pct=0.0,
            details_payload_json=json.dumps(details, ensure_ascii=False),
        )

    # MFE/MAE on the whole window
    min_low = float(pd.to_numeric(future["low"], errors="coerce").min())
    max_high = float(pd.to_numeric(future["high"], errors="coerce").max())
    if side_norm == "short":
        mfe = max(0.0, (entry_price - min_low) / entry_price) if entry_price > 0 else 0.0
        mae = max(0.0, (max_high - entry_price) / entry_price) if entry_price > 0 else 0.0
    else:
        mfe = max(0.0, (max_high - entry_price) / entry_price) if entry_price > 0 else 0.0
        mae = max(0.0, (entry_price - min_low) / entry_price) if entry_price > 0 else 0.0

    end_reason = "TIMEOUT"
    hit_ts: Optional[pd.Timestamp] = None
    tp_sl_same_candle = False
    conflict_candle_high: float | None = None
    conflict_candle_low: float | None = None
    decision_candle_high: float | None = None
    decision_candle_low: float | None = None

    for _, row in future.iterrows():
        hi = float(row["high"])
        lo = float(row["low"])
        close = float(row["close"])
        ts_row = pd.Timestamp(row["ts"])

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
            decision_candle_high = hi
            decision_candle_low = lo
            policy = _normalize_conflict_policy(conflict_policy or OUTCOME_TP_SL_CONFLICT)
            if policy == "NEUTRAL":
                end_reason = "CONFLICT"
            elif policy == "TP_FIRST":
                end_reason = "TP_hit"
            else:
                end_reason = "SL_hit"
            hit_ts = ts_row
            break
        if sl_hit:
            end_reason = "SL_hit"
            decision_candle_high = hi
            decision_candle_low = lo
            hit_ts = ts_row
            break
        if tp_hit:
            end_reason = "TP_hit"
            decision_candle_high = hi
            decision_candle_low = lo
            hit_ts = ts_row
            break

    if end_reason in ("TP_hit", "SL_hit", "CONFLICT") and hit_ts is not None:
        exit_time_utc = hit_ts.isoformat()
        hit_time_utc = hit_ts.isoformat()
        if end_reason == "TP_hit":
            exit_price = float(tp_price)
        elif end_reason == "SL_hit":
            exit_price = float(sl_price)
        else:
            exit_price = float(entry_price)
    else:
        # TIMEOUT: take last close before end_ts
        sub = future[future["ts"] <= end_ts]
        last_close = float(pd.to_numeric(sub["close"], errors="coerce").iloc[-1]) if not sub.empty else float(entry_price)
        exit_time_utc = end_ts.isoformat()
        hit_time_utc = ""
        exit_price = last_close

    if side_norm == "short":
        pnl_pct = ((entry_price - exit_price) / entry_price) * 100.0 if entry_price > 0 else 0.0
    else:
        pnl_pct = ((exit_price - entry_price) / entry_price) * 100.0 if entry_price > 0 else 0.0

    hold_seconds = float((pd.Timestamp(exit_time_utc) - entry_ts_utc).total_seconds())
    hold_seconds = max(1.0, hold_seconds)

    resolved_policy = _normalize_conflict_policy(conflict_policy or OUTCOME_TP_SL_CONFLICT)
    details_payload: Dict[str, Any] = {
        "tp_hit": end_reason == "TP_hit",
        "sl_hit": end_reason == "SL_hit",
        "tp_sl_same_candle": 1 if tp_sl_same_candle else 0,
        "conflict_policy": resolved_policy,
        "use_candle_hilo": OUTCOME_USE_CANDLE_HILO,
        "side": side_norm,
    }
    if decision_candle_high is not None and decision_candle_low is not None:
        details_payload["candle_high"] = decision_candle_high
        details_payload["candle_low"] = decision_candle_low
    if tp_sl_same_candle:
        # keep compat with factor-report diagnostics fields
        if side_norm == "short":
            tp_pct = ((entry_price - tp_price) / entry_price) * 100.0 if entry_price > 0 else 0.0
            sl_pct = ((sl_price - entry_price) / entry_price) * 100.0 if entry_price > 0 else 0.0
        else:
            tp_pct = ((tp_price - entry_price) / entry_price) * 100.0 if entry_price > 0 else 0.0
            sl_pct = ((entry_price - sl_price) / entry_price) * 100.0 if entry_price > 0 else 0.0
        details_payload.update(
            {
                "candle_high": conflict_candle_high,
                "candle_low": conflict_candle_low,
                "alt_outcome_tp_first": "TP_hit",
                "alt_pnl_tp_first": float(tp_pct),
                "alt_outcome_sl_first": "SL_hit",
                "alt_pnl_sl_first": float(-sl_pct),
            }
        )

    return OutcomeResult(
        end_reason=end_reason,
        hit_time_utc=hit_time_utc,
        exit_time_utc=exit_time_utc,
        exit_price=float(exit_price),
        pnl_pct=float(pnl_pct),
        hold_seconds=float(hold_seconds),
        mae_pct=float(mae * 100.0),
        mfe_pct=float(mfe * 100.0),
        details_payload_json=json.dumps(details_payload, ensure_ascii=False),
    )

