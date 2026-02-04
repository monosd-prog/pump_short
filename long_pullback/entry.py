from __future__ import annotations

import json
import os
import uuid
from typing import Any, Dict, Tuple

import pandas as pd

from common.context_score import compute_context_score
from common.io_dataset import write_event_row, write_trade_row
from common.runtime import code_version, wall_time_utc
from long_pullback.config import Config
from long_pullback.features import cvd_delta_ratio, oi_change_pct


def decide_entry_long(
    cfg: Config,
    *,
    run_id: str,
    symbol: str,
    candles_1m: pd.DataFrame,
    trades: pd.DataFrame,
    oi_1m: pd.DataFrame | None,
    ctx_parts: Dict[str, float],
    mode: str = "live",
) -> Tuple[bool, Dict[str, Any]]:
    event_id = str(uuid.uuid4())
    skip_reasons = []

    if candles_1m is None or candles_1m.empty or len(candles_1m) < 3:
        skip_reasons.append("no_1m_data")
        _write_event(cfg, run_id, symbol, mode, event_id, False, skip_reasons, {}, 0.0, ctx_parts)
        return False, {"reason": "no_1m_data"}

    last = candles_1m.iloc[-1]
    price = float(last["close"])
    now_ts = last["ts"]

    # Absorption via CVD
    cvd_30s = cvd_delta_ratio(trades, now_ts - pd.Timedelta(seconds=30))
    cvd_1m = cvd_delta_ratio(trades, now_ts - pd.Timedelta(minutes=1))
    absorption_ok = (
        cvd_30s is not None
        and cvd_1m is not None
        and cvd_30s >= cfg.cvd_30s_min
        and cvd_1m >= cfg.cvd_1m_min
    )
    if not absorption_ok:
        skip_reasons.append("absorption_fail")

    # OI filter (soft, for logging only)
    oi_change = None
    if oi_1m is not None and not oi_1m.empty:
        oi_change = oi_change_pct(oi_1m, lookback_minutes=3)

    # Breakout: price breaks recent highs
    recent_high = float(candles_1m["high"].iloc[-3:-1].max())
    breakout_ok = price > recent_high
    if not breakout_ok:
        skip_reasons.append("no_breakout")

    parts = dict(ctx_parts)
    parts["absorption"] = cfg.weight_absorption if absorption_ok else 0.0
    parts["breakout"] = cfg.weight_breakout if breakout_ok else 0.0
    context_score, parts = compute_context_score(parts)

    entry_ok = absorption_ok and breakout_ok

    payload = {
        "event_id": event_id,
        "time_utc": str(now_ts),
        "price": price,
        "cvd_delta_ratio_30s": cvd_30s,
        "cvd_delta_ratio_1m": cvd_1m,
        "oi_change_1m_pct": oi_change,
        "entry_source": "1m",
        "entry_type": "PULLBACK",
        "context_score": context_score,
        "context_parts": parts,
    }

    _write_event(cfg, run_id, symbol, mode, event_id, entry_ok, skip_reasons, payload, context_score, parts)

    if entry_ok:
        tp_price = price * (1.0 + cfg.tp_pct)
        sl_price = price * (1.0 - cfg.sl_pct)
        trade_id = str(uuid.uuid4())
        payload.update(
            {
                "tp_price": tp_price,
                "sl_price": sl_price,
                "tp_pct": float(cfg.tp_pct * 100.0),
                "sl_pct": float(cfg.sl_pct * 100.0),
                "trade_id": trade_id,
            }
        )
        trade_row = {
            "trade_id": trade_id,
            "event_id": event_id,
            "run_id": run_id,
            "symbol": symbol,
            "strategy": cfg.strategy_name,
            "mode": mode,
            "side": "LONG",
            "entry_time_utc": str(now_ts),
            "entry_price": price,
            "tp_price": tp_price,
            "sl_price": sl_price,
            "time_utc": str(now_ts),
            "price": price,
            "entry_payload": json.dumps(payload, ensure_ascii=False),
            "trade_type": "PAPER" if os.getenv("PAPER_MODE", "1") == "1" else "LIVE",
            "paper_entry_time_utc": str(now_ts),
            "paper_entry_price": price,
            "paper_tp_price": tp_price,
            "paper_sl_price": sl_price,
        }
        write_trade_row(trade_row, strategy=cfg.strategy_name, mode=mode, wall_time_utc=wall_time_utc())

    return entry_ok, payload


def _write_event(
    cfg: Config,
    run_id: str,
    symbol: str,
    mode: str,
    event_id: str,
    entry_ok: bool,
    skip_reasons: list[str],
    payload: Dict[str, Any],
    context_score: float,
    parts: Dict[str, float],
) -> None:
    row = {
        "run_id": run_id,
        "event_id": event_id,
        "symbol": symbol,
        "wall_time_utc": wall_time_utc(),
        "strategy": cfg.strategy_name,
        "mode": mode,
        "side": "LONG",
        "stage": "",
        "feature_schema_version": cfg.feature_schema_version,
        "code_version": code_version(),
        "entry_ok": bool(entry_ok),
        "skip_reasons": "|".join(skip_reasons),
        "entry_payload": json.dumps(payload, ensure_ascii=False),
        "context_score": context_score,
        "context_parts": json.dumps(parts, ensure_ascii=False),
        "time_utc": payload.get("time_utc", ""),
        "price": payload.get("price", ""),
        "cvd_delta_ratio_30s": payload.get("cvd_delta_ratio_30s", ""),
        "cvd_delta_ratio_1m": payload.get("cvd_delta_ratio_1m", ""),
        "oi_change_1m_pct": payload.get("oi_change_1m_pct", ""),
    }
    try:
        write_event_row(row, strategy=cfg.strategy_name, mode=mode, wall_time_utc=row["wall_time_utc"], schema_version=2)
    except Exception as e:
        raise RuntimeError(f"write_event_row failed: {e}") from e
