from __future__ import annotations

import logging
from typing import Any, Dict, Tuple

import pandas as pd

from short_pump.features import delta_ratio
from short_pump.false_pump.config import FalsePumpConfig

logger = logging.getLogger(__name__)


def _safe_float(val: Any, default: float = 0.0) -> float:
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def detect_pump(candles_1m: pd.DataFrame, cfg: FalsePumpConfig) -> Tuple[bool, float]:
    if candles_1m is None or candles_1m.empty or "close" not in candles_1m.columns:
        return False, 0.0
    lookback = max(2, int(cfg.pump_lookback_candles))
    recent = candles_1m.tail(lookback).copy()
    if recent.empty:
        return False, 0.0
    recent["close"] = pd.to_numeric(recent["close"], errors="coerce")
    recent = recent.dropna(subset=["close"])
    if len(recent) < 2:
        return False, 0.0

    current_price = float(recent["close"].iloc[-1])
    local_min = float(recent["close"].min())
    if local_min <= 0:
        return False, 0.0
    pump_pct = ((current_price - local_min) / local_min) * 100.0

    idx_min = int(recent["close"].idxmin())
    idx_last = int(recent.index[-1])
    bars_since_min = idx_last - idx_min
    in_recent_window = bars_since_min <= int(cfg.pump_candles_count)
    pump_detected = in_recent_window and (pump_pct >= float(cfg.pump_price_pct))
    return bool(pump_detected), float(pump_pct)


def detect_oi_weak(oi_1m: pd.DataFrame, cfg: FalsePumpConfig) -> bool:
    if oi_1m is None or oi_1m.empty:
        return False
    if "openInterest" not in oi_1m.columns:
        return False
    n = max(1, int(cfg.pump_candles_count))
    x = oi_1m.tail(n + 1).copy()
    if len(x) < 2:
        return False

    x["openInterest"] = pd.to_numeric(x["openInterest"], errors="coerce")
    x = x.dropna(subset=["openInterest"])
    if len(x) < 2:
        return False

    oi_start = float(x["openInterest"].iloc[0])
    oi_end = float(x["openInterest"].iloc[-1])
    if oi_start <= 0:
        return False
    oi_change_pct = ((oi_end - oi_start) / oi_start) * 100.0
    return bool(oi_change_pct < float(cfg.oi_max_reaction_pct))


def detect_false_pump(
    candles_1m: pd.DataFrame,
    oi_1m: pd.DataFrame,
    trades: pd.DataFrame,
    liq_features: dict,
    funding_rate: float,
    peak_price_5m: float,
    cfg: FalsePumpConfig,
) -> Tuple[bool, Dict[str, Any]]:
    if funding_rate < float(cfg.funding_min_threshold):
        logger.warning(
            f"[false_pump.detector] BLOCKED by negative funding "
            f"funding={funding_rate:.4f} threshold={cfg.funding_min_threshold}"
        )
        return False, {}
    if candles_1m is None or candles_1m.empty:
        return False, {}

    current_price = _safe_float(candles_1m["close"].iloc[-1], 0.0)
    if current_price <= 0:
        return False, {}

    pump_detected, pump_pct = detect_pump(candles_1m, cfg)
    oi_weak = detect_oi_weak(oi_1m, cfg)

    dist_to_peak_pct = 0.0
    if peak_price_5m and peak_price_5m > 0:
        dist_to_peak_pct = ((peak_price_5m - current_price) / peak_price_5m) * 100.0
    near_top = dist_to_peak_pct <= float(cfg.near_top_pct)

    now_ts = candles_1m["ts"].iloc[-1] if "ts" in candles_1m.columns else pd.Timestamp.now(tz="UTC")
    if getattr(now_ts, "tzinfo", None) is None:
        now_ts = pd.Timestamp(now_ts).tz_localize("UTC")
    dr = delta_ratio(trades if trades is not None else pd.DataFrame(), now_ts - pd.Timedelta(minutes=1))
    delta_ok = dr < 0

    lookback = max(1, int(cfg.pump_lookback_candles))
    max_high = _safe_float(candles_1m["high"].tail(lookback).max(), 0.0) if "high" in candles_1m.columns else 0.0
    no_new_high = current_price < max_high if max_high > 0 else False
    liq_short_usd = _safe_float((liq_features or {}).get("short_liq_usd", 0.0), 0.0)
    liq_present = liq_short_usd >= float(cfg.liq_min_usd)

    optional_flags = {
        "delta_ok": bool(delta_ok),
        "no_new_high": bool(no_new_high),
        "liq_present": bool(liq_present),
    }
    flags_hit = sum(1 for v in optional_flags.values() if v)
    total_flags = len(optional_flags)

    mandatory_ok = bool(pump_detected and oi_weak and near_top)
    signal_ok = mandatory_ok and (flags_hit >= int(cfg.min_flags_required))

    oi_change_short_term = None
    if oi_1m is not None and not oi_1m.empty and "openInterest" in oi_1m.columns:
        x = oi_1m.tail(max(2, int(cfg.pump_candles_count) + 1)).copy()
        x["openInterest"] = pd.to_numeric(x["openInterest"], errors="coerce")
        x = x.dropna(subset=["openInterest"])
        if len(x) >= 2 and float(x["openInterest"].iloc[0]) > 0:
            oi_change_short_term = (
                (float(x["openInterest"].iloc[-1]) - float(x["openInterest"].iloc[0]))
                / float(x["openInterest"].iloc[0])
                * 100.0
            )

    details: Dict[str, Any] = {
        "price": current_price,
        "peak_price_5m": float(peak_price_5m),
        "dist_to_peak_pct": float(dist_to_peak_pct),
        "pump_price_pct": float(pump_pct),
        "oi_change_pct": oi_change_short_term,
        "funding_rate": float(funding_rate),
        "delta_ratio_1m": float(dr),
        "flags": {
            "pump_detected": bool(pump_detected),
            "oi_weak": bool(oi_weak),
            "near_top": bool(near_top),
            "delta_ok": bool(delta_ok),
            "no_new_high": bool(no_new_high),
            "liq_present": bool(liq_present),
        },
        "flags_hit": int(flags_hit),
        "total_flags": int(total_flags),
        "context_score": round(flags_hit / total_flags, 2) if total_flags else 0.0,
    }
    return bool(signal_ok), details
