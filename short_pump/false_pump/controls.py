from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Any, Dict

_LOCK = threading.Lock()
_LIVE_CONFIG_PATH = Path(__file__).resolve().parents[2] / "live_config.json"
_CFG_KEY = "oi_screener_controls"


def _safe_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _defaults() -> Dict[str, Any]:
    return {
        "min_oi_growth_pct": _safe_float(os.getenv("FALSE_PUMP_MIN_OI_GROWTH_PCT"), 80.0),
        "max_growth_period_min": _safe_int(os.getenv("FALSE_PUMP_MAX_GROWTH_PERIOD_MIN"), 90),
        "pump_price_pct": _safe_float(os.getenv("FALSE_PUMP_PUMP_PRICE_PCT"), 0.8),
        "oi_max_reaction_pct": _safe_float(os.getenv("FALSE_PUMP_OI_MAX_REACTION_PCT"), 0.8),
        "near_top_pct": _safe_float(os.getenv("FALSE_PUMP_NEAR_TOP_PCT"), 5.0),
        "min_flags_required": _safe_int(os.getenv("FALSE_PUMP_MIN_FLAGS_REQUIRED"), 2),
        "mandatory_min_hits": _safe_int(os.getenv("FALSE_PUMP_MANDATORY_MIN_HITS"), 3),
        "liq_min_usd": _safe_float(os.getenv("FALSE_PUMP_LIQ_MIN_USD"), 30000.0),
    }


def _normalize(raw: Dict[str, Any]) -> Dict[str, Any]:
    defaults = _defaults()
    min_oi = _safe_float(raw.get("min_oi_growth_pct"), defaults["min_oi_growth_pct"])
    max_period = _safe_int(raw.get("max_growth_period_min"), defaults["max_growth_period_min"])
    pump_price_pct = _safe_float(raw.get("pump_price_pct"), defaults["pump_price_pct"])
    oi_max_reaction_pct = _safe_float(
        raw.get("oi_max_reaction_pct"), defaults["oi_max_reaction_pct"]
    )
    near_top_pct = _safe_float(raw.get("near_top_pct"), defaults["near_top_pct"])
    min_flags_required = _safe_int(
        raw.get("min_flags_required"), defaults["min_flags_required"]
    )
    mandatory_min_hits = _safe_int(
        raw.get("mandatory_min_hits"), defaults["mandatory_min_hits"]
    )
    liq_min_usd = _safe_float(raw.get("liq_min_usd"), defaults["liq_min_usd"])
    if max_period < 1:
        max_period = 1
    if pump_price_pct < 0.0:
        pump_price_pct = 0.0
    if near_top_pct < 0.0:
        near_top_pct = 0.0
    if min_flags_required < 0:
        min_flags_required = 0
    if mandatory_min_hits < 1:
        mandatory_min_hits = 1
    if mandatory_min_hits > 3:
        mandatory_min_hits = 3
    if liq_min_usd < 0.0:
        liq_min_usd = 0.0
    return {
        "min_oi_growth_pct": float(min_oi),
        "max_growth_period_min": int(max_period),
        "pump_price_pct": float(pump_price_pct),
        "oi_max_reaction_pct": float(oi_max_reaction_pct),
        "near_top_pct": float(near_top_pct),
        "min_flags_required": int(min_flags_required),
        "mandatory_min_hits": int(mandatory_min_hits),
        "liq_min_usd": float(liq_min_usd),
    }


def get_controls() -> Dict[str, Any]:
    with _LOCK:
        try:
            cfg = json.loads(_LIVE_CONFIG_PATH.read_text(encoding="utf-8"))
            raw = cfg.get(_CFG_KEY, {})
            if not isinstance(raw, dict):
                raw = {}
        except Exception:
            raw = {}
        return _normalize(raw)


def set_controls(**updates: Any) -> Dict[str, Any]:
    with _LOCK:
        try:
            cfg = json.loads(_LIVE_CONFIG_PATH.read_text(encoding="utf-8"))
            if not isinstance(cfg, dict):
                cfg = {}
        except Exception:
            cfg = {}

        current_raw = cfg.get(_CFG_KEY, {})
        if not isinstance(current_raw, dict):
            current_raw = {}
        merged = {**_normalize(current_raw), **updates}
        next_cfg = _normalize(merged)
        cfg[_CFG_KEY] = next_cfg
        _LIVE_CONFIG_PATH.write_text(json.dumps(cfg, indent=2, ensure_ascii=False), encoding="utf-8")
        return next_cfg
