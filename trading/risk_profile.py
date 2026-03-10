"""
Risk profile selection: strategy + liq bucket -> profile name, risk_mult, notional.
Used by broker gating, runner sizing, and Telegram formatting.
"""
from __future__ import annotations

import logging
import os
from typing import Any, Tuple

logger = logging.getLogger(__name__)


def _float_env(name: str, default: float) -> float:
    try:
        v = os.getenv(name)
        if not v or not str(v).strip():
            return default
        return float(str(v).replace(",", "."))
    except (TypeError, ValueError):
        return default


def _int_env(name: str, default: int) -> int:
    try:
        v = os.getenv(name)
        if not v or not str(v).strip():
            return default
        return int(v)
    except (TypeError, ValueError):
        return default


def _bool_env(name: str, default: bool = False) -> bool:
    v = (os.getenv(name) or "").strip().lower()
    if not v:
        return default
    return v in ("1", "true", "yes", "y", "on")


# short_pump active filter
SHORT_PUMP_AUTO_STAGE4_ONLY = _bool_env("SHORT_PUMP_AUTO_STAGE4_ONLY", True)
SHORT_PUMP_AUTO_DIST_MIN = _float_env("SHORT_PUMP_AUTO_DIST_MIN", 3.5)

# fast0
FAST0_AUTO_ENABLE = _bool_env("FAST0_AUTO_ENABLE", True)
FAST0_BASE_RISK_MULT = _float_env("FAST0_BASE_RISK_MULT", 1.0)

FAST0_LIQ_5K_25K_ENABLE = _bool_env("FAST0_LIQ_5K_25K_ENABLE", True)
FAST0_LIQ_5K_25K_MIN_USD = _float_env("FAST0_LIQ_5K_25K_MIN_USD", 5000.0)
FAST0_LIQ_5K_25K_MAX_USD = _float_env("FAST0_LIQ_5K_25K_MAX_USD", 25000.0)
FAST0_LIQ_5K_25K_RISK_MULT = _float_env("FAST0_LIQ_5K_25K_RISK_MULT", 1.5)

FAST0_LIQ_100K_ENABLE = _bool_env("FAST0_LIQ_100K_ENABLE", True)
FAST0_LIQ_100K_MIN_USD = _float_env("FAST0_LIQ_100K_MIN_USD", 100000.0)
FAST0_LIQ_100K_RISK_MULT = _float_env("FAST0_LIQ_100K_RISK_MULT", 2.0)

# fast0 base: dist_to_peak_pct must be <= FAST0_BASE_DIST_MAX for tradeability and TG
FAST0_BASE_DIST_MAX = _float_env("FAST0_BASE_DIST_MAX", 2.0)


def is_fast0_entry_allowed(liq_long_usd_30s: Any, dist_to_peak_pct: Any) -> Tuple[bool, str]:
    """
    Single source of truth for fast0 tradeability (auto + TG).
    For base profile: dist_to_peak_pct must be <= FAST0_BASE_DIST_MAX.
    For 5k-25k and 100k+: no dist filter.
    Returns (allowed, reason).
    """
    try:
        liq_val = float(liq_long_usd_30s) if liq_long_usd_30s is not None else None
    except (TypeError, ValueError):
        return False, "liq_missing"
    if liq_val is None or liq_val <= 0:
        return False, "liq_invalid"
    dist_val = None
    try:
        dist_val = float(dist_to_peak_pct) if dist_to_peak_pct is not None else None
    except (TypeError, ValueError):
        pass
    is_100k = FAST0_LIQ_100K_ENABLE and liq_val >= FAST0_LIQ_100K_MIN_USD
    is_5k_25k = FAST0_LIQ_5K_25K_ENABLE and liq_val > FAST0_LIQ_5K_25K_MIN_USD and liq_val <= FAST0_LIQ_5K_25K_MAX_USD
    if is_100k or is_5k_25k:
        return True, ""
    if dist_val is None or dist_val > FAST0_BASE_DIST_MAX:
        return False, "fast0_base_dist_gt_2.0"
    return True, ""


# live execution
LIVE_FIXED_NOTIONAL_USD = _float_env("LIVE_FIXED_NOTIONAL_USD", 0.0)
if LIVE_FIXED_NOTIONAL_USD <= 0:
    try:
        _fp = os.getenv("FIXED_POSITION_USD") or "0"
        LIVE_FIXED_NOTIONAL_USD = float(str(_fp).replace(",", "."))
    except (TypeError, ValueError):
        LIVE_FIXED_NOTIONAL_USD = 10.0
if LIVE_FIXED_NOTIONAL_USD <= 0:
    LIVE_FIXED_NOTIONAL_USD = 10.0
LIVE_LEVERAGE = _int_env("LIVE_LEVERAGE", _int_env("TRADING_LEVERAGE", 4))
LIVE_MARGIN_MODE = (os.getenv("LIVE_MARGIN_MODE") or "isolated").strip().lower()


def get_risk_profile(
    strategy: str,
    stage: Any = None,
    dist_to_peak_pct: Any = None,
    liq_long_usd_30s: Any = None,
    *,
    event_id: str = "",
    trade_id: str = "",
    symbol: str = "",
) -> Tuple[str, float, float]:
    """
    Returns (profile_name, risk_mult, notional_mult).
    notional_mult: multiplier on base notional (10 USD).
    Priority for fast0: 100k+ > 5k-25k > base.
    """
    s = (strategy or "").strip()
    if s == "short_pump":
        stage_i = None
        try:
            stage_i = int(stage) if stage is not None else None
        except (TypeError, ValueError):
            pass
        dist_val = None
        try:
            dist_val = float(dist_to_peak_pct) if dist_to_peak_pct is not None else None
        except (TypeError, ValueError):
            pass
        if stage_i == 4 and dist_val is not None and dist_val >= SHORT_PUMP_AUTO_DIST_MIN:
            profile = "short_pump_active_1R"
            mult = 1.0
            logger.info(
                "RISK_PROFILE | strategy=%s symbol=%s event_id=%s trade_id=%s liq=N/A "
                "selected_profile=%s risk_mult=%.1f fixed_notional_usd=%.0f leverage=%s margin_mode=%s",
                strategy, symbol, event_id, trade_id, profile, mult, LIVE_FIXED_NOTIONAL_USD, LIVE_LEVERAGE, LIVE_MARGIN_MODE,
            )
            return profile, mult, mult
        return ("", 0.0, 0.0)

    if s == "short_pump_fast0":
        allowed, reason = is_fast0_entry_allowed(liq_long_usd_30s, dist_to_peak_pct)
        if not allowed:
            if reason == "fast0_base_dist_gt_2.0":
                logger.info(
                    "RISK_PROFILE_REJECT | reason=fast0_base_dist_gt_2.0 symbol=%s event_id=%s liq_long_usd_30s=%s dist_to_peak_pct=%s",
                    symbol, event_id, liq_long_usd_30s, dist_to_peak_pct,
                )
            return ("", 0.0, 0.0)
        if not FAST0_AUTO_ENABLE:
            return ("", 0.0, 0.0)
        liq_val = None
        try:
            liq_val = float(liq_long_usd_30s) if liq_long_usd_30s is not None else None
        except (TypeError, ValueError):
            pass
        if liq_val is None:
            return ("", 0.0, 0.0)
        profile = ""
        mult = FAST0_BASE_RISK_MULT
        if FAST0_LIQ_100K_ENABLE and liq_val >= FAST0_LIQ_100K_MIN_USD:
            profile = "fast0_liq_100k_plus_2R"
            mult = FAST0_LIQ_100K_RISK_MULT
        elif FAST0_LIQ_5K_25K_ENABLE and liq_val > FAST0_LIQ_5K_25K_MIN_USD and liq_val <= FAST0_LIQ_5K_25K_MAX_USD:
            profile = "fast0_liq_5k_25k_1.5R"
            mult = FAST0_LIQ_5K_25K_RISK_MULT
        else:
            profile = "fast0_base_1R"
            mult = FAST0_BASE_RISK_MULT
        logger.info(
            "RISK_PROFILE | strategy=%s symbol=%s event_id=%s trade_id=%s liq_long_usd_30s=%.0f "
            "selected_profile=%s risk_mult=%.1f fixed_notional_usd=%.0f leverage=%s margin_mode=%s",
            strategy, symbol, event_id, trade_id, liq_val or 0, profile, mult, LIVE_FIXED_NOTIONAL_USD, LIVE_LEVERAGE, LIVE_MARGIN_MODE,
        )
        return profile, mult, mult

    return ("", 0.0, 0.0)


def get_notional_and_leverage(risk_mult: float = 1.0) -> Tuple[float, int, str]:
    """Return (notional_usd, leverage, margin_mode) for display/sizing."""
    notional = LIVE_FIXED_NOTIONAL_USD * risk_mult
    return (notional, LIVE_LEVERAGE, LIVE_MARGIN_MODE)
