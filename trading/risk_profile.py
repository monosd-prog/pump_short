"""
Risk profile selection: strategy + liq bucket -> profile name, risk_mult, notional.
Used by broker gating, runner sizing, and Telegram formatting.
"""
from __future__ import annotations

import logging
import os
from typing import Any, Tuple

from trading.config import LIVE_FIXED_NOTIONAL_USD, LIVE_LEVERAGE, LIVE_MARGIN_MODE
from short_pump.rollout import SHORT_PUMP_FILTERED_DIST_TO_PEAK_MAX

logger = logging.getLogger(__name__)


FALSE_PUMP_LIVE_RISK_R = 1.0
FALSE_PUMP_LIVE_NOTIONAL_USD = 10.0
FALSE_PUMP_LIVE_LEVERAGE = 3
FALSE_PUMP_LIVE_MARGIN_MODE = "isolated"


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

# fast0: dist must be <= FAST0_DIST_MAX; liq in allowed buckets only
FAST0_DIST_MAX = _float_env("FAST0_DIST_MAX", 1.5)
# Allowed liq buckets: 0, (5k, 25k], >100k. Use existing env for boundaries.
FAST0_LIQ_5K = FAST0_LIQ_5K_25K_MIN_USD
FAST0_LIQ_25K = FAST0_LIQ_5K_25K_MAX_USD
FAST0_LIQ_100K = FAST0_LIQ_100K_MIN_USD


def is_fast0_entry_allowed(liq_long_usd_30s: Any, dist_to_peak_pct: Any) -> Tuple[bool, str]:
    """
    Single source of truth for fast0 tradeability (auto + TG).
    Allowed ONLY if: dist_to_peak_pct <= 1.5 AND liq in [0, (5k,25k], >100k].
    Reject: dist>1.5 -> fast0_dist_gt_1_5; liq not in bucket -> fast0_liq_not_in_allowed_bucket.
    """
    try:
        liq_val = float(liq_long_usd_30s) if liq_long_usd_30s is not None else None
    except (TypeError, ValueError):
        return False, "liq_missing"
    if liq_val is None:
        return False, "liq_missing"
    if liq_val < 0:
        return False, "fast0_liq_not_in_allowed_bucket"
    dist_val = None
    try:
        dist_val = float(dist_to_peak_pct) if dist_to_peak_pct is not None else None
    except (TypeError, ValueError):
        pass
    if dist_val is not None and dist_val > FAST0_DIST_MAX:
        return False, "fast0_dist_gt_1_5"
    # Allowed buckets: liq==0, 5k<liq<=25k, liq>100k
    in_base = liq_val == 0
    in_5k_25k = liq_val > FAST0_LIQ_5K and liq_val <= FAST0_LIQ_25K
    in_100k = liq_val > FAST0_LIQ_100K
    if in_base or in_5k_25k or in_100k:
        return True, ""
    return False, "fast0_liq_not_in_allowed_bucket"


def get_risk_profile(
    strategy: str,
    stage: Any = None,
    dist_to_peak_pct: Any = None,
    liq_long_usd_30s: Any = None,
    context_score: Any = None,
    funding_rate_abs: Any = None,
    volume_1m: Any = None,
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
    if s == "short_pump_fast0_filtered":
        s = "short_pump_fast0"
    if s == "short_pump_premium":
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
            profile = "short_pump_premium_1R"
            mult = 1.0
            logger.info(
                "RISK_PROFILE | strategy=%s symbol=%s event_id=%s trade_id=%s liq=N/A "
                "selected_profile=%s risk_mult=%.1f fixed_notional_usd=%.0f leverage=%s margin_mode=%s",
                strategy, symbol, event_id, trade_id, profile, mult, LIVE_FIXED_NOTIONAL_USD, LIVE_LEVERAGE, LIVE_MARGIN_MODE,
            )
            return profile, mult, mult
        return ("", 0.0, 0.0)

    if s == "short_pump_wick":
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
            profile = "short_pump_wick_1R"
            mult = 1.0
            logger.info(
                "RISK_PROFILE | strategy=%s symbol=%s event_id=%s trade_id=%s liq=N/A "
                "selected_profile=%s risk_mult=%.1f fixed_notional_usd=%.0f leverage=%s margin_mode=%s",
                strategy, symbol, event_id, trade_id, profile, mult, LIVE_FIXED_NOTIONAL_USD, LIVE_LEVERAGE, LIVE_MARGIN_MODE,
            )
            return profile, mult, mult
        return ("", 0.0, 0.0)

    if s == "short_pump_filtered":
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
        if stage_i == 4 and dist_val is not None and dist_val <= SHORT_PUMP_FILTERED_DIST_TO_PEAK_MAX:
            profile = "short_pump_filtered_1R"
            mult = 1.0
            logger.info(
                "RISK_PROFILE | strategy=%s symbol=%s event_id=%s trade_id=%s selected_profile=%s risk_mult=%.1f fixed_notional_usd=%.0f leverage=%s margin_mode=%s",
                strategy, symbol, event_id, trade_id, profile, mult, LIVE_FIXED_NOTIONAL_USD, LIVE_LEVERAGE, LIVE_MARGIN_MODE,
            )
            return profile, mult, mult
        return ("", 0.0, 0.0)

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
        ctx_val = None
        try:
            ctx_val = float(context_score) if context_score is not None else None
        except (TypeError, ValueError):
            pass
        fr_val = None
        try:
            fr_val = abs(float(funding_rate_abs)) if funding_rate_abs is not None else None
        except (TypeError, ValueError):
            pass
        liq_val = None
        try:
            liq_val = float(liq_long_usd_30s) if liq_long_usd_30s is not None else None
        except (TypeError, ValueError):
            pass

        # New live submode: short_pump_funding_1R
        # Condition: stage==3 and funding_rate_abs in [0.0005,0.001) OR [0.005,0.01)
        if (
            stage_i == 3
            and fr_val is not None
            and ((0.0005 <= fr_val < 0.001) or (0.005 <= fr_val < 0.01))
        ):
            profile = "short_pump_funding_1R"
            mult = 1.0
            logger.info(
                "RISK_PROFILE | strategy=%s symbol=%s event_id=%s trade_id=%s funding_rate_abs=%.6f "
                "selected_profile=%s risk_mult=%.1f fixed_notional_usd=%.0f leverage=%s margin_mode=%s",
                strategy, symbol, event_id, trade_id, fr_val, profile, mult,
                LIVE_FIXED_NOTIONAL_USD, LIVE_LEVERAGE, LIVE_MARGIN_MODE,
            )
            return profile, mult, mult

        # New live submodes (ACTIVE-by-default in auto-risk-guard):
        # - short_pump_deep: dist in [7.5,10), ctx in [0.4,0.6), liqL30s==0
        if (
            dist_val is not None
            and 7.5 <= dist_val < 10.0
            and ctx_val is not None
            and 0.4 <= ctx_val < 0.6
            and (liq_val is None or liq_val < 100.0)
        ):
            profile = "short_pump_deep"
            mult = _float_env("SHORT_PUMP_DEEP_RISK_MULT", 0.7)
            logger.info(
                "RISK_PROFILE | strategy=%s symbol=%s event_id=%s trade_id=%s liq_long_usd_30s=%.0f "
                "selected_profile=%s risk_mult=%.2f fixed_notional_usd=%.0f leverage=%s margin_mode=%s",
                strategy, symbol, event_id, trade_id, liq_val or 0, profile, mult, LIVE_FIXED_NOTIONAL_USD, LIVE_LEVERAGE, LIVE_MARGIN_MODE,
            )
            return profile, mult, mult

        # - short_pump_mid: stage==3, dist in [3.5,5), ctx in [0.4,0.6)
        if (
            stage_i == 3
            and dist_val is not None
            and 3.5 <= dist_val < 5.0
            and ctx_val is not None
            and 0.4 <= ctx_val < 0.6
        ):
            profile = "short_pump_mid"
            mult = _float_env("SHORT_PUMP_MID_RISK_MULT", 0.7)
            logger.info(
                "RISK_PROFILE | strategy=%s symbol=%s event_id=%s trade_id=%s liq_long_usd_30s=%s "
                "selected_profile=%s risk_mult=%.2f fixed_notional_usd=%.0f leverage=%s margin_mode=%s",
                strategy, symbol, event_id, trade_id,
                f"{liq_val:.0f}" if liq_val is not None else "N/A",
                profile, mult, LIVE_FIXED_NOTIONAL_USD, LIVE_LEVERAGE, LIVE_MARGIN_MODE,
            )
            return profile, mult, mult

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
            logger.info(
                "RISK_PROFILE_REJECT | reason=%s symbol=%s event_id=%s liq_long_usd_30s=%s dist_to_peak_pct=%s",
                reason, symbol, event_id, liq_long_usd_30s, dist_to_peak_pct,
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
        ctx_val = None
        try:
            ctx_val = float(context_score) if context_score is not None else None
        except (TypeError, ValueError):
            pass
        vol1_val = None
        try:
            vol1_val = float(volume_1m) if volume_1m is not None else None
        except (TypeError, ValueError):
            pass

        # New live submode: FAST0 SELECTIVE (context_score in [0.4,0.6), optional volume_1m>100k when available)
        _FAST0_SELECTIVE_VOL_MIN = 100_000.0
        if ctx_val is not None and 0.4 <= ctx_val < 0.6:
            if vol1_val is None or vol1_val > _FAST0_SELECTIVE_VOL_MIN:
                profile = "fast0_selective"
                mult = _float_env("FAST0_SELECTIVE_RISK_MULT", 0.7) * FAST0_BASE_RISK_MULT
                logger.info(
                    "RISK_PROFILE | strategy=%s symbol=%s event_id=%s trade_id=%s liq_long_usd_30s=%.0f "
                    "context_score=%.2f volume_1m=%s vol_threshold=%.0f "
                    "selected_profile=%s risk_mult=%.2f fixed_notional_usd=%.0f leverage=%s margin_mode=%s",
                    strategy, symbol, event_id, trade_id, liq_val or 0,
                    ctx_val, f"{vol1_val:.0f}" if vol1_val is not None else "None", _FAST0_SELECTIVE_VOL_MIN,
                    profile, mult, LIVE_FIXED_NOTIONAL_USD, LIVE_LEVERAGE, LIVE_MARGIN_MODE,
                )
                return profile, mult, mult
            else:
                logger.info(
                    "RISK_PROFILE | strategy=%s symbol=%s event_id=%s context_score=%.2f "
                    "volume_1m=%.0f vol_threshold=%.0f → vol_gate_blocked → fallback to liq_bucket",
                    strategy, symbol, event_id, ctx_val, vol1_val, _FAST0_SELECTIVE_VOL_MIN,
                )
        # Buckets: liq==0 -> 1R, 5k<liq<=25k -> 1.5R, liq>100k -> 2R
        if liq_val > FAST0_LIQ_100K:
            profile = "fast0_2R"
            mult = FAST0_LIQ_100K_RISK_MULT
        elif liq_val > FAST0_LIQ_5K and liq_val <= FAST0_LIQ_25K:
            profile = "fast0_1p5R"
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

    if s == "false_pump":
        profile = "false_pump_live_1R"
        mult = FALSE_PUMP_LIVE_RISK_R
        logger.info(
            "RISK_PROFILE | strategy=%s symbol=%s profile=%s risk_r=%.1f fixed_notional_usd=%.0f leverage=%s margin_mode=%s",
            strategy,
            symbol,
            profile,
            mult,
            FALSE_PUMP_LIVE_NOTIONAL_USD,
            FALSE_PUMP_LIVE_LEVERAGE,
            FALSE_PUMP_LIVE_MARGIN_MODE,
        )
        return profile, mult, mult

    return ("", 0.0, 0.0)


def get_notional_and_leverage(risk_mult: float = 1.0) -> Tuple[float, int, str]:
    """Return (notional_usd, leverage, margin_mode) for display/sizing."""
    notional = LIVE_FIXED_NOTIONAL_USD * risk_mult
    return (notional, LIVE_LEVERAGE, LIVE_MARGIN_MODE)
