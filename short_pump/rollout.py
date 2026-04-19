from __future__ import annotations

import math
import os
from typing import Any


def _get_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "y", "on")


def _get_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or not str(raw).strip():
        return default
    try:
        return float(str(raw).replace(",", "."))
    except (TypeError, ValueError):
        return default


SHORT_PUMP_FILTERED_ENABLE = _get_bool("SHORT_PUMP_FILTERED_ENABLE", False)
SHORT_PUMP_FILTERED_DIST_TO_PEAK_MAX = _get_float("SHORT_PUMP_FILTERED_DIST_TO_PEAK_MAX", 1.0)

SHORT_PUMP_PREMIUM_ENABLE = _get_bool("SHORT_PUMP_PREMIUM_ENABLE", False)
SHORT_PUMP_PREMIUM_FUNDING_MIN = _get_float("SHORT_PUMP_PREMIUM_FUNDING_MIN", 0.005)
SHORT_PUMP_PREMIUM_FUNDING_MAX = _get_float("SHORT_PUMP_PREMIUM_FUNDING_MAX", 0.01)
SHORT_PUMP_PREMIUM_DELTA_RATIO_MAX = _get_float("SHORT_PUMP_PREMIUM_DELTA_RATIO_MAX", 0.0)
SHORT_PUMP_PREMIUM_DELTA_RATIO_MIN = _get_float("SHORT_PUMP_PREMIUM_DELTA_RATIO_MIN", -0.5)

SHORT_PUMP_WICK_ENABLE = _get_bool("SHORT_PUMP_WICK_ENABLE", False)
SHORT_PUMP_WICK_DELTA_MIN = _get_float("SHORT_PUMP_WICK_DELTA_MIN", -0.5)
SHORT_PUMP_WICK_DELTA_MAX = _get_float("SHORT_PUMP_WICK_DELTA_MAX", 0.0)
SHORT_PUMP_WICK_RATIO_MIN = _get_float("SHORT_PUMP_WICK_RATIO_MIN", 2.0)
SHORT_PUMP_WICK_RATIO_MAX = _get_float("SHORT_PUMP_WICK_RATIO_MAX", 4.0)


def _coerce_dist(dist_to_peak_pct: Any) -> float | None:
    try:
        value = float(dist_to_peak_pct)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(value):
        return None
    return value


def _coerce_float_optional(val: Any) -> float | None:
    try:
        f = float(val)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(f):
        return None
    return f


def _premium_funding_delta_match(funding_rate_abs: Any, delta_ratio_30s: Any) -> bool:
    """True when premium sub-mode should route (funding band x delta_ratio_30s band)."""
    if not SHORT_PUMP_PREMIUM_ENABLE:
        return False
    f = _coerce_float_optional(funding_rate_abs)
    d = _coerce_float_optional(delta_ratio_30s)
    if f is None or d is None:
        return False
    if not (SHORT_PUMP_PREMIUM_FUNDING_MIN <= f < SHORT_PUMP_PREMIUM_FUNDING_MAX):
        return False
    if not (SHORT_PUMP_PREMIUM_DELTA_RATIO_MIN <= d < SHORT_PUMP_PREMIUM_DELTA_RATIO_MAX):
        return False
    return True


def _wick_delta_wick_match(delta_ratio_30s: Any, wick_body_ratio_last: Any) -> bool:
    """delta_ratio_30s + wick_body_ratio_last bands (after premium; lower EV than premium)."""
    if not SHORT_PUMP_WICK_ENABLE:
        return False
    d = _coerce_float_optional(delta_ratio_30s)
    w = _coerce_float_optional(wick_body_ratio_last)
    if d is None or w is None:
        return False
    if not (SHORT_PUMP_WICK_DELTA_MIN <= d < SHORT_PUMP_WICK_DELTA_MAX):
        return False
    if not (SHORT_PUMP_WICK_RATIO_MIN <= w < SHORT_PUMP_WICK_RATIO_MAX):
        return False
    return True


def resolve_short_pump_route(
    dist_to_peak_pct: Any,
    *,
    funding_rate_abs: Any = None,
    delta_ratio_30s: Any = None,
    wick_body_ratio_last: Any = None,
) -> dict[str, Any]:
    """
    Decide whether a short_pump entry should stay on the baseline path, route to
    short_pump_premium, short_pump_wick, or short_pump_filtered.

    Returns a dict with:
    - route_strategy
    - would_pass_dist_filter
    - filter_reason
    - strategy_candidate
    - dist_to_peak_pct
    """
    dist_val = _coerce_dist(dist_to_peak_pct)
    would_pass = dist_val is not None and dist_val <= SHORT_PUMP_FILTERED_DIST_TO_PEAK_MAX

    if _premium_funding_delta_match(funding_rate_abs, delta_ratio_30s):
        return {
            "route_strategy": "short_pump_premium",
            "would_pass_dist_filter": would_pass,
            "filter_reason": "premium_funding_delta",
            "strategy_candidate": "short_pump_premium",
            "dist_to_peak_pct": dist_val,
        }

    if _wick_delta_wick_match(delta_ratio_30s, wick_body_ratio_last):
        return {
            "route_strategy": "short_pump_wick",
            "would_pass_dist_filter": would_pass,
            "filter_reason": "wick_delta_wick",
            "strategy_candidate": "short_pump_wick",
            "dist_to_peak_pct": dist_val,
        }

    if dist_val is None:
        filter_reason = "missing_dist"
    elif would_pass and SHORT_PUMP_FILTERED_ENABLE:
        filter_reason = "pass"
    elif would_pass and not SHORT_PUMP_FILTERED_ENABLE:
        filter_reason = "disabled"
    else:
        filter_reason = "dist_gt_max"

    route_strategy = "short_pump_filtered" if (SHORT_PUMP_FILTERED_ENABLE and would_pass) else "short_pump"
    strategy_candidate = "short_pump_filtered" if would_pass else "baseline_only"
    return {
        "route_strategy": route_strategy,
        "would_pass_dist_filter": would_pass,
        "filter_reason": filter_reason,
        "strategy_candidate": strategy_candidate,
        "dist_to_peak_pct": dist_val,
    }


def is_tradeable_short_pump_filtered(stage: int, dist_to_peak_pct: float | None = None) -> bool:
    """Return True when the filtered strategy should be allowed to trade."""
    if stage != 4:
        return False
    dist_val = _coerce_dist(dist_to_peak_pct)
    if dist_val is None:
        return False
    return dist_val <= SHORT_PUMP_FILTERED_DIST_TO_PEAK_MAX
