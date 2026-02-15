import os
from dataclasses import dataclass
from typing import Optional

from short_pump.logging_utils import get_logger, log_exception, log_info, log_warning

logger = get_logger(__name__)


def _get_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


def _get_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None:
        return default
    try:
        return int(v)
    except Exception as e:
        log_exception(logger, f"Failed to parse int env var {name}", step="CONFIG", extra={"value": v, "default": default})
        return default


def _get_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None:
        return default
    try:
        return float(v.replace(",", "."))
    except Exception as e:
        log_exception(logger, f"Failed to parse float env var {name}", step="CONFIG", extra={"value": v, "default": default})
        return default


def _get_optional_float(name: str, default: Optional[float]) -> Optional[float]:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    try:
        return float(v)
    except Exception as e:
        log_exception(logger, f"Failed to parse optional float env var {name}", step="CONFIG", extra={"value": v, "default": default})
        return default


def _get_str(name: str, default: str) -> str:
    v = os.environ.get(name)
    return default if v is None else v


@dataclass
class Config:
    # ===== watcher base =====
    symbol: str = "BTCUSDT"
    category: str = "linear"

    watch_minutes: int = 90
    poll_seconds: int = 300  # 5m polling before ARMED
    vol_z_lookback: int = 50

    # ===== Server =====
    max_concurrent: int = 3
    cooldown_minutes: int = 45

    # ===== Logging paths =====
    logs_root: str = "logs"
    logs_short_dir: str = "logs"
    logs_long_dir: str = "logs/logs_long"

    # ===== Entry mode =====
    entry_mode: str = "HYBRID"  # HYBRID or FAST_ONLY

    # ===== Pump filter (server) =====
    min_pump_pct: float = 8.0
    require_10m_window: bool = False

    # ===== 5m structure thresholds =====
    drop1_min_pct: float = 0.03
    bounce1_min_pct: float = 0.012
    drop2_min_pct: float = 0.010
    bounce2_min_pct: float = 0.006
    dist_to_peak_max_pct: float = 0.12

    # ===== 1m / fast cadence =====
    poll_seconds_1m: int = 60
    poll_seconds_fast: int = 15

    # ===== Entry thresholds =====
    delta_ratio_30s_min: Optional[float] = None
    delta_ratio_30s_max: Optional[float] = -0.12
    delta_ratio_fast_late_max: float = -0.18
    delta_ratio_1m_max: float = -0.05
    entry_candidate_ttl_seconds: int = 120
    entry_context_min_score: float = 0.45
    entry_context_oi_change_max_pct: Optional[float] = None
    entry_context_funding_abs_max: Optional[float] = None
    entry_liq_long_bonus_usd_30s: Optional[float] = None
    entry_liq_short_penalty_usd_30s: Optional[float] = None
    entry_liq_short_veto_usd_30s: Optional[float] = None
    entry_liq_long_bonus_score: float = 0.05
    entry_liq_short_penalty_score: float = 0.05

    break_low_lookback: int = 3
    no_new_high_lookback: int = 5

    # ===== Stop / outcome =====
    stop_on: str = "ANY"

    outcome_watch_minutes: int = 120
    outcome_poll_seconds: int = 60
    conflict_policy: str = "SL_FIRST"

    # ===== TP / SL =====
    tp_pct_confirm: float = 0.006
    sl_pct_confirm: float = 0.004
    tp_pct_early: float = 0.006
    sl_pct_early: float = 0.008

    # ===== Late-entry =====
    late_dist_pct: float = 8.0
    delta_ratio_early_late_max: float = -0.12

    # ===== CVD (Cumulative Volume Delta) =====
    cvd_delta_ratio_30s_max: float = -0.12
    cvd_delta_ratio_1m_max: float = -0.05
    cvd_weight: float = 0.2

    @classmethod
    def from_env(cls) -> "Config":
        c = cls()

        # base
        c.symbol = _get_str("SYMBOL", c.symbol)
        c.category = _get_str("CATEGORY", c.category)

        c.watch_minutes = _get_int("WATCH_MINUTES", c.watch_minutes)
        c.poll_seconds = _get_int("POLL_SECONDS", c.poll_seconds)
        c.vol_z_lookback = _get_int("VOL_Z_LOOKBACK", c.vol_z_lookback)

        # server
        c.max_concurrent = _get_int("MAX_CONCURRENT", c.max_concurrent)
        c.cooldown_minutes = _get_int("COOLDOWN_MINUTES", c.cooldown_minutes)
        raw_entry_mode = os.environ.get("ENTRY_MODE")
        get_str_entry_mode = _get_str("ENTRY_MODE", c.entry_mode)
        normalized_entry_mode = get_str_entry_mode.strip().upper()
        if normalized_entry_mode not in ("FAST_ONLY", "HYBRID"):
            log_warning(
                logger,
                "Invalid ENTRY_MODE, falling back to HYBRID",
                step="CONFIG",
                extra={"raw_env_entry_mode": raw_entry_mode, "get_str_entry_mode": get_str_entry_mode},
            )
            normalized_entry_mode = "HYBRID"
        c.entry_mode = normalized_entry_mode
        log_info(
            logger,
            "ENTRY_MODE resolved",
            step="CONFIG",
            extra={
                "raw_env_entry_mode": raw_entry_mode,
                "get_str_entry_mode": get_str_entry_mode,
                "final_entry_mode": c.entry_mode,
            },
        )

        # pump filter
        c.min_pump_pct = _get_float("MIN_PUMP_PCT", c.min_pump_pct)
        c.require_10m_window = _get_bool("REQUIRE_10M_WINDOW", c.require_10m_window)

        # cadence
        c.poll_seconds_1m = _get_int("POLL_SECONDS_1M", c.poll_seconds_1m)
        c.poll_seconds_fast = _get_int("POLL_SECONDS_FAST", c.poll_seconds_fast)

        # entry thresholds
        c.delta_ratio_30s_min = _get_optional_float("DELTA_RATIO_30S_MIN", c.delta_ratio_30s_min)
        c.delta_ratio_30s_max = _get_optional_float("DELTA_RATIO_30S_MAX", c.delta_ratio_30s_max)
        c.entry_candidate_ttl_seconds = _get_int("ENTRY_CANDIDATE_TTL_SECONDS", c.entry_candidate_ttl_seconds)
        c.entry_context_min_score = _get_float("ENTRY_CONTEXT_MIN_SCORE", c.entry_context_min_score)
        c.entry_context_oi_change_max_pct = _get_optional_float("ENTRY_CONTEXT_OI_CHANGE_MAX_PCT", c.entry_context_oi_change_max_pct)
        c.entry_context_funding_abs_max = _get_optional_float("ENTRY_CONTEXT_FUNDING_ABS_MAX", c.entry_context_funding_abs_max)
        c.entry_liq_long_bonus_usd_30s = _get_optional_float("ENTRY_LIQ_LONG_BONUS_USD_30S", c.entry_liq_long_bonus_usd_30s)
        c.entry_liq_short_penalty_usd_30s = _get_optional_float("ENTRY_LIQ_SHORT_PENALTY_USD_30S", c.entry_liq_short_penalty_usd_30s)
        c.entry_liq_short_veto_usd_30s = _get_optional_float("ENTRY_LIQ_SHORT_VETO_USD_30S", c.entry_liq_short_veto_usd_30s)
        c.entry_liq_long_bonus_score = _get_float("ENTRY_LIQ_LONG_BONUS_SCORE", c.entry_liq_long_bonus_score)
        c.entry_liq_short_penalty_score = _get_float("ENTRY_LIQ_SHORT_PENALTY_SCORE", c.entry_liq_short_penalty_score)

        # outcome
        c.outcome_watch_minutes = _get_int("OUTCOME_WATCH_MINUTES", c.outcome_watch_minutes)
        c.outcome_poll_seconds = _get_int("OUTCOME_POLL_SECONDS", c.outcome_poll_seconds)
        raw_conflict = _get_str("CONFLICT_POLICY", c.conflict_policy).strip().upper()
        if raw_conflict not in ("SL_FIRST", "TP_FIRST", "NEUTRAL"):
            log_warning(
                logger,
                "Invalid CONFLICT_POLICY, falling back to SL_FIRST",
                step="CONFIG",
                extra={"conflict_policy": raw_conflict},
            )
            raw_conflict = "SL_FIRST"
        c.conflict_policy = raw_conflict

        # TP / SL overrides
        c.tp_pct_confirm = _get_float("TP_PCT_CONFIRM", c.tp_pct_confirm)
        c.sl_pct_confirm = _get_float("SL_PCT_CONFIRM", c.sl_pct_confirm)
        c.tp_pct_early = _get_float("TP_PCT_EARLY", c.tp_pct_early)
        c.sl_pct_early = _get_float("SL_PCT_EARLY", c.sl_pct_early)

        # CVD
        c.cvd_delta_ratio_30s_max = _get_float("CVD_DELTA_RATIO_30S_MAX", c.cvd_delta_ratio_30s_max)
        c.cvd_delta_ratio_1m_max = _get_float("CVD_DELTA_RATIO_1M_MAX", c.cvd_delta_ratio_1m_max)
        c.cvd_weight = _get_float("CVD_WEIGHT", c.cvd_weight)

        if _get_bool("DEBUG_CONFIG", False):
            log_info(
                logger,
                "TP_SL_CONFIG",
                step="CONFIG",
                extra={
                    "tp_pct_confirm": c.tp_pct_confirm,
                    "sl_pct_confirm": c.sl_pct_confirm,
                    "tp_pct_early": c.tp_pct_early,
                    "sl_pct_early": c.sl_pct_early,
                    "conflict_policy": c.conflict_policy,
                },
            )

        return c