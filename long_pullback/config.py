from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Config:
    strategy_name: str = "long_pullback"
    logs_root: str = "logs/logs_long"
    category: str = "linear"

    # thresholds
    pullback_min_pct: float = 0.8
    pullback_max_pct: float = 2.5
    pullback_time_min_minutes: int = 5
    pullback_time_max_minutes: int = 25

    cvd_30s_min: float = -0.12
    cvd_1m_min: float = -0.05

    feature_schema_version: int = 1

    poll_seconds_5m: int = 60

    # outcome / TP-SL
    outcome_watch_minutes: int = 120
    outcome_poll_seconds: int = 60
    tp_pct: float = 0.006
    sl_pct: float = 0.004
    conflict_policy: str = "SL_FIRST"

    # context weights (v1)
    weight_pump: float = 0.25
    weight_pullback: float = 0.25
    weight_absorption: float = 0.25
    weight_breakout: float = 0.25
