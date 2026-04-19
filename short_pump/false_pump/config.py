from __future__ import annotations

from dataclasses import dataclass


@dataclass
class FalsePumpConfig:
    trigger_min_oi_pct: float = 80.0
    trigger_min_price_pct: float = 1.0
    monitor_timeout_sec: int = 14400
    poll_interval_sec: int = 10
    trigger_cooldown_sec: int = 300
    pump_price_pct: float = 0.8
    pump_candles_count: int = 3
    pump_lookback_candles: int = 20
    oi_max_reaction_pct: float = 0.8
    near_top_pct: float = 5.0
    funding_min_threshold: float = -0.015
    liq_window_sec: int = 300
    liq_min_usd: float = 30000.0
    min_flags_required: int = 2
    webhook_host: str = "0.0.0.0"
    webhook_port: int = 8441
    signal_name: str = "false_pump"
    sl_pct: float = 7.0
    tp_pct: float = 15.0
