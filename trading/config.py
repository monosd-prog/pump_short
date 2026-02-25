"""Trading configuration. PAPER mode first; live adapter plugged later via interface."""
from __future__ import annotations

import os
from pathlib import Path

# Feature flags (env)
AUTO_TRADING_ENABLE = os.getenv("AUTO_TRADING_ENABLE", "0").strip().lower() in ("1", "true", "yes")
AUTO_TRADING_MODE = os.getenv("AUTO_TRADING_MODE", "paper")

# Mode: "paper" | "live" (live uses exchange adapter - TODO)
MODE = AUTO_TRADING_MODE

# Paper account
PAPER_EQUITY_USD = float(os.getenv("PAPER_EQUITY_USD", "1000.0"))
LEVERAGE = int(os.getenv("TRADING_LEVERAGE", "4"))

# Risk
RISK_PCT = float(os.getenv("TRADING_RISK_PCT", "0.0025"))  # 0.25% per trade (1R)
MAX_OPEN_PER_STRATEGY = int(os.getenv("MAX_OPEN_PER_STRATEGY", "1"))
MAX_TOTAL_RISK_PCT = float(os.getenv("MAX_TOTAL_RISK_PCT", "0.005"))  # 0.5% total

# Stop distance validation: reject if stop < 0.2% or > 5% of entry
STOP_DISTANCE_MIN_PCT = float(os.getenv("STOP_DISTANCE_MIN_PCT", "0.002"))
STOP_DISTANCE_MAX_PCT = float(os.getenv("STOP_DISTANCE_MAX_PCT", "0.05"))

# Paths (relative to project root or absolute)
_ROOT = Path(__file__).resolve().parent.parent
STATE_PATH = os.getenv("TRADING_STATE_PATH", str(_ROOT / "datasets" / "trading_state.json"))
LOG_PATH = os.getenv("TRADING_LOG_PATH", str(_ROOT / "datasets" / "trading_trades.csv"))
# Queue path for enqueue_signal (default: datasets/signals_queue.jsonl)
SIGNALS_QUEUE_PATH = os.getenv("SIGNALS_QUEUE_PATH", str(_ROOT / "datasets" / "signals_queue.jsonl"))
