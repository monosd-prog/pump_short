"""Trading configuration. PAPER mode first; live adapter plugged later via interface."""
from __future__ import annotations

import os
from pathlib import Path

# Feature flags (env)
AUTO_TRADING_ENABLE = os.getenv("AUTO_TRADING_ENABLE", "0").strip().lower() in ("1", "true", "yes")
AUTO_TRADING_MODE = os.getenv("AUTO_TRADING_MODE", "paper")

# Execution: "paper" | "live" (paper = gating + simulated fills; live = exchange adapter)
EXECUTION_MODE = os.getenv("EXECUTION_MODE", os.getenv("AUTO_TRADING_MODE", "paper")).strip().lower()
if EXECUTION_MODE not in ("paper", "live"):
    EXECUTION_MODE = "paper"

# Mode: "paper" | "live" (live uses exchange adapter - TODO)
MODE = AUTO_TRADING_MODE

# Paper account
PAPER_EQUITY_USD = float(os.getenv("PAPER_EQUITY_USD", "1000.0"))
LEVERAGE = int(os.getenv("TRADING_LEVERAGE", "4"))

# Risk
RISK_PCT = float(os.getenv("TRADING_RISK_PCT", "0.0025"))  # 0.25% per trade (1R)
try:
    _fp = os.getenv("FIXED_POSITION_USD") or "0"
    FIXED_POSITION_USD = float(str(_fp).replace(",", "."))
except (TypeError, ValueError):
    FIXED_POSITION_USD = 0.0
if FIXED_POSITION_USD < 0:
    FIXED_POSITION_USD = 0.0
MAX_OPEN_PER_STRATEGY = int(os.getenv("MAX_OPEN_PER_STRATEGY", "1"))
MAX_TOTAL_RISK_PCT = float(os.getenv("MAX_TOTAL_RISK_PCT", "0.005"))  # 0.5% total

# Strategies to trade (comma-separated). Default single strategy for backward compat.
STRATEGIES_ENV = os.getenv("STRATEGIES", "short_pump").strip()
ALLOWED_STRATEGIES: list[str] = [s.strip() for s in STRATEGIES_ENV.split(",") if s.strip()] or ["short_pump"]

# LIVE micro-risk guardrails
MAX_RISK_USD_PER_TRADE = float(os.getenv("MAX_RISK_USD_PER_TRADE", "7.0"))
MAX_LEVERAGE = int(os.getenv("MAX_LEVERAGE", "4"))
MAX_CONCURRENT_TRADES = int(os.getenv("MAX_CONCURRENT_TRADES", "1"))
MAX_DAILY_LOSS_USD = float(os.getenv("MAX_DAILY_LOSS_USD", "21.0"))

# Stop distance validation: reject if stop < 0.2% or > 5% of entry
STOP_DISTANCE_MIN_PCT = float(os.getenv("STOP_DISTANCE_MIN_PCT", "0.002"))
STOP_DISTANCE_MAX_PCT = float(os.getenv("STOP_DISTANCE_MAX_PCT", "0.05"))

# Paths (relative to project root or absolute)
_ROOT = Path(__file__).resolve().parent.parent

# Paper simulation (when EXECUTION_MODE=paper)
PAPER_FEE_BPS = int(os.getenv("PAPER_FEE_BPS", "6"))  # 0.06% per side
PAPER_SLIPPAGE_BPS = int(os.getenv("PAPER_SLIPPAGE_BPS", "2"))

# TTL failsafe: auto-close PAPER positions older than TTL
POSITION_TTL_SECONDS = int(os.getenv("POSITION_TTL_SECONDS", "1800"))  # 30 min
TIMEOUT_EXIT_MODE = os.getenv("TIMEOUT_EXIT_MODE", "entry").strip().lower()  # "entry" or "sl"

# Datasets root for writing outcomes (same pipeline as live).
# Env: DATASET_BASE_DIR (default: {project_root}/datasets).
DATASET_BASE_DIR = os.getenv("DATASET_BASE_DIR", str(_ROOT / "datasets"))
STATE_PATH = os.getenv("TRADING_STATE_PATH", str(_ROOT / "datasets" / "trading_state.json"))
LOG_PATH = os.getenv("TRADING_LOG_PATH", str(_ROOT / "datasets" / "trading_trades.csv"))
CLOSES_PATH = os.getenv("TRADING_CLOSES_PATH", str(_ROOT / "datasets" / "trading_closes.csv"))
RUNNER_LOCK_PATH = os.getenv("TRADING_RUNNER_LOCK_PATH", str(_ROOT / "datasets" / "trading_runner.lock"))
# Queue path for enqueue_signal (default: datasets/signals_queue.jsonl)
SIGNALS_QUEUE_PATH = os.getenv("SIGNALS_QUEUE_PATH", str(_ROOT / "datasets" / "signals_queue.jsonl"))
PROCESSING_PATH = SIGNALS_QUEUE_PATH + ".processing"
PROCESSED_PATH = SIGNALS_QUEUE_PATH + ".processed"
