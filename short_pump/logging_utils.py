# short_pump/logging_utils.py
"""
Centralized logging utilities for production debugging.
All exceptions are logged with full traceback and context.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional

# Initialize logging once at module import
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    force=True,  # Override any existing config
)

# Context storage (thread-local would be better, but simple dict works for now)
_context: Dict[str, Any] = {}


def get_logger(
    name: str,
    *,
    strategy_name: Optional[str] = None,
    logs_subdir: Optional[str] = None,
    symbol: Optional[str] = None,
) -> logging.Logger:
    """
    Get a logger instance for a module.
    Writes to {LOG_ROOT}/{LOG_*_SUBDIR}/{YYYY-MM-DD}/{symbol}.log.
    """
    logger = logging.getLogger(name)

    if strategy_name:
        s = strategy_name.strip().lower()
        is_long = s in ("long", "long_pullback", "long_only")
    else:
        is_long = False

    log_root = os.environ.get("LOG_ROOT") or os.environ.get("LOG_DIR") or "logs"
    short_subdir = os.environ.get("LOG_SHORT_SUBDIR") or "logs_short"
    long_subdir = os.environ.get("LOG_LONG_SUBDIR") or "logs_long"
    subdir = logs_subdir or (long_subdir if is_long else short_subdir)

    date_dir = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    sym = (symbol or "app").strip().upper() or "app"
    log_dir = os.path.join(log_root, subdir, date_dir)
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, f"{sym}.log")

    if not any(
        isinstance(h, logging.FileHandler) and getattr(h, "baseFilename", "") == log_path
        for h in logger.handlers
    ):
        fh = logging.FileHandler(log_path, encoding="utf-8")
        fh.setLevel(logging.INFO)
        fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
        logger.addHandler(fh)

    return logger


def demo_logger() -> None:
    """Example: logs to logs/logs_long/app.log."""
    long_logger = get_logger("long_strategy", strategy_name="long_pullback", symbol="APP")
    long_logger.info("demo long strategy log")


def bind_context(**kwargs: Any) -> None:
    """Bind context variables (symbol, run_id, stage, step) for current execution."""
    _context.update(kwargs)


def clear_context() -> None:
    """Clear context variables."""
    _context.clear()


def _format_context_msg(msg: str, **extra: Any) -> str:
    """Format message with context."""
    parts = [msg]
    ctx = {**_context, **extra}
    
    if ctx.get("symbol"):
        parts.append(f"symbol={ctx['symbol']}")
    if ctx.get("run_id"):
        parts.append(f"run_id={ctx['run_id']}")
    if ctx.get("stage") is not None:
        parts.append(f"stage={ctx['stage']}")
    if ctx.get("step"):
        parts.append(f"step={ctx['step']}")
    
    # Add any extra context
    for k, v in ctx.items():
        if k not in ("symbol", "run_id", "stage", "step") and v is not None:
            parts.append(f"{k}={v}")
    
    return " | ".join(parts)


def log_exception(
    logger: logging.Logger,
    msg: str,
    *,
    symbol: Optional[str] = None,
    run_id: Optional[str] = None,
    stage: Optional[int] = None,
    step: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
    exc_info: bool = True,
) -> None:
    """
    Log exception with full traceback and context.
    
    Args:
        logger: Logger instance
        msg: Error message
        symbol: Trading symbol (e.g., "BTCUSDT")
        run_id: Run identifier
        stage: Structure stage (0-4)
        step: Step name (e.g., "FETCH_5M", "ARMED", "ENTRY_DECISION")
        extra: Additional context dict
        exc_info: Whether to include exception info (default True)
    """
    extra_dict = extra or {}
    formatted_msg = _format_context_msg(
        msg,
        symbol=symbol,
        run_id=run_id,
        stage=stage,
        step=step,
        **extra_dict,
    )
    logger.exception(formatted_msg, exc_info=exc_info)


def log_info(
    logger: logging.Logger,
    msg: str,
    *,
    symbol: Optional[str] = None,
    run_id: Optional[str] = None,
    stage: Optional[int] = None,
    step: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    """Log info message with context."""
    extra_dict = extra or {}
    formatted_msg = _format_context_msg(
        msg,
        symbol=symbol,
        run_id=run_id,
        stage=stage,
        step=step,
        **extra_dict,
    )
    logger.info(formatted_msg)


def log_warning(
    logger: logging.Logger,
    msg: str,
    *,
    symbol: Optional[str] = None,
    run_id: Optional[str] = None,
    stage: Optional[int] = None,
    step: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    """Log warning message with context."""
    extra_dict = extra or {}
    formatted_msg = _format_context_msg(msg, symbol=symbol, run_id=run_id, stage=stage, step=step, **extra_dict)
    logger.warning(formatted_msg)
