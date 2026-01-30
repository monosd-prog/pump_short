# short_pump/logging_utils.py
"""
Centralized logging utilities for production debugging.
All exceptions are logged with full traceback and context.
"""
from __future__ import annotations

import logging
import sys
from typing import Any, Dict, Optional

# Initialize logging once at module import
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stderr,  # stderr for systemd journalctl
    force=True,  # Override any existing config
)

# Context storage (thread-local would be better, but simple dict works for now)
_context: Dict[str, Any] = {}


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance for a module."""
    return logging.getLogger(name)


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
