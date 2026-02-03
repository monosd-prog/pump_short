from __future__ import annotations

from typing import Optional

from short_pump.logging_utils import get_logger as _get_logger


def get_logger(
    name: str,
    *,
    strategy: Optional[str] = None,
    logs_subdir: Optional[str] = None,
    symbol: Optional[str] = None,
    mode: Optional[str] = None,
):
    """
    Thin wrapper around short_pump.logging_utils.get_logger.
    Use strategy="long_pullback" to route logs to logs/logs_long.
    """
    _ = mode
    return _get_logger(name, strategy_name=strategy, logs_subdir=logs_subdir, symbol=symbol)
