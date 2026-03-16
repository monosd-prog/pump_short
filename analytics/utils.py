from __future__ import annotations

from typing import Any


DEBUG_ENABLED = False


def set_debug(enabled: bool) -> None:
    global DEBUG_ENABLED
    DEBUG_ENABLED = bool(enabled)


def dprint(enabled: bool, *args: Any, **kwargs: Any) -> None:
    if enabled:
        print(*args, **kwargs)
