"""Per-symbol cooldown gate for v2 entry."""
from __future__ import annotations

import threading
import time
from typing import Dict

_default_gate: "EntryGate | None" = None


class EntryGate:
    """Thread-safe cooldown per symbol."""

    def __init__(self, cooldown_seconds: int = 120):
        self._cooldown = cooldown_seconds
        self._last_enqueue: Dict[str, float] = {}
        self._lock = threading.Lock()

    def allow_and_record(self, symbol: str) -> bool:
        """True if cooldown elapsed; records timestamp when True."""
        now = time.time()
        with self._lock:
            last = self._last_enqueue.get(symbol, 0.0)
            if now - last >= self._cooldown:
                self._last_enqueue[symbol] = now
                return True
            return False

    def reset(self, symbol: str) -> None:
        """Clear cooldown for symbol (tests)."""
        with self._lock:
            self._last_enqueue.pop(symbol, None)


def get_default_gate() -> EntryGate:
    global _default_gate
    if _default_gate is None:
        _default_gate = EntryGate(cooldown_seconds=120)
    return _default_gate
