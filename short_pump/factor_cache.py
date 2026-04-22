# short_pump/factor_cache.py
"""
In-memory cache + background refresh for the factor report.

Rationale:
- `build_factor_report()` is CPU-heavy (~75s). We cannot call it synchronously inside an API handler.
- A single default preset (days=7 + enabled strategies + live mode) is refreshed every N minutes in the background.
- Other (days, strategies, mode) combos are computed on-demand in a thread pool; results are cached by key with LRU eviction.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from analytics.factor_report import build_factor_report

logger = logging.getLogger(__name__)

CacheKey = Tuple[int, Tuple[str, ...], str]  # (days, strategies_sorted_tuple, mode)

_DEFAULT_ETA_SEC = float(os.getenv("FACTOR_REPORT_ETA_SEC", "75"))
_MAX_ENTRIES = int(os.getenv("FACTOR_CACHE_MAX", "6"))
_BASE_DIR = os.getenv("FACTOR_CACHE_BASE_DIR", "/root/pump_short")

_factor_cache: Dict[CacheKey, Dict[str, Any]] = {}
_inflight: Dict[CacheKey, asyncio.Lock] = {}
_last_duration_sec: float = _DEFAULT_ETA_SEC
_factor_task: Optional[asyncio.Task] = None


def _to_utc_iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def make_key(days: int, strategies: Sequence[str], mode: str = "live") -> CacheKey:
    return (int(days), tuple(sorted(str(s) for s in strategies)), str(mode))


def get_entry(key: CacheKey) -> Optional[Dict[str, Any]]:
    return _factor_cache.get(key)


def last_duration_sec() -> float:
    return _last_duration_sec


def is_inflight(key: CacheKey) -> bool:
    lock = _inflight.get(key)
    return bool(lock and lock.locked())


def _evict_lru(max_size: int = _MAX_ENTRIES) -> None:
    if len(_factor_cache) <= max_size:
        return
    # Entries keep an `access_ts` field we update on reads; fallback to computed_ts.
    ordered = sorted(
        _factor_cache.items(),
        key=lambda kv: kv[1].get("access_ts", kv[1].get("computed_ts", 0.0)),
    )
    to_drop = len(_factor_cache) - max_size
    for key, _ in ordered[:to_drop]:
        _factor_cache.pop(key, None)
        _inflight.pop(key, None)


async def recompute(key: CacheKey) -> Dict[str, Any]:
    """
    Recompute factor report for the given key. Dedup via per-key asyncio.Lock.
    Writes result into the cache; returns the cache entry.
    """
    global _last_duration_sec
    lock = _inflight.setdefault(key, asyncio.Lock())
    async with lock:
        days, strategies, mode = key
        t0 = time.monotonic()
        try:
            data, _txt = await asyncio.to_thread(
                build_factor_report,
                base_dir=_BASE_DIR,
                days=days,
                strategies=list(strategies) if strategies else None,
                mode=mode,
            )
            dur = time.monotonic() - t0
            _last_duration_sec = dur
            entry = {
                "status": "ready",
                "data": data,
                "computed_at": _to_utc_iso(time.time()),
                "computed_ts": time.time(),
                "access_ts": time.time(),
                "duration_sec": round(dur, 2),
            }
            _factor_cache[key] = entry
            _evict_lru()
            logger.info(
                "FACTOR_CACHE_READY | key=%s duration_sec=%.1f entries=%d",
                key, dur, len(_factor_cache),
            )
            return entry
        except Exception as exc:  # pragma: no cover - defensive
            logger.exception("FACTOR_CACHE_ERROR | key=%s", key)
            entry = {
                "status": "error",
                "error": str(exc),
                "computed_at": _to_utc_iso(time.time()),
                "computed_ts": time.time(),
                "access_ts": time.time(),
                "duration_sec": round(time.monotonic() - t0, 2),
            }
            _factor_cache[key] = entry
            return entry


def kick_off(key: CacheKey) -> None:
    """Fire-and-forget on-demand recompute if not already running."""
    if is_inflight(key):
        return
    try:
        asyncio.create_task(recompute(key))
    except RuntimeError:
        # No running loop - ignore (endpoint always has loop)
        pass


async def factor_refresh_loop(
    *,
    interval_min: int,
    default_key_fn: Callable[[], CacheKey],
) -> None:
    """
    Background loop: periodically refresh the default preset.
    Runs forever until cancelled on shutdown.
    """
    logger.info("FACTOR_REFRESH_START | interval_min=%d", interval_min)
    # Small initial delay so server startup doesn't contend with CSV loads.
    await asyncio.sleep(2)
    while True:
        try:
            key = default_key_fn()
            await recompute(key)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("FACTOR_REFRESH_ERROR")
        try:
            await asyncio.sleep(max(60, interval_min * 60))
        except asyncio.CancelledError:
            raise


def start_background(
    *,
    interval_min: int,
    default_key_fn: Callable[[], CacheKey],
) -> asyncio.Task:
    global _factor_task
    if _factor_task is not None and not _factor_task.done():
        return _factor_task
    _factor_task = asyncio.create_task(
        factor_refresh_loop(interval_min=interval_min, default_key_fn=default_key_fn)
    )
    return _factor_task


async def stop_background() -> None:
    global _factor_task
    if _factor_task is None:
        return
    _factor_task.cancel()
    try:
        await _factor_task
    except (asyncio.CancelledError, Exception):
        pass
    _factor_task = None


def touch_access(key: CacheKey) -> None:
    entry = _factor_cache.get(key)
    if entry is not None:
        entry["access_ts"] = time.time()
