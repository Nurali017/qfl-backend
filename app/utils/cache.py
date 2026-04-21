"""Bounded in-process TTL cache for hot API endpoints.

Caches serialized JSON bytes only — no ORM/Pydantic objects.
Thread-safe via threading.Lock (gunicorn uses forked workers,
each gets its own dict).
"""

import logging
import time
import threading

logger = logging.getLogger(__name__)

_cache: dict[str, tuple[float, bytes]] = {}
_lock = threading.Lock()
_MAX_SIZE = 512


def cache_get(key: str) -> bytes | None:
    with _lock:
        entry = _cache.get(key)
        if entry is None:
            logger.debug("cache miss: %s", key)
            return None
        expires_at, value = entry
        if time.monotonic() > expires_at:
            del _cache[key]
            logger.debug("cache expired: %s", key)
            return None
        logger.debug("cache hit: %s", key)
        return value


def cache_set(key: str, value: bytes, ttl: int) -> None:
    with _lock:
        if len(_cache) >= _MAX_SIZE:
            # Evict the entry closest to expiry
            oldest_key = min(_cache, key=lambda k: _cache[k][0])
            del _cache[oldest_key]
        _cache[key] = (time.monotonic() + ttl, value)


def cache_delete(key: str) -> None:
    with _lock:
        if key in _cache:
            del _cache[key]
            logger.debug("cache delete: %s", key)


def cache_clear() -> None:
    with _lock:
        _cache.clear()
        logger.debug("cache clear")
