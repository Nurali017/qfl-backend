"""Bounded in-process TTL cache for hot API endpoints.

Caches serialized JSON bytes only — no ORM/Pydantic objects.
Thread-safe via threading.Lock (gunicorn uses forked workers,
each gets its own dict).
"""

import asyncio
import logging
import threading
import time
from typing import Awaitable, Callable

logger = logging.getLogger(__name__)

_cache: dict[str, tuple[float, bytes]] = {}
_lock = threading.Lock()
# 8192: player/team detail endpoints cache per-entity keys; the full hot
# keyspace is ~5700 (700 players × 2 langs × {detail, tournaments} + stats
# + team keys). Below that, eviction (earliest-expiry) thrashes under crawler
# traffic and long-tail keys never survive their TTL. Payloads are small JSON
# bytes (~2-10 KB) → worst case ~60 MB/worker.
_MAX_SIZE = 8192

# Singleflight: per-key asyncio.Lock to coalesce concurrent compute() calls
# on the same cold key. Without this, N concurrent handlers hitting a cold
# /table key all run the expensive query in parallel — the cache-stampede
# pattern observed on 2026-05-28 (RU /table 5-12s under burst, KZ <1s because
# RU traffic is ~6× higher). With singleflight the first caller runs, the
# rest wait and read the freshly cached value.
_singleflight_locks: dict[str, asyncio.Lock] = {}


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


async def cache_get_or_compute(
    key: str,
    ttl: int | Callable[[bytes], int],
    compute: Callable[[], Awaitable[bytes]],
) -> bytes:
    """Cache-aware fetch with singleflight protection.

    1. Fast path: if `key` is hot, return cached bytes immediately.
    2. Otherwise acquire the per-key asyncio lock; under it, re-check the
       cache (a concurrent coroutine may have just populated it) and
       otherwise run `compute()`, store its result, and return it.

    Concurrent callers on the same cold key serialize on this lock — only
    one of them actually executes `compute()`; the rest read the value it
    just cached. Lock dict grows to at most _MAX_SIZE × 2 entries (~200B
    each), which is small enough to leave alone without cleanup.

    `ttl` may be a callable receiving the computed bytes — used to cache
    negative results (b"null") for a shorter time than real payloads.
    """
    cached = cache_get(key)
    if cached is not None:
        return cached

    lock = _singleflight_locks.setdefault(key, asyncio.Lock())
    async with lock:
        cached = cache_get(key)
        if cached is not None:
            return cached
        value = await compute()
        ttl_value = ttl(value) if callable(ttl) else ttl
        cache_set(key, value, ttl_value)
        return value
