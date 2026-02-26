"""Redis caching setup using fastapi-cache2."""

import hashlib
import logging
from typing import Optional

from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend

from app.config import get_settings

logger = logging.getLogger(__name__)


def _cache_key_builder(func, namespace: str = "", *, request=None, response=None, args=None, kwargs=None):
    """Custom key builder that includes query params and lang header."""
    prefix = FastAPICache.get_prefix()
    parts = [prefix, namespace, func.__module__, func.__qualname__]

    if request:
        # Include query string for cache variation
        query = str(request.query_params)
        if query:
            parts.append(hashlib.md5(query.encode()).hexdigest())
        # Include Accept-Language or lang param for localization
        lang = request.headers.get("Accept-Language", "")
        if lang:
            parts.append(lang[:5])

    return ":".join(parts)


async def init_cache():
    """Initialize Redis cache. Call from app lifespan."""
    settings = get_settings()
    if not settings.cache_enabled:
        logger.info("Redis cache disabled via CACHE_ENABLED=false")
        return
    try:
        from redis import asyncio as aioredis
        redis = aioredis.from_url(
            settings.redis_cache_url,
            encoding="utf-8",
            decode_responses=True,
        )
        FastAPICache.init(
            RedisBackend(redis),
            prefix="qfl",
            key_builder=_cache_key_builder,
        )
        logger.info("Redis cache initialized (DB 1)")
    except Exception as e:
        logger.warning("Redis cache init failed, caching disabled: %s", e)


async def invalidate_pattern(pattern: str):
    """Delete all cache keys matching a pattern (e.g. 'qfl:season:123:*')."""
    try:
        backend = FastAPICache.get_backend()
        redis = backend.redis
        full_pattern = f"qfl:{pattern}"
        keys = []
        async for key in redis.scan_iter(match=full_pattern):
            keys.append(key)
        if keys:
            await redis.delete(*keys)
            logger.debug("Invalidated %d cache keys matching %s", len(keys), full_pattern)
    except Exception as e:
        logger.warning("Cache invalidation failed for pattern %s: %s", pattern, e)
