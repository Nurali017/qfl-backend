"""Redis flag to skip live-game polling when no games are live.

When no flag is set, the 5-second Celery task returns instantly
without opening a DB session — zero disk I/O.
"""

from redis import asyncio as aioredis

from app.config import get_settings

LIVE_FLAG_KEY = "qfl:has_live_games"
_FLAG_TTL = 300  # 5 min safety net — auto-expires if nothing refreshes

_redis = None


async def _get_redis():
    global _redis
    if _redis is None:
        _redis = aioredis.from_url(get_settings().redis_url, socket_timeout=1)
    return _redis


async def has_live_games() -> bool:
    """Check flag. Returns True on error (fail open — proceed with DB check)."""
    try:
        r = await _get_redis()
        return bool(await r.exists(LIVE_FLAG_KEY))
    except Exception:
        return True


async def set_live_flag():
    """Set flag with TTL. Called on start_live_tracking and refreshed by the task."""
    try:
        r = await _get_redis()
        await r.set(LIVE_FLAG_KEY, "1", ex=_FLAG_TTL)
    except Exception:
        pass


async def clear_live_flag():
    """Clear flag. Called when no live games remain."""
    try:
        r = await _get_redis()
        await r.delete(LIVE_FLAG_KEY)
    except Exception:
        pass
