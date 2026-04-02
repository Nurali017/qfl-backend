"""Redis flag indicating live games are active.

Advisory only — the sync dispatcher always queries DB directly.
The flag is maintained for other consumers (e.g. frontend polling hints)
but must NOT be used as a hard gate for sync operations.
"""

from redis import asyncio as aioredis

from app.config import get_settings

LIVE_FLAG_KEY = "qfl:has_live_games"
_FLAG_TTL = 300  # 5 min safety net — auto-expires if nothing refreshes

_redis = None


async def get_redis():
    global _redis
    if _redis is None:
        _redis = aioredis.from_url(get_settings().redis_url, socket_timeout=1)
    return _redis


async def has_live_games() -> bool:
    """Check flag. Returns True on error (fail open — proceed with DB check)."""
    try:
        r = await get_redis()
        return bool(await r.exists(LIVE_FLAG_KEY))
    except Exception:
        return True


async def set_live_flag():
    """Set flag with TTL. Called on start_live_tracking and refreshed by the task."""
    try:
        r = await get_redis()
        await r.set(LIVE_FLAG_KEY, "1", ex=_FLAG_TTL)
    except Exception:
        pass


async def clear_live_flag():
    """Clear flag. Called when no live games remain."""
    try:
        r = await get_redis()
        await r.delete(LIVE_FLAG_KEY)
    except Exception:
        pass
