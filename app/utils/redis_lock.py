"""Shared Redis token-lock utilities (compare-and-swap pattern).

Extracted from live_tasks.py so multiple task families can use the same
acquire / release pattern without duplication.
"""

import uuid

# Lua script: delete key only if its value matches our token.
# Prevents a late-finishing worker from deleting a lock that was
# already re-acquired by a newer dispatch cycle.
CAS_DELETE_SCRIPT = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
else
    return 0
end
"""


async def acquire_token_lock(key: str, ttl: int) -> str | None:
    """SET key <token> NX EX ttl.  Returns token on success, None if held."""
    token = uuid.uuid4().hex
    try:
        from app.utils.live_flag import get_redis
        r = await get_redis()
        ok = await r.set(key, token, nx=True, ex=ttl)
        return token if ok else None
    except Exception:
        # Fail open — return a token so the task proceeds
        return token


async def release_token_lock(key: str, token: str) -> None:
    """Compare-and-delete: remove key only if it still holds our token."""
    try:
        from app.utils.live_flag import get_redis
        r = await get_redis()
        await r.eval(CAS_DELETE_SCRIPT, 1, key, token)
    except Exception:
        pass


async def is_lock_held(key: str) -> bool:
    """Check if a lock key is currently held (for status checks)."""
    try:
        from app.utils.live_flag import get_redis
        r = await get_redis()
        return await r.exists(key) > 0
    except Exception:
        return False
