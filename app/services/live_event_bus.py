"""
Redis-backed event bus for live match updates.

Why:
- WebSocket connections live inside the FastAPI process.
- Celery workers run in separate processes and cannot access in-memory WS state.

Solution:
- Celery publishes live updates to Redis Pub/Sub.
- FastAPI subscribes and broadcasts to connected WebSocket clients.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

_SENDER_ID = uuid.uuid4().hex
_redis: Redis | None = None


def get_sender_id() -> str:
    return _SENDER_ID


def _get_redis() -> Redis:
    global _redis
    if _redis is None:
        _redis = Redis.from_url(settings.redis_url, decode_responses=True)
    return _redis


async def close_redis() -> None:
    global _redis
    if _redis is None:
        return
    try:
        await _redis.close()
    finally:
        _redis = None


async def publish_live_message(message: dict[str, Any]) -> None:
    """
    Publish a live message to Redis Pub/Sub.

    Message must be JSON-serializable and should include:
    - type: "event" | "lineup" | "status"
    - game_id: str
    - data/status: payload
    """
    payload = dict(message)
    payload.setdefault("sender_id", _SENDER_ID)

    redis = _get_redis()
    data = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    await redis.publish(settings.live_events_channel, data)


async def listen_live_messages(
    handler: Callable[[dict[str, Any]], Awaitable[None]],
    *,
    stop_event: asyncio.Event,
    reconnect_delay_seconds: float = 2.0,
) -> None:
    """
    Subscribe to live messages and call handler(message) for each.

    Keeps reconnecting on transient Redis failures until stop_event is set.
    """
    channel = settings.live_events_channel

    while not stop_event.is_set():
        pubsub = None
        try:
            redis = _get_redis()
            pubsub = redis.pubsub()
            await pubsub.subscribe(channel)
            logger.info("Subscribed to Redis live channel: %s", channel)

            async for msg in pubsub.listen():
                if stop_event.is_set():
                    break

                if not isinstance(msg, dict) or msg.get("type") != "message":
                    continue

                raw = msg.get("data")
                if not raw:
                    continue

                try:
                    data = json.loads(raw)
                except Exception:
                    logger.warning("Failed to decode live message: %r", raw)
                    continue

                if not isinstance(data, dict):
                    continue

                # Ignore messages published by this same process (prevents duplicates
                # when API both publishes and broadcasts directly).
                if data.get("sender_id") == _SENDER_ID:
                    continue

                try:
                    await handler(data)
                except Exception:
                    logger.exception("Live message handler failed: %s", data)

        except asyncio.CancelledError:
            break
        except RedisError as e:
            logger.warning("Redis live subscription error: %s", e)
            await asyncio.sleep(reconnect_delay_seconds)
        except Exception as e:
            logger.exception("Unexpected live subscription error: %s", e)
            await asyncio.sleep(reconnect_delay_seconds)
        finally:
            if pubsub is not None:
                try:
                    await pubsub.unsubscribe(channel)
                except Exception:
                    pass
                try:
                    await pubsub.close()
                except Exception:
                    pass

