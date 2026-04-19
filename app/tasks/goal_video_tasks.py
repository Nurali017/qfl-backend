"""Celery task for ingesting goal video clips from Google Drive."""

from __future__ import annotations

import logging

from app.tasks import celery_app
from app.utils.async_celery import run_async

logger = logging.getLogger(__name__)

_LOCK_KEY = "qfl:goal-video-sync"
_LOCK_TTL = 120  # 2 min — matches the beat interval


@celery_app.task(name="app.tasks.goal_video_tasks.sync_goal_videos_task")
def sync_goal_videos_task():
    """Pull new goal clips from Drive into MinIO and link them to GameEvent rows."""
    return run_async(_sync_goal_videos_impl())


async def _sync_goal_videos_impl() -> dict:
    from app.config import get_settings
    from app.database import AsyncSessionLocal
    from app.services.goal_video_sync_service import sync_goal_videos
    from app.utils.redis_lock import acquire_token_lock, release_token_lock

    settings = get_settings()
    if not settings.google_drive_enabled:
        return {"status": "disabled"}

    token = await acquire_token_lock(_LOCK_KEY, _LOCK_TTL)
    if token is None:
        return {"status": "already_running"}

    try:
        async with AsyncSessionLocal() as db:
            try:
                result = await sync_goal_videos(db)
            except Exception:
                await db.rollback()
                logger.exception("Goal video sync failed")
                raise
        return {
            "status": "ok",
            "listed": result.listed,
            "matched": result.matched,
            "ai_folder_matched": result.ai_folder_matched,
            "ai_event_matched": result.ai_event_matched,
            "unmatched": result.unmatched,
            "skipped_already_processed": result.skipped_already_processed,
            "skipped_no_game": result.skipped_no_game,
            "errors": result.errors,
        }
    finally:
        await release_token_lock(_LOCK_KEY, token)
