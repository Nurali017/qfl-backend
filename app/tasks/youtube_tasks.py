"""Celery tasks for YouTube auto-linking and view_count sync."""

import logging

from app.tasks import celery_app
from app.database import AsyncSessionLocal
from app.services.youtube_linker import link_youtube_videos
from app.services.youtube_stats import sync_tier
from app.utils.async_celery import run_async

logger = logging.getLogger(__name__)


async def _link_youtube_videos():
    async with AsyncSessionLocal() as db:
        result = await link_youtube_videos(db)
        await db.commit()
        logger.info("YouTube auto-link completed: %s", result)
        return result


@celery_app.task(name="app.tasks.youtube_tasks.link_youtube_videos")
def link_youtube_videos_task():
    return run_async(_link_youtube_videos())


@celery_app.task(name="app.tasks.youtube_tasks.sync_view_counts_live")
def sync_view_counts_live_task():
    return run_async(sync_tier("live"))


@celery_app.task(name="app.tasks.youtube_tasks.sync_view_counts_fresh")
def sync_view_counts_fresh_task():
    return run_async(sync_tier("fresh"))


@celery_app.task(name="app.tasks.youtube_tasks.sync_view_counts_medium")
def sync_view_counts_medium_task():
    return run_async(sync_tier("medium"))


@celery_app.task(name="app.tasks.youtube_tasks.sync_view_counts_old")
def sync_view_counts_old_task():
    return run_async(sync_tier("old"))


@celery_app.task(name="app.tasks.youtube_tasks.sync_view_counts_media")
def sync_view_counts_media_task():
    return run_async(sync_tier("media"))
