"""Celery tasks for YouTube auto-linking."""

import logging

from app.tasks import celery_app
from app.database import AsyncSessionLocal
from app.services.youtube_linker import link_youtube_videos
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
