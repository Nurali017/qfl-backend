"""Celery tasks for weather data fetching."""

import logging

from app.tasks import celery_app
from app.database import AsyncSessionLocal
from app.services.weather import fetch_and_update_weather
from app.utils.async_celery import run_async

logger = logging.getLogger(__name__)


async def _fetch_weather():
    async with AsyncSessionLocal() as db:
        result = await fetch_and_update_weather(db)
        await db.commit()
        logger.info("Weather fetch completed: %s", result)
        return result


@celery_app.task(name="app.tasks.weather_tasks.fetch_weather")
def fetch_weather_task():
    return run_async(_fetch_weather())
