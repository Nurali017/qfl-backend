"""Celery tasks for ticket URL search."""

import logging

from app.tasks import celery_app
from app.database import AsyncSessionLocal
from app.services.ticket_search import search_and_update_tickets
from app.utils.async_celery import run_async

logger = logging.getLogger(__name__)


async def _search_tickets():
    async with AsyncSessionLocal() as db:
        result = await search_and_update_tickets(db)
        await db.commit()
        logger.info("Ticket search completed: %s", result)
        return result


@celery_app.task(name="app.tasks.ticket_tasks.search_tickets")
def search_tickets_task():
    return run_async(_search_tickets())
