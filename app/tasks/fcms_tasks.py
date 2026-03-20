"""Celery tasks for FCMS integration: bulk import, pre-match lineups, post-match protocols."""

import logging

from app.tasks import celery_app
from app.utils.async_celery import run_async

logger = logging.getLogger(__name__)


@celery_app.task(name="app.tasks.fcms_tasks.fcms_bulk_import")
def fcms_bulk_import():
    """Sync FCMS match IDs and update game times. Runs 2x/day."""
    return run_async(_fcms_bulk_import())


async def _fcms_bulk_import() -> dict:
    from scripts.fcms_bulk_import import bulk_import
    await bulk_import()
    return {"status": "done"}


@celery_app.task(name="app.tasks.fcms_tasks.fetch_fcms_pregame_lineups")
def fetch_fcms_pregame_lineups():
    """Fetch pre-match lineups from FCMS for games starting within 90 min."""
    return run_async(_fetch_fcms_pregame_lineups())


async def _fetch_fcms_pregame_lineups() -> dict:
    from app.database import AsyncSessionLocal
    from app.services.fcms_client import get_fcms_client
    from app.services.fcms_sync_service import FcmsSyncService

    async with AsyncSessionLocal() as db:
        client = get_fcms_client()
        service = FcmsSyncService(db, client)

        games = await service.get_games_for_fcms_lineup()
        if not games:
            return {"games_found": 0}

        results = []
        for game in games:
            try:
                result = await service.sync_fcms_lineup(game.id)
                results.append(result)
            except Exception:
                logger.exception("Failed to sync FCMS lineup for game %d", game.id)

        return {"games_found": len(games), "results": results}


@celery_app.task(name="app.tasks.fcms_tasks.sync_fcms_post_match_protocol")
def sync_fcms_post_match_protocol():
    """Poll FCMS for protocol PDFs of recently finished games."""
    return run_async(_sync_fcms_post_match_protocol())


async def _sync_fcms_post_match_protocol() -> dict:
    from app.database import AsyncSessionLocal
    from app.services.fcms_client import get_fcms_client
    from app.services.fcms_sync_service import FcmsSyncService

    async with AsyncSessionLocal() as db:
        client = get_fcms_client()
        service = FcmsSyncService(db, client)

        games = await service.get_games_for_fcms_protocol()
        if not games:
            return {"games_found": 0}

        results = []
        for game in games:
            try:
                result = await service.sync_fcms_protocol_pdf(game.id)
                results.append(result)
            except Exception:
                logger.exception("Failed to sync FCMS protocol for game %d", game.id)

        return {"games_found": len(games), "results": results}
