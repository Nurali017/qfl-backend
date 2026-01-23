import asyncio
from datetime import date, timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.tasks import celery_app
from app.database import AsyncSessionLocal
from app.services.sync_service import SyncService
from app.models import Game
from app.config import get_settings

settings = get_settings()


def run_async(coro):
    """Helper to run async code in sync context."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


async def _sync_references():
    """Sync tournaments, seasons, and teams."""
    async with AsyncSessionLocal() as db:
        sync_service = SyncService(db)
        results = {
            "tournaments": await sync_service.sync_tournaments(),
            "seasons": await sync_service.sync_seasons(),
            "teams": await sync_service.sync_teams(),
        }
        return results


async def _sync_games():
    """Sync games for current season."""
    async with AsyncSessionLocal() as db:
        sync_service = SyncService(db)
        count = await sync_service.sync_games(settings.current_season_id)
        return {"games_synced": count}


async def _sync_live_stats():
    """Sync statistics for recent games (last 3 days)."""
    async with AsyncSessionLocal() as db:
        sync_service = SyncService(db)

        # Find games from last 3 days with stats
        three_days_ago = date.today() - timedelta(days=3)
        result = await db.execute(
            select(Game.id).where(
                Game.season_id == settings.current_season_id,
                Game.date >= three_days_ago,
                Game.has_stats == True,
            )
        )
        game_ids = [str(g[0]) for g in result.fetchall()]

        stats_synced = 0
        for game_id in game_ids:
            await sync_service.sync_game_stats(game_id)
            stats_synced += 1

        return {"games_stats_synced": stats_synced}


async def _full_sync():
    """Full synchronization."""
    async with AsyncSessionLocal() as db:
        sync_service = SyncService(db)
        return await sync_service.full_sync(settings.current_season_id)


@celery_app.task(name="app.tasks.sync_tasks.sync_references")
def sync_references():
    """Celery task: Sync reference data (tournaments, seasons, teams)."""
    return run_async(_sync_references())


@celery_app.task(name="app.tasks.sync_tasks.sync_games")
def sync_games():
    """Celery task: Sync games for current season."""
    return run_async(_sync_games())


@celery_app.task(name="app.tasks.sync_tasks.sync_live_stats")
def sync_live_stats():
    """Celery task: Sync statistics for recent games."""
    return run_async(_sync_live_stats())


@celery_app.task(name="app.tasks.sync_tasks.full_sync")
def full_sync():
    """Celery task: Full data synchronization."""
    return run_async(_full_sync())
