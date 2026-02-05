from datetime import date, timedelta

from sqlalchemy import select

from app.tasks import celery_app
from app.database import AsyncSessionLocal
from app.services.sync import SyncOrchestrator
from app.models import Game
from app.config import get_settings
from app.utils.async_celery import run_async

settings = get_settings()


async def _sync_references():
    """Sync tournaments, seasons, and teams."""
    async with AsyncSessionLocal() as db:
        orchestrator = SyncOrchestrator(db)
        return await orchestrator.sync_references()


async def _sync_games():
    """Sync games for all configured seasons."""
    async with AsyncSessionLocal() as db:
        orchestrator = SyncOrchestrator(db)
        results = {}
        for season_id in settings.sync_season_ids:
            count = await orchestrator.sync_games(season_id)
            results[f"season_{season_id}"] = count
        return {"games_synced": results}


async def _sync_live_stats():
    """Sync statistics for recent games across all configured seasons."""
    async with AsyncSessionLocal() as db:
        orchestrator = SyncOrchestrator(db)
        three_days_ago = date.today() - timedelta(days=3)

        total_synced = 0
        results_by_season = {}

        for season_id in settings.sync_season_ids:
            result = await db.execute(
                select(Game.id).where(
                    Game.season_id == season_id,
                    Game.date >= three_days_ago,
                    Game.has_stats == True,
                )
            )
            game_ids = [str(g[0]) for g in result.fetchall()]

            season_synced = 0
            for game_id in game_ids:
                await orchestrator.sync_game_stats(game_id)
                season_synced += 1

            results_by_season[f"season_{season_id}"] = season_synced
            total_synced += season_synced

        return {"games_stats_synced": total_synced, "by_season": results_by_season}


async def _full_sync():
    """Full synchronization for all configured seasons."""
    async with AsyncSessionLocal() as db:
        orchestrator = SyncOrchestrator(db)
        results = {}
        for season_id in settings.sync_season_ids:
            results[f"season_{season_id}"] = await orchestrator.full_sync(season_id)
        return results


@celery_app.task(name="app.tasks.sync_tasks.sync_references")
def sync_references():
    """Celery task: Sync reference data (tournaments, seasons, teams)."""
    return run_async(_sync_references())


@celery_app.task(name="app.tasks.sync_tasks.sync_games")
def sync_games():
    """Celery task: Sync games for all configured seasons."""
    return run_async(_sync_games())


@celery_app.task(name="app.tasks.sync_tasks.sync_live_stats")
def sync_live_stats():
    """Celery task: Sync statistics for recent games across all configured seasons."""
    return run_async(_sync_live_stats())


@celery_app.task(name="app.tasks.sync_tasks.full_sync")
def full_sync():
    """Celery task: Full data synchronization for all configured seasons."""
    return run_async(_full_sync())
