import logging
from datetime import date, timedelta

from sqlalchemy import select

from app.tasks import celery_app
from app.database import AsyncSessionLocal
from app.services.sync import SyncOrchestrator
from app.models import Game
from app.config import get_settings
from app.utils.async_celery import run_async

logger = logging.getLogger(__name__)

settings = get_settings()


async def _sync_games():
    """Sync games for all configured seasons."""
    async with AsyncSessionLocal() as db:
        try:
            orchestrator = SyncOrchestrator(db)
            results = {}
            for season_id in settings.sync_season_ids:
                if not await orchestrator.is_sync_enabled(season_id):
                    logger.info("Season %d: sync disabled, skipping games task", season_id)
                    results[f"season_{season_id}"] = "skipped"
                    continue
                count = await orchestrator.sync_games(season_id)
                results[f"season_{season_id}"] = count
            await db.commit()
            return {"games_synced": results}
        except Exception:
            await db.rollback()
            raise


async def _sync_live_stats():
    """Sync statistics for recent games across all configured seasons."""
    async with AsyncSessionLocal() as db:
        try:
            orchestrator = SyncOrchestrator(db)
            three_days_ago = date.today() - timedelta(days=3)

            total_synced = 0
            results_by_season = {}

            for season_id in settings.sync_season_ids:
                if not await orchestrator.is_sync_enabled(season_id):
                    logger.info("Season %d: sync disabled, skipping live stats task", season_id)
                    results_by_season[f"season_{season_id}"] = "skipped"
                    continue

                result = await db.execute(
                    select(Game.id).where(
                        Game.season_id == season_id,
                        Game.date >= three_days_ago,
                        Game.has_stats == True,
                        Game.sync_disabled == False,
                    )
                )
                game_ids = [g[0] for g in result.fetchall()]

                season_synced = 0
                for gid in game_ids:
                    await orchestrator.sync_game_stats(gid)
                    season_synced += 1

                results_by_season[f"season_{season_id}"] = season_synced
                total_synced += season_synced

            await db.commit()
            return {"games_stats_synced": total_synced, "by_season": results_by_season}
        except Exception:
            await db.rollback()
            raise


async def _sync_best_players():
    """Sync goals + assists from best_players endpoint for all configured seasons."""
    async with AsyncSessionLocal() as db:
        try:
            orchestrator = SyncOrchestrator(db)
            total = 0
            results_by_season = {}
            for season_id in settings.sync_season_ids:
                if not await orchestrator.is_sync_enabled(season_id):
                    logger.info("Season %d: sync disabled, skipping best_players task", season_id)
                    results_by_season[f"season_{season_id}"] = "skipped"
                    continue
                count = await orchestrator.sync_best_players(season_id)
                results_by_season[f"season_{season_id}"] = count
                total += count
            await db.commit()
            return {"best_players_synced": total, "by_season": results_by_season}
        except Exception:
            await db.rollback()
            raise


@celery_app.task(name="app.tasks.sync_tasks.sync_games")
def sync_games():
    """Celery task: Sync games for all configured seasons."""
    return run_async(_sync_games())


@celery_app.task(name="app.tasks.sync_tasks.sync_live_stats")
def sync_live_stats():
    """Celery task: Sync statistics for recent games across all configured seasons."""
    return run_async(_sync_live_stats())


@celery_app.task(name="app.tasks.sync_tasks.sync_best_players")
def sync_best_players():
    """Celery task: Sync goals + assists from best_players endpoint."""
    return run_async(_sync_best_players())
