"""
Celery tasks for live match synchronization.

Tasks:
- check_upcoming_games: Check for games starting soon, sync lineups
- sync_live_game_events: Sync events for active games
- end_finished_games: Auto-end games that should have finished
"""
import logging

from app.tasks import celery_app
from app.database import AsyncSessionLocal
from app.services.live_sync_service import LiveSyncService
from app.services.sota_client import get_sota_client
from app.utils.async_celery import run_async

logger = logging.getLogger(__name__)


async def _check_upcoming_games():
    """Check for games starting within 30 minutes and sync their lineups."""
    async with AsyncSessionLocal() as db:
        client = get_sota_client()
        service = LiveSyncService(db, client)

        upcoming_games = await service.get_upcoming_games(minutes_ahead=30)

        results = []
        for game in upcoming_games:
            try:
                result = await service.sync_pregame_lineup(game.id)
                results.append(result)
                logger.info(f"Pre-game lineup synced for game {game.id}")
            except Exception as e:
                logger.error(f"Failed to sync lineup for game {game.id}: {e}")
                results.append({"game_id": game.id, "error": str(e)})

        return {
            "upcoming_games_found": len(upcoming_games),
            "lineups_synced": len([r for r in results if "error" not in r]),
            "results": results,
        }


async def _sync_live_game_events():
    """Sync events for all active (live) games."""
    async with AsyncSessionLocal() as db:
        client = get_sota_client()
        service = LiveSyncService(db, client)

        active_games = await service.get_active_games()

        results = []
        total_new_events = 0

        for game in active_games:
            try:
                new_events = await service.sync_live_events(game.id)
                total_new_events += len(new_events)

                # Also sync live lineup (starters/substitutes from live feed)
                try:
                    await service.sync_live_lineup(game.id)
                except Exception as lineup_err:
                    logger.warning(f"Failed to sync lineup for game {game.id}: {lineup_err}")

                # Also sync live stats (score, shots, possession, etc.)
                try:
                    await service.sync_live_stats(game.id)
                except Exception as stats_err:
                    logger.warning(f"Failed to sync stats for game {game.id}: {stats_err}")

                results.append({
                    "game_id": game.id,
                    "new_events": len(new_events),
                })

                if new_events:
                    logger.info(f"Synced {len(new_events)} new events for game {game.id}")

            except Exception as e:
                logger.error(f"Failed to sync events for game {game.id}: {e}")
                results.append({"game_id": game.id, "error": str(e)})

        return {
            "active_games": len(active_games),
            "total_new_events": total_new_events,
            "results": results,
        }


async def _end_finished_games():
    """Auto-end games that have been live for too long (> 2 hours)."""
    async with AsyncSessionLocal() as db:
        client = get_sota_client()
        service = LiveSyncService(db, client)

        games_to_end = await service.get_games_to_end()

        results = []
        for game in games_to_end:
            try:
                await service.stop_live_tracking(game.id)
                results.append({"game_id": game.id, "status": "ended"})
                logger.info(f"Auto-ended game {game.id}")
            except Exception as e:
                logger.error(f"Failed to end game {game.id}: {e}")
                results.append({"game_id": game.id, "error": str(e)})

        return {
            "games_ended": len([r for r in results if "status" in r]),
            "results": results,
        }


# ==================== Celery Tasks ====================


@celery_app.task(name="app.tasks.live_tasks.check_upcoming_games")
def check_upcoming_games():
    """Celery task: Check for upcoming games and sync their lineups. Runs every 5 minutes."""
    return run_async(_check_upcoming_games())


@celery_app.task(name="app.tasks.live_tasks.sync_live_game_events")
def sync_live_game_events():
    """Celery task: Sync events for all active games. Runs every 30 seconds."""
    return run_async(_sync_live_game_events())


@celery_app.task(name="app.tasks.live_tasks.end_finished_games")
def end_finished_games():
    """Celery task: Auto-end games that should have finished. Runs every 10 minutes."""
    return run_async(_end_finished_games())
