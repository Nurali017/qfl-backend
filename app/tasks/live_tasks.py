"""
Celery tasks for live match synchronization.

Tasks:
- auto_start_live_games: Auto-start live tracking for games whose scheduled time has passed
- sync_live_game_events: Sync events, lineup, and stats for active games
- auto_end_finished_games: Auto-end games that have been live for over 2h15m
"""
import logging

from app.tasks import celery_app
from app.database import AsyncSessionLocal
from app.services.live_sync_service import LiveSyncService
from app.services.sota_client import get_sota_client
from app.utils.async_celery import run_async

logger = logging.getLogger(__name__)


async def _sync_live_game_events():
    """Sync events for all active (live) games."""
    from app.utils.live_flag import has_live_games, set_live_flag, clear_live_flag

    if not await has_live_games():
        return {"active_games": 0, "total_new_events": 0, "results": [], "skipped": True}

    async with AsyncSessionLocal() as db:
        try:
            client = get_sota_client()
            service = LiveSyncService(db, client)

            active_games = await service.get_active_games()

            if not active_games:
                await clear_live_flag()
                await db.commit()
                return {"active_games": 0, "total_new_events": 0, "results": []}

            # Refresh flag TTL while games are live
            await set_live_flag()

            results = []
            total_new_events = 0

            for game in active_games:
                try:
                    sync_result = await service.sync_live_events(game.id)
                    events_added = sync_result.get("added", 0)
                    total_new_events += events_added

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

                    # Sync per-player stats (shots, cards, etc. per player)
                    try:
                        await service.sync_live_player_stats(game.id)
                    except Exception as ps_err:
                        logger.warning(f"Failed to sync player stats for game {game.id}: {ps_err}")

                    results.append({
                        "game_id": game.id,
                        "new_events": events_added,
                        "updated_events": sync_result.get("updated", 0),
                        "deleted_events": sync_result.get("deleted", 0),
                    })

                    if events_added:
                        logger.info(f"Synced {events_added} new events for game {game.id}")

                except Exception as e:
                    logger.error(f"Failed to sync events for game {game.id}: {e}")
                    results.append({"game_id": game.id, "error": str(e)})

            await db.commit()
            return {
                "active_games": len(active_games),
                "total_new_events": total_new_events,
                "results": results,
            }
        except Exception:
            await db.rollback()
            raise


async def _auto_start_live_games():
    """Find games whose scheduled time has passed and start live tracking."""
    async with AsyncSessionLocal() as db:
        try:
            client = get_sota_client()
            service = LiveSyncService(db, client)

            games = await service.get_games_to_start()
            if not games:
                await db.commit()
                return {"started": 0, "results": []}

            results = []
            for game in games:
                try:
                    result = await service.start_live_tracking(game.id)
                    results.append(result)
                    logger.info(f"Auto-started live tracking for game {game.id}")
                except Exception as e:
                    logger.error(f"Failed to auto-start game {game.id}: {e}")
                    results.append({"game_id": game.id, "error": str(e)})

            await db.commit()
            return {
                "started": len([r for r in results if r.get("is_live")]),
                "results": results,
            }
        except Exception:
            await db.rollback()
            raise


async def _auto_end_finished_games():
    """End games that have been live for over 2h15m."""
    async with AsyncSessionLocal() as db:
        try:
            client = get_sota_client()
            service = LiveSyncService(db, client)
            games = await service.get_games_to_end()
            if not games:
                await db.commit()
                return {"ended": 0, "results": []}
            results = []
            for game in games:
                try:
                    await service.stop_live_tracking(game.id)
                    results.append({"game_id": game.id, "status": "ended"})
                    logger.info(f"Auto-ended game {game.id}")
                except Exception as e:
                    logger.error(f"Failed to auto-end game {game.id}: {e}")
                    results.append({"game_id": game.id, "error": str(e)})
            await db.commit()
            return {"ended": len([r for r in results if "status" in r]), "results": results}
        except Exception:
            await db.rollback()
            raise


# ==================== Celery Tasks ====================


@celery_app.task(name="app.tasks.live_tasks.auto_start_live_games")
def auto_start_live_games():
    """Celery task: Auto-start live tracking when game time arrives. Runs every 2 minutes."""
    return run_async(_auto_start_live_games())


@celery_app.task(name="app.tasks.live_tasks.sync_live_game_events")
def sync_live_game_events():
    """Celery task: Sync events for all active games. Runs every 15 seconds."""
    return run_async(_sync_live_game_events())


@celery_app.task(name="app.tasks.live_tasks.auto_end_finished_games")
def auto_end_finished_games():
    """Celery task: Auto-end games that have been live for over 2h15m. Runs every 5 minutes."""
    return run_async(_auto_end_finished_games())
