"""
Celery tasks for live match synchronization.

Tasks:
- check_upcoming_games: Check for games starting soon, sync lineups
- sync_live_game_events: Sync events for active games
- end_finished_games: Auto-end games that should have finished
"""
import asyncio
import logging

from app.tasks import celery_app
from app.database import AsyncSessionLocal
from app.services.live_sync_service import LiveSyncService
from app.services.sota_client import get_sota_client
from app.services.websocket_manager import get_websocket_manager
from app.schemas.live import GameEventResponse

logger = logging.getLogger(__name__)


def run_async(coro):
    """Helper to run async code in sync context."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


async def _check_upcoming_games():
    """
    Check for games starting within 30 minutes.

    For each upcoming game:
    - Sync pre-game lineup from SOTA /em/ endpoints
    - Broadcast lineup to connected WebSocket clients
    """
    async with AsyncSessionLocal() as db:
        client = get_sota_client()
        manager = get_websocket_manager()
        service = LiveSyncService(db, client)

        # Get games starting in next 30 minutes
        upcoming_games = await service.get_upcoming_games(minutes_ahead=30)

        results = []
        for game in upcoming_games:
            game_id = str(game.id)
            try:
                # Sync lineup
                result = await service.sync_pregame_lineup(game_id)
                results.append(result)

                # Broadcast lineup update
                await manager.broadcast_lineup(game_id, result)

                logger.info(f"Pre-game lineup synced for game {game_id}")
            except Exception as e:
                logger.error(f"Failed to sync lineup for game {game_id}: {e}")
                results.append({"game_id": game_id, "error": str(e)})

        return {
            "upcoming_games_found": len(upcoming_games),
            "lineups_synced": len([r for r in results if "error" not in r]),
            "results": results,
        }


async def _sync_live_game_events():
    """
    Sync events for all active (live) games.

    For each live game:
    - Fetch new events from SOTA
    - Save to database
    - Broadcast new events via WebSocket
    """
    async with AsyncSessionLocal() as db:
        client = get_sota_client()
        manager = get_websocket_manager()
        service = LiveSyncService(db, client)

        # Get active games
        active_games = await service.get_active_games()

        results = []
        total_new_events = 0

        for game in active_games:
            game_id = str(game.id)
            try:
                # Sync events
                new_events = await service.sync_live_events(game_id)
                total_new_events += len(new_events)

                # Broadcast each new event
                for event in new_events:
                    event_data = GameEventResponse.model_validate(event).model_dump(mode="json")
                    await manager.broadcast_event(game_id, event_data)

                results.append({
                    "game_id": game_id,
                    "new_events": len(new_events),
                })

                if new_events:
                    logger.info(f"Synced {len(new_events)} new events for game {game_id}")

            except Exception as e:
                logger.error(f"Failed to sync events for game {game_id}: {e}")
                results.append({"game_id": game_id, "error": str(e)})

        return {
            "active_games": len(active_games),
            "total_new_events": total_new_events,
            "results": results,
        }


async def _end_finished_games():
    """
    Auto-end games that have been live for too long (> 2 hours).

    This handles cases where manual stop wasn't called.
    """
    async with AsyncSessionLocal() as db:
        client = get_sota_client()
        manager = get_websocket_manager()
        service = LiveSyncService(db, client)

        # Get games that should have ended
        games_to_end = await service.get_games_to_end()

        results = []
        for game in games_to_end:
            game_id = str(game.id)
            try:
                await service.stop_live_tracking(game_id)
                await manager.broadcast_game_status(game_id, "ended")
                results.append({"game_id": game_id, "status": "ended"})
                logger.info(f"Auto-ended game {game_id}")
            except Exception as e:
                logger.error(f"Failed to end game {game_id}: {e}")
                results.append({"game_id": game_id, "error": str(e)})

        return {
            "games_ended": len([r for r in results if "status" in r]),
            "results": results,
        }


async def _start_game_by_schedule():
    """
    Start live tracking for games that should have started.

    For games where current_time >= game.time and is_live=False.
    """
    from datetime import datetime
    from sqlalchemy import select, and_
    from app.models import Game

    async with AsyncSessionLocal() as db:
        client = get_sota_client()
        manager = get_websocket_manager()
        service = LiveSyncService(db, client)

        now = datetime.now()
        today = now.date()
        current_time = now.time()

        # Find games that should have started but aren't marked as live
        result = await db.execute(
            select(Game).where(
                and_(
                    Game.date == today,
                    Game.time <= current_time,
                    Game.is_live == False,
                    Game.has_lineup == True,  # Only if lineup was synced
                )
            )
        )
        games_to_start = list(result.scalars().all())

        results = []
        for game in games_to_start:
            game_id = str(game.id)
            try:
                result = await service.start_live_tracking(game_id)
                await manager.broadcast_game_status(game_id, "started")
                results.append(result)
                logger.info(f"Auto-started live tracking for game {game_id}")
            except Exception as e:
                logger.error(f"Failed to start game {game_id}: {e}")
                results.append({"game_id": game_id, "error": str(e)})

        return {
            "games_started": len([r for r in results if "error" not in r]),
            "results": results,
        }


# ==================== Celery Tasks ====================


@celery_app.task(name="app.tasks.live_tasks.check_upcoming_games")
def check_upcoming_games():
    """
    Celery task: Check for upcoming games and sync their lineups.

    Runs every 5 minutes.
    """
    return run_async(_check_upcoming_games())


@celery_app.task(name="app.tasks.live_tasks.sync_live_game_events")
def sync_live_game_events():
    """
    Celery task: Sync events for all active games.

    Runs every 30 seconds during match time.
    """
    return run_async(_sync_live_game_events())


@celery_app.task(name="app.tasks.live_tasks.end_finished_games")
def end_finished_games():
    """
    Celery task: Auto-end games that should have finished.

    Runs every 10 minutes.
    """
    return run_async(_end_finished_games())


@celery_app.task(name="app.tasks.live_tasks.start_game_by_schedule")
def start_game_by_schedule():
    """
    Celery task: Start live tracking for games based on schedule.

    Runs every minute during potential match times.
    """
    return run_async(_start_game_by_schedule())
