"""Celery tasks for FCMS integration: bulk import, pre-match lineups, post-match protocols, roster sync."""

import logging

from sqlalchemy import func, select

from app.database import AsyncSessionLocal
from app.models import Game, GameStatus
from app.tasks import celery_app
from app.utils.async_celery import run_async

logger = logging.getLogger(__name__)


async def _any_game_live() -> int:
    """Count games currently in 'live' status. Tasks that wrap an HTTP loop
    inside a DB session use this to skip while the web pool is under
    burst load — backfill is naturally caught up by the next beat tick.
    pregame_lineups / rosters intentionally do NOT use this guard:
    they're needed BEFORE the live window."""
    async with AsyncSessionLocal() as db:
        return (await db.scalar(
            select(func.count()).select_from(Game).where(Game.status == GameStatus.live)
        )) or 0

_ROSTER_LOCK_KEY = "qfl:fcms-roster-sync"
_ROSTER_LOCK_TTL = 600  # 10 min — sync takes ~30-60s for 30 teams

_REFEREES_LOCK_KEY = "qfl:fcms-referee-sync"
_REFEREES_LOCK_TTL = 600  # 10 min

_BULK_IMPORT_LOCK_KEY = "qfl:fcms-bulk-import"
_BULK_IMPORT_LOCK_TTL = 600  # 10 min — guards the */15 beat against overlap


@celery_app.task(name="app.tasks.fcms_tasks.fcms_bulk_import")
def fcms_bulk_import():
    """Sync FCMS match IDs and update game dates/times. Runs every 15 min."""
    return run_async(_fcms_bulk_import())


async def _fcms_bulk_import() -> dict:
    # Skip during live: bulk_import takes ~40s and overlaps the FCMS HTTP loop
    # with DB writes. One holder during a HT/FT burst is enough to push the
    # web pool over the edge (observed at 15:15:40 UTC on 2026-05-28). Beat
    # re-pushes every 15 min so live windows naturally heal.
    live_count = await _any_game_live()
    if live_count:
        logger.info("fcms_bulk_import: skipped, %d live game(s) in progress", live_count)
        return {"status": "skipped", "reason": "live_games_active", "live_games": live_count}

    from scripts.fcms_bulk_import import bulk_import
    from app.utils.redis_lock import acquire_token_lock, release_token_lock

    # Mutex: at a 15-min cadence a slow FCMS run must not overlap the next beat
    # (concurrent runs race on game bind/create). Mirrors roster/referee sync.
    token = await acquire_token_lock(_BULK_IMPORT_LOCK_KEY, _BULK_IMPORT_LOCK_TTL)
    if token is None:
        logger.info("FCMS bulk import already running, skipping")
        return {"status": "already_running"}
    try:
        await bulk_import()
        return {"status": "done"}
    finally:
        await release_token_lock(_BULK_IMPORT_LOCK_KEY, token)


@celery_app.task(name="app.tasks.fcms_tasks.fetch_fcms_pregame_lineups")
def fetch_fcms_pregame_lineups():
    """Fetch pre-match lineups from FCMS for games starting within 90 min."""
    return run_async(_fetch_fcms_pregame_lineups())


async def _fetch_fcms_pregame_lineups() -> dict:
    from app.database import AsyncSessionLocal
    from app.services.fcms_client import get_fcms_client
    from app.services.fcms_referee_sync import FcmsRefereeSyncService
    from app.services.fcms_sync_service import FcmsSyncService

    async with AsyncSessionLocal() as db:
        client = get_fcms_client()
        service = FcmsSyncService(db, client)
        referee_service = FcmsRefereeSyncService(db, client)

        games = await service.get_games_for_fcms_lineup()
        if not games:
            return {"games_found": 0}

        results = []
        referee_totals = {"added": 0, "updated": 0, "removed": 0, "created_referees": 0}
        for game in games:
            try:
                result = await service.sync_fcms_lineup(game.id)
                results.append(result)
                if result.get("lineup_count", 0) > 0:
                    try:
                        from app.tasks.telegram_tasks import post_pregame_lineup_task
                        post_pregame_lineup_task.delay(game.id)
                    except Exception:
                        logger.exception(
                            "Failed to enqueue post_pregame_lineup_task for game %d",
                            game.id,
                        )
            except Exception:
                logger.exception("Failed to sync FCMS lineup for game %d", game.id)

            try:
                ref_res = await referee_service.sync_match_referees(game.id)
                await db.commit()
                if "error" not in ref_res:
                    for k in referee_totals:
                        referee_totals[k] += ref_res.get(k, 0)
            except Exception:
                logger.exception("Failed to sync FCMS referees for game %d", game.id)
                await db.rollback()

        return {"games_found": len(games), "results": results, "referees": referee_totals}


@celery_app.task(name="app.tasks.fcms_tasks.sync_fcms_post_match_protocol")
def sync_fcms_post_match_protocol():
    """Poll FCMS for protocol PDFs of recently finished games."""
    return run_async(_sync_fcms_post_match_protocol())


async def _sync_fcms_post_match_protocol() -> dict:
    # Skip during live: protocol PDF fetch holds a DB session open across an
    # FCMS HTTP loop (observed 11.5s on 2026-05-28). Strictly post-match, so
    # delaying until live closes is harmless — next beat tick picks it up.
    live_count = await _any_game_live()
    if live_count:
        logger.info(
            "sync_fcms_post_match_protocol: skipped, %d live game(s) in progress",
            live_count,
        )
        return {"status": "skipped", "reason": "live_games_active", "live_games": live_count}

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


@celery_app.task(name="app.tasks.fcms_tasks.sync_fcms_referees_daily")
def sync_fcms_referees_daily():
    """Daily bulk sync of match referees from FCMS for all upcoming games."""
    return run_async(_sync_fcms_referees_daily())


async def _sync_fcms_referees_daily(horizon_days: int = 7) -> dict:
    from app.database import AsyncSessionLocal
    from app.services.fcms_client import FcmsClient
    from app.services.fcms_referee_sync import FcmsRefereeSyncService
    from app.utils.redis_lock import acquire_token_lock, release_token_lock

    token = await acquire_token_lock(_REFEREES_LOCK_KEY, _REFEREES_LOCK_TTL)
    if token is None:
        logger.info("FCMS referee sync already running, skipping")
        return {"status": "already_running"}

    try:
        client = FcmsClient()
        try:
            async with AsyncSessionLocal() as db:
                service = FcmsRefereeSyncService(db, client)
                games = await service.get_games_for_referee_sync(horizon_days=horizon_days)
                if not games:
                    return {"status": "done", "games_found": 0}
                totals = await service.sync_many(g.id for g in games)
                await db.commit()
                return {"status": "done", "games_found": len(games), **totals}
        finally:
            await client.close()
    finally:
        await release_token_lock(_REFEREES_LOCK_KEY, token)


@celery_app.task(name="app.tasks.fcms_tasks.sync_fcms_rosters")
def sync_fcms_rosters(triggered_by: str = "celery_beat"):
    """Sync player rosters from FCMS. Daily at 06:00 + manual trigger."""
    return run_async(_sync_fcms_rosters(triggered_by))


async def _sync_fcms_rosters(triggered_by: str) -> dict:
    from app.database import AsyncSessionLocal
    from app.services.fcms_client import FcmsClient
    from app.services.fcms_roster_sync import FcmsRosterSyncService
    from app.utils.redis_lock import acquire_token_lock, release_token_lock

    token = await acquire_token_lock(_ROSTER_LOCK_KEY, _ROSTER_LOCK_TTL)
    if token is None:
        logger.info("FCMS roster sync already running, skipping")
        return {"status": "already_running"}

    try:
        client = FcmsClient()
        try:
            async with AsyncSessionLocal() as db:
                service = FcmsRosterSyncService(db, client)
                results = await service.sync_all_competitions(triggered_by)
                return {"status": "done", "results": results}
        finally:
            await client.close()
    finally:
        await release_token_lock(_ROSTER_LOCK_KEY, token)
