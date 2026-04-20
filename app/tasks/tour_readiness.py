"""Tour readiness helpers: mark tours as synced, trigger revalidation."""

import logging

import redis
from sqlalchemy import select, func, case
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.models import Game, GameStatus
from app.models.tour_sync_status import TourSyncStatus
from app.utils.timestamps import utcnow

NON_BLOCKING_STATUSES = (GameStatus.postponed, GameStatus.cancelled)

logger = logging.getLogger(__name__)
settings = get_settings()


async def mark_tour_synced(db: AsyncSession, season_id: int, tour: int) -> None:
    """Insert TourSyncStatus marker. Idempotent upsert by (season_id, tour)."""
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    now = utcnow()
    stmt = pg_insert(TourSyncStatus).values(
        season_id=season_id, tour=tour, synced_at=now,
    ).on_conflict_do_update(
        index_elements=["season_id", "tour"],
        set_={"synced_at": now},
    )
    await db.execute(stmt)
    await db.flush()


async def maybe_trigger_tour_revalidation(
    db: AsyncSession, season_id: int, tour: int,
) -> bool:
    """Check all 3 conditions and trigger revalidation if met.

    1. All games in tour have scores
    2. All games have extended_stats_synced_at
    3. TourSyncStatus row exists (aggregates done)

    Returns True if revalidation was triggered.
    """
    from app.tasks.sync_tasks import trigger_stats_revalidation

    # Condition 1+2: game-level completeness.  Postponed/cancelled games are
    # excluded from the total — a rescheduled fixture must not block the rest
    # of the tour from being marked ready.
    playable = Game.status.notin_(NON_BLOCKING_STATUSES)
    row = (await db.execute(
        select(
            func.count(case((playable, 1))).label("total"),
            func.count(case((
                playable
                & Game.home_score.isnot(None)
                & Game.away_score.isnot(None),
                1,
            ))).label("scored"),
            func.count(case((
                playable & Game.extended_stats_synced_at.isnot(None), 1
            ))).label("ext_synced"),
        ).where(Game.season_id == season_id, Game.tour == tour)
    )).one()

    if row.total == 0 or row.scored != row.total or row.ext_synced != row.total:
        return False

    # Condition 3: aggregate marker
    marker = await db.scalar(
        select(TourSyncStatus.id).where(
            TourSyncStatus.season_id == season_id,
            TourSyncStatus.tour == tour,
        )
    )
    if marker is None:
        return False

    # All met — dedupe via Redis and trigger
    r = redis.from_url(settings.redis_url)
    redis_key = f"tour_reval:{season_id}:{tour}"
    if r.exists(redis_key):
        return False

    r.set(redis_key, "1", ex=48 * 3600)
    trigger_stats_revalidation.delay(season_id=season_id, tour=tour)
    logger.info("Tour revalidation triggered for season %d tour %d", season_id, tour)
    return True
