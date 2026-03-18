"""One-off backfill: run aggregate bundle for candidate tours, mark TourSyncStatus on success.

Usage: python3 -m scripts.backfill_tour_sync_status

Uses the same production aggregate flow as live sync:
1. Find tours where all games scored + all extended_stats_synced
2. For each: run _sync_extended_aggregate_bundle (team_season, player_season, player_tour)
3. Only mark TourSyncStatus if bundle returns no errors
"""
import asyncio
import logging

from sqlalchemy import select, func, case

from app.database import AsyncSessionLocal
from app.models import Game
from app.tasks.sync_tasks import _sync_extended_aggregate_bundle
from app.tasks.tour_readiness import mark_tour_synced

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


async def backfill_tour_sync_status():
    """Find candidate tours and run aggregate bundle for each."""
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(Game.season_id, Game.tour)
            .where(Game.tour.isnot(None))
            .group_by(Game.season_id, Game.tour)
            .having(
                func.count() == func.count(case((
                    Game.home_score.isnot(None) & Game.away_score.isnot(None), 1
                ))),
                func.count() == func.count(case((
                    Game.extended_stats_synced_at.isnot(None), 1
                ))),
            )
        )
        candidates = result.all()

    logger.info("Found %d candidate tours", len(candidates))

    created = 0
    for season_id, tour in candidates:
        bundle_result = await _sync_extended_aggregate_bundle(
            season_id, tours={tour}, force=True,
        )

        if not bundle_result.get("errors"):
            async with AsyncSessionLocal() as db2:
                await mark_tour_synced(db2, season_id, tour)
                await db2.commit()
            created += 1
            logger.info("Backfill: marked season %d tour %d", season_id, tour)
        else:
            logger.warning(
                "Backfill: skipped season %d tour %d, errors: %s",
                season_id, tour, bundle_result["errors"],
            )

    logger.info("Backfill complete: checked=%d, created=%d", len(candidates), created)
    return {"checked": len(candidates), "created": created}


if __name__ == "__main__":
    asyncio.run(backfill_tour_sync_status())
