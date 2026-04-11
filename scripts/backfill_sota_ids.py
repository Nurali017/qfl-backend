"""
One-off script to backfill sota_id for players in finished games.
Runs _get_or_create_player_by_sota via LiveSyncService.sync_live_lineup
for all finished games in given seasons.

Usage: docker exec qfl-backend python scripts/backfill_sota_ids.py
"""
import asyncio
import logging

from app.database import AsyncSessionLocal
from app.services.live_sync_service import LiveSyncService
from app.services.sota_client import SotaClient
from app.models import Game, GameStatus

from sqlalchemy import select, or_, and_

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SEASON_IDS = [204, 203]  # 1L, 2L


async def main():
    async with AsyncSessionLocal() as db:
        # Get all finished games for target seasons
        result = await db.execute(
            select(Game.id, Game.season_id, Game.sota_id)
            .where(
                Game.season_id.in_(SEASON_IDS),
                Game.sota_id.isnot(None),
                or_(
                    Game.status == GameStatus.finished,
                    and_(Game.home_score.isnot(None), Game.away_score.isnot(None)),
                ),
            )
            .order_by(Game.season_id, Game.id)
        )
        games = result.all()
        logger.info("Found %d finished games for seasons %s", len(games), SEASON_IDS)

        if not games:
            logger.info("No games to process")
            return

        client = SotaClient()
        linked = 0
        errors = 0
        try:
            for game_id, season_id, sota_id in games:
                try:
                    service = LiveSyncService(db, client)
                    details = await service.sync_live_lineup(game_id)
                    lineup_count = details.get("lineup_count", 0)
                    if lineup_count > 0:
                        linked += 1
                    logger.info(
                        "Game %d (season %d): lineup_count=%d %s",
                        game_id, season_id, lineup_count,
                        details.get("error", ""),
                    )
                    await db.commit()
                except Exception as exc:
                    logger.error("Game %d failed: %s", game_id, exc)
                    await db.rollback()
                    errors += 1

        finally:
            if hasattr(client, 'close'):
                await client.close()

        logger.info(
            "Done: %d games processed, %d with lineups, %d errors",
            len(games), linked, errors,
        )


if __name__ == "__main__":
    asyncio.run(main())
