"""
Mark known technical wins (3-0 forfeits) in the database.
Also cleans up junk data (lineups, team stats) that was auto-synced
for games that were never actually played.

Games where Туркестан was withdrawn from Первая Лига 2025:
- Game 141: Алтай 3-0 Туркестан (2025-08-21)
- Game 55: Актобе-М 3-0 Туркестан (2025-10-02)
- Game 355: Шахтёр 3-0 Туркестан (2025-10-24)

Usage: python -m scripts.mark_technical_wins
"""
import asyncio
import logging

from sqlalchemy import delete, update

from app.database import AsyncSessionLocal
from app.models.game import Game
from app.models.game_lineup import GameLineup
from app.models.game_team_stats import GameTeamStats

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TECHNICAL_WIN_GAME_IDS = [141, 55, 355]


async def main():
    async with AsyncSessionLocal() as session:
        # 1. Delete junk lineups
        lineups_result = await session.execute(
            delete(GameLineup)
            .where(GameLineup.game_id.in_(TECHNICAL_WIN_GAME_IDS))
        )
        logger.info(
            "Deleted %d junk lineup entries for games %s",
            lineups_result.rowcount, TECHNICAL_WIN_GAME_IDS,
        )

        # 2. Delete junk team stats (all zeros)
        stats_result = await session.execute(
            delete(GameTeamStats)
            .where(GameTeamStats.game_id.in_(TECHNICAL_WIN_GAME_IDS))
        )
        logger.info(
            "Deleted %d junk team stats entries for games %s",
            stats_result.rowcount, TECHNICAL_WIN_GAME_IDS,
        )

        # 3. Mark as technical wins and reset sync flags
        result = await session.execute(
            update(Game)
            .where(Game.id.in_(TECHNICAL_WIN_GAME_IDS))
            .values(is_technical=True, has_lineup=False, has_stats=False)
        )
        logger.info(
            "Marked %d games as technical wins: %s",
            result.rowcount, TECHNICAL_WIN_GAME_IDS,
        )

        await session.commit()
        logger.info("Done. All changes committed.")


if __name__ == "__main__":
    asyncio.run(main())
