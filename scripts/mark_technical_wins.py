"""
Mark known technical wins (3-0 forfeits) in the database.

Games where Туркестан was withdrawn from Первая Лига 2025:
- Game 141: Алтай 3-0 Туркестан (2025-08-21)
- Game 55: Актобе-М 3-0 Туркестан (2025-10-02)
- Game 355: Шахтёр 3-0 Туркестан (2025-10-24)

Usage: python -m scripts.mark_technical_wins
"""
import asyncio
import logging

from sqlalchemy import update

from app.database import AsyncSessionLocal
from app.models.game import Game

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TECHNICAL_WIN_GAME_IDS = [141, 55, 355]


async def main():
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            update(Game)
            .where(Game.id.in_(TECHNICAL_WIN_GAME_IDS))
            .values(is_technical=True)
        )
        await session.commit()
        logger.info("Marked %d games as technical wins: %s", result.rowcount, TECHNICAL_WIN_GAME_IDS)


if __name__ == "__main__":
    asyncio.run(main())
