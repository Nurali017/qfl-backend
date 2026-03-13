"""V2-enrich game stats for a season — fills minutes_played + pass_accuracy from SOTA v2 API.

Usage: python3 -m scripts.resync_season_game_stats <season_id> [concurrency]
"""
import asyncio
import logging
import sys

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import selectinload

from app.config import get_settings
from app.models import Game, GamePlayerStats
from app.services.sync.game_sync import GameSyncService

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


async def enrich_game(svc: GameSyncService, db: AsyncSession, game_id: int, sota_uuid: str) -> int:
    """Run v2 enrichment for one game. Returns enriched player count."""
    result = await db.execute(
        select(GamePlayerStats)
        .where(GamePlayerStats.game_id == game_id)
        .options(selectinload(GamePlayerStats.player))
    )
    rows = list(result.scalars().all())
    enriched = 0
    for ps in rows:
        if not ps.player or not ps.player.sota_id:
            continue
        try:
            v2_data = await svc.client.get_player_game_stats_v2(
                str(ps.player.sota_id), sota_uuid
            )
            if not v2_data:
                continue
            # Merge into extra_stats
            ps.extra_stats = {**(ps.extra_stats or {}), **v2_data}
            # Populate main columns
            if ps.minutes_played is None and v2_data.get("time_on_field_total"):
                try:
                    ps.minutes_played = int(v2_data["time_on_field_total"])
                except (ValueError, TypeError):
                    pass
            if ps.pass_accuracy is None and v2_data.get("pass_ratio"):
                try:
                    ps.pass_accuracy = float(v2_data["pass_ratio"])
                except (ValueError, TypeError):
                    pass
            enriched += 1
        except Exception as e:
            logger.debug("v2 failed for player %s game %s: %s", ps.player_id, game_id, e)
        await asyncio.sleep(0.15)
    if enriched:
        await db.commit()
    return enriched


async def resync_season(season_id: int, concurrency: int = 1):
    settings = get_settings()
    engine = create_async_engine(settings.database_url, pool_size=concurrency + 2)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with session_factory() as db:
        result = await db.execute(
            select(Game.id, Game.sota_id, Game.tour)
            .where(
                Game.season_id == season_id,
                Game.sota_id.isnot(None),
                Game.home_score.isnot(None),
            )
            .order_by(Game.tour, Game.date)
        )
        games = [(row[0], str(row[1]), row[2]) for row in result.all()]
    logger.info("Found %d finished games for season %d", len(games), season_id)

    sem = asyncio.Semaphore(concurrency)
    ok = 0
    total_enriched = 0
    errors = []
    done = 0

    async def process_game(game_id: int, sota_uuid: str, tour: int):
        nonlocal ok, total_enriched, done
        async with sem:
            async with session_factory() as db:
                svc = GameSyncService(db)
                try:
                    enriched = await enrich_game(svc, db, game_id, sota_uuid)
                    ok += 1
                    total_enriched += enriched
                except Exception as e:
                    errors.append((game_id, str(e)))
                    logger.warning("Game %d failed: %s", game_id, e)
                finally:
                    done += 1
                    if done % 20 == 0 or done == len(games):
                        logger.info("Progress: %d/%d (%d players enriched)", done, len(games), total_enriched)

    tasks = [process_game(gid, sid, tour) for gid, sid, tour in games]
    await asyncio.gather(*tasks)

    logger.info("Done: %d ok, %d errors, %d players enriched", ok, len(errors), total_enriched)
    if errors:
        for gid, err in errors[:10]:
            logger.error("  Game %d: %s", gid, err)

    await engine.dispose()


if __name__ == "__main__":
    sid = int(sys.argv[1]) if len(sys.argv) > 1 else 61
    conc = int(sys.argv[2]) if len(sys.argv) > 2 else 3
    asyncio.run(resync_season(sid, conc))
