"""
Derived has_stats utility.

Computes has_stats from existence of game_team_stats or game_player_stats
records instead of relying on the stored (always-false) Game.has_stats column.
"""
from sqlalchemy import select, union
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Game, GameTeamStats, GamePlayerStats


async def enrich_games_has_stats(
    db: AsyncSession, games: list[Game]
) -> None:
    """Set derived has_stats on a list of Game objects (single DB query)."""
    if not games:
        return
    game_ids = [g.id for g in games]

    q = union(
        select(GameTeamStats.game_id).where(
            GameTeamStats.game_id.in_(game_ids)
        ),
        select(GamePlayerStats.game_id).where(
            GamePlayerStats.game_id.in_(game_ids)
        ),
    )
    result = await db.execute(select(q.c.game_id))
    ids_with_stats = set(result.scalars().all())

    for game in games:
        game.has_stats = game.id in ids_with_stats


async def compute_single_has_stats(db: AsyncSession, game_id: int) -> bool:
    """Compute derived has_stats for a single game."""
    ts = await db.execute(
        select(GameTeamStats.game_id)
        .where(GameTeamStats.game_id == game_id)
        .limit(1)
    )
    if ts.scalar_one_or_none() is not None:
        return True
    ps = await db.execute(
        select(GamePlayerStats.game_id)
        .where(GamePlayerStats.game_id == game_id)
        .limit(1)
    )
    return ps.scalar_one_or_none() is not None
