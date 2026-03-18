"""Helpers for season-scoped statistics windows."""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Season, TourSyncStatus


async def compute_season_stats_scope(
    db: AsyncSession,
    season_id: int,
    season: Season,
    max_round: int | None = None,
) -> tuple[int | None, int | None]:
    """Return ``(max_completed_round, effective_max_round)`` for season stats.

    ``effective_max_round`` caps round-robin stats to the highest completed tour.
    For knockout seasons the scope stays uncapped unless ``max_round`` is passed.
    """
    completed_round_query = (
        select(TourSyncStatus.tour)
        .where(TourSyncStatus.season_id == season_id)
        .order_by(TourSyncStatus.tour.desc())
        .limit(1)
    )
    max_completed_round = (await db.execute(completed_round_query)).scalar()

    is_round_robin = season.tournament_format == "round_robin" or (
        season.tournament_format is None and season.has_table
    )

    if max_round is not None:
        effective_max_round = max_round
    elif is_round_robin:
        # ``0`` means "no completed round yet", which should exclude all tour-based data.
        effective_max_round = max_completed_round if max_completed_round is not None else 0
    else:
        effective_max_round = None

    return max_completed_round, effective_max_round
