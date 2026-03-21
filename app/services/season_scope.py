"""Helpers for season-scoped statistics windows."""

from sqlalchemy import case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Game, Season, TourSyncStatus


async def compute_season_stats_scope(
    db: AsyncSession,
    season_id: int,
    season: Season,
    max_round: int | None = None,
) -> tuple[int | None, int | None]:
    """Return ``(max_completed_round, effective_max_round)`` for season stats.

    ``max_completed_round`` is the highest consecutive tour where **all** games
    have scores AND aggregate stats have been synced (``TourSyncStatus``).

    ``effective_max_round`` caps round-robin stats to that value.
    For knockout seasons the scope stays uncapped unless ``max_round`` is passed.
    """

    # 1. First incomplete tour: min tour where not all games have both scores.
    incomplete_sq = (
        select(Game.tour)
        .where(
            Game.season_id == season_id,
            Game.tour.isnot(None),
        )
        .group_by(Game.tour)
        .having(
            func.count()
            != func.count(
                case(
                    (
                        Game.home_score.isnot(None) & Game.away_score.isnot(None),
                        1,
                    )
                )
            )
        )
    ).subquery()

    first_incomplete_tour: int | None = (
        await db.execute(select(func.min(incomplete_sq.c.tour)))
    ).scalar()

    # 2. Highest existing tour (for the case when every tour is fully scored).
    max_existing_tour: int | None = (
        await db.execute(
            select(func.max(Game.tour)).where(
                Game.season_id == season_id,
                Game.tour.isnot(None),
            )
        )
    ).scalar()

    # 3. Derive last_consecutive_complete_round from game scores.
    if first_incomplete_tour is not None:
        last_consecutive_complete = (
            first_incomplete_tour - 1 if first_incomplete_tour > 1 else None
        )
    else:
        # All existing tours are fully scored.
        last_consecutive_complete = max_existing_tour

    # 4. Highest tour with synced aggregates (TourSyncStatus gate).
    max_synced_round: int | None = (
        await db.execute(
            select(func.max(TourSyncStatus.tour)).where(
                TourSyncStatus.season_id == season_id,
            )
        )
    ).scalar()

    # 5. Final max_completed_round = min(score-complete, sync-complete).
    if last_consecutive_complete is not None and max_synced_round is not None:
        max_completed_round = min(last_consecutive_complete, max_synced_round)
    else:
        max_completed_round = None

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
