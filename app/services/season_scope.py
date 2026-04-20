"""Helpers for season-scoped statistics windows."""

from sqlalchemy import case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Game, GameStatus, Season, TourSyncStatus

NON_BLOCKING_STATUSES = (GameStatus.postponed, GameStatus.cancelled)


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

    # 1. First incomplete tour: min tour where not all *playable* games have
    #    scores AND extended_stats_synced_at.  Postponed/cancelled games are
    #    excluded from the total — a postponed match must not block the rest
    #    of the tour from being considered complete.
    playable_predicate = Game.status.notin_(NON_BLOCKING_STATUSES)
    incomplete_sq = (
        select(Game.tour)
        .where(
            Game.season_id == season_id,
            Game.tour.isnot(None),
        )
        .group_by(Game.tour)
        .having(
            func.count(case((playable_predicate, 1)))
            != func.count(
                case(
                    (
                        Game.home_score.isnot(None)
                        & Game.away_score.isnot(None)
                        & Game.extended_stats_synced_at.isnot(None)
                        & playable_predicate,
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


async def compute_current_rounds(
    db: AsyncSession,
    season_ids: list[int],
) -> dict[int, int]:
    """Return ``{season_id: current_round}`` for tour-based seasons.

    ``current_round`` is the highest tour T such that for every k in
    ``[min_tour, T]`` at least one match in tour k has reached a terminal
    status (``finished`` / ``technical_defeat``).  Tours beyond the first
    consecutive gap are ignored — this drops "orphan" future-tour fixtures
    (e.g. a rescheduled match assigned to tour 25 while tour 7 has not yet
    started) that previously pulled ``current_round`` forward and desynced
    the UI.

    Returns nothing for seasons that have no terminal games yet.
    """
    if not season_ids:
        return {}

    result = await db.execute(
        select(Game.season_id, Game.tour)
        .where(
            Game.season_id.in_(season_ids),
            Game.tour.isnot(None),
            Game.status.in_((GameStatus.finished, GameStatus.technical_defeat)),
        )
        .distinct()
    )

    tours_by_season: dict[int, set[int]] = {}
    for sid, tour in result.all():
        if sid is None or tour is None:
            continue
        tours_by_season.setdefault(sid, set()).add(int(tour))

    out: dict[int, int] = {}
    for sid, tours in tours_by_season.items():
        ordered = sorted(tours)
        current = ordered[0]
        for t in ordered[1:]:
            if t == current + 1:
                current = t
            else:
                break
        out[sid] = current
    return out
