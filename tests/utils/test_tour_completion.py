"""Tests for the shared tour-completion predicates.

Regression: a tour never generated "team of the week" when one game was
rescheduled into the future but left in status `created` (not postponed/
cancelled). It inflated `total` but never `completed`, so the gate
`completed == total` never passed. `tour_playable_predicate` now excludes
future-dated games from `total`.

These run the REAL count query against SQLite (not mocks) so they exercise the
actual SQL the three gates use.
"""

from datetime import date, time
from uuid import uuid4

import pytest
from sqlalchemy import case, func, select

from app.models import Game, GameStatus
from app.utils.tour_completion import (
    tour_completed_predicate,
    tour_playable_predicate,
)

TODAY = date(2026, 6, 16)


async def _add_game(session, season_id, teams, *, tour, status, gdate, hs=None, as_=None):
    session.add(
        Game(
            sota_id=uuid4(),
            date=gdate,
            time=time(17, 0),
            tour=tour,
            season_id=season_id,
            home_team_id=teams[0].id,
            away_team_id=teams[1].id,
            status=status,
            home_score=hs,
            away_score=as_,
        )
    )


async def _gate(session, season_id, tour, today):
    res = await session.execute(
        select(
            func.count(case((tour_playable_predicate(today), 1))).label("total"),
            func.count(case((tour_completed_predicate(), 1))).label("completed"),
        ).where(Game.season_id == season_id, Game.tour == tour)
    )
    return res.one()


@pytest.mark.asyncio
async def test_future_created_game_excluded_from_total(test_session, sample_season, sample_teams):
    """Core fix: 6 finished + 1 future-dated created → gate passes (total==completed==6)."""
    for _ in range(6):
        await _add_game(
            test_session, sample_season.id, sample_teams,
            tour=9, status=GameStatus.finished, gdate=date(2026, 6, 11), hs=1, as_=0,
        )
    # Rescheduled fixture parked in the future.
    await _add_game(
        test_session, sample_season.id, sample_teams,
        tour=9, status=GameStatus.created, gdate=date(2026, 9, 4),
    )
    await test_session.commit()

    row = await _gate(test_session, sample_season.id, 9, TODAY)
    assert row.total == 6
    assert row.completed == 6
    assert row.total > 0 and row.completed == row.total  # gate passes → TOW dispatched

    # Contrast with the pre-fix predicate (no date filter): it counted the
    # future game, giving total=7 and stalling the gate forever.
    old_total = await test_session.scalar(
        select(func.count(case((
            Game.status.notin_((GameStatus.postponed, GameStatus.cancelled)), 1,
        )))).where(Game.season_id == sample_season.id, Game.tour == 9)
    )
    assert old_total == 7


@pytest.mark.asyncio
async def test_today_unplayed_game_still_blocks(test_session, sample_season, sample_teams):
    """A game scheduled today but not yet played must still block the gate."""
    for _ in range(2):
        await _add_game(
            test_session, sample_season.id, sample_teams,
            tour=3, status=GameStatus.finished, gdate=date(2026, 6, 14), hs=2, as_=1,
        )
    await _add_game(
        test_session, sample_season.id, sample_teams,
        tour=3, status=GameStatus.created, gdate=TODAY,
    )
    await test_session.commit()

    row = await _gate(test_session, sample_season.id, 3, TODAY)
    assert row.total == 3
    assert row.completed == 2
    assert not (row.total > 0 and row.completed == row.total)  # gate blocked


@pytest.mark.asyncio
async def test_postponed_excluded_regardless_of_date(test_session, sample_season, sample_teams):
    """Postponed games are non-blocking even when dated in the past."""
    for _ in range(3):
        await _add_game(
            test_session, sample_season.id, sample_teams,
            tour=5, status=GameStatus.finished, gdate=date(2026, 5, 1), hs=0, as_=0,
        )
    await _add_game(
        test_session, sample_season.id, sample_teams,
        tour=5, status=GameStatus.postponed, gdate=date(2026, 5, 1),
    )
    await test_session.commit()

    row = await _gate(test_session, sample_season.id, 5, TODAY)
    assert row.total == 3
    assert row.completed == 3
    assert row.total > 0 and row.completed == row.total


@pytest.mark.asyncio
async def test_all_finished_baseline_passes(test_session, sample_season, sample_teams):
    for _ in range(7):
        await _add_game(
            test_session, sample_season.id, sample_teams,
            tour=1, status=GameStatus.finished, gdate=date(2026, 4, 1), hs=3, as_=2,
        )
    await test_session.commit()

    row = await _gate(test_session, sample_season.id, 1, TODAY)
    assert row.total == 7
    assert row.completed == 7
