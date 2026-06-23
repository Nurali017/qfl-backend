"""Regression tests for the data-hygiene fixes that back team withdrawal.

Covers the two general fixes applied so a withdrawn team (all fixtures
cancelled) leaves no residue in public surfaces:

1. ``get_next_games_for_teams`` must not surface a cancelled fixture as a
   team's "next game".
2. ``collect_season_team_ids`` must not include a team whose only presence in
   a season is via cancelled games.
"""
from datetime import date, time
from uuid import uuid4

import pytest

from app.models import Game, GameStatus, ScoreTable
from app.models.season_participant import SeasonParticipant
from app.services.standings import get_next_games_for_teams
from app.services.sync.stats_sync import collect_season_team_ids

FUTURE = date(2099, 1, 1)


def _future_game(
    *,
    season_id: int,
    home_team_id: int,
    away_team_id: int,
    status: GameStatus,
    day: date,
) -> Game:
    return Game(
        sota_id=uuid4(),
        date=day,
        time=time(18, 0),
        season_id=season_id,
        home_team_id=home_team_id,
        away_team_id=away_team_id,
        home_score=None,
        away_score=None,
        status=status,
    )


@pytest.mark.asyncio
async def test_next_game_skips_cancelled_fixture(
    test_session, sample_season, sample_teams,
):
    """A cancelled fixture is ignored; the next scheduled game is returned."""
    team_id = sample_teams[0].id  # 91
    # Cancelled fixture is chronologically FIRST — without the fix it would win.
    cancelled = _future_game(
        season_id=sample_season.id,
        home_team_id=team_id, away_team_id=sample_teams[1].id,
        status=GameStatus.cancelled, day=date(2099, 1, 1),
    )
    upcoming = _future_game(
        season_id=sample_season.id,
        home_team_id=team_id, away_team_id=sample_teams[2].id,
        status=GameStatus.created, day=date(2099, 2, 1),
    )
    test_session.add_all([cancelled, upcoming])
    await test_session.commit()

    result = await get_next_games_for_teams(test_session, sample_season.id, [team_id])

    assert team_id in result
    assert result[team_id].game_id == upcoming.id


@pytest.mark.asyncio
async def test_next_game_absent_when_only_cancelled(
    test_session, sample_season, sample_teams,
):
    """If every upcoming fixture is cancelled, the team has no next game."""
    team_id = sample_teams[0].id
    test_session.add(
        _future_game(
            season_id=sample_season.id,
            home_team_id=team_id, away_team_id=sample_teams[1].id,
            status=GameStatus.cancelled, day=FUTURE,
        )
    )
    await test_session.commit()

    result = await get_next_games_for_teams(test_session, sample_season.id, [team_id])

    assert team_id not in result


@pytest.mark.asyncio
async def test_team_set_excludes_fully_cancelled_team(
    test_session, sample_season, sample_teams,
):
    """A team present only via cancelled games drops out of the stats team-set.

    Teams that retain a non-cancelled game, a score_table row, or a
    participant entry stay in the set.
    """
    withdrawn = sample_teams[0].id   # only cancelled games, no score_table/participant
    active = sample_teams[1].id      # has a finished game
    listed = sample_teams[2].id      # only in score_table

    test_session.add_all([
        _future_game(
            season_id=sample_season.id,
            home_team_id=withdrawn, away_team_id=active,
            status=GameStatus.cancelled, day=FUTURE,
        ),
        Game(
            sota_id=uuid4(), date=date(2025, 5, 1), time=time(18, 0),
            season_id=sample_season.id, home_team_id=active, away_team_id=listed,
            home_score=1, away_score=0, status=GameStatus.finished,
        ),
        ScoreTable(season_id=sample_season.id, team_id=listed, position=1),
    ])
    await test_session.commit()

    team_ids = await collect_season_team_ids(test_session, sample_season.id)

    assert withdrawn not in team_ids
    assert active in team_ids
    assert listed in team_ids


@pytest.mark.asyncio
async def test_team_set_keeps_team_with_score_table_despite_cancelled_game(
    test_session, sample_season, sample_teams,
):
    """A score_table row keeps a team in the set even if a game is cancelled."""
    team_id = sample_teams[0].id
    test_session.add_all([
        _future_game(
            season_id=sample_season.id,
            home_team_id=team_id, away_team_id=sample_teams[1].id,
            status=GameStatus.cancelled, day=FUTURE,
        ),
        ScoreTable(season_id=sample_season.id, team_id=team_id, position=1),
    ])
    await test_session.commit()

    team_ids = await collect_season_team_ids(test_session, sample_season.id)

    assert team_id in team_ids
