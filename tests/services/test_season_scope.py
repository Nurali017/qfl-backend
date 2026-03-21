"""Tests for compute_season_stats_scope — consecutive-tour completion logic."""

from datetime import date, datetime, time
from uuid import uuid4

import pytest

from app.models import Championship, Game, Season
from app.models.tour_sync_status import TourSyncStatus
from app.services.season_scope import compute_season_stats_scope


def _make_game(
    *,
    season_id: int,
    tour: int,
    home_score: int | None = None,
    away_score: int | None = None,
) -> Game:
    return Game(
        sota_id=uuid4(),
        date=date(2026, 3, 1),
        time=time(18, 0),
        tour=tour,
        season_id=season_id,
        home_team_id=91,
        away_team_id=13,
        home_score=home_score,
        away_score=away_score,
    )


def _make_tss(season_id: int, tour: int) -> TourSyncStatus:
    return TourSyncStatus(
        season_id=season_id,
        tour=tour,
        synced_at=datetime(2026, 3, 10, 12, 0, 0),
    )


@pytest.mark.asyncio
async def test_no_synced_tours_round_robin(test_session, sample_season, sample_teams):
    """No TourSyncStatus rows → (None, 0) for round-robin."""
    sample_season.has_table = True
    sample_season.tournament_format = "round_robin"
    test_session.add(
        _make_game(season_id=sample_season.id, tour=1, home_score=2, away_score=1)
    )
    await test_session.commit()

    result = await compute_season_stats_scope(test_session, sample_season.id, sample_season)
    assert result == (None, 0)


@pytest.mark.asyncio
async def test_all_tours_scored_but_sync_lags(test_session, sample_season, sample_teams):
    """All tours scored, but TourSyncStatus only up to tour 1 → cap by max_synced."""
    sample_season.has_table = True
    sample_season.tournament_format = "round_robin"

    test_session.add_all([
        _make_game(season_id=sample_season.id, tour=1, home_score=2, away_score=1),
        _make_game(season_id=sample_season.id, tour=2, home_score=0, away_score=0),
        _make_tss(sample_season.id, 1),
    ])
    await test_session.commit()

    mcr, emr = await compute_season_stats_scope(test_session, sample_season.id, sample_season)
    assert mcr == 1
    assert emr == 1


@pytest.mark.asyncio
async def test_first_tour_incomplete(test_session, sample_season, sample_teams):
    """Tour 1 has an unscored game → max_completed_round is None."""
    sample_season.has_table = True
    sample_season.tournament_format = "round_robin"

    test_session.add_all([
        _make_game(season_id=sample_season.id, tour=1, home_score=2, away_score=1),
        _make_game(season_id=sample_season.id, tour=1, home_score=None, away_score=None),
        _make_tss(sample_season.id, 1),
    ])
    await test_session.commit()

    mcr, emr = await compute_season_stats_scope(test_session, sample_season.id, sample_season)
    assert mcr is None
    assert emr == 0


@pytest.mark.asyncio
async def test_max_round_overrides_effective_only(test_session, sample_season, sample_teams):
    """max_round changes effective_max_round but not max_completed_round."""
    sample_season.has_table = True
    sample_season.tournament_format = "round_robin"

    test_session.add_all([
        _make_game(season_id=sample_season.id, tour=1, home_score=2, away_score=1),
        _make_tss(sample_season.id, 1),
    ])
    await test_session.commit()

    mcr, emr = await compute_season_stats_scope(
        test_session, sample_season.id, sample_season, max_round=5
    )
    assert mcr == 1
    assert emr == 5


@pytest.mark.asyncio
async def test_tour2_incomplete_tour3_synced(test_session, sample_season, sample_teams):
    """Regression: tour 2 incomplete, tour 3 fully synced → max_completed_round == 1."""
    sample_season.has_table = True
    sample_season.tournament_format = "round_robin"

    test_session.add_all([
        # Tour 1: complete
        _make_game(season_id=sample_season.id, tour=1, home_score=2, away_score=1),
        _make_game(season_id=sample_season.id, tour=1, home_score=0, away_score=0),
        # Tour 2: incomplete (one game unscored)
        _make_game(season_id=sample_season.id, tour=2, home_score=3, away_score=1),
        _make_game(season_id=sample_season.id, tour=2, home_score=None, away_score=None),
        # Tour 3: complete
        _make_game(season_id=sample_season.id, tour=3, home_score=1, away_score=1),
        _make_game(season_id=sample_season.id, tour=3, home_score=2, away_score=0),
        # TourSyncStatus: tours 1 and 3 synced
        _make_tss(sample_season.id, 1),
        _make_tss(sample_season.id, 3),
    ])
    await test_session.commit()

    mcr, emr = await compute_season_stats_scope(test_session, sample_season.id, sample_season)
    assert mcr == 1
    assert emr == 1


@pytest.mark.asyncio
async def test_knockout_uncapped(test_session, sample_season, sample_teams):
    """Knockout season → effective_max_round is None (uncapped)."""
    sample_season.has_table = False
    sample_season.tournament_format = "knockout"

    test_session.add_all([
        _make_game(season_id=sample_season.id, tour=1, home_score=2, away_score=1),
        _make_tss(sample_season.id, 1),
    ])
    await test_session.commit()

    mcr, emr = await compute_season_stats_scope(test_session, sample_season.id, sample_season)
    assert mcr == 1
    assert emr is None
