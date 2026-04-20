"""Tests for compute_season_stats_scope — consecutive-tour completion logic."""

from datetime import date, datetime, time
from uuid import uuid4

import pytest

from app.models import Championship, Game, GameStatus, Season
from app.models.tour_sync_status import TourSyncStatus
from app.services.season_scope import (
    compute_current_rounds,
    compute_season_stats_scope,
)

EXT_SYNCED = datetime(2026, 3, 10, 12, 0, 0)


def _make_game(
    *,
    season_id: int,
    tour: int,
    home_score: int | None = None,
    away_score: int | None = None,
    extended_stats_synced_at: datetime | None = None,
    status: GameStatus = GameStatus.finished,
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
        extended_stats_synced_at=extended_stats_synced_at,
        status=status,
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
        _make_game(
            season_id=sample_season.id, tour=1,
            home_score=2, away_score=1, extended_stats_synced_at=EXT_SYNCED,
        )
    )
    await test_session.commit()

    result = await compute_season_stats_scope(test_session, sample_season.id, sample_season)
    assert result == (None, 0)


@pytest.mark.asyncio
async def test_all_tours_scored_but_sync_lags(test_session, sample_season, sample_teams):
    """All tours scored + ext stats, but TourSyncStatus only up to tour 1 → cap by max_synced."""
    sample_season.has_table = True
    sample_season.tournament_format = "round_robin"

    test_session.add_all([
        _make_game(
            season_id=sample_season.id, tour=1,
            home_score=2, away_score=1, extended_stats_synced_at=EXT_SYNCED,
        ),
        _make_game(
            season_id=sample_season.id, tour=2,
            home_score=0, away_score=0, extended_stats_synced_at=EXT_SYNCED,
        ),
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
        _make_game(
            season_id=sample_season.id, tour=1,
            home_score=2, away_score=1, extended_stats_synced_at=EXT_SYNCED,
        ),
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
        _make_game(
            season_id=sample_season.id, tour=1,
            home_score=2, away_score=1, extended_stats_synced_at=EXT_SYNCED,
        ),
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
        # Tour 1: complete (scores + ext stats)
        _make_game(
            season_id=sample_season.id, tour=1,
            home_score=2, away_score=1, extended_stats_synced_at=EXT_SYNCED,
        ),
        _make_game(
            season_id=sample_season.id, tour=1,
            home_score=0, away_score=0, extended_stats_synced_at=EXT_SYNCED,
        ),
        # Tour 2: incomplete (one game unscored)
        _make_game(
            season_id=sample_season.id, tour=2,
            home_score=3, away_score=1, extended_stats_synced_at=EXT_SYNCED,
        ),
        _make_game(season_id=sample_season.id, tour=2, home_score=None, away_score=None),
        # Tour 3: complete
        _make_game(
            season_id=sample_season.id, tour=3,
            home_score=1, away_score=1, extended_stats_synced_at=EXT_SYNCED,
        ),
        _make_game(
            season_id=sample_season.id, tour=3,
            home_score=2, away_score=0, extended_stats_synced_at=EXT_SYNCED,
        ),
        # TourSyncStatus: tours 1 and 3 synced
        _make_tss(sample_season.id, 1),
        _make_tss(sample_season.id, 3),
    ])
    await test_session.commit()

    mcr, emr = await compute_season_stats_scope(test_session, sample_season.id, sample_season)
    assert mcr == 1
    assert emr == 1


@pytest.mark.asyncio
async def test_scores_present_but_ext_stats_missing(test_session, sample_season, sample_teams):
    """Tour has scores but no extended_stats_synced_at → not complete."""
    sample_season.has_table = True
    sample_season.tournament_format = "round_robin"

    test_session.add_all([
        # Tour 1: complete (scores + ext stats)
        _make_game(
            season_id=sample_season.id, tour=1,
            home_score=2, away_score=1, extended_stats_synced_at=EXT_SYNCED,
        ),
        # Tour 2: scores present but ext stats missing on one game
        _make_game(
            season_id=sample_season.id, tour=2,
            home_score=3, away_score=1, extended_stats_synced_at=EXT_SYNCED,
        ),
        _make_game(
            season_id=sample_season.id, tour=2,
            home_score=0, away_score=0, extended_stats_synced_at=None,
        ),
        _make_tss(sample_season.id, 1),
        _make_tss(sample_season.id, 2),
    ])
    await test_session.commit()

    mcr, emr = await compute_season_stats_scope(test_session, sample_season.id, sample_season)
    # Tour 2 has scores but missing ext stats → incomplete → max_completed = 1
    assert mcr == 1
    assert emr == 1


@pytest.mark.asyncio
async def test_postponed_match_does_not_block_tour_completion(
    test_session, sample_season, sample_teams,
):
    """Regression: a postponed game must not keep the tour marked incomplete.

    Scenario from PL-2026 tour 6: 7 matches finished + 1 rescheduled.  The
    tour should still count as complete once the aggregate sync marker lands.
    """
    sample_season.has_table = True
    sample_season.tournament_format = "round_robin"

    test_session.add_all([
        _make_game(
            season_id=sample_season.id, tour=1,
            home_score=2, away_score=1, extended_stats_synced_at=EXT_SYNCED,
            status=GameStatus.finished,
        ),
        _make_game(
            season_id=sample_season.id, tour=1,
            home_score=0, away_score=0, extended_stats_synced_at=EXT_SYNCED,
            status=GameStatus.finished,
        ),
        # Postponed match — has no score and no ext stats, but must be skipped.
        _make_game(
            season_id=sample_season.id, tour=1,
            home_score=None, away_score=None,
            status=GameStatus.postponed,
        ),
        _make_tss(sample_season.id, 1),
    ])
    await test_session.commit()

    mcr, emr = await compute_season_stats_scope(
        test_session, sample_season.id, sample_season,
    )
    assert mcr == 1
    assert emr == 1


@pytest.mark.asyncio
async def test_current_rounds_ignores_orphan_early_played_fixture(
    test_session, sample_season, sample_teams,
):
    """A single fixture played in a distant future tour must not jump current_round.

    Scenario from PL-2026: match Qairat vs Kaspiy from tour 6 was moved to
    tour 25 where it was played early while other tour-25 games stay upcoming.
    Pre-fix the API returned current_round=25 (MAX(tour WHERE scored)); the
    helper must return the first tour with pending/live play (6) instead.
    """
    sample_season.has_table = True
    sample_season.tournament_format = "round_robin"

    test_session.add_all([
        # Tour 5: all played
        _make_game(
            season_id=sample_season.id, tour=5,
            home_score=1, away_score=0, extended_stats_synced_at=EXT_SYNCED,
            status=GameStatus.finished,
        ),
        # Tour 6: one still pending
        _make_game(
            season_id=sample_season.id, tour=6,
            home_score=2, away_score=1, extended_stats_synced_at=EXT_SYNCED,
            status=GameStatus.finished,
        ),
        _make_game(
            season_id=sample_season.id, tour=6,
            home_score=None, away_score=None, status=GameStatus.created,
        ),
        # Tour 25: only the moved fixture played, the rest are upcoming
        _make_game(
            season_id=sample_season.id, tour=25,
            home_score=2, away_score=0, extended_stats_synced_at=EXT_SYNCED,
            status=GameStatus.finished,
        ),
        _make_game(
            season_id=sample_season.id, tour=25,
            home_score=None, away_score=None, status=GameStatus.created,
        ),
    ])
    await test_session.commit()

    result = await compute_current_rounds(test_session, [sample_season.id])
    assert result == {sample_season.id: 6}


@pytest.mark.asyncio
async def test_current_rounds_falls_back_to_max_finished_when_all_played(
    test_session, sample_season, sample_teams,
):
    """If every tour is fully played, current_round = max finished tour."""
    sample_season.has_table = True
    sample_season.tournament_format = "round_robin"

    test_session.add_all([
        _make_game(
            season_id=sample_season.id, tour=1,
            home_score=1, away_score=0, extended_stats_synced_at=EXT_SYNCED,
            status=GameStatus.finished,
        ),
        _make_game(
            season_id=sample_season.id, tour=2,
            home_score=2, away_score=2, extended_stats_synced_at=EXT_SYNCED,
            status=GameStatus.finished,
        ),
    ])
    await test_session.commit()

    result = await compute_current_rounds(test_session, [sample_season.id])
    assert result == {sample_season.id: 2}


@pytest.mark.asyncio
async def test_knockout_uncapped(test_session, sample_season, sample_teams):
    """Knockout season → effective_max_round is None (uncapped)."""
    sample_season.has_table = False
    sample_season.tournament_format = "knockout"

    test_session.add_all([
        _make_game(
            season_id=sample_season.id, tour=1,
            home_score=2, away_score=1, extended_stats_synced_at=EXT_SYNCED,
        ),
        _make_tss(sample_season.id, 1),
    ])
    await test_session.commit()

    mcr, emr = await compute_season_stats_scope(test_session, sample_season.id, sample_season)
    assert mcr == 1
    assert emr is None
