from datetime import date, datetime, time, timedelta, timezone
from uuid import uuid4

import pytest

from app.models import Game, GameStatus, Season
from app.services.home_matches import ALMATY_TZ, _fallback, _resolve_season, get_home_widget
import app.services.home_matches as home_matches_service


def _utc_from_almaty(dt: datetime) -> datetime:
    return dt.astimezone(timezone.utc)


def _make_game(
    *,
    season_id: int,
    home_team_id: int,
    away_team_id: int,
    game_date: date,
    game_time: time,
    tour: int,
    status: GameStatus,
    home_score: int | None = None,
    away_score: int | None = None,
    finished_at: datetime | None = None,
) -> Game:
    return Game(
        sota_id=uuid4(),
        date=game_date,
        time=game_time,
        tour=tour,
        season_id=season_id,
        home_team_id=home_team_id,
        away_team_id=away_team_id,
        status=status,
        home_score=home_score,
        away_score=away_score,
        finished_at=finished_at,
    )


@pytest.mark.asyncio
async def test_get_home_widget_completed_window_keeps_finished_default_and_exposes_next_round_upcoming(
    test_session,
    sample_season,
    sample_teams,
):
    now = datetime.now(ALMATY_TZ)

    sample_season.frontend_code = "pl"
    sample_season.is_current = True

    finished_game = _make_game(
        season_id=sample_season.id,
        home_team_id=sample_teams[0].id,
        away_team_id=sample_teams[1].id,
        game_date=now.date(),
        game_time=time(18, 0),
        tour=1,
        status=GameStatus.finished,
        home_score=2,
        away_score=1,
        finished_at=_utc_from_almaty(now - timedelta(hours=2)),
    )
    technical_game = _make_game(
        season_id=sample_season.id,
        home_team_id=sample_teams[1].id,
        away_team_id=sample_teams[2].id,
        game_date=now.date(),
        game_time=time(20, 0),
        tour=1,
        status=GameStatus.technical_defeat,
        home_score=3,
        away_score=0,
        finished_at=_utc_from_almaty(now - timedelta(hours=1)),
    )
    next_round_upcoming = _make_game(
        season_id=sample_season.id,
        home_team_id=sample_teams[2].id,
        away_team_id=sample_teams[0].id,
        game_date=now.date() + timedelta(days=1),
        game_time=time(18, 30),
        tour=2,
        status=GameStatus.created,
    )

    test_session.add_all([finished_game, technical_game, next_round_upcoming])
    await test_session.commit()

    result = await get_home_widget(test_session, "pl", "ru")

    assert result.window_state == "completed_window"
    assert result.default_tab == "finished"
    assert result.show_tabs is True
    assert result.selected_round == 1
    assert result.completed_window_expires_at is not None
    assert result.finished_groups is not None
    assert result.upcoming_groups is not None

    finished_statuses = [
        game.status for group in result.finished_groups for game in group.games
    ]
    upcoming_statuses = [
        game.status for group in result.upcoming_groups for game in group.games
    ]
    assert [group.date for group in result.groups] == [now.date()]
    assert finished_statuses == ["finished", "finished"]
    assert upcoming_statuses == ["upcoming"]


@pytest.mark.asyncio
async def test_get_home_widget_active_round_keeps_full_current_tour_in_upcoming_and_previous_tour_in_finished(
    test_session,
    sample_season,
    sample_teams,
):
    now = datetime.now(ALMATY_TZ)

    sample_season.frontend_code = "pl"
    sample_season.is_current = True

    previous_tour_finished = _make_game(
        season_id=sample_season.id,
        home_team_id=sample_teams[2].id,
        away_team_id=sample_teams[0].id,
        game_date=now.date() - timedelta(days=2),
        game_time=time(18, 0),
        tour=1,
        status=GameStatus.finished,
        home_score=2,
        away_score=1,
        finished_at=_utc_from_almaty(now - timedelta(days=2, hours=1)),
    )
    current_tour_finished = _make_game(
        season_id=sample_season.id,
        home_team_id=sample_teams[0].id,
        away_team_id=sample_teams[1].id,
        game_date=now.date(),
        game_time=time(16, 0),
        tour=2,
        status=GameStatus.finished,
        home_score=1,
        away_score=0,
        finished_at=_utc_from_almaty(now - timedelta(hours=1)),
    )
    upcoming_game = _make_game(
        season_id=sample_season.id,
        home_team_id=sample_teams[1].id,
        away_team_id=sample_teams[2].id,
        game_date=now.date() + timedelta(days=1),
        game_time=time(18, 0),
        tour=2,
        status=GameStatus.created,
    )

    test_session.add_all([previous_tour_finished, current_tour_finished, upcoming_game])
    await test_session.commit()

    result = await get_home_widget(test_session, "pl", "ru")

    assert result.window_state == "active_round"
    assert result.selected_round == 2
    assert result.default_tab == "upcoming"
    assert result.show_tabs is True
    assert result.finished_groups is not None
    assert result.upcoming_groups is not None
    assert [game.id for group in result.groups for game in group.games] == [
        current_tour_finished.id,
        upcoming_game.id,
    ]

    finished_statuses = {
        game.id: game.status
        for group in result.finished_groups
        for game in group.games
    }
    upcoming_statuses = {
        game.id: game.status
        for group in result.upcoming_groups
        for game in group.games
    }
    assert finished_statuses == {previous_tour_finished.id: "finished"}
    assert upcoming_statuses[current_tour_finished.id] == "finished"
    assert upcoming_statuses[upcoming_game.id] == "upcoming"
    assert current_tour_finished.id not in finished_statuses


@pytest.mark.asyncio
async def test_get_home_widget_after_completed_window_switches_to_next_round(
    test_session,
    sample_season,
    sample_teams,
):
    now = datetime.now(ALMATY_TZ)

    sample_season.frontend_code = "pl"
    sample_season.is_current = True

    finished_game = _make_game(
        season_id=sample_season.id,
        home_team_id=sample_teams[0].id,
        away_team_id=sample_teams[1].id,
        game_date=(now - timedelta(days=3)).date(),
        game_time=time(16, 0),
        tour=2,
        status=GameStatus.finished,
        home_score=1,
        away_score=0,
        finished_at=_utc_from_almaty(now - timedelta(hours=60)),
    )
    next_round_upcoming = _make_game(
        season_id=sample_season.id,
        home_team_id=sample_teams[1].id,
        away_team_id=sample_teams[2].id,
        game_date=now.date(),
        game_time=time(18, 0),
        tour=3,
        status=GameStatus.created,
    )

    test_session.add_all([finished_game, next_round_upcoming])
    await test_session.commit()

    result = await get_home_widget(test_session, "pl", "ru")

    assert result.window_state == "active_round"
    assert result.selected_round == 3
    assert result.default_tab == "upcoming"
    assert result.show_tabs is True
    assert result.finished_groups is not None
    assert result.upcoming_groups is not None
    all_group_ids = {game.id for group in result.groups for game in group.games}
    assert next_round_upcoming.id in all_group_ids
    finished_ids = {game.id for group in result.finished_groups for game in group.games}
    assert finished_game.id in finished_ids
    upcoming_ids = {game.id for group in result.upcoming_groups for game in group.games}
    assert next_round_upcoming.id in upcoming_ids


@pytest.mark.asyncio
async def test_get_home_widget_treats_postponed_tour_as_completed(
    test_session,
    sample_season,
    sample_teams,
):
    """Regression (PL-2026 tour 6): a tour with all playable games finished and
    one rescheduled fixture must open the 24h "finished" window, not stay
    flagged as an active round."""
    now = datetime.now(ALMATY_TZ)

    sample_season.frontend_code = "pl"
    sample_season.is_current = True

    finished_game_1 = _make_game(
        season_id=sample_season.id,
        home_team_id=sample_teams[0].id,
        away_team_id=sample_teams[1].id,
        game_date=now.date(),
        game_time=time(18, 0),
        tour=1,
        status=GameStatus.finished,
        home_score=2,
        away_score=1,
        finished_at=_utc_from_almaty(now - timedelta(hours=2)),
    )
    finished_game_2 = _make_game(
        season_id=sample_season.id,
        home_team_id=sample_teams[1].id,
        away_team_id=sample_teams[2].id,
        game_date=now.date(),
        game_time=time(20, 0),
        tour=1,
        status=GameStatus.finished,
        home_score=0,
        away_score=0,
        finished_at=_utc_from_almaty(now - timedelta(hours=1)),
    )
    postponed_game = _make_game(
        season_id=sample_season.id,
        home_team_id=sample_teams[2].id,
        away_team_id=sample_teams[0].id,
        game_date=now.date(),
        game_time=time(19, 0),
        tour=1,
        status=GameStatus.postponed,
    )

    test_session.add_all([finished_game_1, finished_game_2, postponed_game])
    await test_session.commit()

    result = await get_home_widget(test_session, "pl", "ru")

    assert result.selected_round == 1
    assert result.window_state == "completed_window"
    assert result.default_tab == "finished"
    assert result.completed_window_expires_at is not None


@pytest.mark.asyncio
async def test_get_home_widget_treats_stale_created_game_as_non_blocking(
    test_session,
    sample_season,
    sample_teams,
):
    """Regression (PL-2026 tour 6 live state): tour has finished games plus a
    created fixture whose match-day has already passed — the match-day has
    slipped but it was never explicitly marked postponed.  The widget must
    treat the tour as closed for UI purposes (completed_window), not as an
    active round dragging on forever."""
    now = datetime.now(ALMATY_TZ)

    sample_season.frontend_code = "pl"
    sample_season.is_current = True

    finished_game_1 = _make_game(
        season_id=sample_season.id,
        home_team_id=sample_teams[0].id,
        away_team_id=sample_teams[1].id,
        game_date=now.date(),
        game_time=time(16, 0),
        tour=1,
        status=GameStatus.finished,
        home_score=2,
        away_score=1,
        finished_at=_utc_from_almaty(now - timedelta(hours=2)),
    )
    finished_game_2 = _make_game(
        season_id=sample_season.id,
        home_team_id=sample_teams[1].id,
        away_team_id=sample_teams[2].id,
        game_date=now.date(),
        game_time=time(18, 0),
        tour=1,
        status=GameStatus.finished,
        home_score=0,
        away_score=0,
        finished_at=_utc_from_almaty(now - timedelta(hours=1)),
    )
    stale_created = _make_game(
        season_id=sample_season.id,
        home_team_id=sample_teams[2].id,
        away_team_id=sample_teams[0].id,
        game_date=now.date() - timedelta(days=3),
        game_time=time(19, 0),
        tour=1,
        status=GameStatus.created,
    )

    test_session.add_all([finished_game_1, finished_game_2, stale_created])
    await test_session.commit()

    result = await get_home_widget(test_session, "pl", "ru")

    assert result.selected_round == 1
    assert result.window_state == "completed_window"
    assert result.default_tab == "finished"


@pytest.mark.asyncio
async def test_get_home_widget_stays_in_completed_window_within_48h(
    test_session,
    sample_season,
    sample_teams,
):
    """Completed tour stays on default_tab=finished for 48h after the last
    terminal match, then flips to upcoming (regardless of when the next
    tour physically starts)."""
    now = datetime.now(ALMATY_TZ)

    sample_season.frontend_code = "pl"
    sample_season.is_current = True

    recent_finished = _make_game(
        season_id=sample_season.id,
        home_team_id=sample_teams[0].id,
        away_team_id=sample_teams[1].id,
        game_date=now.date() - timedelta(days=1),
        game_time=time(18, 0),
        tour=1,
        status=GameStatus.finished,
        home_score=2,
        away_score=1,
        finished_at=_utc_from_almaty(now - timedelta(hours=40)),
    )
    next_tour_far = _make_game(
        season_id=sample_season.id,
        home_team_id=sample_teams[1].id,
        away_team_id=sample_teams[2].id,
        game_date=now.date() + timedelta(days=5),
        game_time=time(18, 0),
        tour=2,
        status=GameStatus.created,
    )

    test_session.add_all([recent_finished, next_tour_far])
    await test_session.commit()

    result = await get_home_widget(test_session, "pl", "ru")

    assert result.selected_round == 1
    assert result.window_state == "completed_window"
    assert result.default_tab == "finished"


@pytest.mark.asyncio
async def test_get_home_widget_ignores_orphan_future_tour_fixture(
    test_session,
    sample_season,
    sample_teams,
):
    """A single fixture played in a far-off tour must not jump the hint forward.

    Mirrors the PL-2026 scenario where one game was played in tour 25 while
    tours 2..24 have not started yet — the widget should stay on tour 1.
    """
    now = datetime.now(ALMATY_TZ)

    sample_season.frontend_code = "pl"
    sample_season.is_current = True

    tour_1_finished = _make_game(
        season_id=sample_season.id,
        home_team_id=sample_teams[0].id,
        away_team_id=sample_teams[1].id,
        game_date=now.date(),
        game_time=time(18, 0),
        tour=1,
        status=GameStatus.finished,
        home_score=2,
        away_score=0,
        finished_at=_utc_from_almaty(now - timedelta(hours=3)),
    )
    tour_1_upcoming = _make_game(
        season_id=sample_season.id,
        home_team_id=sample_teams[1].id,
        away_team_id=sample_teams[2].id,
        game_date=now.date() + timedelta(days=1),
        game_time=time(19, 0),
        tour=1,
        status=GameStatus.created,
    )
    orphan_future = _make_game(
        season_id=sample_season.id,
        home_team_id=sample_teams[2].id,
        away_team_id=sample_teams[0].id,
        game_date=now.date() - timedelta(days=5),
        game_time=time(17, 0),
        tour=25,
        status=GameStatus.finished,
        home_score=2,
        away_score=0,
        finished_at=_utc_from_almaty(now - timedelta(days=5)),
    )

    test_session.add_all([tour_1_finished, tour_1_upcoming, orphan_future])
    await test_session.commit()

    result = await get_home_widget(test_session, "pl", "ru")

    assert result.selected_round == 1
    assert result.window_state == "active_round"


@pytest.mark.asyncio
async def test_resolve_season_uses_almaty_today_helper(
    test_session,
    sample_championship,
):
    old_season = Season(
        id=70,
        name="2025",
        championship_id=sample_championship.id,
        frontend_code="pl",
        date_start=date(2025, 3, 1),
        date_end=date(2025, 11, 30),
        is_visible=True,
    )
    active_season = Season(
        id=71,
        name="2026",
        championship_id=sample_championship.id,
        frontend_code="pl",
        date_start=date(2026, 3, 1),
        date_end=date(2026, 11, 30),
        is_visible=True,
    )
    test_session.add_all([old_season, active_season])
    await test_session.commit()

    def fake_today() -> date:
        return date(2026, 3, 18)

    original_today = home_matches_service._today_almaty
    home_matches_service._today_almaty = fake_today
    try:
        result = await _resolve_season(test_session, "pl")
    finally:
        home_matches_service._today_almaty = original_today

    assert result is not None
    assert result.id == active_season.id


@pytest.mark.asyncio
async def test_fallback_uses_almaty_today_helper(
    test_session,
    sample_season,
    sample_teams,
):
    sample_season.frontend_code = "pl"
    sample_season.is_current = True

    finished_game = _make_game(
        season_id=sample_season.id,
        home_team_id=sample_teams[0].id,
        away_team_id=sample_teams[1].id,
        game_date=date(2026, 3, 17),
        game_time=time(18, 0),
        tour=1,
        status=GameStatus.finished,
        home_score=2,
        away_score=1,
    )
    upcoming_game = _make_game(
        season_id=sample_season.id,
        home_team_id=sample_teams[1].id,
        away_team_id=sample_teams[2].id,
        game_date=date(2026, 3, 19),
        game_time=time(19, 0),
        tour=2,
        status=GameStatus.created,
    )
    test_session.add_all([finished_game, upcoming_game])
    await test_session.commit()

    def fake_today() -> date:
        return date(2026, 3, 18)

    original_today = home_matches_service._today_almaty
    home_matches_service._today_almaty = fake_today
    try:
        result = await _fallback(test_session, sample_season.id, "pl", "ru")
    finally:
        home_matches_service._today_almaty = original_today

    assert result.window_state == "fallback"
    assert result.selected_round == 2
    assert [group.date for group in result.groups] == [date(2026, 3, 19)]
