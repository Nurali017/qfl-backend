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


def _set_clock(now: datetime):
    """Override _now_almaty/_today_almaty for deterministic tests.

    Returns a callable that restores the originals.
    """
    original_now = home_matches_service._now_almaty
    original_today = home_matches_service._today_almaty
    home_matches_service._now_almaty = lambda: now
    home_matches_service._today_almaty = lambda: now.date()

    def restore() -> None:
        home_matches_service._now_almaty = original_now
        home_matches_service._today_almaty = original_today

    return restore


@pytest.mark.asyncio
async def test_get_home_widget_anchor_week_includes_rescheduled_other_tour(
    test_session,
    sample_season,
    sample_teams,
):
    """Real PL-2026 scenario: tour 20 fixture played on Wed 6 May before
    tour 9's main slate on Sat-Sun 9-10 May. Anchor week = current ISO
    week (May 4..10) — every match in that week shows in upcoming,
    regardless of tour number. Previous week's terminal games go to
    finished tab."""
    today = date(2026, 5, 4)  # Monday
    now = datetime(2026, 5, 4, 14, 0, tzinfo=ALMATY_TZ)
    restore = _set_clock(now)

    sample_season.frontend_code = "pl"
    sample_season.is_current = True
    sid = sample_season.id

    prev_may2_a = _make_game(season_id=sid, home_team_id=sample_teams[0].id, away_team_id=sample_teams[1].id,
                             game_date=date(2026, 5, 2), game_time=time(15, 0), tour=8,
                             status=GameStatus.finished, home_score=0, away_score=1,
                             finished_at=_utc_from_almaty(datetime(2026, 5, 2, 17, 0, tzinfo=ALMATY_TZ)))
    prev_may2_b = _make_game(season_id=sid, home_team_id=sample_teams[1].id, away_team_id=sample_teams[2].id,
                             game_date=date(2026, 5, 2), game_time=time(17, 0), tour=8,
                             status=GameStatus.finished, home_score=0, away_score=1,
                             finished_at=_utc_from_almaty(datetime(2026, 5, 2, 19, 0, tzinfo=ALMATY_TZ)))
    prev_may3_a = _make_game(season_id=sid, home_team_id=sample_teams[2].id, away_team_id=sample_teams[0].id,
                             game_date=date(2026, 5, 3), game_time=time(15, 0), tour=8,
                             status=GameStatus.finished, home_score=2, away_score=0,
                             finished_at=_utc_from_almaty(datetime(2026, 5, 3, 17, 0, tzinfo=ALMATY_TZ)))
    prev_may3_b = _make_game(season_id=sid, home_team_id=sample_teams[0].id, away_team_id=sample_teams[2].id,
                             game_date=date(2026, 5, 3), game_time=time(20, 0), tour=8,
                             status=GameStatus.finished, home_score=1, away_score=1,
                             finished_at=_utc_from_almaty(datetime(2026, 5, 3, 22, 0, tzinfo=ALMATY_TZ)))
    rescheduled_may6 = _make_game(season_id=sid, home_team_id=sample_teams[1].id, away_team_id=sample_teams[2].id,
                                  game_date=date(2026, 5, 6), game_time=time(19, 0), tour=20,
                                  status=GameStatus.created)
    upcoming_may9_a = _make_game(season_id=sid, home_team_id=sample_teams[0].id, away_team_id=sample_teams[1].id,
                                 game_date=date(2026, 5, 9), game_time=time(15, 0), tour=9,
                                 status=GameStatus.created)
    upcoming_may9_b = _make_game(season_id=sid, home_team_id=sample_teams[1].id, away_team_id=sample_teams[0].id,
                                 game_date=date(2026, 5, 9), game_time=time(18, 0), tour=9,
                                 status=GameStatus.created)
    upcoming_may10_tour22 = _make_game(season_id=sid, home_team_id=sample_teams[2].id, away_team_id=sample_teams[1].id,
                                       game_date=date(2026, 5, 10), game_time=time(15, 0), tour=22,
                                       status=GameStatus.created)
    upcoming_may10_tour9 = _make_game(season_id=sid, home_team_id=sample_teams[0].id, away_team_id=sample_teams[2].id,
                                      game_date=date(2026, 5, 10), game_time=time(17, 0), tour=9,
                                      status=GameStatus.created)

    test_session.add_all([
        prev_may2_a, prev_may2_b, prev_may3_a, prev_may3_b,
        rescheduled_may6,
        upcoming_may9_a, upcoming_may9_b,
        upcoming_may10_tour22, upcoming_may10_tour9,
    ])
    await test_session.commit()

    try:
        result = await get_home_widget(test_session, "pl", "ru")
    finally:
        restore()

    upcoming_dates = [g.date for grp in result.upcoming_groups for g in grp.games]
    upcoming_ids = [g.id for grp in result.upcoming_groups for g in grp.games]
    finished_ids = [g.id for grp in result.finished_groups for g in grp.games]

    # Rescheduled tour-20 match must show in upcoming, alongside tour-9 and tour-22 of the same week.
    assert rescheduled_may6.id in upcoming_ids
    assert upcoming_may10_tour22.id in upcoming_ids
    assert {date(2026, 5, 6), date(2026, 5, 9), date(2026, 5, 10)} == set(upcoming_dates)

    # Previous-week tour-8 games go to finished, not upcoming.
    assert {prev_may2_a.id, prev_may2_b.id, prev_may3_a.id, prev_may3_b.id}.issubset(finished_ids)
    assert rescheduled_may6.id not in finished_ids
    assert upcoming_may10_tour22.id not in finished_ids

    assert result.show_tabs is True


@pytest.mark.asyncio
async def test_get_home_widget_active_week_keeps_finished_games_in_upcoming_tab(
    test_session,
    sample_season,
    sample_teams,
):
    """While a matchday week is in progress (any unfinished playable game
    remains), ALL games of that week — including ones that have already
    been played and a live one — ride along in the upcoming tab. Finished
    tab in this state shows only the previous week's results.
    """
    today = date(2026, 5, 4)  # Monday
    now = datetime(2026, 5, 4, 14, 30, tzinfo=ALMATY_TZ)
    restore = _set_clock(now)

    sample_season.frontend_code = "pl"
    sample_season.is_current = True
    sid = sample_season.id

    prev_week_finished = _make_game(
        season_id=sid, home_team_id=sample_teams[0].id, away_team_id=sample_teams[1].id,
        game_date=date(2026, 4, 30), game_time=time(18, 0), tour=1,
        status=GameStatus.finished, home_score=1, away_score=0,
        finished_at=_utc_from_almaty(datetime(2026, 4, 30, 20, 0, tzinfo=ALMATY_TZ)),
    )
    week_finished_early = _make_game(
        season_id=sid, home_team_id=sample_teams[1].id, away_team_id=sample_teams[2].id,
        game_date=date(2026, 5, 4), game_time=time(12, 0), tour=2,
        status=GameStatus.finished, home_score=0, away_score=1,
        finished_at=_utc_from_almaty(datetime(2026, 5, 4, 14, 0, tzinfo=ALMATY_TZ)),
    )
    week_live = _make_game(
        season_id=sid, home_team_id=sample_teams[2].id, away_team_id=sample_teams[0].id,
        game_date=date(2026, 5, 4), game_time=time(14, 0), tour=2,
        status=GameStatus.live,
    )
    week_upcoming = _make_game(
        season_id=sid, home_team_id=sample_teams[0].id, away_team_id=sample_teams[2].id,
        game_date=date(2026, 5, 5), game_time=time(18, 0), tour=2,
        status=GameStatus.created,
    )
    test_session.add_all([prev_week_finished, week_finished_early, week_live, week_upcoming])
    await test_session.commit()

    try:
        result = await get_home_widget(test_session, "pl", "ru")
    finally:
        restore()

    assert result.window_state == "active_round"
    assert result.default_tab == "upcoming"
    assert result.completed_window_expires_at is None

    upcoming_ids = {g.id for grp in result.upcoming_groups for g in grp.games}
    finished_ids = {g.id for grp in result.finished_groups for g in grp.games}

    # All anchor-week games — finished, live, upcoming — sit in the upcoming tab.
    assert {week_finished_early.id, week_live.id, week_upcoming.id} <= upcoming_ids
    # Anchor-week finished game must NOT also leak into the finished tab.
    assert week_finished_early.id not in finished_ids
    # Previous-week game stays in finished tab.
    assert prev_week_finished.id in finished_ids


@pytest.mark.asyncio
async def test_get_home_widget_active_round_defaults_to_upcoming_after_48h(
    test_session,
    sample_season,
    sample_teams,
):
    """Once the 48h completion window has elapsed, default tab flips to
    upcoming — the user is no longer "fresh from the weekend"."""
    today = date(2026, 5, 6)  # Wed
    now = datetime(2026, 5, 6, 12, 0, tzinfo=ALMATY_TZ)
    restore = _set_clock(now)

    sample_season.frontend_code = "pl"
    sample_season.is_current = True
    sid = sample_season.id

    finished_old = _make_game(season_id=sid, home_team_id=sample_teams[0].id, away_team_id=sample_teams[1].id,
                              game_date=date(2026, 5, 3), game_time=time(15, 0), tour=8,
                              status=GameStatus.finished, home_score=1, away_score=0,
                              finished_at=_utc_from_almaty(datetime(2026, 5, 3, 17, 0, tzinfo=ALMATY_TZ)))
    upcoming_in_week = _make_game(season_id=sid, home_team_id=sample_teams[1].id, away_team_id=sample_teams[2].id,
                                  game_date=date(2026, 5, 9), game_time=time(18, 0), tour=9,
                                  status=GameStatus.created)
    test_session.add_all([finished_old, upcoming_in_week])
    await test_session.commit()

    try:
        result = await get_home_widget(test_session, "pl", "ru")
    finally:
        restore()

    # Now > 48h after the last finished match.
    assert result.completed_window_expires_at is None
    assert result.window_state == "active_round"
    assert result.default_tab == "upcoming"


@pytest.mark.asyncio
async def test_get_home_widget_anchor_week_only_finished_within_48h(
    test_session,
    sample_season,
    sample_teams,
):
    """Anchor week is fully completed and within the 48h window; finished
    tab is default and there are no upcoming games to show."""
    today = date(2026, 5, 4)  # Mon
    now = datetime(2026, 5, 4, 14, 0, tzinfo=ALMATY_TZ)
    restore = _set_clock(now)

    sample_season.frontend_code = "pl"
    sample_season.is_current = True
    sid = sample_season.id

    finished_a = _make_game(season_id=sid, home_team_id=sample_teams[0].id, away_team_id=sample_teams[1].id,
                            game_date=date(2026, 5, 3), game_time=time(15, 0), tour=8,
                            status=GameStatus.finished, home_score=2, away_score=0,
                            finished_at=_utc_from_almaty(datetime(2026, 5, 3, 17, 0, tzinfo=ALMATY_TZ)))
    finished_b = _make_game(season_id=sid, home_team_id=sample_teams[1].id, away_team_id=sample_teams[2].id,
                            game_date=date(2026, 5, 3), game_time=time(20, 0), tour=8,
                            status=GameStatus.finished, home_score=1, away_score=1,
                            finished_at=_utc_from_almaty(datetime(2026, 5, 3, 22, 0, tzinfo=ALMATY_TZ)))
    test_session.add_all([finished_a, finished_b])
    await test_session.commit()

    try:
        result = await get_home_widget(test_session, "pl", "ru")
    finally:
        restore()

    assert result.window_state == "completed_window"
    assert result.default_tab == "finished"
    assert result.upcoming_groups == []
    finished_ids = {g.id for grp in result.finished_groups for g in grp.games}
    assert {finished_a.id, finished_b.id} <= finished_ids


@pytest.mark.asyncio
async def test_get_home_widget_postponed_game_excluded_from_upcoming(
    test_session,
    sample_season,
    sample_teams,
):
    """Postponed/cancelled games are not surfaced in the upcoming tab."""
    today = date(2026, 5, 4)
    now = datetime(2026, 5, 4, 14, 0, tzinfo=ALMATY_TZ)
    restore = _set_clock(now)

    sample_season.frontend_code = "pl"
    sample_season.is_current = True
    sid = sample_season.id

    upcoming = _make_game(season_id=sid, home_team_id=sample_teams[0].id, away_team_id=sample_teams[1].id,
                          game_date=date(2026, 5, 9), game_time=time(18, 0), tour=9,
                          status=GameStatus.created)
    postponed = _make_game(season_id=sid, home_team_id=sample_teams[1].id, away_team_id=sample_teams[2].id,
                           game_date=date(2026, 5, 9), game_time=time(20, 0), tour=9,
                           status=GameStatus.postponed)
    test_session.add_all([upcoming, postponed])
    await test_session.commit()

    try:
        result = await get_home_widget(test_session, "pl", "ru")
    finally:
        restore()

    upcoming_ids = {g.id for grp in result.upcoming_groups for g in grp.games}
    assert upcoming.id in upcoming_ids
    assert postponed.id not in upcoming_ids


@pytest.mark.asyncio
async def test_get_home_widget_orphan_distant_future_does_not_jump_anchor(
    test_session,
    sample_season,
    sample_teams,
):
    """A single far-future fixture (e.g. tentative end-of-season slot) must
    not pull the anchor weeks ahead. The widget anchors on the *nearest*
    upcoming game, so the closer week is shown."""
    today = date(2026, 5, 4)
    now = datetime(2026, 5, 4, 14, 0, tzinfo=ALMATY_TZ)
    restore = _set_clock(now)

    sample_season.frontend_code = "pl"
    sample_season.is_current = True
    sid = sample_season.id

    near_upcoming = _make_game(season_id=sid, home_team_id=sample_teams[0].id, away_team_id=sample_teams[1].id,
                               game_date=date(2026, 5, 9), game_time=time(18, 0), tour=9,
                               status=GameStatus.created)
    far_future = _make_game(season_id=sid, home_team_id=sample_teams[1].id, away_team_id=sample_teams[2].id,
                            game_date=date(2026, 11, 30), game_time=time(20, 0), tour=30,
                            status=GameStatus.created)
    test_session.add_all([near_upcoming, far_future])
    await test_session.commit()

    try:
        result = await get_home_widget(test_session, "pl", "ru")
    finally:
        restore()

    upcoming_ids = {g.id for grp in result.upcoming_groups for g in grp.games}
    assert near_upcoming.id in upcoming_ids
    assert far_future.id not in upcoming_ids
    assert result.selected_round == 9


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
