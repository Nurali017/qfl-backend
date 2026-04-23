from datetime import date, time
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select

from app.models import (
    Championship,
    Game,
    Season,
    SeasonParticipant,
    Team,
    TelegramDailyResultPost,
)
from app.models.game import GameStatus
from app.services.telegram_posts import (
    build_daily_results_digest_payload,
    build_daily_results_digest_text,
    find_ready_daily_results_payloads,
    post_daily_results_digest,
)


async def _create_championship(
    test_session,
    championship_id: int,
    *,
    name_kz: str,
    short_name_kz: str | None = None,
) -> Championship:
    championship = Championship(
        id=championship_id,
        name=name_kz,
        name_kz=name_kz,
        short_name=short_name_kz or name_kz,
        short_name_kz=short_name_kz or name_kz,
    )
    test_session.add(championship)
    await test_session.commit()
    await test_session.refresh(championship)
    return championship


async def _create_season(
    test_session,
    season_id: int,
    championship: Championship,
    *,
    name_kz: str,
    frontend_code: str | None = None,
    tg_custom_emoji_id: str | None = None,
) -> Season:
    season = Season(
        id=season_id,
        name=name_kz,
        name_kz=name_kz,
        championship_id=championship.id,
        frontend_code=frontend_code,
        tg_custom_emoji_id=tg_custom_emoji_id,
        is_visible=True,
    )
    test_session.add(season)
    await test_session.commit()
    await test_session.refresh(season)
    return season


async def _create_team(
    test_session,
    team_id: int,
    *,
    name_kz: str,
    tg_custom_emoji_id: str | None = None,
) -> Team:
    team = Team(
        id=team_id,
        name=name_kz,
        name_kz=name_kz,
        tg_custom_emoji_id=tg_custom_emoji_id,
    )
    test_session.add(team)
    await test_session.commit()
    await test_session.refresh(team)
    return team


async def _create_game(
    test_session,
    *,
    game_id: int,
    season_id: int,
    match_date: date,
    home_team_id: int,
    away_team_id: int,
    status: GameStatus,
    tour: int | None = 3,
    home_score: int | None = 1,
    away_score: int | None = 0,
    kickoff: time | None = time(15, 0),
) -> Game:
    game = Game(
        id=game_id,
        date=match_date,
        time=kickoff,
        season_id=season_id,
        home_team_id=home_team_id,
        away_team_id=away_team_id,
        status=status,
        tour=tour,
        home_score=home_score,
        away_score=away_score,
    )
    test_session.add(game)
    await test_session.commit()
    await test_session.refresh(game)
    return game


@pytest.mark.asyncio
async def test_daily_results_payload_not_ready_while_any_game_is_live(test_session):
    championship = await _create_championship(test_session, 301, name_kz="Премьер-Лига")
    season = await _create_season(
        test_session,
        401,
        championship,
        name_kz="Премьер-Лига 2026",
        frontend_code="pl",
    )
    home = await _create_team(test_session, 1001, name_kz="Астана")
    away = await _create_team(test_session, 1002, name_kz="Қайрат")
    third = await _create_team(test_session, 1003, name_kz="Тобыл")

    match_date = date(2026, 4, 21)
    await _create_game(
        test_session,
        game_id=9001,
        season_id=season.id,
        match_date=match_date,
        home_team_id=home.id,
        away_team_id=away.id,
        status=GameStatus.finished,
        home_score=2,
        away_score=1,
    )
    await _create_game(
        test_session,
        game_id=9002,
        season_id=season.id,
        match_date=match_date,
        home_team_id=third.id,
        away_team_id=home.id,
        status=GameStatus.live,
        home_score=1,
        away_score=1,
    )

    payload = await build_daily_results_digest_payload(test_session, season.id, match_date)
    assert payload is None


@pytest.mark.asyncio
async def test_daily_results_payload_ready_when_all_games_terminal(test_session):
    championship = await _create_championship(test_session, 302, name_kz="Екінші лига")
    season = await _create_season(
        test_session,
        402,
        championship,
        name_kz="Екінші лига 2026",
        frontend_code="2l",
    )
    home = await _create_team(test_session, 1011, name_kz="Жас Қыран")
    away = await _create_team(
        test_session,
        1012,
        name_kz="Талас",
        tg_custom_emoji_id="1111111111111111111",
    )

    match_date = date(2026, 4, 21)
    await _create_game(
        test_session,
        game_id=9011,
        season_id=season.id,
        match_date=match_date,
        home_team_id=home.id,
        away_team_id=away.id,
        status=GameStatus.finished,
        home_score=3,
        away_score=0,
    )

    payload = await build_daily_results_digest_payload(test_session, season.id, match_date)

    assert payload is not None
    assert payload.tour == 3
    assert "3-тур" in payload.headline
    assert payload.game_count == 1
    assert payload.sections[0].games[0].home_team_name == "Жас Қыран"
    assert payload.sections[0].games[0].home_team_emoji == ""
    assert payload.sections[0].games[0].away_team_emoji == (
        '<tg-emoji emoji-id="1111111111111111111">⚽</tg-emoji>'
    )
    assert payload.sections[0].games[0].home_score == 3
    assert payload.sections[0].label is None


@pytest.mark.asyncio
async def test_daily_results_grouped_sections_use_generic_labels(test_session):
    championship = await _create_championship(test_session, 303, name_kz="Әйелдер Лигасы")
    season = await _create_season(
        test_session,
        405,
        championship,
        name_kz="Әйелдер Лигасы 2026",
        frontend_code="el",
    )
    teams = [
        await _create_team(test_session, 1101, name_kz="Астана Ж"),
        await _create_team(test_session, 1102, name_kz="Қайрат Ж"),
        await _create_team(test_session, 1103, name_kz="Елімай Ж"),
        await _create_team(test_session, 1104, name_kz="Жеңіс Ж"),
    ]
    test_session.add_all(
        [
            SeasonParticipant(team_id=teams[0].id, season_id=season.id, group_name="A"),
            SeasonParticipant(team_id=teams[1].id, season_id=season.id, group_name="A"),
            SeasonParticipant(team_id=teams[2].id, season_id=season.id, group_name="B"),
            SeasonParticipant(team_id=teams[3].id, season_id=season.id, group_name="B"),
        ]
    )
    await test_session.commit()

    match_date = date(2026, 4, 22)
    await _create_game(
        test_session,
        game_id=9021,
        season_id=season.id,
        match_date=match_date,
        home_team_id=teams[0].id,
        away_team_id=teams[1].id,
        status=GameStatus.finished,
        home_score=2,
        away_score=1,
    )
    await _create_game(
        test_session,
        game_id=9022,
        season_id=season.id,
        match_date=match_date,
        home_team_id=teams[2].id,
        away_team_id=teams[3].id,
        status=GameStatus.finished,
        home_score=1,
        away_score=1,
    )

    payload = await build_daily_results_digest_payload(test_session, season.id, match_date)

    assert payload is not None
    assert [section.label for section in payload.sections] == [
        "A конференциясы",
        "B конференциясы",
    ]


@pytest.mark.asyncio
async def test_daily_results_second_league_2026_uses_special_group_labels(test_session):
    championship = await _create_championship(test_session, 304, name_kz="Екінші лига")
    season = await _create_season(
        test_session,
        203,
        championship,
        name_kz="Екінші Лига 2026",
        frontend_code="2l",
    )
    teams = [
        await _create_team(test_session, 1201, name_kz="Тұран М"),
        await _create_team(test_session, 1202, name_kz="Қаршыға"),
        await _create_team(test_session, 1203, name_kz="Ұлытау Ж"),
        await _create_team(test_session, 1204, name_kz="Алтай Ж"),
    ]
    test_session.add_all(
        [
            SeasonParticipant(team_id=teams[0].id, season_id=season.id, group_name="A"),
            SeasonParticipant(team_id=teams[1].id, season_id=season.id, group_name="A"),
            SeasonParticipant(team_id=teams[2].id, season_id=season.id, group_name="B"),
            SeasonParticipant(team_id=teams[3].id, season_id=season.id, group_name="B"),
        ]
    )
    await test_session.commit()

    match_date = date(2026, 4, 22)
    await _create_game(
        test_session,
        game_id=9031,
        season_id=season.id,
        match_date=match_date,
        home_team_id=teams[0].id,
        away_team_id=teams[1].id,
        status=GameStatus.finished,
        home_score=3,
        away_score=1,
    )
    await _create_game(
        test_session,
        game_id=9032,
        season_id=season.id,
        match_date=match_date,
        home_team_id=teams[2].id,
        away_team_id=teams[3].id,
        status=GameStatus.finished,
        home_score=0,
        away_score=2,
    )

    payload = await build_daily_results_digest_payload(test_session, season.id, match_date)

    assert payload is not None
    assert [section.label for section in payload.sections] == [
        "Оңтүстік-Батыс конференциясы",
        "Солтүстік-Шығыс конференциясы",
    ]


@pytest.mark.asyncio
async def test_find_ready_daily_results_payloads_returns_separate_tournaments(test_session):
    championship_one = await _create_championship(test_session, 305, name_kz="Премьер-Лига")
    championship_two = await _create_championship(test_session, 306, name_kz="Бірінші Лига")
    season_one = await _create_season(
        test_session,
        410,
        championship_one,
        name_kz="Премьер-Лига 2026",
        frontend_code="pl",
    )
    season_two = await _create_season(
        test_session,
        411,
        championship_two,
        name_kz="Бірінші Лига 2026",
        frontend_code="1l",
    )
    teams = [
        await _create_team(test_session, 1301, name_kz="Астана"),
        await _create_team(test_session, 1302, name_kz="Қайрат"),
        await _create_team(test_session, 1303, name_kz="Тараз"),
        await _create_team(test_session, 1304, name_kz="Жайық"),
    ]

    match_date = date(2026, 4, 23)
    await _create_game(
        test_session,
        game_id=9041,
        season_id=season_one.id,
        match_date=match_date,
        home_team_id=teams[0].id,
        away_team_id=teams[1].id,
        status=GameStatus.finished,
        home_score=2,
        away_score=0,
    )
    await _create_game(
        test_session,
        game_id=9042,
        season_id=season_two.id,
        match_date=match_date,
        home_team_id=teams[2].id,
        away_team_id=teams[3].id,
        status=GameStatus.finished,
        home_score=1,
        away_score=1,
    )

    payloads = await find_ready_daily_results_payloads(
        test_session,
        locale="kz",
        date_from=match_date,
        date_to=match_date,
    )

    assert {(payload.season_id, payload.for_date) for payload in payloads} == {
        (season_one.id, match_date),
        (season_two.id, match_date),
    }


@pytest.mark.asyncio
async def test_post_daily_results_digest_is_idempotent(test_session):
    championship = await _create_championship(test_session, 307, name_kz="Премьер-Лига")
    season = await _create_season(
        test_session,
        420,
        championship,
        name_kz="Премьер-Лига 2026",
        frontend_code="pl",
        tg_custom_emoji_id="4444444444444444444",
    )
    home = await _create_team(
        test_session,
        1401,
        name_kz="Астана",
        tg_custom_emoji_id="2222222222222222222",
    )
    away = await _create_team(
        test_session,
        1402,
        name_kz="Қайрат",
        tg_custom_emoji_id="3333333333333333333",
    )
    second_home = await _create_team(test_session, 1403, name_kz="Тобыл")
    second_away = await _create_team(test_session, 1404, name_kz="Ордабасы")
    match_date = date(2026, 4, 24)
    await _create_game(
        test_session,
        game_id=9051,
        season_id=season.id,
        match_date=match_date,
        home_team_id=home.id,
        away_team_id=away.id,
        status=GameStatus.finished,
        home_score=1,
        away_score=0,
    )
    await _create_game(
        test_session,
        game_id=9052,
        season_id=season.id,
        match_date=match_date,
        home_team_id=second_home.id,
        away_team_id=second_away.id,
        status=GameStatus.finished,
        home_score=2,
        away_score=2,
    )

    payload = await build_daily_results_digest_payload(test_session, season.id, match_date)
    assert payload is not None
    text = build_daily_results_digest_text(payload)
    assert "<b>KFF LEAGUE</b>" in text
    assert (
        '<tg-emoji emoji-id="4444444444444444444">🏆</tg-emoji> '
        "<b>Премьер-Лига. 3-турда өткен матчтардың нәтижесі</b>"
    ) in text
    assert "⚡ <b>24 сәуір:</b>" in text
    first_match_line = (
        '<tg-emoji emoji-id="2222222222222222222">⚽</tg-emoji> '
        'Астана 1:0 Қайрат '
        '<tg-emoji emoji-id="3333333333333333333">⚽</tg-emoji>'
    )
    second_match_line = "Тобыл 2:2 Ордабасы"
    assert first_match_line in text
    assert second_match_line in text
    assert f"{first_match_line}\n\n{second_match_line}" in text

    with patch(
        "app.services.telegram_posts.send_public_telegram_message",
        new=AsyncMock(return_value=777),
    ) as send_mock:
        first = await post_daily_results_digest(test_session, season.id, match_date)
        second = await post_daily_results_digest(test_session, season.id, match_date)

    assert first is True
    assert second is False
    assert send_mock.await_count == 1

    rows = (
        await test_session.execute(select(TelegramDailyResultPost).order_by(TelegramDailyResultPost.id))
    ).scalars().all()
    assert len(rows) == 1
    assert rows[0].season_id == season.id
    assert rows[0].for_date == match_date
    assert rows[0].message_id == 777
