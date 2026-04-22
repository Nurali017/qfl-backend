"""Unit tests for app.services.telegram_posts."""
from datetime import date, datetime, time
from unittest.mock import AsyncMock, patch

import pytest

from app.models import Game, GameEvent, Team
from app.models.game import GameStatus
from app.models.game_event import GameEventType
from app.services import telegram_posts as tp


# ---------- Pure helpers ---------- #

def test_surname_picks_last_token():
    assert tp._surname("Ivan Shushenachev") == "Shushenachev"
    assert tp._surname("Shushenachev") == "Shushenachev"
    assert tp._surname("  ") == ""
    assert tp._surname(None) == ""


def test_clock_emoji_full_and_half():
    assert tp._clock_emoji(time(15, 0)) == "🕒"
    assert tp._clock_emoji(time(17, 0)) == "🕔"
    assert tp._clock_emoji(time(19, 0)) == "🕖"
    assert tp._clock_emoji(time(0, 0)) == "🕛"  # midnight = 12
    assert tp._clock_emoji(time(15, 30)) == "🕞"  # 3:30 half
    assert tp._clock_emoji(None) == "🕒"


def test_team_emoji_with_custom_id_renders_tg_emoji_tag():
    team = Team(id=1, name="Kairat", tg_custom_emoji_id="5368324170671202286")
    out = tp._team_emoji(team)
    assert '<tg-emoji emoji-id="5368324170671202286">' in out
    assert "⚽</tg-emoji>" in out


def test_team_emoji_falls_back_without_id():
    team = Team(id=1, name="Kairat")
    assert tp._team_emoji(team) == "⚽"
    assert tp._team_emoji(None) == "⚽"


def test_score_block_uses_kz_names_and_emojis():
    home = Team(id=1, name="Aktobe", name_kz="Ақтөбе")
    away = Team(id=2, name="Caspy", name_kz="Каспий")
    game = Game(
        id=99,
        date=date(2026, 5, 1),
        home_team=home,
        away_team=away,
        home_score=1,
        away_score=0,
    )
    out = tp._score_block(game)
    assert "«Ақтөбе»" in out
    assert "«Каспий»" in out
    assert "1:0" in out


def test_goal_tag_variants():
    ev_goal = GameEvent(event_type=GameEventType.goal, half=1, minute=10)
    assert tp._goal_tag(ev_goal) == ""

    ev_pen = GameEvent(event_type=GameEventType.penalty, half=1, minute=45)
    assert tp._goal_tag(ev_pen) == " (пен.)"

    ev_og = GameEvent(event_type=GameEventType.own_goal, half=2, minute=67)
    assert tp._goal_tag(ev_og) == " (автогол)"

    ev_assist = GameEvent(
        event_type=GameEventType.goal,
        half=1,
        minute=22,
        assist_player_name="Ivan Petrov",
    )
    assert tp._goal_tag(ev_assist) == " (ассист: Petrov)"


def test_scorer_summary_groups_by_scorer():
    events = [
        GameEvent(event_type=GameEventType.goal, half=1, minute=12, player_name="Ivanov"),
        GameEvent(event_type=GameEventType.goal, half=2, minute=67, player_name="Ivanov"),
        GameEvent(event_type=GameEventType.penalty, half=1, minute=45, player_name="Petrov"),
        GameEvent(event_type=GameEventType.red_card, half=2, minute=89, player_name="Smirnov"),
    ]
    out = tp._scorer_summary(events)
    assert "Ivanov 12', 67'" in out
    assert "Petrov 45' (пен.)" in out
    assert "Smirnov" not in out  # red card not included


def test_broadcast_lines_uses_prefix_or_type_fallback():
    from app.models import Broadcaster, GameBroadcaster

    game = Game(id=1, date=date(2026, 5, 1))
    br_yt = Broadcaster(
        id=1, name="KFF League YouTube channel", type="youtube", is_active=True,
        telegram_prefix=None, sort_order=1,
    )
    br_tv = Broadcaster(
        id=2, name="QAZSPORT", type="tv", is_active=True,
        telegram_prefix=None, sort_order=2,
    )
    br_web = Broadcaster(
        id=3, name="Кинопоиск", type="web", is_active=True,
        telegram_prefix="📱", sort_order=3,
    )
    game.broadcasters = [
        GameBroadcaster(id=1, game_id=1, broadcaster_id=1, sort_order=1, broadcaster=br_yt),
        GameBroadcaster(id=2, game_id=1, broadcaster_id=2, sort_order=2, broadcaster=br_tv),
        GameBroadcaster(id=3, game_id=1, broadcaster_id=3, sort_order=3, broadcaster=br_web),
    ]
    out = tp._broadcast_lines(game)
    assert out == [
        "📱KFF League YouTube channel",
        "📺QAZSPORT",
        "📱Кинопоиск",
    ]


def test_broadcast_lines_skips_inactive():
    from app.models import Broadcaster, GameBroadcaster

    game = Game(id=1, date=date(2026, 5, 1))
    br = Broadcaster(
        id=1, name="Dead", type="tv", is_active=False, telegram_prefix=None, sort_order=1,
    )
    game.broadcasters = [GameBroadcaster(id=1, game_id=1, broadcaster_id=1, sort_order=1, broadcaster=br)]
    assert tp._broadcast_lines(game) == []


# ---------- Post functions (with mocked Telegram HTTP) ---------- #

@pytest.fixture
async def sample_public_game(test_session, sample_season, sample_teams) -> Game:
    home, away = sample_teams[0], sample_teams[1]
    home.name_kz = "Астана"
    home.tg_custom_emoji_id = "111"
    away.name_kz = "Қайрат"
    test_session.add_all([home, away])
    game = Game(
        sota_id=None,
        date=date(2026, 5, 1),
        time=time(18, 0),
        tour=6,
        season_id=sample_season.id,
        home_team_id=home.id,
        away_team_id=away.id,
        home_score=1,
        away_score=0,
        status=GameStatus.live,
    )
    test_session.add(game)
    await test_session.commit()
    await test_session.refresh(game)
    return game


@pytest.mark.asyncio
async def test_post_match_start_sets_dedup_flag(test_session, sample_public_game, monkeypatch):
    from app.config import get_settings
    get_settings.cache_clear()
    monkeypatch.setenv("TELEGRAM_MATCH_START_ENABLED", "true")
    get_settings.cache_clear()
    with patch(
        "app.services.telegram_posts.send_public_telegram_message",
        new=AsyncMock(return_value=True),
    ) as send_mock:
        ok = await tp.post_match_start(test_session, sample_public_game.id)
    assert ok is True
    send_mock.assert_awaited_once()
    text = send_mock.await_args.args[0]
    assert "ТІКЕЛЕЙ ЭФИР" in text
    assert "«Астана»" in text and "«Қайрат»" in text
    assert '<tg-emoji emoji-id="111">' in text

    await test_session.refresh(sample_public_game)
    assert sample_public_game.start_telegram_sent_at is not None


@pytest.mark.asyncio
async def test_post_match_start_idempotent(test_session, sample_public_game, monkeypatch):
    from app.config import get_settings
    get_settings.cache_clear()
    monkeypatch.setenv("TELEGRAM_MATCH_START_ENABLED", "true")
    get_settings.cache_clear()
    sample_public_game.start_telegram_sent_at = datetime(2026, 5, 1, 18, 0)
    await test_session.commit()
    with patch(
        "app.services.telegram_posts.send_public_telegram_message",
        new=AsyncMock(return_value=True),
    ) as send_mock:
        ok = await tp.post_match_start(test_session, sample_public_game.id)
    assert ok is False
    send_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_post_match_start_disabled_by_flag(test_session, sample_public_game, monkeypatch):
    """P1.2 — scenario 1 gated by telegram_match_start_enabled flag."""
    from app.config import get_settings
    get_settings.cache_clear()
    monkeypatch.delenv("TELEGRAM_MATCH_START_ENABLED", raising=False)
    get_settings.cache_clear()
    with patch(
        "app.services.telegram_posts.send_public_telegram_message",
        new=AsyncMock(return_value=True),
    ) as send_mock:
        ok = await tp.post_match_start(test_session, sample_public_game.id)
    assert ok is False
    send_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_post_game_event_goal_format(test_session, sample_public_game):
    ev = GameEvent(
        game_id=sample_public_game.id,
        half=1,
        minute=45,
        event_type=GameEventType.penalty,
        player_name="Ivan Shushenachev",
        team_id=sample_public_game.home_team_id,
    )
    test_session.add(ev)
    await test_session.commit()

    with patch(
        "app.services.telegram_posts.send_public_telegram_message",
        new=AsyncMock(return_value=True),
    ) as send_mock:
        ok = await tp.post_game_event(test_session, ev.id)
    assert ok is True
    text = send_mock.await_args.args[0]
    assert "ГООООЛ" in text
    assert "<b>Shushenachev</b> 45' (пен.)" in text
    assert "1:0" in text

    await test_session.refresh(ev)
    assert ev.telegram_sent_at is not None


@pytest.mark.asyncio
async def test_post_game_event_enqueues_goal_video_when_video_already_present(
    test_session,
    sample_public_game,
):
    ev = GameEvent(
        game_id=sample_public_game.id,
        half=1,
        minute=45,
        event_type=GameEventType.goal,
        player_name="Ivan Shushenachev",
        team_id=sample_public_game.home_team_id,
        video_url="goal_videos/1/2-demo.mp4",
    )
    test_session.add(ev)
    await test_session.commit()

    with patch(
        "app.services.telegram_posts.send_public_telegram_message",
        new=AsyncMock(return_value=987),
    ), patch(
        "app.tasks.telegram_tasks.post_goal_video_task.delay",
    ) as delay_mock:
        ok = await tp.post_game_event(test_session, ev.id)

    assert ok is True
    delay_mock.assert_called_once_with(ev.id)


@pytest.mark.asyncio
async def test_post_goal_video_from_file_updates_sent_timestamp(
    test_session,
    sample_public_game,
):
    ev = GameEvent(
        game_id=sample_public_game.id,
        half=1,
        minute=12,
        event_type=GameEventType.goal,
        player_name="Ivan Shushenachev",
        team_id=sample_public_game.home_team_id,
        telegram_message_id=456,
        video_url="goal_videos/1/2-demo.mp4",
    )
    test_session.add(ev)
    await test_session.commit()

    with patch(
        "app.services.telegram_user_client.edit_public_user_message_media",
        new=AsyncMock(return_value=True),
    ) as edit_mock:
        ok = await tp.post_goal_video_from_file(test_session, ev.id, "/tmp/goal.mp4")

    assert ok is True
    assert edit_mock.await_count == 1
    await test_session.refresh(ev)
    assert ev.telegram_video_sent_at is not None


@pytest.mark.asyncio
async def test_post_game_event_red_card_format(test_session, sample_public_game):
    ev = GameEvent(
        game_id=sample_public_game.id,
        half=2,
        minute=76,
        event_type=GameEventType.red_card,
        player_name="Ivan Shushenachev",
        team_id=sample_public_game.home_team_id,
    )
    test_session.add(ev)
    await test_session.commit()

    with patch(
        "app.services.telegram_posts.send_public_telegram_message",
        new=AsyncMock(return_value=True),
    ) as send_mock:
        ok = await tp.post_game_event(test_session, ev.id)
    assert ok is True
    text = send_mock.await_args.args[0]
    assert text.startswith("🟥ҚЫЗЫЛ")
    assert "Shushenachev 76'" in text


@pytest.mark.asyncio
async def test_post_match_finish_includes_scorer_summary(test_session, sample_public_game):
    test_session.add_all([
        GameEvent(
            game_id=sample_public_game.id, half=1, minute=12,
            event_type=GameEventType.goal, player_name="Alice Ivanov",
        ),
        GameEvent(
            game_id=sample_public_game.id, half=2, minute=67,
            event_type=GameEventType.goal, player_name="Alice Ivanov",
        ),
    ])
    await test_session.commit()

    with patch(
        "app.services.telegram_posts.send_public_telegram_message",
        new=AsyncMock(return_value=True),
    ) as send_mock:
        ok = await tp.post_match_finish(test_session, sample_public_game.id)
    assert ok is True
    text = send_mock.await_args.args[0]
    assert "Гол авторлары" in text
    assert "<b>Ivanov</b> 12'" in text
    assert "67'" in text


@pytest.mark.asyncio
async def test_post_match_start_no_send_on_http_failure(test_session, sample_public_game):
    with patch(
        "app.services.telegram_posts.send_public_telegram_message",
        new=AsyncMock(return_value=False),
    ):
        ok = await tp.post_match_start(test_session, sample_public_game.id)
    assert ok is False
    await test_session.refresh(sample_public_game)
    assert sample_public_game.start_telegram_sent_at is None
