"""Tests for player-name localization in GET /api/v1/live/events/{game_id}.

The events endpoint stores a denormalized RU `player_name` snapshot but must
resolve names from the linked Player per the requested `lang`, falling back to
the stored string when no player is linked or no localized name exists.
"""
from uuid import uuid4

import pytest

from app.models.game_event import GameEvent, GameEventType
from app.models.player import Player


@pytest.fixture
async def localized_players(test_session) -> dict[str, Player]:
    """Players with RU + KZ names, plus one with no KZ name."""
    scorer = Player(
        sota_id=uuid4(),
        first_name="Иван", last_name="Петров",
        first_name_kz="Иван", last_name_kz="Петровтегі",
    )
    assistant = Player(
        sota_id=uuid4(),
        first_name="Олег", last_name="Сидоров",
        first_name_kz="Олег", last_name_kz="Сидоровтегі",
    )
    ru_only = Player(
        sota_id=uuid4(),
        first_name="Сергей", last_name="Ким",  # no *_kz
    )
    test_session.add_all([scorer, assistant, ru_only])
    await test_session.commit()
    for p in (scorer, assistant, ru_only):
        await test_session.refresh(p)
    return {"scorer": scorer, "assistant": assistant, "ru_only": ru_only}


@pytest.fixture
async def sample_events(test_session, sample_game, localized_players) -> None:
    """A goal with assist (linked players) + a manual event without player_id."""
    scorer = localized_players["scorer"]
    assistant = localized_players["assistant"]
    ru_only = localized_players["ru_only"]

    events = [
        GameEvent(
            game_id=sample_game.id, half=1, minute=12,
            event_type=GameEventType.goal,
            team_id=sample_game.home_team_id, team_name="Astana",
            player_id=scorer.id, player_name="Иван Петров",
            assist_player_id=assistant.id, assist_player_name="Олег Сидоров",
        ),
        GameEvent(
            game_id=sample_game.id, half=2, minute=67,
            event_type=GameEventType.yellow_card,
            team_id=sample_game.home_team_id, team_name="Astana",
            player_id=ru_only.id, player_name="Сергей Ким",
        ),
        # Substitution — player2 (coming on) is same team; player2_team_name set.
        GameEvent(
            game_id=sample_game.id, half=2, minute=70,
            event_type=GameEventType.substitution,
            team_id=sample_game.home_team_id, team_name="Astana",
            player_id=ru_only.id, player_name="Сергей Ким",
            player2_id=scorer.id, player2_name="Иван Петров",
            player2_team_name="Astana",
        ),
        # Manual event with no linked player — must keep stored string.
        GameEvent(
            game_id=sample_game.id, half=2, minute=80,
            event_type=GameEventType.red_card,
            team_id=sample_game.away_team_id, team_name="Kairat",
            player_id=None, player_name="Неизвестный Игрок",
        ),
    ]
    test_session.add_all(events)
    await test_session.commit()


@pytest.fixture
async def kz_team_names(test_session, sample_teams):
    """Give the sample teams KZ names so team_name localization is observable."""
    sample_teams[0].name_kz = "Астана-KZ"   # home
    sample_teams[1].name_kz = "Қайрат"       # away
    await test_session.commit()
    return sample_teams


def _by_minute(events: list[dict]) -> dict[int, dict]:
    return {e["minute"]: e for e in events}


async def test_events_kz_localizes_linked_player(client, sample_game, sample_events):
    resp = await client.get(f"/api/v1/live/events/{sample_game.id}?lang=kz")
    assert resp.status_code == 200
    events = _by_minute(resp.json()["events"])

    # Linked player + assist resolve to KZ names.
    assert events[12]["player_name"] == "Иван Петровтегі"
    assert events[12]["assist_player_name"] == "Олег Сидоровтегі"


async def test_events_ru_uses_russian_names(client, sample_game, sample_events):
    resp = await client.get(f"/api/v1/live/events/{sample_game.id}?lang=ru")
    assert resp.status_code == 200
    events = _by_minute(resp.json()["events"])

    assert events[12]["player_name"] == "Иван Петров"
    assert events[12]["assist_player_name"] == "Олег Сидоров"


async def test_events_kz_falls_back_when_no_kz_name(client, sample_game, sample_events):
    """Player without *_kz keeps the RU name even under lang=kz."""
    resp = await client.get(f"/api/v1/live/events/{sample_game.id}?lang=kz")
    events = _by_minute(resp.json()["events"])
    assert events[67]["player_name"] == "Сергей Ким"


async def test_events_manual_event_keeps_stored_name(client, sample_game, sample_events):
    """Event without player_id keeps the denormalized stored string."""
    resp = await client.get(f"/api/v1/live/events/{sample_game.id}?lang=kz")
    events = _by_minute(resp.json()["events"])
    assert events[80]["player_name"] == "Неизвестный Игрок"


async def test_events_default_lang_is_russian(client, sample_game, sample_events):
    """No lang param defaults to RU (preserves prior behavior)."""
    resp = await client.get(f"/api/v1/live/events/{sample_game.id}")
    events = _by_minute(resp.json()["events"])
    assert events[12]["player_name"] == "Иван Петров"


async def test_events_kz_localizes_team_name(
    client, sample_game, kz_team_names, sample_events
):
    resp = await client.get(f"/api/v1/live/events/{sample_game.id}?lang=kz")
    events = _by_minute(resp.json()["events"])
    assert events[12]["team_name"] == "Астана-KZ"   # home
    assert events[80]["team_name"] == "Қайрат"        # away


async def test_events_kz_localizes_player2_team_name(
    client, sample_game, kz_team_names, sample_events
):
    """Substitution player2_team_name resolves via the same team."""
    resp = await client.get(f"/api/v1/live/events/{sample_game.id}?lang=kz")
    events = _by_minute(resp.json()["events"])
    assert events[70]["player2_team_name"] == "Астана-KZ"


async def test_events_ru_keeps_stored_team_name(
    client, sample_game, kz_team_names, sample_events
):
    resp = await client.get(f"/api/v1/live/events/{sample_game.id}?lang=ru")
    events = _by_minute(resp.json()["events"])
    assert events[12]["team_name"] == "Astana"
