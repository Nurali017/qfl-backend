from datetime import date
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from sqlalchemy import select

from app.models import Game, GameEvent, GameLineup, LineupType, Player, Team
from app.services.live_sync_service import LiveSyncService


def _build_service() -> LiveSyncService:
    return LiveSyncService(db=Mock(), client=Mock())


def test_match_team_id_matches_name_kz():
    game = Game(
        sota_id=uuid4(),
        date=date(2025, 10, 24),
        home_team_id=91,
        away_team_id=649,
    )
    game.home_team = Team(id=91, name="Altai", name_kz="Алтай Өскемен")
    game.away_team = Team(id=649, name="Ontustik", name_kz="Оңтүстік")

    service = _build_service()
    assert service._match_team_id(game, "Алтай Өскемен") == 91


def test_match_team_id_handles_safe_alias_case():
    game = Game(
        sota_id=uuid4(),
        date=date(2025, 10, 24),
        home_team_id=17,
        away_team_id=51,
    )
    game.home_team = Team(id=17, name="Zhenis", name_kz="Жеңіс Ә")
    game.away_team = Team(id=51, name="Kairat", name_kz="Қайрат")

    service = _build_service()
    assert service._match_team_id(game, "Жеңіс") == 17


def test_match_team_id_returns_none_on_ambiguous_alias():
    game = Game(
        sota_id=uuid4(),
        date=date(2025, 10, 24),
        home_team_id=17,
        away_team_id=18,
    )
    game.home_team = Team(id=17, name="Zhenis A", name_kz="Жеңіс Ә")
    game.away_team = Team(id=18, name="Zhenis B", name_kz="Жеңіс Б")

    service = _build_service()
    assert service._match_team_id(game, "Жеңіс") is None


@pytest.mark.asyncio
async def test_sync_live_events_falls_back_to_player_lineup_team(
    test_session,
    sample_game,
    sample_player,
):
    lineup = GameLineup(
        game_id=sample_game.id,
        team_id=sample_game.home_team_id,
        player_id=sample_player.id,
        lineup_type=LineupType.starter,
    )
    test_session.add(lineup)
    await test_session.commit()

    mock_client = Mock()
    mock_client.get_live_match_events = AsyncMock(
        return_value=[
            {
                "action": "ГОЛ",
                "half": 1,
                "time": 42,
                "first_name1": sample_player.first_name,
                "last_name1": sample_player.last_name,
                "team1": "",
            }
        ]
    )

    service = LiveSyncService(test_session, mock_client)
    events = await service.sync_live_events(sample_game.id)

    assert len(events) == 1
    assert events[0].team_id == sample_game.home_team_id


@pytest.mark.asyncio
async def test_sync_live_events_keeps_team_id_none_when_unresolved(
    test_session,
    sample_game,
):
    same_name_home = Player(
        first_name="Ivan",
        last_name="Ivanov",
        birthday=date(1998, 1, 1),
    )
    same_name_away = Player(
        first_name="Ivan",
        last_name="Ivanov",
        birthday=date(1999, 1, 1),
    )
    test_session.add_all([same_name_home, same_name_away])
    await test_session.flush()

    test_session.add_all(
        [
            GameLineup(
                game_id=sample_game.id,
                team_id=sample_game.home_team_id,
                player_id=same_name_home.id,
                lineup_type=LineupType.starter,
            ),
            GameLineup(
                game_id=sample_game.id,
                team_id=sample_game.away_team_id,
                player_id=same_name_away.id,
                lineup_type=LineupType.starter,
            ),
        ]
    )
    await test_session.commit()

    mock_client = Mock()
    mock_client.get_live_match_events = AsyncMock(
        return_value=[
            {
                "action": "ЖК",
                "half": 1,
                "time": 15,
                "first_name1": "Ivan",
                "last_name1": "Ivanov",
                "team1": "",
            }
        ]
    )

    service = LiveSyncService(test_session, mock_client)
    await service.sync_live_events(sample_game.id)

    result = await test_session.execute(
        select(GameEvent).where(GameEvent.game_id == sample_game.id)
    )
    event = result.scalars().one()
    assert event.team_id is None
