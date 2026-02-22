from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy import select

from app.models import GameEvent, GameEventType, GameLineup, LineupType
from app.services.sync.game_sync import GameSyncService
from app.services.sync_service import SyncService


@pytest.mark.asyncio
class TestSyncServices:
    async def test_game_sync_events_dedupes_by_normalized_name(
        self,
        test_session,
        sample_game,
        sample_teams,
        sample_player,
    ):
        # Existing event stored with player_id.
        existing_event = GameEvent(
            game_id=sample_game.id,
            half=1,
            minute=15,
            event_type=GameEventType.goal,
            team_id=sample_game.home_team_id,
            team_name=sample_teams[0].name,
            player_id=sample_player.id,
            player_name=f"{sample_player.first_name} {sample_player.last_name}",
        )
        test_session.add(existing_event)
        await test_session.commit()

        mock_client = Mock()
        mock_client.get_live_match_events = AsyncMock(
            return_value=[
                {
                    "action": "ГОЛ",
                    "half": 1,
                    "time": 15,
                    "first_name1": sample_player.first_name,
                    "last_name1": sample_player.last_name,
                    "team1": sample_teams[0].name,
                }
            ]
        )

        service = GameSyncService(test_session, mock_client)
        result = await service.sync_game_events(sample_game.id)

        assert result["events_added"] == 0

        all_events = await test_session.execute(
            select(GameEvent).where(GameEvent.game_id == sample_game.id)
        )
        assert len(list(all_events.scalars().all())) == 1

    async def test_sync_live_lineup_positions_does_not_downgrade_starter_on_empty_amplua(
        self,
        test_session,
        sample_game,
        sample_player,
    ):
        lineup = GameLineup(
            game_id=sample_game.id,
            team_id=sample_game.home_team_id,
            player_id=sample_player.id,
            lineup_type=LineupType.starter,
            shirt_number=10,
            is_captain=False,
        )
        test_session.add(lineup)
        await test_session.commit()

        service = SyncService(test_session)
        service.client = Mock()
        service.client.get_live_team_lineup = AsyncMock(
            side_effect=[
                [
                    {"number": "ОСНОВНЫЕ"},
                    {
                        "number": 10,
                        "id": str(sample_player.sota_id),
                        "first_name": sample_player.first_name,
                        "last_name": sample_player.last_name,
                        "amplua": "",
                        "position": "",
                        "capitan": False,
                    },
                ],
                [],
            ]
        )

        await service.sync_live_lineup_positions(sample_game.id)

        refreshed = await test_session.execute(
            select(GameLineup).where(
                GameLineup.game_id == sample_game.id,
                GameLineup.player_id == sample_player.id,
            )
        )
        updated = refreshed.scalar_one()
        assert updated.lineup_type == LineupType.starter

    async def test_sync_service_events_dedupes_by_normalized_name(
        self,
        test_session,
        sample_game,
        sample_teams,
        sample_player,
    ):
        existing_event = GameEvent(
            game_id=sample_game.id,
            half=2,
            minute=55,
            event_type=GameEventType.goal,
            team_id=sample_game.home_team_id,
            team_name=sample_teams[0].name,
            player_id=sample_player.id,
            player_name=f"{sample_player.first_name} {sample_player.last_name}",
        )
        test_session.add(existing_event)
        await test_session.commit()

        service = SyncService(test_session)
        service.client = Mock()
        service.client.get_live_match_events = AsyncMock(
            return_value=[
                {
                    "action": "ГОЛ",
                    "half": 2,
                    "time": 55,
                    "first_name1": sample_player.first_name,
                    "last_name1": sample_player.last_name,
                    "team1": sample_teams[0].name,
                }
            ]
        )

        result = await service.sync_game_events(sample_game.id)
        assert result["events_added"] == 0

        all_events = await test_session.execute(
            select(GameEvent).where(GameEvent.game_id == sample_game.id)
        )
        assert len(list(all_events.scalars().all())) == 1
