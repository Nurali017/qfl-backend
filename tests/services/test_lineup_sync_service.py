from datetime import date, time
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from sqlalchemy import select

from app.models import Game, GameLineup, LineupType, Player
from app.services.sync.lineup_sync import LineupSyncService


@pytest.mark.asyncio
async def test_sync_pre_game_lineup_enriches_live_positions(test_session, sample_game, sample_player):
    service = LineupSyncService(test_session)
    service.client = Mock()

    service.client.get_pre_game_lineup = AsyncMock(
        return_value={
            "home_team": {
                "lineup": [
                    {
                        "id": str(sample_player.sota_id),
                        "first_name": sample_player.first_name,
                        "last_name": sample_player.last_name,
                        "number": 10,
                        "is_captain": False,
                    }
                ]
            },
            "away_team": {
                "lineup": []
            },
        }
    )
    service.client.get_live_team_lineup = AsyncMock(
        side_effect=[
            [
                {"number": "FORMATION", "first_name": "4-2-3-1", "full_name": "#E21A1A"},
                {"number": "ОСНОВНЫЕ"},
                {
                    "number": 10,
                    "id": str(sample_player.sota_id),
                    "first_name": sample_player.first_name,
                    "last_name": sample_player.last_name,
                    "amplua": "DM",
                    "position": "RC",
                    "capitan": False,
                },
            ],
            [{"number": "FORMATION", "first_name": "4-4-2", "full_name": "#1122CC"}],
        ]
    )

    result = await service.sync_pre_game_lineup(sample_game.id)

    assert result["lineups"] == 1
    assert result["positions_updated"] >= 1
    assert result["formations_updated"] >= 1

    lineup_result = await test_session.execute(
        select(GameLineup).where(
            GameLineup.game_id == sample_game.id,
            GameLineup.player_id == sample_player.id,
        )
    )
    lineup = lineup_result.scalar_one()
    assert lineup.lineup_type == LineupType.starter
    assert lineup.amplua == "DM"
    assert lineup.field_position == "RC"

    game_result = await test_session.execute(select(Game).where(Game.id == sample_game.id))
    updated_game = game_result.scalar_one()
    assert updated_game.home_formation == "4-2-3-1"
    assert updated_game.away_formation == "4-4-2"
    assert updated_game.home_kit_color == "#E21A1A"
    assert updated_game.away_kit_color == "#1122CC"


@pytest.mark.asyncio
async def test_sync_pre_game_lineup_does_not_downgrade_starter_on_empty_amplua(
    test_session,
    sample_game,
    sample_player,
):
    service = LineupSyncService(test_session)
    service.client = Mock()

    service.client.get_pre_game_lineup = AsyncMock(
        return_value={
            "home_team": {
                "lineup": [
                    {
                        "id": str(sample_player.sota_id),
                        "first_name": sample_player.first_name,
                        "last_name": sample_player.last_name,
                        "number": 10,
                        "is_captain": False,
                    }
                ]
            },
            "away_team": {
                "lineup": []
            },
        }
    )
    service.client.get_live_team_lineup = AsyncMock(
        side_effect=[
            [
                {"number": "ЗАПАСНЫЕ"},
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

    await service.sync_pre_game_lineup(sample_game.id)

    lineup_result = await test_session.execute(
        select(GameLineup).where(
            GameLineup.game_id == sample_game.id,
            GameLineup.player_id == sample_player.id,
        )
    )
    lineup = lineup_result.scalar_one()
    assert lineup.lineup_type == LineupType.starter


@pytest.mark.asyncio
async def test_sync_pre_game_lineup_fallbacks_to_player_id_when_shirt_number_mismatch(
    test_session,
    sample_game,
    sample_player,
):
    service = LineupSyncService(test_session)
    service.client = Mock()

    service.client.get_pre_game_lineup = AsyncMock(
        return_value={
            "home_team": {
                "lineup": [
                    {
                        "id": str(sample_player.sota_id),
                        "first_name": sample_player.first_name,
                        "last_name": sample_player.last_name,
                        "number": 99,
                        "is_captain": False,
                    }
                ]
            },
            "away_team": {
                "lineup": []
            },
        }
    )
    service.client.get_live_team_lineup = AsyncMock(
        side_effect=[
            [
                {"number": "ОСНОВНЫЕ"},
                {
                    # Deliberately different shirt number to force fallback by player_id.
                    "number": 10,
                    "id": str(sample_player.sota_id),
                    "first_name": sample_player.first_name,
                    "last_name": sample_player.last_name,
                    "amplua": "AM",
                    "position": "C",
                    "capitan": False,
                },
            ],
            [],
        ]
    )

    await service.sync_pre_game_lineup(sample_game.id)

    lineup_result = await test_session.execute(
        select(GameLineup).where(
            GameLineup.game_id == sample_game.id,
            GameLineup.player_id == sample_player.id,
        )
    )
    lineup = lineup_result.scalar_one()
    assert lineup.shirt_number == 99
    assert lineup.amplua == "AM"
    assert lineup.field_position == "C"


@pytest.mark.asyncio
async def test_sync_live_positions_and_kits_validates_amplua_position_and_kit_color(
    test_session,
    sample_game,
    sample_player,
):
    service = LineupSyncService(test_session)
    service.client = Mock()

    test_session.add(
        GameLineup(
            game_id=sample_game.id,
            team_id=sample_game.home_team_id,
            player_id=sample_player.id,
            lineup_type=LineupType.starter,
            shirt_number=10,
        )
    )
    await test_session.commit()

    service.client.get_live_team_lineup = AsyncMock(
        side_effect=[
            [
                {"number": "FORMATION", "first_name": "4-3-3", "full_name": "#11aa22"},
                {"number": "ОСНОВНЫЕ"},
                {
                    "number": 10,
                    "id": str(sample_player.sota_id),
                    "amplua": "BAD",
                    "position": "XX",
                },
            ],
            [{"number": "FORMATION", "first_name": "4-4-2", "full_name": "invalid"}],
        ]
    )

    result = await service.sync_live_positions_and_kits(
        sample_game.id,
        mode="live_read",
    )

    assert result["positions_updated"] == 0
    assert result["kit_colors_updated"] == 1

    lineup_result = await test_session.execute(
        select(GameLineup).where(
            GameLineup.game_id == sample_game.id,
            GameLineup.player_id == sample_player.id,
        )
    )
    lineup = lineup_result.scalar_one()
    assert lineup.amplua is None
    assert lineup.field_position is None

    game_result = await test_session.execute(select(Game).where(Game.id == sample_game.id))
    updated_game = game_result.scalar_one()
    assert updated_game.home_kit_color == "#11AA22"
    assert updated_game.away_kit_color is None


@pytest.mark.asyncio
async def test_finished_repair_demotes_stale_starters_not_present_in_sota_starter_section(
    test_session,
    sample_game,
    sample_player,
):
    service = LineupSyncService(test_session)
    service.client = Mock()

    stale_player = Player(sota_id=uuid4(), first_name="Stale", last_name="Starter")
    test_session.add(stale_player)
    await test_session.commit()
    await test_session.refresh(stale_player)

    test_session.add_all(
        [
            GameLineup(
                game_id=sample_game.id,
                team_id=sample_game.home_team_id,
                player_id=sample_player.id,
                lineup_type=LineupType.starter,
                shirt_number=10,
            ),
            GameLineup(
                game_id=sample_game.id,
                team_id=sample_game.home_team_id,
                player_id=stale_player.id,
                lineup_type=LineupType.starter,
                shirt_number=99,
            ),
        ]
    )
    await test_session.commit()

    service.client.get_live_team_lineup = AsyncMock(
        side_effect=[
            [
                {"number": "FORMATION", "first_name": "4-3-3", "full_name": "#111111"},
                {"number": "ОСНОВНЫЕ"},
                {
                    "number": 10,
                    "id": str(sample_player.sota_id),
                    "amplua": "DM",
                    "position": "C",
                },
                {"number": "ЗАПАСНЫЕ"},
                {"number": 99, "id": str(stale_player.sota_id), "amplua": "", "position": ""},
            ],
            [{"number": "FORMATION", "first_name": "4-4-2", "full_name": "#222222"}],
        ]
    )

    result = await service.sync_live_positions_and_kits(
        sample_game.id,
        mode="finished_repair",
    )

    assert result["status"] == "updated"

    home_lineups = (
        await test_session.execute(
            select(GameLineup).where(
                GameLineup.game_id == sample_game.id,
                GameLineup.team_id == sample_game.home_team_id,
            )
        )
    ).scalars().all()
    by_player_id = {row.player_id: row for row in home_lineups}

    assert by_player_id[sample_player.id].lineup_type == LineupType.starter
    assert by_player_id[sample_player.id].amplua == "DM"
    assert by_player_id[sample_player.id].field_position == "C"
    assert by_player_id[stale_player.id].lineup_type == LineupType.substitute


@pytest.mark.asyncio
async def test_backfill_finished_games_positions_and_kits_only_processes_finished_matches(
    test_session,
    sample_game,
    sample_player,
    sample_teams,
    sample_season,
):
    service = LineupSyncService(test_session)
    service.client = Mock()

    upcoming_game = Game(
        sota_id=uuid4(),
        date=date(2028, 1, 1),
        time=time(18, 0),
        season_id=sample_season.id,
        home_team_id=sample_teams[0].id,
        away_team_id=sample_teams[1].id,
        is_live=False,
        home_score=None,
        away_score=None,
    )
    test_session.add(upcoming_game)
    await test_session.commit()

    test_session.add(
        GameLineup(
            game_id=sample_game.id,
            team_id=sample_game.home_team_id,
            player_id=sample_player.id,
            lineup_type=LineupType.starter,
            shirt_number=10,
        )
    )
    await test_session.commit()

    service.client.get_live_team_lineup = AsyncMock(
        side_effect=[
            [
                {"number": "FORMATION", "first_name": "4-3-3", "full_name": "#123456"},
                {"number": "ОСНОВНЫЕ"},
                {
                    "number": 10,
                    "id": str(sample_player.sota_id),
                    "amplua": "DM",
                    "position": "RC",
                },
            ],
            [{"number": "FORMATION", "first_name": "4-4-2", "full_name": "#654321"}],
        ]
    )

    result = await service.backfill_finished_games_positions_and_kits(
        batch_size=10,
    )

    assert result["processed"] == 1
    assert result["updated_games"] == 1
    assert result["positions_updated"] >= 1
    assert service.client.get_live_team_lineup.await_count == 2


@pytest.mark.asyncio
async def test_backfill_finished_games_positions_and_kits_skips_when_one_side_missing(
    test_session,
    sample_game,
    sample_player,
):
    service = LineupSyncService(test_session)
    service.client = Mock()

    test_session.add(
        GameLineup(
            game_id=sample_game.id,
            team_id=sample_game.home_team_id,
            player_id=sample_player.id,
            lineup_type=LineupType.starter,
            shirt_number=10,
        )
    )
    await test_session.commit()

    service.client.get_live_team_lineup = AsyncMock(
        side_effect=[
            [{"number": "FORMATION", "first_name": "4-3-3", "full_name": "#123456"}],
            Exception("away unavailable"),
        ]
    )

    result = await service.backfill_finished_games_positions_and_kits(
        game_ids=[sample_game.id],
        batch_size=10,
    )

    assert result["processed"] == 1
    assert result["updated_games"] == 0
    assert len(result["failed_games"]) == 1
    assert result["failed_games"][0]["reason"] == "skipped_missing_side"
