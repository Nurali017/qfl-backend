from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import select

from app.models import Player, PlayerSeasonStats, PlayerTeam, Season, Team, Tournament
from app.services.sync.player_sync import PlayerSyncService


@pytest.mark.asyncio
async def test_sync_players_upserts_by_sota_id(test_session):
    sota_id = str(uuid4())

    client = SimpleNamespace()
    client.get_players = AsyncMock(
        side_effect=[
            # RU
            [
                {
                    "id": sota_id,
                    "first_name": "Ivan",
                    "last_name": "Petrov",
                    "birthday": "1999-01-01",
                    "type": "midfielder",
                    "country_name": None,
                    "photo": None,
                    "age": 27,
                    "top_role": "CM",
                }
            ],
            # KZ
            [{"id": sota_id, "first_name": "Иван", "last_name": "Петров"}],
            # EN
            [{"id": sota_id, "first_name": "Ivan", "last_name": "Petrov", "top_role": "CM"}],
        ]
    )

    service = PlayerSyncService(test_session, client)
    synced = await service.sync_players(season_id=61)
    assert synced == 1

    players_result = await test_session.execute(select(Player))
    players = players_result.scalars().all()
    assert len(players) == 1
    assert str(players[0].sota_id) == sota_id
    assert players[0].first_name == "Ivan"

    # second run with changed names must update the same record (no duplicate by internal id)
    client.get_players = AsyncMock(
        side_effect=[
            [{"id": sota_id, "first_name": "Ivan-Updated", "last_name": "Petrov", "top_role": "DM"}],
            [{"id": sota_id, "first_name": "Иван", "last_name": "Петров"}],
            [{"id": sota_id, "first_name": "Ivan", "last_name": "Petrov", "top_role": "DM"}],
        ]
    )
    synced_second = await service.sync_players(season_id=61)
    assert synced_second == 1

    test_session.expire_all()
    refreshed_result = await test_session.execute(select(Player))
    refreshed_players = refreshed_result.scalars().all()
    assert len(refreshed_players) == 1
    assert refreshed_players[0].first_name == "Ivan-Updated"
    assert refreshed_players[0].top_role == "DM"


@pytest.mark.asyncio
async def test_sync_player_season_stats_skips_players_without_sota_id(test_session):
    tournament = Tournament(id=1, name="Test League")
    season = Season(
        id=61,
        name="2025",
        tournament_id=tournament.id,
        date_start=date(2025, 1, 1),
        date_end=date(2025, 12, 31),
    )
    team = Team(id=91, name="Astana")
    linked_player = Player(sota_id=uuid4(), first_name="Linked", last_name="Player")
    manual_player = Player(sota_id=None, first_name="Manual", last_name="Player")

    test_session.add_all([tournament, season, team, linked_player, manual_player])
    await test_session.flush()

    test_session.add_all(
        [
            PlayerTeam(player_id=linked_player.id, team_id=team.id, season_id=season.id, number=10),
            PlayerTeam(player_id=manual_player.id, team_id=team.id, season_id=season.id, number=11),
        ]
    )
    await test_session.commit()

    stats_payload = {
        "games_played": 1,
        "games_starting": 1,
        "games_as_subst": 0,
        "games_be_subst": 0,
        "games_unused": 0,
        "time_on_field_total": 90,
        "goal": 1,
        "goal_pass": 0,
        "goal_and_assist": 1,
        "goal_out_box": 0,
        "owngoal": 0,
        "penalty_success": 0,
        "xg": 0.4,
        "xg_per_90": 0.4,
        "shot": 2,
        "shots_on_goal": 1,
        "shots_blocked_opponent": 0,
        "pass": 40,
        "pass_ratio": 90,
        "pass_acc": 36,
        "key_pass": 1,
        "pass_forward": 20,
        "pass_forward_ratio": 80,
        "pass_progressive": 5,
        "pass_cross": 0,
        "pass_cross_acc": 0,
        "pass_cross_ratio": 0,
        "pass_cross_per_90": 0,
        "pass_to_box": 2,
        "pass_to_box_ratio": 50,
        "pass_to_3rd": 4,
        "pass_to_3rd_ratio": 60,
        "duel": 10,
        "duel_success": 6,
        "aerial_duel": 2,
        "aerial_duel_success": 1,
        "ground_duel": 8,
        "ground_duel_success": 5,
        "tackle": 2,
        "tackle_per_90": 2,
        "interception": 1,
        "recovery": 3,
        "dribble": 2,
        "dribble_success": 1,
        "dribble_per_90": 2,
        "corner": 0,
        "offside": 0,
        "foul": 1,
        "foul_taken": 1,
        "yellow_cards": 0,
        "second_yellow_cards": 0,
        "red_cards": 0,
        "goals_conceded": 0,
        "goals_conceded_penalty": 0,
        "goals_conceeded_per_90": 0,
        "save_shot": 0,
        "save_shot_ratio": 0,
        "saved_shot_per_90": 0,
        "save_shot_penalty": 0,
        "save_shot_penalty_success": 0,
        "dry_match": 0,
        "exit": 0,
        "exit_success": 0,
    }

    client = SimpleNamespace()
    client.get_player_season_stats = AsyncMock(return_value=stats_payload)

    service = PlayerSyncService(test_session, client)
    synced = await service.sync_player_season_stats(season_id=season.id)
    assert synced == 1

    client.get_player_season_stats.assert_awaited_once_with(str(linked_player.sota_id), season.id)

    stats_result = await test_session.execute(select(PlayerSeasonStats))
    stats_rows = stats_result.scalars().all()
    assert len(stats_rows) == 1
    assert stats_rows[0].player_id == linked_player.id
