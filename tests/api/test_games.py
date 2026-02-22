import pytest
from httpx import AsyncClient
from uuid import uuid4
from datetime import datetime

from app.models import GameLineup, LineupType, Player
from app.main import app
from app.services.sota_client import get_sota_client
from unittest.mock import AsyncMock, Mock


@pytest.mark.asyncio
class TestGamesAPI:
    """Tests for /api/v1/games endpoints."""

    async def test_get_games_empty(self, client: AsyncClient):
        """Test getting games when database is empty."""
        response = await client.get("/api/v1/games")
        assert response.status_code == 200
        data = response.json()
        assert data["items"] == []
        assert data["total"] == 0

    async def test_get_games_with_data(
        self, client: AsyncClient, sample_season, sample_game
    ):
        """Test getting all games."""
        response = await client.get("/api/v1/games")
        assert response.status_code == 200
        data = response.json()
        assert len(data["items"]) == 1
        assert data["total"] == 1

    async def test_get_games_filter_by_season(
        self, client: AsyncClient, sample_season, sample_game
    ):
        """Test filtering games by season."""
        response = await client.get("/api/v1/games?season_id=61")
        assert response.status_code == 200
        data = response.json()
        assert len(data["items"]) == 1

        response = await client.get("/api/v1/games?season_id=999")
        assert response.status_code == 200
        data = response.json()
        assert len(data["items"]) == 0

    async def test_get_game_by_id(self, client: AsyncClient, sample_game):
        """Test getting game by int ID."""
        response = await client.get(f"/api/v1/games/{sample_game.id}")
        assert response.status_code == 200
        data = response.json()
        assert data["home_score"] == 2
        assert data["away_score"] == 1
        assert data["protocol_url"] is None

    async def test_get_game_by_id_includes_protocol_url(
        self,
        client: AsyncClient,
        test_session,
        sample_game,
    ):
        """Test game detail returns protocol_url when present."""
        sample_game.protocol_url = "document/match_protocols/test-game.pdf"
        await test_session.commit()

        response = await client.get(f"/api/v1/games/{sample_game.id}")
        assert response.status_code == 200

        data = response.json()
        assert data["protocol_url"] == "document/match_protocols/test-game.pdf"

    async def test_get_game_not_found(self, client: AsyncClient):
        """Test 404 for non-existent game."""
        response = await client.get("/api/v1/games/999999")
        assert response.status_code == 404
        assert response.json()["detail"] == "Game not found"

    async def test_get_game_stats(self, client: AsyncClient, sample_game):
        """Test getting game statistics."""
        response = await client.get(f"/api/v1/games/{sample_game.id}/stats")
        assert response.status_code == 200
        data = response.json()
        assert data["game_id"] == sample_game.id
        assert data["team_stats"] == []
        assert data["player_stats"] == []

    async def test_get_games_list_includes_protocol_url(
        self,
        client: AsyncClient,
        test_session,
        sample_season,
        sample_game,
    ):
        """Test games list returns protocol_url field."""
        sample_game.protocol_url = "document/match_protocols/list-protocol.pdf"
        await test_session.commit()

        response = await client.get("/api/v1/games")
        assert response.status_code == 200
        data = response.json()
        assert len(data["items"]) == 1
        assert (
            data["items"][0]["protocol_url"]
            == "document/match_protocols/list-protocol.pdf"
        )

    async def test_get_games_grouped_includes_protocol_url(
        self,
        client: AsyncClient,
        test_session,
        sample_season,
        sample_game,
    ):
        """Test grouped games format includes protocol_url field."""
        sample_game.protocol_url = "document/match_protocols/group-protocol.pdf"
        await test_session.commit()

        response = await client.get("/api/v1/games?group_by_date=true")
        assert response.status_code == 200

        data = response.json()
        assert len(data["groups"]) == 1
        group_games = data["groups"][0]["games"]
        assert len(group_games) == 1
        assert (
            group_games[0]["protocol_url"]
            == "document/match_protocols/group-protocol.pdf"
        )

    async def test_get_game_lineup_orders_starters_by_position_order(
        self,
        client: AsyncClient,
        test_session,
        sample_game,
        sample_player,
    ):
        """Test /games/{id}/lineup returns starters sorted by amplua + field_position."""
        def_left = Player(sota_id=uuid4(), first_name="Def", last_name="Left")
        mid_center = Player(sota_id=uuid4(), first_name="Mid", last_name="Center")
        fwd_center = Player(sota_id=uuid4(), first_name="Fwd", last_name="Center")
        test_session.add_all([def_left, mid_center, fwd_center])
        await test_session.commit()
        await test_session.refresh(def_left)
        await test_session.refresh(mid_center)
        await test_session.refresh(fwd_center)

        # Insert in mixed order to verify backend sorting logic.
        test_session.add_all(
            [
                GameLineup(
                    game_id=sample_game.id,
                    team_id=sample_game.home_team_id,
                    player_id=fwd_center.id,
                    lineup_type=LineupType.starter,
                    shirt_number=9,
                    amplua="F",
                    field_position="C",
                ),
                GameLineup(
                    game_id=sample_game.id,
                    team_id=sample_game.home_team_id,
                    player_id=def_left.id,
                    lineup_type=LineupType.starter,
                    shirt_number=3,
                    amplua="D",
                    field_position="L",
                ),
                GameLineup(
                    game_id=sample_game.id,
                    team_id=sample_game.home_team_id,
                    player_id=sample_player.id,
                    lineup_type=LineupType.starter,
                    shirt_number=1,
                    amplua="Gk",
                    field_position="C",
                ),
                GameLineup(
                    game_id=sample_game.id,
                    team_id=sample_game.home_team_id,
                    player_id=mid_center.id,
                    lineup_type=LineupType.starter,
                    shirt_number=8,
                    amplua="M",
                    field_position="C",
                ),
            ]
        )
        await test_session.commit()

        response = await client.get(f"/api/v1/games/{sample_game.id}/lineup")
        assert response.status_code == 200

        data = response.json()
        home_starters = data["lineups"]["home_team"]["starters"]
        starter_ids = [player["player_id"] for player in home_starters]

        assert starter_ids == [
            sample_player.id,  # Gk C
            def_left.id,       # D L
            mid_center.id,     # M C
            fwd_center.id,     # F C
        ]

    async def test_get_game_lineup_derives_amplua_and_field_position_from_top_role(
        self,
        client: AsyncClient,
        test_session,
        sample_game,
    ):
        gk_player = Player(
            sota_id=uuid4(),
            first_name="Goal",
            last_name="Keeper",
            top_role="ВР (вратарь)",
        )
        center_back = Player(
            sota_id=uuid4(),
            first_name="Center",
            last_name="Back",
            top_role="ЦЗ (центральный защитник)",
        )
        holding_mid = Player(
            sota_id=uuid4(),
            first_name="Holding",
            last_name="Mid",
            top_role="ОП (опорный полузащитник)",
        )
        left_mid = Player(
            sota_id=uuid4(),
            first_name="Left",
            last_name="Mid",
            top_role="ЛП (левый полузащитник)",
        )
        unknown_role = Player(
            sota_id=uuid4(),
            first_name="No",
            last_name="Role",
            top_role=None,
        )
        striker = Player(
            sota_id=uuid4(),
            first_name="Main",
            last_name="Striker",
            top_role="ЦН (центральный нападающий)",
        )

        test_session.add_all([gk_player, center_back, holding_mid, left_mid, unknown_role, striker])
        await test_session.commit()
        await test_session.refresh(gk_player)
        await test_session.refresh(center_back)
        await test_session.refresh(holding_mid)
        await test_session.refresh(left_mid)
        await test_session.refresh(unknown_role)
        await test_session.refresh(striker)

        test_session.add_all(
            [
                GameLineup(
                    game_id=sample_game.id,
                    team_id=sample_game.home_team_id,
                    player_id=gk_player.id,
                    lineup_type=LineupType.starter,
                    shirt_number=1,
                ),
                GameLineup(
                    game_id=sample_game.id,
                    team_id=sample_game.home_team_id,
                    player_id=center_back.id,
                    lineup_type=LineupType.starter,
                    shirt_number=4,
                ),
                GameLineup(
                    game_id=sample_game.id,
                    team_id=sample_game.home_team_id,
                    player_id=holding_mid.id,
                    lineup_type=LineupType.starter,
                    shirt_number=6,
                ),
                GameLineup(
                    game_id=sample_game.id,
                    team_id=sample_game.home_team_id,
                    player_id=left_mid.id,
                    lineup_type=LineupType.starter,
                    shirt_number=8,
                ),
                GameLineup(
                    game_id=sample_game.id,
                    team_id=sample_game.home_team_id,
                    player_id=unknown_role.id,
                    lineup_type=LineupType.starter,
                    shirt_number=11,
                ),
                GameLineup(
                    game_id=sample_game.id,
                    team_id=sample_game.home_team_id,
                    player_id=striker.id,
                    lineup_type=LineupType.starter,
                    shirt_number=9,
                ),
            ]
        )
        await test_session.commit()

        response = await client.get(f"/api/v1/games/{sample_game.id}/lineup")
        assert response.status_code == 200

        data = response.json()
        by_player_id = {
            player["player_id"]: player
            for player in data["lineups"]["home_team"]["starters"]
        }

        assert by_player_id[gk_player.id]["amplua"] == "Gk"
        assert by_player_id[gk_player.id]["field_position"] == "C"

        assert by_player_id[center_back.id]["amplua"] == "D"
        assert by_player_id[center_back.id]["field_position"] == "C"

        assert by_player_id[holding_mid.id]["amplua"] == "DM"
        assert by_player_id[holding_mid.id]["field_position"] == "C"

        assert by_player_id[left_mid.id]["amplua"] == "M"
        assert by_player_id[left_mid.id]["field_position"] == "L"

        assert by_player_id[unknown_role.id]["amplua"] == "M"
        assert by_player_id[unknown_role.id]["field_position"] == "C"

        assert by_player_id[striker.id]["amplua"] == "F"
        assert by_player_id[striker.id]["field_position"] == "C"

    async def test_get_game_lineup_prefers_persisted_sota_formation_over_detected(
        self,
        client: AsyncClient,
        test_session,
        sample_game,
        sample_player,
    ):
        """
        Lineup endpoint must return formation persisted from SOTA /em,
        even when amplua-based detection suggests another shape.
        """
        sample_game.home_formation = "3-6-1"

        extra_players: list[Player] = []
        for idx in range(10):
            extra_players.append(
                Player(
                    sota_id=uuid4(),
                    first_name=f"Starter{idx}",
                    last_name="Home",
                )
            )
        test_session.add_all(extra_players)
        await test_session.commit()
        for p in extra_players:
            await test_session.refresh(p)

        detected_442_roles = [
            ("D", "L"),
            ("D", "LC"),
            ("D", "RC"),
            ("D", "R"),
            ("M", "L"),
            ("M", "LC"),
            ("M", "RC"),
            ("M", "R"),
            ("F", "LC"),
            ("F", "RC"),
        ]

        starters = [
            GameLineup(
                game_id=sample_game.id,
                team_id=sample_game.home_team_id,
                player_id=sample_player.id,
                lineup_type=LineupType.starter,
                shirt_number=1,
                amplua="Gk",
                field_position="C",
            )
        ]
        for shirt_number, (player, (amplua, field_pos)) in enumerate(
            zip(extra_players, detected_442_roles),
            start=2,
        ):
            starters.append(
                GameLineup(
                    game_id=sample_game.id,
                    team_id=sample_game.home_team_id,
                    player_id=player.id,
                    lineup_type=LineupType.starter,
                    shirt_number=shirt_number,
                    amplua=amplua,
                    field_position=field_pos,
                )
            )

        test_session.add_all(starters)
        await test_session.commit()

        response = await client.get(f"/api/v1/games/{sample_game.id}/lineup")
        assert response.status_code == 200
        data = response.json()

        assert data["lineups"]["home_team"]["formation"] == "3-6-1"

    async def test_get_game_lineup_live_refresh_updates_positions_and_kit_color(
        self,
        client: AsyncClient,
        test_session,
        sample_game,
        sample_player,
    ):
        sample_game.is_live = True
        sample_game.lineup_live_synced_at = None
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

        mock_client = Mock()
        mock_client.get_live_team_lineup = AsyncMock(
            side_effect=[
                [
                    {"number": "FORMATION", "first_name": "4-3-3", "full_name": "#AA1100"},
                    {"number": "ОСНОВНЫЕ"},
                    {
                        "number": 10,
                        "id": str(sample_player.sota_id),
                        "amplua": "DM",
                        "position": "RC",
                    },
                ],
                [{"number": "FORMATION", "first_name": "4-4-2", "full_name": "#00AA11"}],
            ]
        )
        app.dependency_overrides[get_sota_client] = lambda: mock_client
        try:
            response = await client.get(f"/api/v1/games/{sample_game.id}/lineup")
        finally:
            app.dependency_overrides.pop(get_sota_client, None)

        assert response.status_code == 200
        data = response.json()
        home_starter = data["lineups"]["home_team"]["starters"][0]
        assert home_starter["amplua"] == "DM"
        assert home_starter["field_position"] == "RC"
        assert data["lineups"]["home_team"]["kit_color"] == "#AA1100"
        assert data["lineups"]["away_team"]["kit_color"] == "#00AA11"
        assert mock_client.get_live_team_lineup.await_count == 2

    async def test_get_game_lineup_live_refresh_respects_ttl(
        self,
        client: AsyncClient,
        test_session,
        sample_game,
        sample_player,
    ):
        sample_game.is_live = True
        sample_game.lineup_live_synced_at = datetime.utcnow()
        test_session.add(
            GameLineup(
                game_id=sample_game.id,
                team_id=sample_game.home_team_id,
                player_id=sample_player.id,
                lineup_type=LineupType.starter,
                shirt_number=10,
                amplua="D",
                field_position="L",
            )
        )
        await test_session.commit()

        mock_client = Mock()
        mock_client.get_live_team_lineup = AsyncMock(return_value=[])
        app.dependency_overrides[get_sota_client] = lambda: mock_client
        try:
            response = await client.get(f"/api/v1/games/{sample_game.id}/lineup")
        finally:
            app.dependency_overrides.pop(get_sota_client, None)

        assert response.status_code == 200
        assert mock_client.get_live_team_lineup.await_count == 0

    async def test_get_game_lineup_live_refresh_failure_returns_snapshot(
        self,
        client: AsyncClient,
        test_session,
        sample_game,
        sample_player,
    ):
        sample_game.is_live = True
        sample_game.lineup_live_synced_at = None
        test_session.add(
            GameLineup(
                game_id=sample_game.id,
                team_id=sample_game.home_team_id,
                player_id=sample_player.id,
                lineup_type=LineupType.starter,
                shirt_number=10,
                amplua="D",
                field_position="L",
            )
        )
        await test_session.commit()

        mock_client = Mock()
        mock_client.get_live_team_lineup = AsyncMock(
            side_effect=[Exception("home failed"), Exception("away failed")]
        )
        app.dependency_overrides[get_sota_client] = lambda: mock_client
        try:
            response = await client.get(f"/api/v1/games/{sample_game.id}/lineup")
        finally:
            app.dependency_overrides.pop(get_sota_client, None)

        assert response.status_code == 200
        data = response.json()
        home_starter = data["lineups"]["home_team"]["starters"][0]
        assert home_starter["amplua"] == "D"
        assert home_starter["field_position"] == "L"
