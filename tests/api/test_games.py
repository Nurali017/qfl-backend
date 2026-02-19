import pytest
from httpx import AsyncClient
from uuid import uuid4


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
        """Test getting game by UUID."""
        game_id = str(sample_game.id)
        response = await client.get(f"/api/v1/games/{game_id}")
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
        random_uuid = str(uuid4())
        response = await client.get(f"/api/v1/games/{random_uuid}")
        assert response.status_code == 404
        assert response.json()["detail"] == "Game not found"

    async def test_get_game_stats(self, client: AsyncClient, sample_game):
        """Test getting game statistics."""
        game_id = str(sample_game.id)
        response = await client.get(f"/api/v1/games/{game_id}/stats")
        assert response.status_code == 200
        data = response.json()
        assert data["game_id"] == game_id
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
