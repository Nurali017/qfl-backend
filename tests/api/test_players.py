import pytest
from httpx import AsyncClient
from uuid import uuid4


@pytest.mark.asyncio
class TestPlayersAPI:
    """Tests for /api/v1/players endpoints."""

    async def test_get_players_empty(self, client: AsyncClient):
        """Test getting players when database is empty."""
        response = await client.get("/api/v1/players")
        assert response.status_code == 200
        data = response.json()
        assert data["items"] == []
        assert data["total"] == 0

    async def test_get_players_with_data(self, client: AsyncClient, sample_player):
        """Test getting all players."""
        response = await client.get("/api/v1/players")
        assert response.status_code == 200
        data = response.json()
        assert len(data["items"]) == 1
        assert data["total"] == 1

    async def test_get_players_pagination(self, client: AsyncClient, sample_player):
        """Test player pagination."""
        response = await client.get("/api/v1/players?limit=1&offset=0")
        assert response.status_code == 200
        data = response.json()
        assert len(data["items"]) == 1
        assert data["total"] == 1

    async def test_get_player_by_id(self, client: AsyncClient, sample_player):
        """Test getting player by UUID."""
        player_id = str(sample_player.id)
        response = await client.get(f"/api/v1/players/{player_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["first_name"] == "Test"
        assert data["last_name"] == "Player"

    async def test_get_player_not_found(self, client: AsyncClient):
        """Test 404 for non-existent player."""
        random_uuid = str(uuid4())
        response = await client.get(f"/api/v1/players/{random_uuid}")
        assert response.status_code == 404
        # Error message may be localized (ru/kz/en)
        assert "detail" in response.json()

    async def test_get_player_invalid_uuid(self, client: AsyncClient):
        """Test invalid UUID format."""
        response = await client.get("/api/v1/players/invalid-uuid")
        assert response.status_code == 422

    async def test_get_player_stats(self, client: AsyncClient, sample_player):
        """Test getting player stats returns 404 when no stats exist."""
        player_id = str(sample_player.id)
        response = await client.get(f"/api/v1/players/{player_id}/stats")
        # PlayerSeasonStats table is empty, so API returns 404
        assert response.status_code == 404
        assert "detail" in response.json()

    async def test_get_player_games_empty(self, client: AsyncClient, sample_player):
        """Test getting player games when no games played."""
        player_id = str(sample_player.id)
        response = await client.get(f"/api/v1/players/{player_id}/games")
        assert response.status_code == 200
        data = response.json()
        assert data["items"] == []
        assert data["total"] == 0
