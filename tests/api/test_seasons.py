import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
class TestSeasonsAPI:
    """Tests for /api/v1/seasons endpoints."""

    async def test_get_seasons_empty(self, client: AsyncClient):
        """Test getting seasons when database is empty."""
        response = await client.get("/api/v1/seasons")
        assert response.status_code == 200
        data = response.json()
        assert data["items"] == []
        assert data["total"] == 0

    async def test_get_seasons_with_data(self, client: AsyncClient, sample_season):
        """Test getting seasons with data."""
        response = await client.get("/api/v1/seasons")
        assert response.status_code == 200
        data = response.json()
        assert len(data["items"]) == 1
        assert data["items"][0]["id"] == 61
        assert data["items"][0]["name"] == "2025"
        assert data["total"] == 1

    async def test_get_season_by_id(self, client: AsyncClient, sample_season):
        """Test getting a specific season."""
        response = await client.get("/api/v1/seasons/61")
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == 61
        assert data["name"] == "2025"

    async def test_get_season_not_found(self, client: AsyncClient):
        """Test 404 for non-existent season."""
        response = await client.get("/api/v1/seasons/99999")
        assert response.status_code == 404
        assert response.json()["detail"] == "Season not found"

    async def test_get_season_table_empty(self, client: AsyncClient, sample_season):
        """Test getting empty season table."""
        response = await client.get("/api/v1/seasons/61/table")
        assert response.status_code == 200
        data = response.json()
        assert data["season_id"] == 61
        assert data["table"] == []

    async def test_get_season_table_with_data(
        self, client: AsyncClient, sample_season, sample_score_table
    ):
        """Test getting season table with data."""
        response = await client.get("/api/v1/seasons/61/table")
        assert response.status_code == 200
        data = response.json()
        assert data["season_id"] == 61
        assert len(data["table"]) == 3
        assert data["table"][0]["position"] == 1

    async def test_get_season_games(self, client: AsyncClient, sample_season, sample_game):
        """Test getting games for a season."""
        response = await client.get("/api/v1/seasons/61/games")
        assert response.status_code == 200
        data = response.json()
        assert len(data["items"]) == 1
        assert data["total"] == 1

    async def test_get_season_games_by_tour(
        self, client: AsyncClient, sample_season, sample_game
    ):
        """Test filtering games by tour."""
        response = await client.get("/api/v1/seasons/61/games?tour=1")
        assert response.status_code == 200
        data = response.json()
        assert len(data["items"]) == 1

        response = await client.get("/api/v1/seasons/61/games?tour=99")
        assert response.status_code == 200
        data = response.json()
        assert len(data["items"]) == 0
