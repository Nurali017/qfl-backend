import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
class TestTeamsAPI:
    """Tests for /api/v1/teams endpoints."""

    async def test_get_teams_empty(self, client: AsyncClient):
        """Test getting teams when database is empty."""
        response = await client.get("/api/v1/teams")
        assert response.status_code == 200
        data = response.json()
        assert data["items"] == []
        assert data["total"] == 0

    async def test_get_teams_with_data(self, client: AsyncClient, sample_teams):
        """Test getting all teams."""
        response = await client.get("/api/v1/teams")
        assert response.status_code == 200
        data = response.json()
        assert len(data["items"]) == 3
        assert data["total"] == 3

    async def test_get_team_by_id(self, client: AsyncClient, sample_teams):
        """Test getting team by ID."""
        response = await client.get("/api/v1/teams/91")
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "Astana"
        assert data["id"] == 91

    async def test_get_team_not_found(self, client: AsyncClient):
        """Test 404 for non-existent team."""
        response = await client.get("/api/v1/teams/99999")
        assert response.status_code == 404
        assert response.json()["detail"] == "Team not found"

    async def test_get_team_players_empty(self, client: AsyncClient, sample_teams):
        """Test getting team players when no players assigned."""
        response = await client.get("/api/v1/teams/91/players")
        assert response.status_code == 200
        data = response.json()
        assert data["items"] == []
        assert data["total"] == 0

    async def test_get_team_games(
        self, client: AsyncClient, sample_teams, sample_season, sample_game
    ):
        """Test getting team games."""
        response = await client.get("/api/v1/teams/91/games?season_id=61")
        assert response.status_code == 200
        data = response.json()
        assert len(data["items"]) == 1

    async def test_get_team_stats(
        self, client: AsyncClient, sample_teams, sample_season, sample_game
    ):
        """Test getting team statistics."""
        response = await client.get("/api/v1/teams/91/stats?season_id=61")
        assert response.status_code == 200
        data = response.json()
        assert data["team_id"] == 91
        assert data["games_played"] == 1
        assert data["wins"] == 1
        assert data["goals_scored"] == 2
