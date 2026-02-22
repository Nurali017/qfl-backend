import pytest
from httpx import AsyncClient


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
        """Test getting player by id."""
        player_id = sample_player.id
        response = await client.get(f"/api/v1/players/{player_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["first_name"] == "Test"
        assert data["last_name"] == "Player"

    async def test_get_player_not_found(self, client: AsyncClient):
        """Test 404 for non-existent player."""
        response = await client.get("/api/v1/players/999999")
        assert response.status_code == 404
        # Error message may be localized (ru/kz/en)
        assert "detail" in response.json()

    async def test_get_player_invalid_id(self, client: AsyncClient):
        """Test invalid player id format."""
        response = await client.get("/api/v1/players/not-a-number")
        assert response.status_code == 422

    async def test_get_player_stats(self, client: AsyncClient, sample_player):
        """Test getting player stats returns 404 when no stats exist."""
        player_id = sample_player.id
        response = await client.get(f"/api/v1/players/{player_id}/stats")
        # PlayerSeasonStats table is empty, so API returns 404
        assert response.status_code == 404
        assert "detail" in response.json()

    async def test_get_player_stats_sanitizes_nan_metrics(
        self,
        client: AsyncClient,
        test_session,
        sample_player,
        sample_season,
        sample_teams,
    ):
        """Player stats endpoint should replace non-JSON NaN values with null."""
        from app.models import PlayerSeasonStats

        stats = PlayerSeasonStats(
            player_id=sample_player.id,
            season_id=sample_season.id,
            team_id=sample_teams[0].id,
            games_played=1,
            minutes_played=90,
            goals=1,
            assists=0,
            xg=float("nan"),
            pass_accuracy=float("nan"),
        )
        test_session.add(stats)
        await test_session.commit()

        response = await client.get(
            f"/api/v1/players/{sample_player.id}/stats?season_id={sample_season.id}"
        )
        assert response.status_code == 200
        data = response.json()
        assert data["player_id"] == sample_player.id
        assert data["xg"] is None
        assert data["pass_accuracy"] is None

    async def test_get_player_games_empty(self, client: AsyncClient, sample_player):
        """Test getting player games when no games played."""
        player_id = sample_player.id
        response = await client.get(f"/api/v1/players/{player_id}/games")
        assert response.status_code == 200
        data = response.json()
        assert data["items"] == []
        assert data["total"] == 0
