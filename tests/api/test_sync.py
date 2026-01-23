import pytest
from httpx import AsyncClient
from unittest.mock import AsyncMock, patch


@pytest.mark.asyncio
class TestSyncAPI:
    """Tests for /api/v1/sync endpoints."""

    async def test_sync_teams(self, client: AsyncClient):
        """Test teams synchronization endpoint."""
        with patch(
            'app.api.sync.sync_service.sync_teams',
            new_callable=AsyncMock
        ) as mock_sync:
            mock_sync.return_value = 10

            response = await client.post("/api/v1/sync/teams")
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "success"
            assert "10 teams synced" in data["message"]

    async def test_sync_games(self, client: AsyncClient, sample_season):
        """Test games synchronization endpoint."""
        with patch(
            'app.api.sync.sync_service.sync_games',
            new_callable=AsyncMock
        ) as mock_sync:
            mock_sync.return_value = 5

            response = await client.post("/api/v1/sync/games?season_id=61")
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "success"
            assert "5 games synced" in data["message"]

    async def test_sync_players(self, client: AsyncClient, sample_season):
        """Test players synchronization endpoint."""
        with patch(
            'app.api.sync.sync_service.sync_players',
            new_callable=AsyncMock
        ) as mock_sync:
            mock_sync.return_value = 50

            response = await client.post("/api/v1/sync/players?season_id=61")
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "success"
            assert "50 players synced" in data["message"]

    async def test_sync_score_table(self, client: AsyncClient, sample_season):
        """Test score table synchronization endpoint."""
        with patch(
            'app.api.sync.sync_service.sync_score_table',
            new_callable=AsyncMock
        ) as mock_sync:
            mock_sync.return_value = 14

            response = await client.post("/api/v1/sync/score-table?season_id=61")
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "success"
            assert "14 entries synced" in data["message"]

    async def test_sync_full(self, client: AsyncClient, sample_season):
        """Test full synchronization endpoint."""
        with patch(
            'app.api.sync.sync_service.full_sync',
            new_callable=AsyncMock
        ) as mock_sync:
            mock_sync.return_value = {"teams": 10, "players": 100, "games": 50}

            response = await client.post("/api/v1/sync/full?season_id=61")
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "success"

    async def test_sync_game_stats(self, client: AsyncClient, sample_game):
        """Test game stats synchronization endpoint."""
        game_id = str(sample_game.id)
        with patch(
            'app.api.sync.sync_service.sync_game_stats',
            new_callable=AsyncMock
        ) as mock_sync:
            mock_sync.return_value = {"teams": 2, "players": 22}

            response = await client.post(f"/api/v1/sync/game-stats/{game_id}")
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "success"

    async def test_sync_failure_handling(self, client: AsyncClient):
        """Test sync failure response."""
        with patch(
            'app.api.sync.sync_service.sync_teams',
            new_callable=AsyncMock
        ) as mock_sync:
            mock_sync.side_effect = Exception("API connection failed")

            response = await client.post("/api/v1/sync/teams")
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "failed"
            assert "API connection failed" in data["message"]
