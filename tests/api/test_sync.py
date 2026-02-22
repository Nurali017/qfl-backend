import pytest
from httpx import AsyncClient
from unittest.mock import AsyncMock, patch, MagicMock


@pytest.mark.asyncio
class TestSyncAPI:
    """Tests for /api/v1/sync endpoints."""

    async def test_sync_teams(self, client: AsyncClient):
        """Test teams synchronization endpoint."""
        with patch('app.api.sync.SyncOrchestrator') as MockOrchestrator:
            mock_instance = MagicMock()
            mock_instance.reference = MagicMock()
            mock_instance.reference.sync_teams = AsyncMock(return_value=10)
            MockOrchestrator.return_value = mock_instance

            response = await client.post("/api/v1/sync/teams")
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "success"
            assert "10 teams synced" in data["message"]

    async def test_sync_games(self, client: AsyncClient, sample_season):
        """Test games synchronization endpoint."""
        with patch('app.api.sync.SyncOrchestrator') as MockOrchestrator:
            mock_instance = MagicMock()
            mock_instance.is_sync_enabled = AsyncMock(return_value=True)
            mock_instance.sync_games = AsyncMock(return_value=5)
            MockOrchestrator.return_value = mock_instance

            response = await client.post("/api/v1/sync/games?season_id=61")
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "success"
            assert "5 games synced" in data["message"]

    async def test_sync_players(self, client: AsyncClient, sample_season):
        """Test players synchronization endpoint."""
        with patch('app.api.sync.SyncOrchestrator') as MockOrchestrator:
            mock_instance = MagicMock()
            mock_instance.is_sync_enabled = AsyncMock(return_value=True)
            mock_instance.sync_players = AsyncMock(return_value=50)
            MockOrchestrator.return_value = mock_instance

            response = await client.post("/api/v1/sync/players?season_id=61")
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "success"
            assert "50 players synced" in data["message"]

    async def test_sync_score_table(self, client: AsyncClient, sample_season):
        """Test score table synchronization endpoint."""
        with patch('app.api.sync.SyncOrchestrator') as MockOrchestrator:
            mock_instance = MagicMock()
            mock_instance.is_sync_enabled = AsyncMock(return_value=True)
            mock_instance.sync_score_table = AsyncMock(return_value=14)
            MockOrchestrator.return_value = mock_instance

            response = await client.post("/api/v1/sync/score-table?season_id=61")
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "success"
            assert "14 entries synced" in data["message"]

    async def test_sync_full(self, client: AsyncClient, sample_season):
        """Test full synchronization endpoint."""
        with patch('app.api.sync.SyncOrchestrator') as MockOrchestrator:
            mock_instance = MagicMock()
            mock_instance.is_sync_enabled = AsyncMock(return_value=True)
            mock_instance.full_sync = AsyncMock(return_value={"teams": 10, "players": 100, "games": 50})
            MockOrchestrator.return_value = mock_instance

            response = await client.post("/api/v1/sync/full?season_id=61")
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "success"

    async def test_sync_game_stats(self, client: AsyncClient, sample_game):
        """Test game stats synchronization endpoint."""
        game_id = sample_game.id
        with patch('app.api.sync.SyncOrchestrator') as MockOrchestrator:
            mock_instance = MagicMock()
            mock_instance.sync_game_stats = AsyncMock(return_value={"teams": 2, "players": 22})
            MockOrchestrator.return_value = mock_instance

            response = await client.post(f"/api/v1/sync/game-stats/{game_id}")
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "success"

    async def test_sync_failure_handling(self, client: AsyncClient):
        """Test sync failure response."""
        with patch('app.api.sync.SyncOrchestrator') as MockOrchestrator:
            mock_instance = MagicMock()
            mock_instance.reference = MagicMock()
            mock_instance.reference.sync_teams = AsyncMock(side_effect=Exception("API connection failed"))
            MockOrchestrator.return_value = mock_instance

            response = await client.post("/api/v1/sync/teams")
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "failed"
            assert "API connection failed" in data["message"]
