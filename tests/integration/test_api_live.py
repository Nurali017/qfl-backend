"""
Integration tests that run against the live server.
These tests require the server to be running at http://localhost:8000
"""
import pytest
import httpx


BASE_URL = "http://localhost:8000"


@pytest.fixture
def live_client():
    """Create a client for the live server."""
    return httpx.Client(base_url=BASE_URL, timeout=30.0)


class TestHealthAPI:
    """Test health check endpoint."""

    def test_health_check(self, live_client):
        response = live_client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "healthy"


class TestSeasonsAPI:
    """Test seasons endpoints against live server."""

    def test_get_seasons(self, live_client):
        response = live_client.get("/api/v1/seasons")
        assert response.status_code == 200
        data = response.json()
        assert "items" in data
        assert "total" in data
        assert data["total"] >= 1

    def test_get_season_by_id(self, live_client):
        response = live_client.get("/api/v1/seasons/61")
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == 61
        assert data["name"] == "2025"

    def test_get_season_not_found(self, live_client):
        response = live_client.get("/api/v1/seasons/99999")
        assert response.status_code == 404
        assert response.json()["detail"] == "Season not found"

    def test_get_season_table(self, live_client):
        response = live_client.get("/api/v1/seasons/61/table")
        assert response.status_code == 200
        data = response.json()
        assert data["season_id"] == 61
        assert len(data["table"]) > 0
        assert "position" in data["table"][0]
        assert "team_name" in data["table"][0]

    def test_get_season_games(self, live_client):
        response = live_client.get("/api/v1/seasons/61/games")
        assert response.status_code == 200
        data = response.json()
        assert "items" in data
        assert data["total"] > 0


class TestTeamsAPI:
    """Test teams endpoints against live server."""

    def test_get_teams(self, live_client):
        response = live_client.get("/api/v1/teams")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] > 0

    def test_get_team_by_id(self, live_client):
        response = live_client.get("/api/v1/teams/91")
        assert response.status_code == 200
        data = response.json()
        # Check structure, not specific name (can be "Астана" or "Astana" depending on language)
        assert "id" in data
        assert "name" in data
        assert data["id"] == 91

    def test_get_team_not_found(self, live_client):
        response = live_client.get("/api/v1/teams/99999")
        assert response.status_code == 404

    def test_get_team_players(self, live_client):
        response = live_client.get("/api/v1/teams/91/players?season_id=61")
        assert response.status_code == 200
        data = response.json()
        assert "items" in data

    def test_get_team_games(self, live_client):
        response = live_client.get("/api/v1/teams/91/games?season_id=61")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] > 0

    def test_get_team_stats(self, live_client):
        response = live_client.get("/api/v1/teams/91/stats?season_id=61")
        assert response.status_code == 200
        data = response.json()
        assert data["team_id"] == 91
        assert "games_played" in data


class TestPlayersAPI:
    """Test players endpoints against live server."""

    def test_get_players(self, live_client):
        response = live_client.get("/api/v1/players?season_id=61&limit=10")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] > 0
        assert len(data["items"]) <= 10

    def test_get_player_by_id(self, live_client):
        # First get a player from the list
        response = live_client.get("/api/v1/players?limit=1")
        data = response.json()
        if data["items"]:
            player_id = data["items"][0]["id"]
            response = live_client.get(f"/api/v1/players/{player_id}")
            assert response.status_code == 200
            player = response.json()
            assert player["id"] == player_id

    def test_get_player_not_found(self, live_client):
        response = live_client.get("/api/v1/players/999999999")
        assert response.status_code == 404


class TestGamesAPI:
    """Test games endpoints against live server."""

    def test_get_games(self, live_client):
        response = live_client.get("/api/v1/games?season_id=61&limit=10")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] > 0

    def test_get_game_by_id(self, live_client):
        # First get a game from the list
        response = live_client.get("/api/v1/games?limit=1")
        data = response.json()
        if data["items"]:
            game_id = data["items"][0]["id"]
            response = live_client.get(f"/api/v1/games/{game_id}")
            assert response.status_code == 200
            game = response.json()
            assert game["id"] == game_id

    def test_get_game_stats(self, live_client):
        # First get a game with stats
        response = live_client.get("/api/v1/games?limit=1")
        data = response.json()
        if data["items"]:
            game_id = data["items"][0]["id"]
            response = live_client.get(f"/api/v1/games/{game_id}/stats")
            assert response.status_code == 200
            stats = response.json()
            assert stats["game_id"] == game_id


class TestPagesAPI:
    """Test pages endpoints against live server."""

    def test_get_pages_ru(self, live_client):
        response = live_client.get("/api/v1/pages?language=ru")
        assert response.status_code == 200
        data = response.json()
        assert len(data) >= 1

    def test_get_pages_kz(self, live_client):
        response = live_client.get("/api/v1/pages?language=kz")
        assert response.status_code == 200

    def test_get_contacts_page(self, live_client):
        response = live_client.get("/api/v1/pages/contacts/ru")
        assert response.status_code == 200
        data = response.json()
        assert "title" in data

    def test_get_documents_page(self, live_client):
        response = live_client.get("/api/v1/pages/documents/ru")
        assert response.status_code == 200

    def test_get_leadership_page(self, live_client):
        response = live_client.get("/api/v1/pages/leadership/ru")
        assert response.status_code == 200


class TestNewsAPI:
    """Test news endpoints against live server."""

    def test_get_news_list(self, live_client):
        response = live_client.get("/api/v1/news?language=ru&per_page=5")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] > 0
        assert len(data["items"]) <= 5

    def test_get_news_by_tournament(self, live_client):
        response = live_client.get("/api/v1/news?language=ru&championship_code=pl&per_page=1")
        assert response.status_code == 200
        data = response.json()
        assert "items" in data

    def test_get_latest_news(self, live_client):
        response = live_client.get("/api/v1/news/latest?language=ru&limit=5")
        assert response.status_code == 200
        data = response.json()
        assert len(data) <= 5

    def test_get_news_item(self, live_client):
        # First get news list
        response = live_client.get("/api/v1/news?language=ru&per_page=1")
        data = response.json()
        if data["items"]:
            news_id = data["items"][0]["id"]
            response = live_client.get(f"/api/v1/news/{news_id}?language=ru")
            assert response.status_code == 200

    def test_get_news_not_found(self, live_client):
        response = live_client.get("/api/v1/news/999999?language=ru")
        assert response.status_code == 404


class TestSyncAPI:
    """Test sync endpoints against live server (non-destructive checks)."""

    def test_sync_teams(self, live_client):
        response = live_client.post("/api/v1/sync/teams")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] in ["success", "failed"]

    def test_sync_score_table(self, live_client):
        response = live_client.post("/api/v1/sync/score-table?season_id=61")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] in ["success", "failed"]
