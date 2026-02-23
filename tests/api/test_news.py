import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
class TestNewsAPI:
    """Tests for /api/v1/news endpoints."""

    async def test_get_news_list_empty(self, client: AsyncClient):
        """Test getting news when database is empty."""
        response = await client.get("/api/v1/news?lang=ru")
        assert response.status_code == 200
        data = response.json()
        assert data["items"] == []
        assert data["total"] == 0

    async def test_get_news_list_with_data(self, client: AsyncClient, sample_news):
        """Test getting news list."""
        response = await client.get("/api/v1/news?lang=ru")
        assert response.status_code == 200
        data = response.json()
        assert len(data["items"]) == 2
        assert data["total"] == 2
        assert data["page"] == 1

    async def test_get_news_pagination(self, client: AsyncClient, sample_news):
        """Test news pagination."""
        response = await client.get("/api/v1/news?lang=ru&page=1&per_page=1")
        assert response.status_code == 200
        data = response.json()
        assert len(data["items"]) == 1
        assert data["pages"] == 2

    async def test_get_news_by_tournament(self, client: AsyncClient, sample_news):
        """Test filtering news by championship_code."""
        response = await client.get("/api/v1/news?lang=ru&championship_code=pl")
        assert response.status_code == 200
        data = response.json()
        assert len(data["items"]) == 1
        assert data["items"][0]["championship_code"] == "pl"

    async def test_get_news_by_article_type(self, client: AsyncClient, sample_news):
        response = await client.get("/api/v1/news?lang=ru&article_type=analytics")
        assert response.status_code == 200
        data = response.json()
        assert len(data["items"]) == 1
        assert data["items"][0]["article_type"] == "ANALYTICS"

    async def test_get_news_search(self, client: AsyncClient, sample_news):
        response = await client.get("/api/v1/news?lang=ru&search=xg")
        assert response.status_code == 200
        data = response.json()
        assert len(data["items"]) == 1
        assert data["items"][0]["id"] == 2

    async def test_get_news_sort_views_desc(self, client: AsyncClient, sample_news):
        response = await client.get("/api/v1/news?lang=ru&sort=views_desc")
        assert response.status_code == 200
        data = response.json()
        assert [item["id"] for item in data["items"]] == [1, 2]

    async def test_get_news_sort_likes_desc(self, client: AsyncClient, sample_news):
        response = await client.get("/api/v1/news?lang=ru&sort=likes_desc")
        assert response.status_code == 200
        data = response.json()
        assert [item["id"] for item in data["items"]] == [2, 1]

    async def test_get_news_by_date_range(self, client: AsyncClient, sample_news):
        response = await client.get("/api/v1/news?lang=ru&date_from=2025-05-02&date_to=2025-05-02")
        assert response.status_code == 200
        data = response.json()
        assert len(data["items"]) == 1
        assert data["items"][0]["id"] == 2

    async def test_get_latest_news(self, client: AsyncClient, sample_news):
        """Test getting latest news."""
        response = await client.get("/api/v1/news/latest?lang=ru&limit=5")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        # Verify ordering by date descending
        assert data[0]["title"] == "Тактический анализ матча тура"

    async def test_get_news_item(self, client: AsyncClient, sample_news, monkeypatch):
        """Test getting single news article."""
        from app.services.file_storage import FileStorageService

        async def fake_get_files(_news_id: str):
            return []

        monkeypatch.setattr(FileStorageService, "get_files_by_news_id", fake_get_files)

        response = await client.get("/api/v1/news/1?lang=ru")
        assert response.status_code == 200
        data = response.json()
        assert data["title"] == "Официально: новый трансфер в клубе"

    async def test_get_news_not_found(self, client: AsyncClient):
        """Test 404 for non-existent news."""
        response = await client.get("/api/v1/news/99999?lang=ru")
        assert response.status_code == 404
        # Error message may be localized (ru/kz/en)
        assert "detail" in response.json()
