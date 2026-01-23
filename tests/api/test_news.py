import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
class TestNewsAPI:
    """Tests for /api/v1/news endpoints."""

    async def test_get_news_list_empty(self, client: AsyncClient):
        """Test getting news when database is empty."""
        response = await client.get("/api/v1/news?language=ru")
        assert response.status_code == 200
        data = response.json()
        assert data["items"] == []
        assert data["total"] == 0

    async def test_get_news_list_with_data(self, client: AsyncClient, sample_news):
        """Test getting news list."""
        response = await client.get("/api/v1/news?language=ru")
        assert response.status_code == 200
        data = response.json()
        assert len(data["items"]) == 2
        assert data["total"] == 2
        assert data["page"] == 1

    async def test_get_news_pagination(self, client: AsyncClient, sample_news):
        """Test news pagination."""
        response = await client.get("/api/v1/news?language=ru&page=1&per_page=1")
        assert response.status_code == 200
        data = response.json()
        assert len(data["items"]) == 1
        assert data["pages"] == 2

    async def test_get_news_by_category(self, client: AsyncClient, sample_news):
        """Test filtering news by category."""
        response = await client.get("/api/v1/news?language=ru&category=PREMIER-LIGA")
        assert response.status_code == 200
        data = response.json()
        assert len(data["items"]) == 1
        assert data["items"][0]["category"] == "PREMIER-LIGA"

    async def test_get_news_categories(self, client: AsyncClient, sample_news):
        """Test getting news categories."""
        response = await client.get("/api/v1/news/categories?language=ru")
        assert response.status_code == 200
        data = response.json()
        assert "PREMIER-LIGA" in data
        assert "CUP" in data

    async def test_get_latest_news(self, client: AsyncClient, sample_news):
        """Test getting latest news."""
        response = await client.get("/api/v1/news/latest?language=ru&limit=5")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        # Verify ordering by date descending
        assert data[0]["title"] == "News Article 2"

    async def test_get_news_item(self, client: AsyncClient, sample_news):
        """Test getting single news article."""
        response = await client.get("/api/v1/news/1?language=ru")
        assert response.status_code == 200
        data = response.json()
        assert data["title"] == "News Article 1"

    async def test_get_news_not_found(self, client: AsyncClient):
        """Test 404 for non-existent news."""
        response = await client.get("/api/v1/news/99999?language=ru")
        assert response.status_code == 404
        assert response.json()["detail"] == "News not found"
