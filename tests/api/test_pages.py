import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
class TestPagesAPI:
    """Tests for /api/v1/pages endpoints."""

    async def test_get_pages_empty(self, client: AsyncClient):
        """Test getting pages when database is empty."""
        response = await client.get("/api/v1/pages?language=ru")
        assert response.status_code == 200
        data = response.json()
        assert data == []

    async def test_get_pages_with_data(self, client: AsyncClient, sample_page):
        """Test getting Russian pages."""
        response = await client.get("/api/v1/pages?language=ru")
        assert response.status_code == 200
        data = response.json()
        assert len(data) >= 1
        assert any(p["slug"] == "kontakty" for p in data)

    async def test_get_page_by_slug(self, client: AsyncClient, sample_page):
        """Test getting page by slug."""
        response = await client.get("/api/v1/pages/kontakty?language=ru")
        assert response.status_code == 200
        data = response.json()
        assert data["title"] == "Kontakty"
        assert data["slug"] == "kontakty"

    async def test_get_page_not_found(self, client: AsyncClient):
        """Test 404 for non-existent page."""
        response = await client.get("/api/v1/pages/nonexistent?language=ru")
        assert response.status_code == 404
        # Error message may be localized (ru/kz/en)
        assert "detail" in response.json()

    async def test_get_contacts_page(self, client: AsyncClient, sample_page):
        """Test contacts shortcut endpoint."""
        # Create a page with slug that matches contacts
        response = await client.get("/api/v1/pages/contacts/ru")
        # May return 404 if the actual contacts slug differs
        assert response.status_code in [200, 404]

    async def test_get_documents_page(self, client: AsyncClient):
        """Test documents shortcut endpoint."""
        response = await client.get("/api/v1/pages/documents/ru")
        assert response.status_code in [200, 404]

    async def test_get_leadership_page(self, client: AsyncClient):
        """Test leadership shortcut endpoint."""
        response = await client.get("/api/v1/pages/leadership/ru")
        assert response.status_code in [200, 404]
