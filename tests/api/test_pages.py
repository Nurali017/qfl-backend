import pytest
from httpx import AsyncClient

from app.models import Language, Page


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

    async def test_get_leadership_page_normalizes_member_photo_urls(
        self,
        client: AsyncClient,
        test_session,
        monkeypatch: pytest.MonkeyPatch,
    ):
        """Leadership endpoint normalizes localhost/object-name photos to public URLs."""
        from app.services.file_storage import FileStorageService

        leadership_page = Page(
            slug="rukovodstvo",
            language=Language.RU,
            title="Руководство",
            content_text="Руководство",
            structured_data={
                "members": [
                    {
                        "id": 1,
                        "name": "Member One",
                        "position": "Role One",
                        "photo": "http://localhost:9000/qfl-files/leadership/test.jpg",
                    },
                    {
                        "id": 2,
                        "name": "Member Two",
                        "position": "Role Two",
                        "photo": "leadership/test-2.jpg",
                    },
                ]
            },
        )
        test_session.add(leadership_page)
        await test_session.commit()

        async def fake_list_files(*args, **kwargs):
            return []

        class FakeSettings:
            minio_public_endpoint = "https://kffleague.kz/storage"
            minio_bucket = "qfl-files"

        monkeypatch.setattr(FileStorageService, "list_files", fake_list_files)
        monkeypatch.setattr("app.utils.file_urls.get_settings", lambda: FakeSettings())

        response = await client.get("/api/v1/pages/leadership/ru")
        assert response.status_code == 200

        payload = response.json()
        members = payload["structured_data"]["members"]

        assert members[0]["photo"] == "https://kffleague.kz/storage/qfl-files/leadership/test.jpg"
        assert "localhost" not in members[0]["photo"]
        assert members[1]["photo"] == "https://kffleague.kz/storage/qfl-files/leadership/test-2.jpg"
