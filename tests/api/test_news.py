import pytest
from httpx import AsyncClient

from app.models import Player, Team


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

    async def test_get_news_item_normalizes_links_and_media(
        self,
        client: AsyncClient,
        sample_news,
        test_session,
        monkeypatch: pytest.MonkeyPatch,
    ):
        from app.services.file_storage import FileStorageService

        team = Team(
            id=9_091,
            legacy_id=19_091,
            name="News Team",
        )
        player = Player(
            id=9_321,
            legacy_id=12_345,
            first_name="Test",
            last_name="Player",
        )
        test_session.add_all([team, player])

        sample = sample_news[0]
        sample.source_url = "https://kffleague.kz/ru/news/1"
        sample.image_url = "http://localhost:9000/internal/main.jpg"
        sample.content = (
            '<p><a href="/teams/19091">Team Link</a></p>'
            '<p><a href="https://kffleague.kz/players/12345?from=news">Player Link</a></p>'
            '<p><a href="https://example.com/ext">External Link</a></p>'
            '<p><a href="javascript:alert(1)">Unsafe Link</a></p>'
            '<p><img src="/uploads/photo.jpg" /></p>'
            '<p><img src="data:image/png;base64,AAAA" /></p>'
        )
        await test_session.commit()

        async def fake_get_files(_news_id: str):
            return [
                {
                    "_id": "img-1",
                    "filename": "inside.jpg",
                    "url": "/media/inside.jpg",
                    "size": 128,
                }
            ]

        monkeypatch.setattr(FileStorageService, "get_files_by_news_id", fake_get_files)

        response = await client.get("/api/v1/news/1?lang=ru")
        assert response.status_code == 200

        data = response.json()
        content = data["content"]

        assert 'href="/team/9091"' in content
        assert 'href="/player/9321"' in content
        assert 'href="https://example.com/ext"' in content
        assert 'target="_blank"' in content
        assert 'rel="noopener noreferrer nofollow"' in content
        assert "javascript:alert(1)" not in content
        assert "data:image/png;base64" not in content
        assert 'src="https://kffleague.kz/uploads/photo.jpg"' in content

        assert data["image_url"] == "https://kffleague.kz/internal/main.jpg"
        assert data["images"][0]["url"] == "https://kffleague.kz/media/inside.jpg"

    async def test_get_news_item_normalizes_minio_urls_in_content(
        self,
        client: AsyncClient,
        sample_news,
        test_session,
        monkeypatch: pytest.MonkeyPatch,
    ):
        """MinIO localhost URLs in content get /storage/ prefix for correct proxying."""
        from app.services.file_storage import FileStorageService

        class FakeSettings:
            minio_public_endpoint = "https://kffleague.kz/storage"
            minio_bucket = "qfl-files"

        monkeypatch.setattr(
            "app.utils.news_html_normalizer.get_settings", lambda: FakeSettings()
        )
        monkeypatch.setattr(
            "app.utils.news_html_normalizer.resolve_file_url",
            lambda obj: f"https://kffleague.kz/storage/qfl-files/{obj}",
        )

        sample = sample_news[0]
        sample.content = (
            '<p><img src="http://localhost:9000/qfl-files/news/abc123.png" /></p>'
            '<p><a href="http://localhost:9000/qfl-files/news/doc.pdf">PDF</a></p>'
            '<p><img src="http://localhost:9000/qfl-files/news/photo.webp" /></p>'
        )
        await test_session.commit()

        async def fake_get_files(_news_id: str):
            return []

        monkeypatch.setattr(FileStorageService, "get_files_by_news_id", fake_get_files)

        response = await client.get("/api/v1/news/1?lang=ru")
        assert response.status_code == 200

        data = response.json()
        content = data["content"]

        assert "localhost:9000" not in content
        assert 'src="https://kffleague.kz/storage/qfl-files/news/abc123.png"' in content
        assert 'src="https://kffleague.kz/storage/qfl-files/news/photo.webp"' in content
        # href is made relative by _to_relative_if_internal since it's an internal host
        assert "/storage/qfl-files/news/doc.pdf" in content

    async def test_get_news_item_non_minio_internal_url_unchanged(
        self,
        client: AsyncClient,
        sample_news,
        test_session,
        monkeypatch: pytest.MonkeyPatch,
    ):
        """Non-MinIO internal URLs (e.g. /uploads/) resolve to base origin without /storage/."""
        from app.services.file_storage import FileStorageService

        class FakeSettings:
            minio_public_endpoint = "https://kffleague.kz/storage"
            minio_bucket = "qfl-files"

        monkeypatch.setattr(
            "app.utils.news_html_normalizer.get_settings", lambda: FakeSettings()
        )

        sample = sample_news[0]
        sample.content = (
            '<p><img src="http://localhost:9000/uploads/photo.jpg" /></p>'
        )
        await test_session.commit()

        async def fake_get_files(_news_id: str):
            return []

        monkeypatch.setattr(FileStorageService, "get_files_by_news_id", fake_get_files)

        response = await client.get("/api/v1/news/1?lang=ru")
        assert response.status_code == 200

        data = response.json()
        content = data["content"]

        assert "localhost" not in content
        assert 'src="https://kffleague.kz/uploads/photo.jpg"' in content
