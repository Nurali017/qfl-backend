"""Integration tests: admin/news endpoints must run content through sanitize_news_html.

XSS payloads written via POST/PATCH /api/v1/admin/news/materials must be
returned (and stored) sanitized. This protects /news/{id} consumers that render
content via dangerouslySetInnerHTML.
"""
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from httpx import AsyncClient
from sqlalchemy import select

from app.models import AdminUser, Language, News
from app.security import hash_password


def _ru_lang() -> Language:
    return Language.RU


@pytest.fixture
async def editor_user(test_session):
    user = AdminUser(
        email="sanitize-editor@test.local",
        password_hash=hash_password("editor-secret"),
        role="editor",
        is_active=True,
    )
    test_session.add(user)
    await test_session.commit()
    await test_session.refresh(user)
    return user


async def _login(client: AsyncClient, email: str, password: str) -> str:
    response = await client.post(
        "/api/v1/admin/auth/login",
        json={"email": email, "password": password},
    )
    assert response.status_code == 200, response.text
    return response.json()["access_token"]


@pytest.mark.asyncio
async def test_create_material_sanitizes_xss_in_content(
    client: AsyncClient,
    editor_user: AdminUser,
    test_session,
):
    token = await _login(client, editor_user.email, "editor-secret")

    evil = (
        "<p>safe text</p>"
        "<script>alert('xss')</script>"
        "<img src=x onerror=\"alert(1)\">"
        "<a href=\"javascript:alert(2)\">click</a>"
    )

    response = await client.post(
        "/api/v1/admin/news/materials",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "ru": {
                "title": "ru",
                "excerpt": "ex",
                "content": evil,
                "publish_date": "2026-01-01",
                "is_slider": False,
            },
            "kz": {
                "title": "kz",
                "excerpt": "ex",
                "content": evil,
                "publish_date": "2026-01-01",
                "is_slider": False,
            },
        },
    )
    assert response.status_code == 201, response.text
    body = response.json()

    for lang_key in ("ru", "kz"):
        content = body[lang_key]["content"] or ""
        assert "<script>" not in content
        assert "alert('xss')" not in content
        assert "onerror" not in content
        assert "javascript:" not in content
        assert "safe text" in content

    # Defense-in-depth: verify storage matches API response.
    group_id = UUID(body["group_id"])
    rows = (await test_session.execute(
        select(News).where(News.translation_group_id == group_id)
    )).scalars().all()
    assert len(rows) == 2
    for row in rows:
        stored = row.content or ""
        assert "<script>" not in stored
        assert "onerror" not in stored
        assert "javascript:" not in stored


@pytest.mark.asyncio
async def test_apply_payload_sanitizes_content_field():
    """Direct test on _apply_payload — the helper called by both POST and PATCH.

    Exercising via the PATCH endpoint hits a pre-existing tz-aware/tz-naive
    comparison bug in _to_material_response on SQLite. The sanitization
    wiring is the same code path either way: this verifies it.
    """
    from app.api.admin.news import _apply_payload
    from app.models.news import News as NewsModel
    from app.schemas.admin.news import AdminNewsTranslationPatchPayload

    item = NewsModel(id=1, language=Language.RU, content="<p>old</p>")
    payload = AdminNewsTranslationPatchPayload(
        content="<p>new</p><script>alert(1)</script><iframe src=\"https://evil.com\"></iframe>",
    )
    await _apply_payload(item, payload, admin_id=1, db=None, partial=True)
    assert "<script>" not in (item.content or "")
    assert "alert(1)" not in (item.content or "")
    assert "evil.com" not in (item.content or "")
    assert "<iframe" not in (item.content or "")
    assert "new" in item.content


@pytest.mark.asyncio
async def test_public_news_endpoint_sanitizes_on_read(
    client: AsyncClient,
    test_session,
    monkeypatch,
):
    """Defense-in-depth: unsafe HTML written directly to DB (bypassing the
    admin sanitizer) must be cleaned before reaching the public reader.
    """
    from datetime import date
    from app.services.file_storage import FileStorageService

    monkeypatch.setattr(
        FileStorageService,
        "get_files_by_news_id",
        AsyncMock(return_value=[]),
    )

    dirty = (
        "<p>safe</p>"
        "<script>alert('legacy-row')</script>"
        "<iframe src=\"https://evil.com\"></iframe>"
        "<a href=\"javascript:alert(1)\">link</a>"
    )
    item = News(
        id=99001,
        language=Language.RU,
        title="legacy",
        excerpt="ex",
        content=dirty,
        content_text="legacy text",
        article_type=None,
        publish_date=date(2026, 1, 1),
    )
    test_session.add(item)
    await test_session.commit()

    response = await client.get(f"/api/v1/news/{item.id}?lang=ru")
    assert response.status_code == 200, response.text
    body = response.json()
    content = body.get("content") or ""

    assert "<script>" not in content
    assert "alert(" not in content
    assert "evil.com" not in content
    assert "javascript:" not in content
    assert "safe" in content


@pytest.mark.asyncio
async def test_youtube_iframe_preserved_through_admin(
    client: AsyncClient,
    editor_user: AdminUser,
):
    token = await _login(client, editor_user.email, "editor-secret")

    content = (
        "<p>Watch:</p>"
        "<iframe src=\"https://www.youtube.com/embed/abc123\" allowfullscreen></iframe>"
    )

    response = await client.post(
        "/api/v1/admin/news/materials",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "ru": {
                "title": "yt",
                "excerpt": "ex",
                "content": content,
                "publish_date": "2026-01-01",
                "is_slider": False,
            },
            "kz": {
                "title": "yt",
                "excerpt": "ex",
                "content": content,
                "publish_date": "2026-01-01",
                "is_slider": False,
            },
        },
    )
    assert response.status_code == 201, response.text
    saved = response.json()["ru"]["content"] or ""
    assert "youtube.com/embed/abc123" in saved
    assert "<iframe" in saved
