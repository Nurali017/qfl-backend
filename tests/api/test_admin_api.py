from datetime import date
from uuid import uuid4

import pytest
from httpx import AsyncClient

from app.models import AdminUser, Language, News, Page
from app.security import hash_password


@pytest.fixture
async def superadmin_user(test_session):
    user = AdminUser(
        email="superadmin@test.local",
        password_hash=hash_password("super-secret"),
        role="superadmin",
        is_active=True,
    )
    test_session.add(user)
    await test_session.commit()
    await test_session.refresh(user)
    return user


@pytest.fixture
async def editor_user(test_session):
    user = AdminUser(
        email="editor@test.local",
        password_hash=hash_password("editor-secret"),
        role="editor",
        is_active=True,
    )
    test_session.add(user)
    await test_session.commit()
    await test_session.refresh(user)
    return user


@pytest.fixture
async def operator_user(test_session):
    user = AdminUser(
        email="operator@test.local",
        password_hash=hash_password("operator-secret"),
        role="operator",
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
async def test_admin_login_me_and_refresh(client: AsyncClient, superadmin_user: AdminUser):
    token = await _login(client, superadmin_user.email, "super-secret")

    me_response = await client.get(
        "/api/v1/admin/auth/me",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert me_response.status_code == 200
    assert me_response.json()["email"] == superadmin_user.email

    refresh_response = await client.post("/api/v1/admin/auth/refresh")
    assert refresh_response.status_code == 200
    assert "access_token" in refresh_response.json()


@pytest.mark.asyncio
async def test_editor_cannot_manage_users(client: AsyncClient, editor_user: AdminUser):
    token = await _login(client, editor_user.email, "editor-secret")

    response = await client.get(
        "/api/v1/admin/users",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_news_material_create_update_and_add_translation(
    client: AsyncClient,
    superadmin_user: AdminUser,
    test_session,
):
    token = await _login(client, superadmin_user.email, "super-secret")

    create_payload = {
        "ru": {
            "title": "RU title",
            "excerpt": "RU excerpt",
            "content": "RU content",
            "publish_date": "2026-01-01",
            "is_slider": False,
        },
        "kz": {
            "title": "KZ title",
            "excerpt": "KZ excerpt",
            "content": "KZ content",
            "publish_date": "2026-01-01",
            "is_slider": False,
        },
    }

    create_response = await client.post(
        "/api/v1/admin/news/materials",
        headers={"Authorization": f"Bearer {token}"},
        json=create_payload,
    )
    assert create_response.status_code == 201, create_response.text
    material = create_response.json()
    group_id = material["group_id"]
    assert material["ru"]["title"] == "RU title"
    assert material["kz"]["title"] == "KZ title"

    patch_response = await client.patch(
        f"/api/v1/admin/news/materials/{group_id}",
        headers={"Authorization": f"Bearer {token}"},
        json={"ru": {"title": "RU title updated", "is_slider": True}},
    )
    assert patch_response.status_code == 200, patch_response.text
    updated = patch_response.json()
    assert updated["ru"]["title"] == "RU title updated"
    assert updated["kz"]["title"] == "KZ title"

    legacy_group = uuid4()
    legacy_ru = News(
        id=10_001,
        language=Language.RU,
        translation_group_id=legacy_group,
        title="Legacy RU",
        excerpt="Legacy",
        content="Legacy",
        publish_date=date(2026, 1, 2),
    )
    test_session.add(legacy_ru)
    await test_session.commit()

    add_translation = await client.post(
        f"/api/v1/admin/news/materials/{legacy_group}/translation/kz",
        headers={"Authorization": f"Bearer {token}"},
        json={"data": {"title": "Legacy KZ", "content": "KZ content", "is_slider": False}},
    )
    assert add_translation.status_code == 200, add_translation.text
    added = add_translation.json()
    assert added["ru"]["title"] == "Legacy RU"
    assert added["kz"]["title"] == "Legacy KZ"


@pytest.mark.asyncio
async def test_pages_material_update_and_add_translation(
    client: AsyncClient,
    superadmin_user: AdminUser,
    test_session,
):
    token = await _login(client, superadmin_user.email, "super-secret")

    group_id = uuid4()
    ru_page = Page(
        translation_group_id=group_id,
        language=Language.RU,
        slug="kontakty-ru",
        title="Contacts RU",
        content="RU content",
    )
    test_session.add(ru_page)
    await test_session.commit()

    add_kz = await client.post(
        f"/api/v1/admin/pages/materials/{group_id}/translation/kz",
        headers={"Authorization": f"Bearer {token}"},
        json={"data": {"slug": "kontakty-kz", "title": "Contacts KZ", "content": "KZ content"}},
    )
    assert add_kz.status_code == 200, add_kz.text
    material = add_kz.json()
    assert material["ru"]["title"] == "Contacts RU"
    assert material["kz"]["title"] == "Contacts KZ"

    update = await client.put(
        f"/api/v1/admin/pages/materials/{group_id}",
        headers={"Authorization": f"Bearer {token}"},
        json={"ru": {"slug": "kontakty-ru", "title": "Contacts RU Updated", "content": "updated"}},
    )
    assert update.status_code == 200, update.text
    updated = update.json()
    assert updated["ru"]["title"] == "Contacts RU Updated"


@pytest.mark.asyncio
async def test_ops_rbac_and_operator_access(
    client: AsyncClient,
    editor_user: AdminUser,
    operator_user: AdminUser,
    monkeypatch,
):
    editor_token = await _login(client, editor_user.email, "editor-secret")

    forbidden = await client.post(
        "/api/v1/admin/ops/sync/full",
        headers={"Authorization": f"Bearer {editor_token}"},
    )
    assert forbidden.status_code == 403

    async def fake_full_sync(_self, season_id: int):
        return {"season_id": season_id, "ok": True}

    monkeypatch.setattr("app.api.admin.ops.SyncOrchestrator.full_sync", fake_full_sync)

    operator_token = await _login(client, operator_user.email, "operator-secret")
    allowed = await client.post(
        "/api/v1/admin/ops/sync/full?season_id=61",
        headers={"Authorization": f"Bearer {operator_token}"},
    )
    assert allowed.status_code == 200, allowed.text
    assert allowed.json()["status"] == "success"
