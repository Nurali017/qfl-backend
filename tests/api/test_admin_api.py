from datetime import date
from uuid import uuid4

import pytest
from httpx import AsyncClient

from app.models import AdminUser, GamePlayerStats, Language, News, Page, Player, Season
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


@pytest.mark.asyncio
async def test_admin_players_rbac_editor_allowed_operator_forbidden(
    client: AsyncClient,
    editor_user: AdminUser,
    operator_user: AdminUser,
):
    editor_token = await _login(client, editor_user.email, "editor-secret")
    editor_response = await client.get(
        "/api/v1/admin/players/meta",
        headers={"Authorization": f"Bearer {editor_token}"},
    )
    assert editor_response.status_code == 200, editor_response.text

    operator_token = await _login(client, operator_user.email, "operator-secret")
    operator_response = await client.get(
        "/api/v1/admin/players/meta",
        headers={"Authorization": f"Bearer {operator_token}"},
    )
    assert operator_response.status_code == 403


@pytest.mark.asyncio
async def test_admin_players_create_without_and_with_sota(
    client: AsyncClient,
    superadmin_user: AdminUser,
):
    token = await _login(client, superadmin_user.email, "super-secret")

    create_without_sota = await client.post(
        "/api/v1/admin/players",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "first_name": "Manual",
            "last_name": "Player",
            "team_bindings": [],
        },
    )
    assert create_without_sota.status_code == 201, create_without_sota.text
    without_data = create_without_sota.json()
    assert isinstance(without_data["id"], int)
    assert without_data["sota_id"] is None

    sota_uuid = str(uuid4())
    create_with_sota = await client.post(
        "/api/v1/admin/players",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "first_name": "Linked",
            "last_name": "Player",
            "sota_id": sota_uuid,
            "team_bindings": [],
        },
    )
    assert create_with_sota.status_code == 201, create_with_sota.text
    with_data = create_with_sota.json()
    assert isinstance(with_data["id"], int)
    assert with_data["sota_id"] == sota_uuid


@pytest.mark.asyncio
async def test_admin_players_create_and_patch_team_bindings_replace_all(
    client: AsyncClient,
    superadmin_user: AdminUser,
    test_session,
    sample_championship,
    sample_season,
    sample_teams,
):
    token = await _login(client, superadmin_user.email, "super-secret")

    season_two = Season(
        id=62,
        name="2026",
        championship_id=sample_championship.id,
        date_start=date(2026, 3, 1),
        date_end=date(2026, 11, 30),
    )
    test_session.add(season_two)
    await test_session.commit()

    create_response = await client.post(
        "/api/v1/admin/players",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "first_name": "Bind",
            "last_name": "Target",
            "team_bindings": [
                {"team_id": sample_teams[0].id, "season_id": sample_season.id, "number": 10},
                {"team_id": sample_teams[1].id, "season_id": season_two.id, "number": 77},
            ],
        },
    )
    assert create_response.status_code == 201, create_response.text
    created = create_response.json()
    assert len(created["team_bindings"]) == 2
    player_id = created["id"]

    patch_response = await client.patch(
        f"/api/v1/admin/players/{player_id}",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "first_name": "Bind-Updated",
            "team_bindings": [
                {"team_id": sample_teams[2].id, "season_id": sample_season.id, "number": 9},
            ],
        },
    )
    assert patch_response.status_code == 200, patch_response.text
    patched = patch_response.json()
    assert patched["first_name"] == "Bind-Updated"
    assert len(patched["team_bindings"]) == 1
    assert patched["team_bindings"][0]["team_id"] == sample_teams[2].id

    clear_response = await client.patch(
        f"/api/v1/admin/players/{player_id}",
        headers={"Authorization": f"Bearer {token}"},
        json={"team_bindings": []},
    )
    assert clear_response.status_code == 200, clear_response.text
    cleared = clear_response.json()
    assert cleared["team_bindings"] == []


@pytest.mark.asyncio
async def test_admin_players_patch_profile_fields(
    client: AsyncClient,
    superadmin_user: AdminUser,
):
    token = await _login(client, superadmin_user.email, "super-secret")

    create_response = await client.post(
        "/api/v1/admin/players",
        headers={"Authorization": f"Bearer {token}"},
        json={"first_name": "Old", "last_name": "Name", "team_bindings": []},
    )
    assert create_response.status_code == 201, create_response.text
    created = create_response.json()

    patch_response = await client.patch(
        f"/api/v1/admin/players/{created['id']}",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "first_name": "New",
            "last_name": "Name",
            "age": 27,
            "photo_url": "https://example.com/player.jpg",
        },
    )
    assert patch_response.status_code == 200, patch_response.text
    updated = patch_response.json()
    assert updated["first_name"] == "New"
    assert updated["age"] == 27
    assert updated["photo_url"] == "https://example.com/player.jpg"


@pytest.mark.asyncio
async def test_admin_players_delete_success_without_dependencies(
    client: AsyncClient,
    superadmin_user: AdminUser,
):
    token = await _login(client, superadmin_user.email, "super-secret")

    create_response = await client.post(
        "/api/v1/admin/players",
        headers={"Authorization": f"Bearer {token}"},
        json={"first_name": "Delete", "last_name": "Me", "team_bindings": []},
    )
    assert create_response.status_code == 201, create_response.text
    player_id = create_response.json()["id"]

    delete_response = await client.delete(
        f"/api/v1/admin/players/{player_id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert delete_response.status_code == 200, delete_response.text

    get_response = await client.get(
        f"/api/v1/admin/players/{player_id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert get_response.status_code == 404


@pytest.mark.asyncio
async def test_admin_players_delete_returns_409_on_dependencies(
    client: AsyncClient,
    superadmin_user: AdminUser,
    test_session,
    sample_game,
    sample_teams,
):
    token = await _login(client, superadmin_user.email, "super-secret")

    player = Player(first_name="Busy", last_name="Player")
    test_session.add(player)
    await test_session.flush()

    test_session.add(
        GamePlayerStats(
            game_id=sample_game.id,
            player_id=player.id,
            team_id=sample_teams[0].id,
        )
    )
    await test_session.commit()

    delete_response = await client.delete(
        f"/api/v1/admin/players/{player.id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert delete_response.status_code == 409
    assert "Cannot delete player with dependencies" in delete_response.json()["detail"]
