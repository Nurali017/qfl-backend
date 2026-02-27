from datetime import date
from uuid import uuid4

import pytest
from httpx import AsyncClient
from sqlalchemy import select

from app.models import AdminUser, GamePlayerStats, Language, News, Page, Player, Season
from app.models.news import ArticleType
from app.security import hash_password
from app.services.news_classifier import ClassificationDecision, NewsClassifierService


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
            "content_text": "RU content text",
            "image_url": "https://cdn.example.com/ru.jpg",
            "source_url": "https://kffleague.kz/ru/news/100",
            "publish_date": "2026-01-01",
            "is_slider": False,
        },
        "kz": {
            "title": "KZ title",
            "excerpt": "KZ excerpt",
            "content": "KZ content",
            "content_text": "KZ content text",
            "image_url": "https://cdn.example.com/kz.jpg",
            "source_url": "https://kffleague.kz/kz/news/100",
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
    assert updated["ru"]["excerpt"] == "RU excerpt"
    assert updated["ru"]["content_text"] == "RU content text"
    assert updated["ru"]["image_url"] == "https://cdn.example.com/ru.jpg"
    assert updated["ru"]["source_url"] == "https://kffleague.kz/ru/news/100"
    assert updated["kz"]["title"] == "KZ title"
    assert updated["kz"]["image_url"] == "https://cdn.example.com/kz.jpg"

    clear_image_response = await client.patch(
        f"/api/v1/admin/news/materials/{group_id}",
        headers={"Authorization": f"Bearer {token}"},
        json={"ru": {"image_url": None}},
    )
    assert clear_image_response.status_code == 200, clear_image_response.text
    cleared = clear_image_response.json()
    assert cleared["ru"]["image_url"] is None
    assert cleared["ru"]["title"] == "RU title updated"

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
async def test_news_materials_filter_article_type_and_search(
    client: AsyncClient,
    superadmin_user: AdminUser,
    test_session,
):
    token = await _login(client, superadmin_user.email, "super-secret")

    group_news = uuid4()
    group_unclassified = uuid4()
    test_session.add_all(
        [
            News(
                id=12_001,
                language=Language.RU,
                translation_group_id=group_news,
                title="Transfer news RU",
                excerpt="Official announcement",
                article_type=ArticleType.NEWS,
                publish_date=date(2026, 1, 3),
            ),
            News(
                id=12_002,
                language=Language.KZ,
                translation_group_id=group_news,
                title="Transfer news KZ",
                excerpt="Official announcement",
                article_type=ArticleType.NEWS,
                publish_date=date(2026, 1, 3),
            ),
            News(
                id=12_003,
                language=Language.RU,
                translation_group_id=group_unclassified,
                title="Pending review",
                excerpt="No type yet",
                article_type=None,
                publish_date=date(2026, 1, 4),
            ),
        ]
    )
    await test_session.commit()

    resp_unclassified = await client.get(
        "/api/v1/admin/news/materials?article_type=UNCLASSIFIED",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp_unclassified.status_code == 200, resp_unclassified.text
    data_unclassified = resp_unclassified.json()
    assert data_unclassified["total"] == 1
    assert data_unclassified["items"][0]["group_id"] == str(group_unclassified)

    resp_news = await client.get(
        "/api/v1/admin/news/materials?article_type=NEWS",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp_news.status_code == 200, resp_news.text
    data_news = resp_news.json()
    assert data_news["total"] == 1
    assert data_news["items"][0]["group_id"] == str(group_news)

    resp_search = await client.get(
        "/api/v1/admin/news/materials?search=transfer",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp_search.status_code == 200, resp_search.text
    data_search = resp_search.json()
    assert data_search["total"] == 1
    assert data_search["items"][0]["group_id"] == str(group_news)


@pytest.mark.asyncio
async def test_news_material_set_group_article_type(
    client: AsyncClient,
    superadmin_user: AdminUser,
    test_session,
):
    token = await _login(client, superadmin_user.email, "super-secret")
    group_id = uuid4()
    test_session.add_all(
        [
            News(
                id=13_001,
                language=Language.RU,
                translation_group_id=group_id,
                title="RU title",
                article_type=None,
                publish_date=date(2026, 1, 5),
            ),
            News(
                id=13_002,
                language=Language.KZ,
                translation_group_id=group_id,
                title="KZ title",
                article_type=None,
                publish_date=date(2026, 1, 5),
            ),
        ]
    )
    await test_session.commit()

    set_response = await client.patch(
        f"/api/v1/admin/news/materials/{group_id}/article-type",
        headers={"Authorization": f"Bearer {token}"},
        json={"article_type": "ANALYTICS"},
    )
    assert set_response.status_code == 200, set_response.text
    payload = set_response.json()
    assert payload["ru"]["article_type"] == "ANALYTICS"
    assert payload["kz"]["article_type"] == "ANALYTICS"

    reset_response = await client.patch(
        f"/api/v1/admin/news/materials/{group_id}/article-type",
        headers={"Authorization": f"Bearer {token}"},
        json={"article_type": None},
    )
    assert reset_response.status_code == 200, reset_response.text
    payload_reset = reset_response.json()
    assert payload_reset["ru"]["article_type"] is None
    assert payload_reset["kz"]["article_type"] is None


@pytest.mark.asyncio
async def test_news_materials_classify_dry_run_and_apply(
    client: AsyncClient,
    superadmin_user: AdminUser,
    test_session,
    monkeypatch,
):
    token = await _login(client, superadmin_user.email, "super-secret")

    analytical_group = uuid4()
    review_group = uuid4()
    test_session.add_all(
        [
            News(
                id=14_001,
                language=Language.RU,
                translation_group_id=analytical_group,
                title="Тактический анализ тура",
                content_text="Подробный разбор игры",
                article_type=None,
                publish_date=date(2026, 1, 10),
            ),
            News(
                id=14_002,
                language=Language.KZ,
                translation_group_id=analytical_group,
                title="Матч талдауы",
                content_text="Ойын сараптамасы",
                article_type=None,
                publish_date=date(2026, 1, 10),
            ),
            News(
                id=14_003,
                language=Language.RU,
                translation_group_id=review_group,
                title="Сомнительный материал",
                content_text="Короткий текст",
                article_type=None,
                publish_date=date(2026, 1, 11),
            ),
        ]
    )
    await test_session.commit()

    async def fake_classify_group(self, items, min_confidence=0.7):
        ru_item = next((item for item in items if item.language == Language.RU), items[0])
        title = (ru_item.title or "").lower()
        if "анализ" in title or "талдау" in title:
            return ClassificationDecision(
                article_type=ArticleType.ANALYTICS,
                confidence=0.92,
                source="rules",
                representative_news_id=ru_item.id,
                representative_title=ru_item.title,
            )
        return ClassificationDecision(
            article_type=None,
            confidence=0.45,
            source="rules",
            reason="low_confidence",
            representative_news_id=ru_item.id,
            representative_title=ru_item.title,
        )

    monkeypatch.setattr(NewsClassifierService, "classify_group", fake_classify_group)

    dry_run_response = await client.post(
        "/api/v1/admin/news/materials/classify",
        headers={"Authorization": f"Bearer {token}"},
        json={"apply": False, "only_unclassified": True, "min_confidence": 0.7},
    )
    assert dry_run_response.status_code == 200, dry_run_response.text
    dry_run_payload = dry_run_response.json()
    assert dry_run_payload["summary"]["dry_run"] is True
    assert dry_run_payload["summary"]["total_groups"] == 2
    assert dry_run_payload["summary"]["updated_groups"] == 1
    assert dry_run_payload["summary"]["needs_review_count"] == 1

    verify_dry_run = await test_session.execute(
        select(News).where(News.translation_group_id == analytical_group)
    )
    assert all(item.article_type is None for item in verify_dry_run.scalars().all())

    apply_response = await client.post(
        "/api/v1/admin/news/materials/classify",
        headers={"Authorization": f"Bearer {token}"},
        json={"apply": True, "only_unclassified": True, "min_confidence": 0.7},
    )
    assert apply_response.status_code == 200, apply_response.text
    apply_payload = apply_response.json()
    assert apply_payload["summary"]["dry_run"] is False
    assert apply_payload["summary"]["updated_groups"] == 1
    assert apply_payload["summary"]["needs_review_count"] == 1

    verify_apply = await test_session.execute(
        select(News).where(News.translation_group_id == analytical_group)
    )
    applied_items = verify_apply.scalars().all()
    assert all(item.article_type == ArticleType.ANALYTICS for item in applied_items)


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
async def test_admin_players_meta_excludes_hidden_seasons(
    client: AsyncClient,
    superadmin_user: AdminUser,
    test_session,
    sample_championship,
    sample_season,
):
    token = await _login(client, superadmin_user.email, "super-secret")
    sample_season.is_visible = False
    test_session.add(
        Season(
            id=62,
            name="2026",
            championship_id=sample_championship.id,
            date_start=date(2026, 3, 1),
            date_end=date(2026, 11, 30),
            is_visible=True,
        )
    )
    await test_session.commit()

    response = await client.get(
        "/api/v1/admin/players/meta",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 200, response.text
    season_ids = [item["id"] for item in response.json()["seasons"]]
    assert season_ids == [62]


@pytest.mark.asyncio
async def test_admin_seasons_exclude_hidden_and_return_404_for_hidden_id(
    client: AsyncClient,
    superadmin_user: AdminUser,
    test_session,
    sample_championship,
    sample_season,
):
    token = await _login(client, superadmin_user.email, "super-secret")
    sample_season.is_visible = False
    test_session.add(
        Season(
            id=62,
            name="2026",
            championship_id=sample_championship.id,
            date_start=date(2026, 3, 1),
            date_end=date(2026, 11, 30),
            is_visible=True,
        )
    )
    await test_session.commit()

    list_response = await client.get(
        "/api/v1/admin/seasons",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert list_response.status_code == 200, list_response.text
    list_data = list_response.json()
    assert list_data["total"] == 1
    assert [item["id"] for item in list_data["items"]] == [62]

    hidden_response = await client.get(
        f"/api/v1/admin/seasons/{sample_season.id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert hidden_response.status_code == 404
    assert hidden_response.json()["detail"] == "Season not found"


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
            "photo_url": "https://example.com/player.jpg",
        },
    )
    assert patch_response.status_code == 200, patch_response.text
    updated = patch_response.json()
    assert updated["first_name"] == "New"
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
