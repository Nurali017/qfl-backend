from datetime import date
from unittest.mock import AsyncMock

import pytest
from httpx import AsyncClient

from app.models import AdminUser, Championship
from app.security import hash_password


async def _login(client: AsyncClient, email: str, password: str) -> str:
    response = await client.post(
        "/api/v1/admin/auth/login",
        json={"email": email, "password": password},
    )
    assert response.status_code == 200, response.text
    return response.json()["access_token"]


@pytest.fixture
async def superadmin_user(test_session):
    user = AdminUser(
        email="season-cache-admin@test.local",
        password_hash=hash_password("super-secret"),
        role="superadmin",
        is_active=True,
    )
    test_session.add(user)
    await test_session.commit()
    await test_session.refresh(user)
    return user


@pytest.mark.asyncio
async def test_get_seasons_cache_hit_skips_db(client: AsyncClient, sample_season, test_session):
    first = await client.get("/api/v1/seasons")
    assert first.status_code == 200

    test_session.execute = AsyncMock(side_effect=AssertionError("cache hit should skip db"))  # type: ignore[method-assign]
    second = await client.get("/api/v1/seasons")

    assert second.status_code == 200
    assert second.json() == first.json()


@pytest.mark.asyncio
async def test_get_season_detail_cache_hit_skips_db(client: AsyncClient, sample_season, test_session):
    first = await client.get(f"/api/v1/seasons/{sample_season.id}")
    assert first.status_code == 200

    test_session.execute = AsyncMock(side_effect=AssertionError("detail cache hit should skip db"))  # type: ignore[method-assign]
    second = await client.get(f"/api/v1/seasons/{sample_season.id}")

    assert second.status_code == 200
    assert second.json() == first.json()


@pytest.mark.asyncio
async def test_public_patch_invalidates_season_cache(client: AsyncClient, sample_season):
    initial = await client.get(f"/api/v1/seasons/{sample_season.id}")
    assert initial.status_code == 200
    assert initial.json()["sync_enabled"] is True

    patch_response = await client.patch(
        f"/api/v1/seasons/{sample_season.id}/sync",
        json={"sync_enabled": False},
    )
    assert patch_response.status_code == 200
    assert patch_response.json()["sync_enabled"] is False

    refreshed = await client.get(f"/api/v1/seasons/{sample_season.id}")
    assert refreshed.status_code == 200
    assert refreshed.json()["sync_enabled"] is False


@pytest.mark.asyncio
async def test_admin_create_invalidates_seasons_list_cache(
    client: AsyncClient,
    sample_championship: Championship,
    superadmin_user: AdminUser,
):
    initial = await client.get("/api/v1/seasons")
    assert initial.status_code == 200
    assert initial.json()["total"] == 0

    token = await _login(client, superadmin_user.email, "super-secret")
    response = await client.post(
        "/api/v1/admin/seasons",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "id": 401,
            "name": "Cup 2026",
            "championship_id": sample_championship.id,
            "date_start": date(2026, 3, 1).isoformat(),
            "date_end": date(2026, 11, 30).isoformat(),
            "frontend_code": "cup",
            "has_table": False,
            "has_bracket": True,
        },
    )

    assert response.status_code == 201, response.text
    refreshed = await client.get("/api/v1/seasons")
    assert refreshed.status_code == 200
    assert any(item["id"] == 401 for item in refreshed.json()["items"])


@pytest.mark.asyncio
async def test_admin_update_invalidates_season_detail_cache(
    client: AsyncClient,
    sample_season,
    superadmin_user: AdminUser,
):
    initial = await client.get(f"/api/v1/seasons/{sample_season.id}")
    assert initial.status_code == 200
    assert initial.json()["name"] == "2025"

    token = await _login(client, superadmin_user.email, "super-secret")
    response = await client.patch(
        f"/api/v1/admin/seasons/{sample_season.id}",
        headers={"Authorization": f"Bearer {token}"},
        json={"name": "2025 Updated"},
    )

    assert response.status_code == 200, response.text
    refreshed = await client.get(f"/api/v1/seasons/{sample_season.id}")
    assert refreshed.status_code == 200
    assert refreshed.json()["name"] == "2025 Updated"
