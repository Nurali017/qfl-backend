from datetime import date, time

import pytest
from httpx import AsyncClient

from app.models import (
    AdminUser,
    Championship,
    Club,
    Game,
    Season,
    SeasonParticipant,
    Stadium,
    Team,
)
from app.security import hash_password

BASE = "/api/v1/admin/stadiums"


@pytest.fixture
async def superadmin_user(test_session) -> AdminUser:
    user = AdminUser(
        email="superadmin-st@test.local",
        password_hash=hash_password("super-secret"),
        role="superadmin",
        is_active=True,
    )
    test_session.add(user)
    await test_session.commit()
    await test_session.refresh(user)
    return user


@pytest.fixture
async def operator_user(test_session) -> AdminUser:
    user = AdminUser(
        email="operator-st@test.local",
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


def _auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


async def _make_stadium(test_session, **kwargs) -> Stadium:
    stadium = Stadium(name=kwargs.pop("name", "Test Arena"), **kwargs)
    test_session.add(stadium)
    await test_session.commit()
    await test_session.refresh(stadium)
    return stadium


# --- list ---

@pytest.mark.asyncio
async def test_list_returns_all_stadiums(client, superadmin_user, test_session):
    await _make_stadium(test_session, name="Astana Arena")
    await _make_stadium(test_session, name="Almaty Central")
    await _make_stadium(test_session, name="Shymkent Stadium")

    token = await _login(client, superadmin_user.email, "super-secret")
    res = await client.get(BASE, headers=_auth(token))
    assert res.status_code == 200, res.text
    data = res.json()
    assert data["total"] == 3
    assert len(data["items"]) == 3
    # dropdowns rely on id + name being present
    for item in data["items"]:
        assert "id" in item and "name" in item


# --- create ---

@pytest.mark.asyncio
async def test_create_stadium(client, superadmin_user):
    token = await _login(client, superadmin_user.email, "super-secret")
    payload = {
        "name": "New Arena",
        "name_ru": "Новая Арена",
        "city": "Astana",
        "capacity": 30000,
        "field_type": "natural",
    }
    res = await client.post(BASE, json=payload, headers=_auth(token))
    assert res.status_code == 201, res.text
    body = res.json()
    assert body["name"] == "New Arena"
    assert body["capacity"] == 30000
    assert body["field_type"] == "natural"
    assert body["id"] > 0


@pytest.mark.asyncio
async def test_create_invalid_field_type_returns_422(client, superadmin_user):
    token = await _login(client, superadmin_user.email, "super-secret")
    res = await client.post(
        BASE,
        json={"name": "Arena", "field_type": "concrete"},
        headers=_auth(token),
    )
    assert res.status_code == 422, res.text


@pytest.mark.asyncio
async def test_create_blank_name_returns_422(client, superadmin_user):
    token = await _login(client, superadmin_user.email, "super-secret")
    res = await client.post(BASE, json={"name": "   "}, headers=_auth(token))
    assert res.status_code == 422, res.text


@pytest.mark.asyncio
async def test_create_negative_capacity_returns_422(client, superadmin_user):
    token = await _login(client, superadmin_user.email, "super-secret")
    res = await client.post(
        BASE,
        json={"name": "Arena", "capacity": -5},
        headers=_auth(token),
    )
    assert res.status_code == 422, res.text


@pytest.mark.asyncio
async def test_create_empty_field_type_becomes_null(client, superadmin_user):
    token = await _login(client, superadmin_user.email, "super-secret")
    res = await client.post(
        BASE,
        json={"name": "Arena", "field_type": ""},
        headers=_auth(token),
    )
    assert res.status_code == 201, res.text
    assert res.json()["field_type"] is None


# --- detail ---

@pytest.mark.asyncio
async def test_get_detail_with_counts(client, superadmin_user, test_session):
    stadium = await _make_stadium(test_session, name="Detail Arena", capacity=100)
    test_session.add(Game(date=date(2025, 5, 1), time=time(18, 0), stadium_id=stadium.id))
    test_session.add(Team(name="Ref Team", stadium_id=stadium.id))
    await test_session.commit()

    token = await _login(client, superadmin_user.email, "super-secret")
    res = await client.get(f"{BASE}/{stadium.id}", headers=_auth(token))
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["id"] == stadium.id
    assert body["games_count"] == 1
    assert body["teams_count"] == 1
    assert body["clubs_count"] == 0
    assert body["participants_count"] == 0


@pytest.mark.asyncio
async def test_get_detail_404(client, superadmin_user):
    token = await _login(client, superadmin_user.email, "super-secret")
    res = await client.get(f"{BASE}/999999", headers=_auth(token))
    assert res.status_code == 404


# --- patch ---

@pytest.mark.asyncio
async def test_patch_updates_all_fields(client, superadmin_user, test_session):
    stadium = await _make_stadium(test_session, name="Old Name", capacity=10)
    token = await _login(client, superadmin_user.email, "super-secret")
    res = await client.patch(
        f"{BASE}/{stadium.id}",
        json={
            "name": "Updated Name",
            "city": "Almaty",
            "capacity": 25000,
            "field_type": "artificial",
        },
        headers=_auth(token),
    )
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["name"] == "Updated Name"
    assert body["city"] == "Almaty"
    assert body["capacity"] == 25000
    assert body["field_type"] == "artificial"


@pytest.mark.asyncio
async def test_patch_explicit_null_name_returns_422(client, superadmin_user, test_session):
    stadium = await _make_stadium(test_session, name="Keep Name")
    token = await _login(client, superadmin_user.email, "super-secret")
    res = await client.patch(
        f"{BASE}/{stadium.id}",
        json={"name": None, "city": "Almaty"},
        headers=_auth(token),
    )
    assert res.status_code == 422, res.text


@pytest.mark.asyncio
async def test_patch_omitted_name_is_preserved(client, superadmin_user, test_session):
    stadium = await _make_stadium(test_session, name="Original Name")
    token = await _login(client, superadmin_user.email, "super-secret")
    res = await client.patch(
        f"{BASE}/{stadium.id}",
        json={"city": "Shymkent"},
        headers=_auth(token),
    )
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["name"] == "Original Name"
    assert body["city"] == "Shymkent"


@pytest.mark.asyncio
async def test_patch_invalid_field_type_returns_422(client, superadmin_user, test_session):
    stadium = await _make_stadium(test_session, name="Patch Arena")
    token = await _login(client, superadmin_user.email, "super-secret")
    res = await client.patch(
        f"{BASE}/{stadium.id}",
        json={"field_type": "grass"},
        headers=_auth(token),
    )
    assert res.status_code == 422, res.text


# --- delete ---

@pytest.mark.asyncio
async def test_delete_without_references_ok(client, superadmin_user, test_session):
    stadium = await _make_stadium(test_session, name="Deletable Arena")
    token = await _login(client, superadmin_user.email, "super-secret")
    res = await client.delete(f"{BASE}/{stadium.id}", headers=_auth(token))
    assert res.status_code == 200, res.text


@pytest.mark.asyncio
async def test_delete_blocked_by_game(client, superadmin_user, test_session):
    stadium = await _make_stadium(test_session, name="Game Arena")
    test_session.add(Game(date=date(2025, 6, 1), time=time(20, 0), stadium_id=stadium.id))
    await test_session.commit()

    token = await _login(client, superadmin_user.email, "super-secret")
    res = await client.delete(f"{BASE}/{stadium.id}", headers=_auth(token))
    assert res.status_code == 409, res.text
    assert "games=1" in res.json()["detail"]


@pytest.mark.asyncio
async def test_delete_blocked_by_team(client, superadmin_user, test_session):
    stadium = await _make_stadium(test_session, name="Team Arena")
    test_session.add(Team(name="Home FC", stadium_id=stadium.id))
    await test_session.commit()

    token = await _login(client, superadmin_user.email, "super-secret")
    res = await client.delete(f"{BASE}/{stadium.id}", headers=_auth(token))
    assert res.status_code == 409, res.text
    assert "teams=1" in res.json()["detail"]


@pytest.mark.asyncio
async def test_delete_blocked_by_club(client, superadmin_user, test_session):
    stadium = await _make_stadium(test_session, name="Club Arena")
    test_session.add(Club(name="Big Club", stadium_id=stadium.id))
    await test_session.commit()

    token = await _login(client, superadmin_user.email, "super-secret")
    res = await client.delete(f"{BASE}/{stadium.id}", headers=_auth(token))
    assert res.status_code == 409, res.text
    assert "clubs=1" in res.json()["detail"]


@pytest.mark.asyncio
async def test_delete_blocked_by_season_participant(client, superadmin_user, test_session):
    stadium = await _make_stadium(test_session, name="Participant Arena")
    championship = Championship(name="Premier League")
    test_session.add(championship)
    await test_session.commit()
    await test_session.refresh(championship)
    season = Season(
        name="2025",
        championship_id=championship.id,
        date_start=date(2025, 3, 1),
        date_end=date(2025, 11, 1),
    )
    team = Team(name="Participant FC")
    test_session.add_all([season, team])
    await test_session.commit()
    await test_session.refresh(season)
    await test_session.refresh(team)
    test_session.add(
        SeasonParticipant(team_id=team.id, season_id=season.id, stadium_id=stadium.id)
    )
    await test_session.commit()

    token = await _login(client, superadmin_user.email, "super-secret")
    res = await client.delete(f"{BASE}/{stadium.id}", headers=_auth(token))
    assert res.status_code == 409, res.text
    assert "season participants=1" in res.json()["detail"]


# --- auth gating ---

@pytest.mark.asyncio
async def test_operator_can_read(client, operator_user, test_session):
    await _make_stadium(test_session, name="Operator-Visible Arena")
    token = await _login(client, operator_user.email, "operator-secret")
    res = await client.get(BASE, headers=_auth(token))
    assert res.status_code == 200, res.text


@pytest.mark.asyncio
async def test_operator_cannot_create(client, operator_user):
    token = await _login(client, operator_user.email, "operator-secret")
    res = await client.post(BASE, json={"name": "Nope Arena"}, headers=_auth(token))
    assert res.status_code == 403, res.text


@pytest.mark.asyncio
async def test_operator_cannot_delete(client, operator_user, test_session):
    stadium = await _make_stadium(test_session, name="Protected Arena")
    token = await _login(client, operator_user.email, "operator-secret")
    res = await client.delete(f"{BASE}/{stadium.id}", headers=_auth(token))
    assert res.status_code == 403, res.text


@pytest.mark.asyncio
async def test_unauthenticated_rejected(client):
    res = await client.get(BASE)
    assert res.status_code == 401
