from datetime import date

import pytest
from httpx import AsyncClient

from app.models import (
    AdminUser,
    Championship,
    CupDraw,
    Season,
    SeasonParticipant,
    Team,
)
from app.security import hash_password


@pytest.fixture
async def superadmin_user(test_session):
    user = AdminUser(
        email="draw-superadmin@test.local",
        password_hash=hash_password("super-secret"),
        role="superadmin",
        is_active=True,
    )
    test_session.add(user)
    await test_session.commit()
    await test_session.refresh(user)
    return user


@pytest.fixture
async def operator_user(test_session):
    user = AdminUser(
        email="draw-operator@test.local",
        password_hash=hash_password("operator-secret"),
        role="operator",
        is_active=True,
    )
    test_session.add(user)
    await test_session.commit()
    await test_session.refresh(user)
    return user


@pytest.fixture
async def cup_championship(test_session):
    championship = Championship(
        id=3001,
        name="Cup Championship",
    )
    test_session.add(championship)
    await test_session.commit()
    await test_session.refresh(championship)
    return championship


async def _login(client: AsyncClient, email: str, password: str) -> str:
    response = await client.post(
        "/api/v1/admin/auth/login",
        json={"email": email, "password": password},
    )
    assert response.status_code == 200, response.text
    return response.json()["access_token"]


async def _create_team_bundle(test_session, start_id: int, size: int) -> list[Team]:
    teams = [Team(id=start_id + idx, name=f"Team {start_id + idx}") for idx in range(size)]
    test_session.add_all(teams)
    await test_session.commit()
    for team in teams:
        await test_session.refresh(team)
    return teams


async def _seed_cup_season(
    test_session,
    championship_id: int,
    season_id: int,
    participants: list[Team],
) -> Season:
    season = Season(
        id=season_id,
        name=f"Cup {season_id}",
        championship_id=championship_id,
        frontend_code="cup",
        has_bracket=True,
        has_table=False,
        date_start=date(2026, 3, 1),
    )
    test_session.add(season)
    await test_session.flush()
    for idx, team in enumerate(participants, start=1):
        test_session.add(
            SeasonParticipant(
                season_id=season.id,
                team_id=team.id,
                sort_order=idx,
                is_disqualified=False,
                fine_points=0,
            )
        )
    await test_session.commit()
    await test_session.refresh(season)
    return season


async def _add_pair(
    client: AsyncClient,
    token: str,
    season_id: int,
    round_key: str,
    team1_id: int,
    team2_id: int,
    sort_order: int,
    side: str = "left",
) -> dict:
    resp = await client.post(
        f"/api/v1/admin/cup-draw/draws/{season_id}/{round_key}/pairs",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "team1_id": team1_id,
            "team2_id": team2_id,
            "sort_order": sort_order,
            "side": side,
        },
    )
    return {"status_code": resp.status_code, "json": resp.json()}


@pytest.mark.asyncio
async def test_add_and_publish_single_pair(
    client: AsyncClient,
    operator_user: AdminUser,
    test_session,
    cup_championship: Championship,
):
    token = await _login(client, operator_user.email, "operator-secret")
    teams = await _create_team_bundle(test_session, start_id=5100, size=4)
    season = await _seed_cup_season(
        test_session,
        championship_id=cup_championship.id,
        season_id=7201,
        participants=teams,
    )

    # Add a pair
    result = await _add_pair(
        client, token, season.id, "1_4",
        teams[0].id, teams[1].id, sort_order=1, side="left",
    )
    assert result["status_code"] == 200
    draw = result["json"]
    assert draw["season_id"] == season.id
    assert draw["round_key"] == "1_4"
    assert draw["status"] == "active"
    assert len(draw["pairs"]) == 1
    assert draw["pairs"][0]["team1_id"] == teams[0].id
    assert draw["pairs"][0]["sort_order"] == 1
    assert draw["pairs"][0]["side"] == "left"
    assert draw["pairs"][0]["is_published"] is False
    assert draw["pairs"][0]["team1"]["name"] == teams[0].name

    # Publish the pair
    pub_resp = await client.post(
        f"/api/v1/admin/cup-draw/draws/{season.id}/1_4/pairs/1/publish",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert pub_resp.status_code == 200
    draw = pub_resp.json()
    assert draw["pairs"][0]["is_published"] is True


@pytest.mark.asyncio
async def test_add_multiple_pairs_publish_one(
    client: AsyncClient,
    operator_user: AdminUser,
    test_session,
    cup_championship: Championship,
):
    token = await _login(client, operator_user.email, "operator-secret")
    teams = await _create_team_bundle(test_session, start_id=5200, size=4)
    season = await _seed_cup_season(
        test_session,
        championship_id=cup_championship.id,
        season_id=7202,
        participants=teams,
    )

    # Add two pairs
    await _add_pair(client, token, season.id, "1_4", teams[0].id, teams[1].id, 1, "left")
    result = await _add_pair(client, token, season.id, "1_4", teams[2].id, teams[3].id, 2, "right")
    assert result["status_code"] == 200
    draw = result["json"]
    assert len(draw["pairs"]) == 2

    # Publish only first pair
    pub_resp = await client.post(
        f"/api/v1/admin/cup-draw/draws/{season.id}/1_4/pairs/1/publish",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert pub_resp.status_code == 200
    draw = pub_resp.json()

    # Check mixed state
    pair1 = next(p for p in draw["pairs"] if p["sort_order"] == 1)
    pair2 = next(p for p in draw["pairs"] if p["sort_order"] == 2)
    assert pair1["is_published"] is True
    assert pair2["is_published"] is False


@pytest.mark.asyncio
async def test_delete_unpublished_pair(
    client: AsyncClient,
    operator_user: AdminUser,
    test_session,
    cup_championship: Championship,
):
    token = await _login(client, operator_user.email, "operator-secret")
    teams = await _create_team_bundle(test_session, start_id=5300, size=4)
    season = await _seed_cup_season(
        test_session,
        championship_id=cup_championship.id,
        season_id=7203,
        participants=teams,
    )

    # Add two pairs
    await _add_pair(client, token, season.id, "1_4", teams[0].id, teams[1].id, 1, "left")
    await _add_pair(client, token, season.id, "1_4", teams[2].id, teams[3].id, 2, "right")

    # Delete pair #2
    del_resp = await client.delete(
        f"/api/v1/admin/cup-draw/draws/{season.id}/1_4/pairs/2",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert del_resp.status_code == 200
    draw = del_resp.json()
    assert len(draw["pairs"]) == 1
    assert draw["pairs"][0]["sort_order"] == 1


@pytest.mark.asyncio
async def test_delete_published_pair(
    client: AsyncClient,
    operator_user: AdminUser,
    test_session,
    cup_championship: Championship,
):
    token = await _login(client, operator_user.email, "operator-secret")
    teams = await _create_team_bundle(test_session, start_id=5400, size=2)
    season = await _seed_cup_season(
        test_session,
        championship_id=cup_championship.id,
        season_id=7204,
        participants=teams,
    )

    # Add and publish
    await _add_pair(client, token, season.id, "final", teams[0].id, teams[1].id, 1, "center")
    await client.post(
        f"/api/v1/admin/cup-draw/draws/{season.id}/final/pairs/1/publish",
        headers={"Authorization": f"Bearer {token}"},
    )

    # Delete published pair — should succeed
    del_resp = await client.delete(
        f"/api/v1/admin/cup-draw/draws/{season.id}/final/pairs/1",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert del_resp.status_code == 200
    draw = del_resp.json()
    assert len(draw["pairs"]) == 0


@pytest.mark.asyncio
async def test_duplicate_team_rejected(
    client: AsyncClient,
    operator_user: AdminUser,
    test_session,
    cup_championship: Championship,
):
    token = await _login(client, operator_user.email, "operator-secret")
    teams = await _create_team_bundle(test_session, start_id=5500, size=4)
    season = await _seed_cup_season(
        test_session,
        championship_id=cup_championship.id,
        season_id=7205,
        participants=teams,
    )

    # Add first pair
    await _add_pair(client, token, season.id, "1_4", teams[0].id, teams[1].id, 1, "left")

    # Try adding pair with a duplicate team
    result = await _add_pair(client, token, season.id, "1_4", teams[0].id, teams[2].id, 2, "right")
    assert result["status_code"] == 400
    assert "Duplicate" in result["json"]["detail"]


@pytest.mark.asyncio
async def test_bracket_shows_only_published(
    client: AsyncClient,
    operator_user: AdminUser,
    test_session,
    cup_championship: Championship,
):
    """Bracket should only contain published pairs."""
    token = await _login(client, operator_user.email, "operator-secret")
    teams = await _create_team_bundle(test_session, start_id=5600, size=4)
    season = await _seed_cup_season(
        test_session,
        championship_id=cup_championship.id,
        season_id=7206,
        participants=teams,
    )

    # Add two pairs, publish only one
    await _add_pair(client, token, season.id, "1_4", teams[0].id, teams[1].id, 1, "left")
    await _add_pair(client, token, season.id, "1_4", teams[2].id, teams[3].id, 2, "right")
    await client.post(
        f"/api/v1/admin/cup-draw/draws/{season.id}/1_4/pairs/1/publish",
        headers={"Authorization": f"Bearer {token}"},
    )

    # Check bracket via service directly
    from app.services.cup_draw import build_bracket_from_cup_draws
    from app.api.deps import get_db

    # Use test_session to call service
    bracket = await build_bracket_from_cup_draws(test_session, season.id)
    assert bracket is not None
    # Should have exactly 1 entry (only the published pair)
    total_entries = sum(len(r.entries) for r in bracket.rounds)
    assert total_entries == 1
    assert bracket.rounds[0].entries[0].game.home_team.name == teams[0].name


@pytest.mark.asyncio
async def test_side_preserved_in_bracket(
    client: AsyncClient,
    operator_user: AdminUser,
    test_session,
    cup_championship: Championship,
):
    """Side from JSON pair should appear in bracket response."""
    token = await _login(client, operator_user.email, "operator-secret")
    teams = await _create_team_bundle(test_session, start_id=5700, size=4)
    season = await _seed_cup_season(
        test_session,
        championship_id=cup_championship.id,
        season_id=7207,
        participants=teams,
    )

    # Add pairs with explicit sides, then publish
    await _add_pair(client, token, season.id, "1_4", teams[0].id, teams[1].id, 1, "left")
    await _add_pair(client, token, season.id, "1_4", teams[2].id, teams[3].id, 2, "right")
    await client.post(
        f"/api/v1/admin/cup-draw/draws/{season.id}/1_4/pairs/1/publish",
        headers={"Authorization": f"Bearer {token}"},
    )
    await client.post(
        f"/api/v1/admin/cup-draw/draws/{season.id}/1_4/pairs/2/publish",
        headers={"Authorization": f"Bearer {token}"},
    )

    from app.services.cup_draw import build_bracket_from_cup_draws
    bracket = await build_bracket_from_cup_draws(test_session, season.id)
    assert bracket is not None
    entries = bracket.rounds[0].entries
    assert len(entries) == 2
    sides = {e.side for e in entries}
    assert sides == {"left", "right"}


@pytest.mark.asyncio
async def test_participants_endpoint(
    client: AsyncClient,
    operator_user: AdminUser,
    test_session,
    cup_championship: Championship,
):
    token = await _login(client, operator_user.email, "operator-secret")
    teams = await _create_team_bundle(test_session, start_id=5800, size=3)
    season = await _seed_cup_season(
        test_session,
        championship_id=cup_championship.id,
        season_id=7208,
        participants=teams,
    )

    resp = await client.get(
        f"/api/v1/admin/cup-draw/participants?season_id={season.id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200
    participants = resp.json()
    assert len(participants) == 3
    assert participants[0]["team_id"] == teams[0].id
    assert "team_name" in participants[0]
