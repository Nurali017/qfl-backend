from datetime import date, time
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from httpx import AsyncClient

from app.models import AdminUser, Championship, Game, Season, Team
from app.models.game import GameStatus
from app.security import hash_password


@pytest.fixture
async def superadmin_user(test_session):
    user = AdminUser(
        email="tg-superadmin@test.local",
        password_hash=hash_password("super-secret"),
        role="superadmin",
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
async def test_admin_daily_results_trigger_reset_allows_resend(
    client: AsyncClient,
    test_session,
    superadmin_user: AdminUser,
):
    championship = Championship(
        id=501,
        name="Премьер-Лига",
        name_kz="Премьер-Лига",
        short_name="Премьер-Лига",
        short_name_kz="Премьер-Лига",
    )
    season = Season(
        id=601,
        name="Премьер-Лига 2026",
        name_kz="Премьер-Лига 2026",
        championship_id=championship.id,
        frontend_code="pl",
        is_visible=True,
    )
    teams = [
        Team(id=701, name="Астана", name_kz="Астана"),
        Team(id=702, name="Қайрат", name_kz="Қайрат"),
    ]
    test_session.add_all([championship, season, *teams])
    await test_session.commit()

    match = Game(
        id=801,
        date=date(2026, 4, 25),
        time=time(15, 0),
        season_id=season.id,
        home_team_id=teams[0].id,
        away_team_id=teams[1].id,
        status=GameStatus.finished,
        home_score=2,
        away_score=1,
        tour=4,
    )
    test_session.add(match)
    await test_session.commit()

    token = await _login(client, superadmin_user.email, "super-secret")
    headers = {"Authorization": f"Bearer {token}"}

    with patch(
        "app.services.telegram_posts.render_daily_results_card_png",
        new=AsyncMock(return_value=Path("/tmp/daily-card.png")),
    ), patch(
        "app.services.telegram_posts.send_public_user_photo",
        new=AsyncMock(return_value=915),
    ) as send_mock:
        first = await client.post(
            "/api/v1/admin/ops/tg/test/daily-results",
            params={"season_id": season.id, "for_date": "2026-04-25"},
            headers=headers,
        )
        second = await client.post(
            "/api/v1/admin/ops/tg/test/daily-results",
            params={"season_id": season.id, "for_date": "2026-04-25"},
            headers=headers,
        )
        reset = await client.post(
            "/api/v1/admin/ops/tg/test/daily-results",
            params={
                "season_id": season.id,
                "for_date": "2026-04-25",
                "reset": "true",
            },
            headers=headers,
        )

    assert first.status_code == 200, first.text
    assert first.json()["sent"] is True
    assert second.status_code == 200, second.text
    assert second.json()["sent"] is False
    assert reset.status_code == 200, reset.text
    assert reset.json()["sent"] is True
    assert send_mock.await_count == 2
