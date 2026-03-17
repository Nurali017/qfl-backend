from datetime import date, time
from uuid import uuid4

import pytest
from httpx import AsyncClient

from app.models import Game, GameStatus


@pytest.mark.asyncio
async def test_games_list_returns_live_phase_and_half(
    client: AsyncClient, test_session, sample_season, sample_teams
):
    game = Game(
        sota_id=uuid4(),
        date=date(2026, 3, 18),
        time=time(18, 0),
        tour=1,
        season_id=sample_season.id,
        home_team_id=sample_teams[0].id,
        away_team_id=sample_teams[1].id,
        status=GameStatus.live,
        live_half=2,
        live_minute=93,
        live_phase="halftime",
    )
    test_session.add(game)
    await test_session.commit()

    response = await client.get(f"/api/v1/games?season_id={sample_season.id}")
    assert response.status_code == 200
    item = response.json()["items"][0]

    assert item["status"] == "live"
    assert item["half"] == 2
    assert item["minute"] == 93
    assert item["live_phase"] == "halftime"


@pytest.mark.asyncio
async def test_game_detail_returns_live_phase(
    client: AsyncClient, test_session, sample_season, sample_teams
):
    game = Game(
        sota_id=uuid4(),
        date=date(2026, 3, 18),
        time=time(18, 0),
        tour=1,
        season_id=sample_season.id,
        home_team_id=sample_teams[0].id,
        away_team_id=sample_teams[1].id,
        status=GameStatus.live,
        live_half=1,
        live_minute=47,
        live_phase="halftime",
    )
    test_session.add(game)
    await test_session.commit()
    await test_session.refresh(game)

    response = await client.get(f"/api/v1/games/{game.id}")
    assert response.status_code == 200
    data = response.json()

    assert data["status"] == "live"
    assert data["half"] == 1
    assert data["minute"] == 47
    assert data["live_phase"] == "halftime"
