from datetime import date, time
from uuid import uuid4

import pytest

from app.models import Game, GameStatus
from app.services.live_sync_service import LiveSyncService


class FakeTimeClient:
    def __init__(self, payload):
        self.payload = payload

    async def get_live_match_time(self, _sota_uuid: str):
        return self.payload


@pytest.mark.asyncio
async def test_sync_live_time_sets_halftime_phase(test_session, sample_season, sample_teams):
    game = Game(
        sota_id=uuid4(),
        date=date(2026, 3, 18),
        time=time(18, 0),
        season_id=sample_season.id,
        home_team_id=sample_teams[0].id,
        away_team_id=sample_teams[1].id,
        status=GameStatus.live,
        sync_disabled=True,
    )
    test_session.add(game)
    await test_session.commit()
    await test_session.refresh(game)

    service = LiveSyncService(
        test_session,
        FakeTimeClient({"half": 1, "actual_time": 47 * 60_000, "status": "halftime"}),
    )
    result = await service.sync_live_time(game.id)
    await test_session.refresh(game)

    assert game.status == GameStatus.live
    assert game.finished_at is None
    assert game.live_half == 1
    assert game.live_minute == 47
    assert game.live_phase == "halftime"
    assert result["live_phase"] == "halftime"


@pytest.mark.asyncio
async def test_sync_live_time_sets_in_progress_without_finishing(test_session, sample_season, sample_teams):
    game = Game(
        sota_id=uuid4(),
        date=date(2026, 3, 18),
        time=time(18, 0),
        season_id=sample_season.id,
        home_team_id=sample_teams[0].id,
        away_team_id=sample_teams[1].id,
        status=GameStatus.live,
        sync_disabled=True,
        live_phase="halftime",
    )
    test_session.add(game)
    await test_session.commit()
    await test_session.refresh(game)

    service = LiveSyncService(
        test_session,
        FakeTimeClient({"half": 2, "actual_time": 48 * 60_000, "status": "in_progress"}),
    )
    result = await service.sync_live_time(game.id)
    await test_session.refresh(game)

    assert game.status == GameStatus.live
    assert game.finished_at is None
    assert game.live_half == 2
    assert game.live_minute == 93
    assert game.live_phase == "in_progress"
    assert result["live_phase"] == "in_progress"


@pytest.mark.asyncio
async def test_sync_live_time_finished_uses_lifecycle_service(test_session, sample_season, sample_teams):
    game = Game(
        sota_id=uuid4(),
        date=date(2026, 3, 18),
        time=time(18, 0),
        season_id=sample_season.id,
        home_team_id=sample_teams[0].id,
        away_team_id=sample_teams[1].id,
        status=GameStatus.live,
        sync_disabled=True,
        live_half=2,
        live_minute=93,
        live_phase="in_progress",
    )
    test_session.add(game)
    await test_session.commit()
    await test_session.refresh(game)

    service = LiveSyncService(
        test_session,
        FakeTimeClient({"half": 2, "actual_time": 48 * 60_000, "status": "finished"}),
    )
    result = await service.sync_live_time(game.id)
    await test_session.refresh(game)

    assert game.status == GameStatus.finished
    assert game.finished_at is not None
    assert game.live_half is None
    assert game.live_minute is None
    assert game.live_phase is None
    assert result["lifecycle_result"]["action"] == "finish_live"


@pytest.mark.asyncio
async def test_sync_live_time_without_status_keeps_no_artificial_halftime(
    test_session, sample_season, sample_teams
):
    game = Game(
        sota_id=uuid4(),
        date=date(2026, 3, 18),
        time=time(18, 0),
        season_id=sample_season.id,
        home_team_id=sample_teams[0].id,
        away_team_id=sample_teams[1].id,
        status=GameStatus.live,
        sync_disabled=True,
    )
    test_session.add(game)
    await test_session.commit()
    await test_session.refresh(game)

    service = LiveSyncService(
        test_session,
        FakeTimeClient({"half": 2, "actual_time": 48 * 60_000}),
    )
    result = await service.sync_live_time(game.id)
    await test_session.refresh(game)

    assert game.status == GameStatus.live
    assert game.live_half == 2
    assert game.live_minute == 93
    assert game.live_phase is None
    assert result["live_phase"] is None
