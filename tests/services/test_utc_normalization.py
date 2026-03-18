from datetime import datetime, timedelta
from uuid import uuid4
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from app.models import Game, GameStatus
from app.models.tour_sync_status import TourSyncStatus
from app.services.game_lifecycle import GameLifecycleService
from app.services.home_matches import ALMATY_TZ
from app.services.live_sync_service import LiveSyncService
from app.tasks.tour_readiness import mark_tour_synced
from app.utils.timestamps import UTC, ensure_utc, utcnow


@pytest.mark.asyncio
async def test_get_games_to_end_uses_almaty_schedule_fallback(
    test_session,
    sample_season,
    sample_teams,
):
    now_almaty = datetime.now(ALMATY_TZ)
    overdue_local = now_almaty - timedelta(hours=2, minutes=20)
    fresh_local = now_almaty - timedelta(hours=1, minutes=45)

    overdue = Game(
        sota_id=uuid4(),
        date=overdue_local.date(),
        time=overdue_local.time().replace(second=0, microsecond=0),
        season_id=sample_season.id,
        home_team_id=sample_teams[0].id,
        away_team_id=sample_teams[1].id,
        status=GameStatus.live,
    )
    fresh = Game(
        sota_id=uuid4(),
        date=fresh_local.date(),
        time=fresh_local.time().replace(second=0, microsecond=0),
        season_id=sample_season.id,
        home_team_id=sample_teams[1].id,
        away_team_id=sample_teams[2].id,
        status=GameStatus.live,
    )
    test_session.add_all([overdue, fresh])
    await test_session.commit()

    games = await LiveSyncService(test_session, object()).get_games_to_end()
    game_ids = {game.id for game in games}

    assert overdue.id in game_ids
    assert fresh.id not in game_ids


@pytest.mark.asyncio
async def test_get_games_to_end_uses_utc_half_start_when_available(
    test_session,
    sample_season,
    sample_teams,
):
    now_almaty = datetime.now(ALMATY_TZ)
    overdue_started_at = utcnow() - timedelta(hours=2, minutes=20)
    fresh_started_at = utcnow() - timedelta(hours=1, minutes=45)

    overdue = Game(
        sota_id=uuid4(),
        date=now_almaty.date(),
        time=now_almaty.time().replace(second=0, microsecond=0),
        season_id=sample_season.id,
        home_team_id=sample_teams[0].id,
        away_team_id=sample_teams[1].id,
        status=GameStatus.live,
        half1_started_at=overdue_started_at,
    )
    fresh = Game(
        sota_id=uuid4(),
        date=now_almaty.date(),
        time=now_almaty.time().replace(second=0, microsecond=0),
        season_id=sample_season.id,
        home_team_id=sample_teams[1].id,
        away_team_id=sample_teams[2].id,
        status=GameStatus.live,
        half1_started_at=fresh_started_at,
    )
    test_session.add_all([overdue, fresh])
    await test_session.commit()

    games = await LiveSyncService(test_session, object()).get_games_to_end()
    game_ids = {game.id for game in games}

    assert overdue.id in game_ids
    assert fresh.id not in game_ids


@pytest.mark.asyncio
async def test_finish_live_records_utc_finished_at(
    test_session,
    sample_season,
    sample_teams,
    monkeypatch,
):
    monkeypatch.setattr("app.utils.live_flag.clear_live_flag", AsyncMock())
    monkeypatch.setattr(
        GameLifecycleService,
        "_enqueue_post_finish",
        staticmethod(lambda _game: None),
    )

    game = Game(
        sota_id=uuid4(),
        date=datetime.now(ALMATY_TZ).date(),
        time=datetime.now(ALMATY_TZ).time().replace(second=0, microsecond=0),
        season_id=sample_season.id,
        home_team_id=sample_teams[0].id,
        away_team_id=sample_teams[1].id,
        status=GameStatus.live,
        sync_disabled=True,
    )
    test_session.add(game)
    await test_session.commit()
    await test_session.refresh(game)

    result = await GameLifecycleService(test_session).finish_live(game.id)
    await test_session.refresh(game)

    finished_at = ensure_utc(game.finished_at)

    assert result["action"] == "finish_live"
    assert finished_at is not None
    assert finished_at.tzinfo is UTC
    assert abs((utcnow() - finished_at).total_seconds()) < 10


@pytest.mark.asyncio
async def test_mark_tour_synced_persists_utc_marker(
    test_session,
    sample_season,
):
    await mark_tour_synced(test_session, sample_season.id, 2)
    await test_session.commit()

    marker = await test_session.scalar(
        select(TourSyncStatus).where(
            TourSyncStatus.season_id == sample_season.id,
            TourSyncStatus.tour == 2,
        )
    )

    assert marker is not None
    assert ensure_utc(marker.synced_at).tzinfo is UTC
