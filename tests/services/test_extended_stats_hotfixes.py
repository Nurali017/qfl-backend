from datetime import date, datetime, time, timedelta, timezone
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import Game, GameStatus, GameTeamStats
from app.services.sync.game_sync import GameSyncService
from app.tasks import live_tasks
from app.utils.timestamps import UTC, ensure_utc, utcnow


class FakeLiveStatsClient:
    async def get_live_match_stats(self, _sota_game_uuid: str):
        return [
            {"metric": "possessions", "home": "51%", "away": "49%"},
            {"metric": "possessions_1", "home": "53%", "away": "47%"},
            {"metric": "possessions_2", "home": "48%", "away": "52%"},
        ]


class FakeSyncOrchestrator:
    def __init__(self, db):
        self.db = db

    async def sync_game_stats(self, game_id: int) -> dict:
        return {"game_id": game_id, "teams": 2, "players": 22, "v2_enriched": 22}

    async def sync_team_season_stats(self, season_id: int) -> int:
        raise RuntimeError(f"boom-team-{season_id}")

    async def sync_player_stats(self, season_id: int) -> int:
        raise RuntimeError(f"boom-player-{season_id}")

    async def sync_player_tour_stats(self, season_id: int, tour: int) -> int:
        raise RuntimeError(f"boom-tour-{season_id}-{tour}")


@pytest.mark.asyncio
async def test_enrich_team_stats_from_live_overwrites_zero_possession(
    test_session, sample_game, sample_teams
):
    row = GameTeamStats(
        game_id=sample_game.id,
        team_id=sample_teams[0].id,
        possession=0,
        possession_percent=0,
    )
    test_session.add(row)
    await test_session.commit()

    service = GameSyncService(test_session, FakeLiveStatsClient())
    ok = await service._enrich_team_stats_from_live(sample_game.id, str(sample_game.sota_id))

    refreshed = await test_session.scalar(
        select(GameTeamStats).where(
            GameTeamStats.game_id == sample_game.id,
            GameTeamStats.team_id == sample_teams[0].id,
        )
    )

    assert ok is True
    assert float(refreshed.possession) == 51.0
    assert refreshed.possession_percent == 51
    assert refreshed.extra_stats["by_half"]["1"]["possessions"] == 53
    assert refreshed.extra_stats["by_half"]["2"]["possessions"] == 48


def test_ensure_utc_normalizes_naive_and_aware_values():
    aware = datetime(2026, 3, 18, 10, 0, tzinfo=timezone.utc)
    naive = datetime(2026, 3, 18, 12, 30)

    normalized_aware = ensure_utc(aware)
    normalized_naive = ensure_utc(naive)

    assert normalized_aware == aware
    assert normalized_aware.tzinfo is UTC
    assert normalized_naive == datetime(2026, 3, 18, 12, 30, tzinfo=UTC)


@pytest.mark.asyncio
async def test_sync_extended_stats_for_game_persists_flag_when_aggregates_fail(
    test_engine, sample_season, sample_teams, monkeypatch
):
    session_factory = async_sessionmaker(
        test_engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autocommit=False,
        autoflush=False,
    )

    async with session_factory() as session:
        game = Game(
            sota_id=uuid4(),
            date=date(2026, 3, 18),
            time=time(18, 0),
            tour=2,
            season_id=sample_season.id,
            home_team_id=sample_teams[0].id,
            away_team_id=sample_teams[1].id,
            status=GameStatus.finished,
            finished_at=utcnow() - timedelta(days=2),
        )
        session.add(game)
        await session.commit()
        game_id = game.id

    monkeypatch.setattr("app.services.sync.SyncOrchestrator", FakeSyncOrchestrator)
    monkeypatch.setattr(live_tasks, "AsyncSessionLocal", session_factory)

    result = await live_tasks._sync_extended_stats_for_game(game_id)

    async with session_factory() as verify_session:
        refreshed = await verify_session.get(Game, game_id)

    assert result["synced"] is True
    assert refreshed.extended_stats_synced_at is not None
    assert result["aggregate_result"]["season_id"] == sample_season.id
    assert any("team_season_stats" in err for err in result["aggregate_result"]["errors"])
    assert any("player_season_stats" in err for err in result["aggregate_result"]["errors"])
