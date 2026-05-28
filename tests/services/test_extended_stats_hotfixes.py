from datetime import date, datetime, time, timedelta, timezone
from unittest.mock import MagicMock
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


class FakeZeroPossessionClient:
    """Reproduces the sota.id bug observed for game 975: /em/ ships 0% but
    v1 /games/{id}/teams/ has the correct values."""

    def __init__(self, v1_payload: list[dict]):
        self.v1_payload = v1_payload
        self.v1_calls = 0

    async def get_live_match_stats(self, _sota_game_uuid: str):
        return [
            {"metric": "possessions", "home": "0%", "away": "0%"},
        ]

    async def get_game_team_stats(self, _sota_game_uuid: str, language: str = "ru"):
        self.v1_calls += 1
        return self.v1_payload


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


@pytest.mark.asyncio
async def test_enrich_falls_back_to_v1_when_em_returns_zero_possession(
    test_session, sample_game, sample_teams
):
    """sota.id bug (game 975): /em/ ships 0% but v1 /games/teams has the truth.

    Without the fallback, the v1 values written by sync_game_stats earlier
    get clobbered with zeros. With it, possession is restored from v1.
    """
    row = GameTeamStats(
        game_id=sample_game.id,
        team_id=sample_teams[0].id,
        possession=23.1,           # pretend sync_game_stats already wrote v1 raw
        possession_percent=47,     # and percent
    )
    test_session.add(row)
    await test_session.commit()

    v1_payload = [
        {
            "id": sample_teams[0].id,
            "name": "Astana",
            "stats": {"possession": 23.1, "possession_percent": 47},
        },
        {
            "id": sample_teams[1].id,
            "name": "Kairat",
            "stats": {"possession": 25.5, "possession_percent": 53},
        },
    ]
    client = FakeZeroPossessionClient(v1_payload)
    service = GameSyncService(test_session, client)

    ok = await service._enrich_team_stats_from_live(sample_game.id, str(sample_game.sota_id))

    refreshed = await test_session.scalar(
        select(GameTeamStats).where(
            GameTeamStats.game_id == sample_game.id,
            GameTeamStats.team_id == sample_teams[0].id,
        )
    )

    assert ok is True
    assert refreshed.possession_percent == 47
    assert float(refreshed.possession) == 23.1
    # v1 fetched at most once even though two sides looked at possession
    assert client.v1_calls == 1


@pytest.mark.asyncio
async def test_enrich_does_not_overwrite_when_em_zero_and_v1_missing(
    test_session, sample_game, sample_teams
):
    """Defensive: if /em/ is 0 AND v1 has no row for the team, leave existing
    possession alone instead of writing zeros."""
    row = GameTeamStats(
        game_id=sample_game.id,
        team_id=sample_teams[0].id,
        possession=23.1,
        possession_percent=47,
    )
    test_session.add(row)
    await test_session.commit()

    # v1 missing this team entirely
    client = FakeZeroPossessionClient(v1_payload=[])
    service = GameSyncService(test_session, client)

    await service._enrich_team_stats_from_live(sample_game.id, str(sample_game.sota_id))

    refreshed = await test_session.scalar(
        select(GameTeamStats).where(
            GameTeamStats.game_id == sample_game.id,
            GameTeamStats.team_id == sample_teams[0].id,
        )
    )
    assert refreshed.possession_percent == 47
    assert float(refreshed.possession) == 23.1


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

    from app.tasks import sync_tasks

    monkeypatch.setattr("app.services.sync.SyncOrchestrator", FakeSyncOrchestrator)
    monkeypatch.setattr(live_tasks, "AsyncSessionLocal", session_factory)
    # The season-aggregate bundle now runs in sync_tasks (each sub-step isolated),
    # so patch the orchestrator/session it actually resolves there too.
    monkeypatch.setattr(sync_tasks, "SyncOrchestrator", FakeSyncOrchestrator)
    monkeypatch.setattr(sync_tasks, "AsyncSessionLocal", session_factory)

    mock_task = MagicMock()
    mock_task.request.retries = 0
    result = await live_tasks._sync_extended_stats_for_game(mock_task, game_id)

    async with session_factory() as verify_session:
        refreshed = await verify_session.get(Game, game_id)

    assert result["synced"] is True
    assert refreshed.extended_stats_synced_at is not None
    assert result["aggregate_result"]["season_id"] == sample_season.id
    assert any("team_season_stats" in err for err in result["aggregate_result"]["errors"])
    assert any("player_season_stats" in err for err in result["aggregate_result"]["errors"])
