from types import SimpleNamespace

import httpx
import pytest

from app.services.sync.guardrails import dead_season_cache_key
from app.services.sync.player_sync import PlayerSyncService
from app.services.sync.player_tour_stats_sync import PlayerTourStatsSyncService


class FakeFetchAllResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return list(self._rows)


class FakeOneOrNoneResult:
    def __init__(self, row):
        self._row = row

    def one_or_none(self):
        return self._row


class FakeRedis:
    def __init__(self):
        self.keys: set[str] = set()

    async def exists(self, key: str) -> int:
        return int(key in self.keys)

    async def set(self, key: str, value: str, ex: int | None = None) -> bool:
        self.keys.add(key)
        return True


class FakeDB:
    def __init__(self, player_rows, sota_row):
        self.player_rows = player_rows
        self.sota_row = sota_row
        self.execute_count = 0
        self.commit_count = 0

    async def execute(self, _stmt):
        self.execute_count += 1
        if self.execute_count == 1:
            return FakeFetchAllResult(self.player_rows)
        if self.execute_count == 2:
            return FakeOneOrNoneResult(self.sota_row)
        return SimpleNamespace()

    async def commit(self):
        self.commit_count += 1


def _http_404() -> httpx.HTTPStatusError:
    request = httpx.Request("GET", "https://sota.test")
    response = httpx.Response(404, request=request)
    return httpx.HTTPStatusError("not found", request=request, response=response)


def _http_500() -> httpx.HTTPStatusError:
    request = httpx.Request("GET", "https://sota.test")
    response = httpx.Response(500, request=request)
    return httpx.HTTPStatusError("server error", request=request, response=response)


class FakeSeasonStatsClient:
    def __init__(self):
        self.calls: list[tuple[str, int]] = []

    async def get_player_season_stats(self, player_id: str, season_id: int, language: str = "ru"):
        self.calls.append((player_id, season_id))
        if season_id == 173:
            raise _http_404()
        return {"games_played": 3, "goal": 1}


class FakeSeasonStats500Client:
    def __init__(self):
        self.calls: list[tuple[str, int]] = []

    async def get_player_season_stats(self, player_id: str, season_id: int, language: str = "ru"):
        self.calls.append((player_id, season_id))
        raise _http_500()


class FakeTourStatsClient:
    def __init__(self):
        self.calls: list[tuple[str, int, int]] = []

    async def get_player_game_stats_v2_by_tour(
        self, player_id: str, season_id: int, tour: int, language: str = "ru"
    ):
        self.calls.append((player_id, season_id, tour))
        if season_id == 173:
            raise _http_404()
        return {"games_played": 2, "goal": 1}


class FakeTourStatsTimingClient:
    async def get_player_game_stats_v2_by_tour(
        self, player_id: str, season_id: int, tour: int, language: str = "ru"
    ):
        return {"games_played": 1, "goal": 1}


@pytest.mark.asyncio
async def test_player_season_stats_marks_only_dead_pair_and_keeps_other_pair(monkeypatch):
    redis = FakeRedis()
    
    async def fake_get_redis():
        return redis

    monkeypatch.setattr("app.utils.live_flag.get_redis", fake_get_redis)

    player_rows = [(idx, 10 + idx, f"player-{idx}") for idx in range(1, 36)]
    db = FakeDB(player_rows, ("173;174", None))
    client = FakeSeasonStatsClient()
    service = PlayerSyncService(db, client)

    count = await service.sync_player_season_stats(200)

    assert count == 35
    assert len([call for call in client.calls if call[1] == 173]) == 30
    assert len([call for call in client.calls if call[1] == 174]) == 35
    assert dead_season_cache_key(200, 173) in redis.keys
    assert dead_season_cache_key(200, 174) not in redis.keys


@pytest.mark.asyncio
async def test_player_tour_stats_respects_existing_dead_pair_flag(monkeypatch):
    redis = FakeRedis()
    redis.keys.add(dead_season_cache_key(200, 173))
    
    async def fake_get_redis():
        return redis

    monkeypatch.setattr("app.utils.live_flag.get_redis", fake_get_redis)

    player_rows = [(1, 11, "player-1"), (2, 12, "player-2")]
    db = FakeDB(player_rows, ("173;174", None))
    client = FakeTourStatsClient()
    service = PlayerTourStatsSyncService(db, client)

    count = await service.sync_tour(200, 7)

    assert count == 2
    assert len([call for call in client.calls if call[1] == 173]) == 0
    assert len([call for call in client.calls if call[1] == 174]) == 2


@pytest.mark.asyncio
async def test_player_season_stats_does_not_mark_dead_pair_on_500(monkeypatch):
    redis = FakeRedis()
    
    async def fake_get_redis():
        return redis

    monkeypatch.setattr("app.utils.live_flag.get_redis", fake_get_redis)

    player_rows = [(idx, 10 + idx, f"player-{idx}") for idx in range(1, 4)]
    db = FakeDB(player_rows, ("173", None))
    client = FakeSeasonStats500Client()
    service = PlayerSyncService(db, client)

    count = await service.sync_player_season_stats(200)

    assert count == 0
    assert redis.keys == set()


@pytest.mark.asyncio
async def test_player_tour_stats_logs_summary_only_when_debug_enabled(monkeypatch, caplog):
    redis = FakeRedis()
    
    async def fake_get_redis():
        return redis

    monkeypatch.setattr("app.utils.live_flag.get_redis", fake_get_redis)
    monkeypatch.setattr(
        "app.services.sync.player_tour_stats_sync.get_settings",
        lambda: SimpleNamespace(
            debug_sync_timings=True,
            sota_dead_season_min_404=30,
            sota_dead_season_404_ratio=0.8,
            sota_dead_season_ttl_seconds=3600,
        ),
    )

    player_rows = [(1, 11, "player-1")]
    db = FakeDB(player_rows, ("174", None))
    service = PlayerTourStatsSyncService(db, FakeTourStatsTimingClient())

    with caplog.at_level("INFO"):
        count = await service.sync_tour(200, 3)

    assert count == 1
    assert sum("sync_timings" in record.message for record in caplog.records) == 1


@pytest.mark.asyncio
async def test_player_tour_stats_does_not_log_summary_when_debug_disabled(monkeypatch, caplog):
    redis = FakeRedis()

    async def fake_get_redis():
        return redis

    monkeypatch.setattr("app.utils.live_flag.get_redis", fake_get_redis)
    monkeypatch.setattr(
        "app.services.sync.player_tour_stats_sync.get_settings",
        lambda: SimpleNamespace(
            debug_sync_timings=False,
            sota_dead_season_min_404=30,
            sota_dead_season_404_ratio=0.8,
            sota_dead_season_ttl_seconds=3600,
        ),
    )

    player_rows = [(1, 11, "player-1")]
    db = FakeDB(player_rows, ("174", None))
    service = PlayerTourStatsSyncService(db, FakeTourStatsTimingClient())

    with caplog.at_level("INFO"):
        count = await service.sync_tour(200, 3)

    assert count == 1
    assert sum("sync_timings" in record.message for record in caplog.records) == 0
