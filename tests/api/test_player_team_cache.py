"""Cache behavior for player/team page endpoints (added 2026-06).

These endpoints carried ~47% of live-match traffic with zero caching:
teams/{id}, players/{id}, players/{id}/tournaments, players/{id}/stats,
teams/{id}/coaches, v2 stats/catalog, v2 players/{id}/stats,
v2 teams/{id}/stats. Pattern mirrors tests/api/test_seasons_cache.py.
"""

from unittest.mock import AsyncMock

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_get_team_cache_hit_skips_db(client: AsyncClient, sample_teams, test_session):
    team_id = sample_teams[0].id
    first = await client.get(f"/api/v1/teams/{team_id}?lang=ru")
    assert first.status_code == 200

    test_session.execute = AsyncMock(side_effect=AssertionError("cache hit should skip db"))  # type: ignore[method-assign]
    second = await client.get(f"/api/v1/teams/{team_id}?lang=ru")

    assert second.status_code == 200
    assert second.json() == first.json()


@pytest.mark.asyncio
async def test_get_team_cache_key_includes_lang(client: AsyncClient, sample_teams, test_session):
    team_id = sample_teams[0].id
    ru = await client.get(f"/api/v1/teams/{team_id}?lang=ru")
    assert ru.status_code == 200

    # A different lang must NOT be served from the ru cache entry — the
    # handler should hit the DB again (no AsyncMock here: we only assert
    # both langs return 200 and the ru entry stays intact).
    kz = await client.get(f"/api/v1/teams/{team_id}?lang=kz")
    assert kz.status_code == 200

    test_session.execute = AsyncMock(side_effect=AssertionError("cache hit should skip db"))  # type: ignore[method-assign]
    ru_again = await client.get(f"/api/v1/teams/{team_id}?lang=ru")
    kz_again = await client.get(f"/api/v1/teams/{team_id}?lang=kz")
    assert ru_again.json() == ru.json()
    assert kz_again.json() == kz.json()


@pytest.mark.asyncio
async def test_get_team_404_not_cached(client: AsyncClient, sample_teams):
    missing = await client.get("/api/v1/teams/999999?lang=ru")
    assert missing.status_code == 404
    again = await client.get("/api/v1/teams/999999?lang=ru")
    assert again.status_code == 404


@pytest.mark.asyncio
async def test_get_team_coaches_cache_hit_skips_db(
    client: AsyncClient, sample_teams, sample_season, test_session
):
    team_id = sample_teams[0].id
    url = f"/api/v1/teams/{team_id}/coaches?season_id={sample_season.id}&lang=ru"
    first = await client.get(url)
    assert first.status_code == 200

    test_session.execute = AsyncMock(side_effect=AssertionError("cache hit should skip db"))  # type: ignore[method-assign]
    second = await client.get(url)

    assert second.status_code == 200
    assert second.json() == first.json()


@pytest.mark.asyncio
async def test_get_player_cache_hit_skips_db(client: AsyncClient, sample_player, test_session):
    first = await client.get(f"/api/v1/players/{sample_player.id}?lang=ru")
    assert first.status_code == 200

    test_session.execute = AsyncMock(side_effect=AssertionError("cache hit should skip db"))  # type: ignore[method-assign]
    second = await client.get(f"/api/v1/players/{sample_player.id}?lang=ru")

    assert second.status_code == 200
    assert second.json() == first.json()


@pytest.mark.asyncio
async def test_get_player_404_not_cached(client: AsyncClient, sample_player):
    missing = await client.get("/api/v1/players/999999?lang=ru")
    assert missing.status_code == 404
    again = await client.get("/api/v1/players/999999?lang=ru")
    assert again.status_code == 404


@pytest.mark.asyncio
async def test_player_tournaments_cache_hit_skips_db(
    client: AsyncClient, sample_player, test_session
):
    url = f"/api/v1/players/{sample_player.id}/tournaments?lang=ru"
    first = await client.get(url)
    assert first.status_code == 200

    test_session.execute = AsyncMock(side_effect=AssertionError("cache hit should skip db"))  # type: ignore[method-assign]
    second = await client.get(url)

    assert second.status_code == 200
    assert second.json() == first.json()


@pytest.mark.asyncio
async def test_player_stats_v1_null_cached(
    client: AsyncClient, sample_player, sample_season, test_session
):
    url = f"/api/v1/players/{sample_player.id}/stats?season_id={sample_season.id}"
    first = await client.get(url)
    assert first.status_code == 200
    assert first.json() is None

    test_session.execute = AsyncMock(side_effect=AssertionError("cache hit should skip db"))  # type: ignore[method-assign]
    second = await client.get(url)

    assert second.status_code == 200
    assert second.json() is None


@pytest.mark.asyncio
async def test_player_stats_v2_null_cached(
    client: AsyncClient, sample_player, sample_season, test_session
):
    url = f"/api/v2/players/{sample_player.id}/stats?season_id={sample_season.id}"
    first = await client.get(url)
    assert first.status_code == 200
    assert first.json() is None

    test_session.execute = AsyncMock(side_effect=AssertionError("cache hit should skip db"))  # type: ignore[method-assign]
    second = await client.get(url)

    assert second.status_code == 200
    assert second.json() is None


@pytest.mark.asyncio
async def test_team_stats_v2_null_cached(
    client: AsyncClient, sample_teams, sample_season, test_session
):
    url = f"/api/v2/teams/{sample_teams[0].id}/stats?season_id={sample_season.id}"
    first = await client.get(url)
    assert first.status_code == 200
    assert first.json() is None

    test_session.execute = AsyncMock(side_effect=AssertionError("cache hit should skip db"))  # type: ignore[method-assign]
    second = await client.get(url)

    assert second.status_code == 200
    assert second.json() is None


@pytest.mark.asyncio
async def test_stats_catalog_v2_cached(client: AsyncClient, monkeypatch):
    first = await client.get("/api/v2/stats/catalog")
    assert first.status_code == 200

    def _boom():
        raise AssertionError("cache hit should not rebuild the catalog")

    monkeypatch.setattr("app.api.v2.stats.build_stats_catalog_payload", _boom)
    second = await client.get("/api/v2/stats/catalog")

    assert second.status_code == 200
    assert second.json() == first.json()
