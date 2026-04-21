"""Regression tests for the `_fetch_pregame_lineups` enqueue decision.

Reason: the previous `if lineup_count > 0` guard skipped games whose lineup
was already loaded in an earlier sync tick (or via FCMS / manual admin
entry). SOTA then returned `players_added=0` here and the Telegram task
was never enqueued. `post_pregame_lineup` itself holds all the real
gates (has_lineup / starters / pregame window / hash dedupe), so the
enqueue step should not gate on SOTA's newly-added count.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from app.tasks import live_tasks


class _FakeDB:
    async def commit(self):
        pass

    async def rollback(self):
        pass


class _FakeSessionContext:
    async def __aenter__(self):
        return _FakeDB()

    async def __aexit__(self, exc_type, exc, tb):
        return False


def _fake_session_factory():
    return _FakeSessionContext()


@pytest.mark.asyncio
async def test_enqueue_when_sota_reports_zero_new_players(monkeypatch):
    """Regression: lineup was loaded earlier → SOTA returns lineup_count=0.

    Before the fix, this silently dropped the post. Now the enqueue must
    fire regardless — `post_pregame_lineup` has its own gates and dedupe.
    """
    game = SimpleNamespace(id=1344)

    class FakeService:
        def __init__(self, db, client):
            pass

        async def get_games_for_pregame_lineup(self):
            return [game]

        async def sync_pregame_lineup(self, game_id, *, sota_only=True):
            return {
                "game_id": game_id,
                "home_formation": "3-5-2",
                "away_formation": "4-4-2",
                "lineup_count": 0,
                "positions_updated": 25,
                "kit_colors_updated": 0,
            }

    delay_mock = MagicMock()
    monkeypatch.setattr(live_tasks, "AsyncSessionLocal", _fake_session_factory)
    monkeypatch.setattr(live_tasks, "LiveSyncService", FakeService)
    monkeypatch.setattr(live_tasks, "get_sota_client", lambda: object())
    monkeypatch.setattr(
        "app.tasks.telegram_tasks.post_pregame_lineup_task.delay",
        delay_mock,
    )

    result = await live_tasks._fetch_pregame_lineups()

    assert result["attempted"] == 1
    delay_mock.assert_called_once_with(1344)


@pytest.mark.asyncio
async def test_no_enqueue_when_no_games_in_window(monkeypatch):
    """If `get_games_for_pregame_lineup` returns nothing — no enqueue, no op."""

    class FakeService:
        def __init__(self, db, client):
            pass

        async def get_games_for_pregame_lineup(self):
            return []

        async def sync_pregame_lineup(self, game_id, *, sota_only=True):
            raise AssertionError("should not be called when no games match")

    delay_mock = MagicMock()
    monkeypatch.setattr(live_tasks, "AsyncSessionLocal", _fake_session_factory)
    monkeypatch.setattr(live_tasks, "LiveSyncService", FakeService)
    monkeypatch.setattr(live_tasks, "get_sota_client", lambda: object())
    monkeypatch.setattr(
        "app.tasks.telegram_tasks.post_pregame_lineup_task.delay",
        delay_mock,
    )

    result = await live_tasks._fetch_pregame_lineups()

    assert result == {"fetched": 0, "results": []}
    delay_mock.assert_not_called()


@pytest.mark.asyncio
async def test_enqueue_when_sota_adds_new_players(monkeypatch):
    """Forward-compatible: positive `lineup_count` still enqueues."""
    game = SimpleNamespace(id=2000)

    class FakeService:
        def __init__(self, db, client):
            pass

        async def get_games_for_pregame_lineup(self):
            return [game]

        async def sync_pregame_lineup(self, game_id, *, sota_only=True):
            return {
                "game_id": game_id,
                "home_formation": "4-3-3",
                "away_formation": "4-4-2",
                "lineup_count": 22,
                "positions_updated": 22,
                "kit_colors_updated": 2,
            }

    delay_mock = MagicMock()
    monkeypatch.setattr(live_tasks, "AsyncSessionLocal", _fake_session_factory)
    monkeypatch.setattr(live_tasks, "LiveSyncService", FakeService)
    monkeypatch.setattr(live_tasks, "get_sota_client", lambda: object())
    monkeypatch.setattr(
        "app.tasks.telegram_tasks.post_pregame_lineup_task.delay",
        delay_mock,
    )

    result = await live_tasks._fetch_pregame_lineups()

    assert result["attempted"] == 1
    delay_mock.assert_called_once_with(2000)
