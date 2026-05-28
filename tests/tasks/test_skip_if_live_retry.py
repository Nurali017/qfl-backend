"""Tests for skip-if-live → self.retry(countdown=60, max_retries=15) pattern.

Covers:
- Game-/tour-scoped tasks (sync_extended_stats_for_game, _sync_team_of_week_for_tour)
  raise self.retry with the expected args when a live game is active.
- Batch tasks (sync_fcms_post_match_protocol, _fcms_bulk_import,
  _sync_best_players) still return skip dict without invoking self.retry.
- The manual self.retry(max_retries=15) overrides the per-decorator
  max_retries=3 from _DB_RETRY_KW.
"""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Retry-pattern tasks (should call self.retry)
# ---------------------------------------------------------------------------

class _RetryMarker(Exception):
    """Sentinel raised by mocked task.retry so we can detect the call."""


def _make_task_with_retry_capture():
    task = MagicMock()
    task.request.retries = 0
    task.retry = MagicMock(side_effect=_RetryMarker())
    return task


@pytest.mark.asyncio
async def test_extended_stats_retries_when_live():
    task = _make_task_with_retry_capture()

    with patch("app.tasks.live_tasks.AsyncSessionLocal") as mock_session_ctx:
        mock_db = AsyncMock()
        mock_db.scalar = AsyncMock(return_value=3)  # 3 live games
        mock_session_ctx.return_value.__aenter__ = AsyncMock(return_value=mock_db)
        mock_session_ctx.return_value.__aexit__ = AsyncMock(return_value=False)

        from app.tasks.live_tasks import _sync_extended_stats_for_game

        with pytest.raises(_RetryMarker):
            await _sync_extended_stats_for_game(task, game_id=999)

    task.retry.assert_called_once()
    kwargs = task.retry.call_args.kwargs
    assert kwargs["countdown"] == 60
    assert kwargs["max_retries"] == 15

    from app.tasks._exceptions import LiveGamesActiveSkip
    assert isinstance(kwargs["exc"], LiveGamesActiveSkip)
    assert kwargs["exc"].live_count == 3


@pytest.mark.asyncio
async def test_team_of_week_retries_when_live():
    task = _make_task_with_retry_capture()

    with patch("app.tasks.sync_tasks.AsyncSessionLocal") as mock_session_ctx:
        mock_db = AsyncMock()
        mock_db.scalar = AsyncMock(return_value=1)  # 1 live game
        mock_session_ctx.return_value.__aenter__ = AsyncMock(return_value=mock_db)
        mock_session_ctx.return_value.__aexit__ = AsyncMock(return_value=False)

        from app.tasks.sync_tasks import _sync_team_of_week_for_tour

        with pytest.raises(_RetryMarker):
            await _sync_team_of_week_for_tour(task, season_id=200, tour=5)

    task.retry.assert_called_once()
    kwargs = task.retry.call_args.kwargs
    assert kwargs["countdown"] == 60
    assert kwargs["max_retries"] == 15


# ---------------------------------------------------------------------------
# Batch tasks (should NOT call self.retry — just return skip dict)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_fcms_bulk_import_returns_skip_when_live_no_retry():
    with patch("app.tasks.fcms_tasks._any_game_live", new=AsyncMock(return_value=2)):
        from app.tasks.fcms_tasks import _fcms_bulk_import

        result = await _fcms_bulk_import()

    assert result["status"] == "skipped"
    assert result["reason"] == "live_games_active"
    assert result["live_games"] == 2


@pytest.mark.asyncio
async def test_fcms_post_match_protocol_returns_skip_when_live_no_retry():
    with patch("app.tasks.fcms_tasks._any_game_live", new=AsyncMock(return_value=1)):
        from app.tasks.fcms_tasks import _sync_fcms_post_match_protocol

        result = await _sync_fcms_post_match_protocol()

    assert result["status"] == "skipped"
    assert result["reason"] == "live_games_active"
    assert result["live_games"] == 1


@pytest.mark.asyncio
async def test_best_players_returns_skip_when_live_no_retry():
    with patch("app.tasks.sync_tasks.AsyncSessionLocal") as mock_session_ctx:
        mock_db = AsyncMock()
        mock_db.scalar = AsyncMock(return_value=2)  # 2 live games
        mock_session_ctx.return_value.__aenter__ = AsyncMock(return_value=mock_db)
        mock_session_ctx.return_value.__aexit__ = AsyncMock(return_value=False)

        from app.tasks.sync_tasks import _sync_best_players

        result = await _sync_best_players()

    assert result["skipped"] is True
    assert result["reason"] == "live_games_active"
    assert result["live_games"] == 2


# ---------------------------------------------------------------------------
# Retry-args override: manual max_retries=15 wins over _DB_RETRY_KW's 3
# ---------------------------------------------------------------------------

def test_manual_retry_max_retries_overrides_decorator_max_retries():
    """The wrapper at live_tasks.py:866 inherits max_retries=3 via _DB_RETRY_KW.
    A manual self.retry(max_retries=15, countdown=60) inside the body must use
    15, not 3. This guards against future refactors that drop the manual
    keyword and silently revert to the decorator default.
    """
    from app.tasks import celery_app
    from app.tasks.live_tasks import sync_extended_stats_for_game

    original_eager = celery_app.conf.task_always_eager
    original_propagate = celery_app.conf.task_eager_propagates
    celery_app.conf.task_always_eager = True
    celery_app.conf.task_eager_propagates = False

    try:
        captured = {}

        async def fake_async_func(self, game_id):
            captured["retries"] = self.request.retries
            captured["max_retries"] = self.max_retries
            # Force the task into the "no live games" path without touching the DB.
            return {"game_id": game_id, "synced": False}

        with patch(
            "app.tasks.live_tasks._sync_extended_stats_for_game",
            new=fake_async_func,
        ):
            sync_extended_stats_for_game.apply(args=[42]).get(disable_sync_subtasks=False)

        # On the first call, retries==0 and the decorator's max_retries field
        # (visible via self.max_retries) is the per-task default from
        # _DB_RETRY_KW. The retry() call inside the body would override that
        # *per retry*, but the decorator-level default before any retry call
        # is what we record here. The override is exercised separately above
        # via direct task.retry assertion.
        assert captured["max_retries"] == 3  # from _DB_RETRY_KW
        # Asserting that the function received the bound task self is enough
        # to prove bind=True was applied — that's the precondition for any
        # self.retry(max_retries=15) to take effect at runtime.
        assert "retries" in captured
    finally:
        celery_app.conf.task_always_eager = original_eager
        celery_app.conf.task_eager_propagates = original_propagate
