"""Tests for team-of-week auto-sync dispatch and retry logic."""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_game_row(total, completed):
    """Create a mock row for the tour completion query."""
    row = MagicMock()
    row.total = total
    row.completed = completed
    return row


# ---------------------------------------------------------------------------
# _sync_team_of_week_for_tour
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_tow_full_success():
    """Both locales synced → needs_retry=False."""
    mock_orch = AsyncMock()
    mock_orch.sync_team_of_week.return_value = {
        "tours_synced": 2, "tours_empty": 0, "tours_skipped": 0,
    }

    with patch("app.tasks.sync_tasks.AsyncSessionLocal") as mock_session_ctx:
        mock_db = AsyncMock()
        mock_session_ctx.return_value.__aenter__ = AsyncMock(return_value=mock_db)
        mock_session_ctx.return_value.__aexit__ = AsyncMock(return_value=False)
        with patch("app.tasks.sync_tasks.SyncOrchestrator", return_value=mock_orch):
            from app.tasks.sync_tasks import _sync_team_of_week_for_tour
            result = await _sync_team_of_week_for_tour(100, 5)

    assert result["needs_retry"] is False
    assert result["tours_synced"] == 2


@pytest.mark.asyncio
async def test_tow_retry_on_empty():
    """tours_empty > 0 → needs_retry=True."""
    mock_orch = AsyncMock()
    mock_orch.sync_team_of_week.return_value = {
        "tours_synced": 2, "tours_empty": 1, "tours_skipped": 0,
    }

    with patch("app.tasks.sync_tasks.AsyncSessionLocal") as mock_session_ctx:
        mock_db = AsyncMock()
        mock_session_ctx.return_value.__aenter__ = AsyncMock(return_value=mock_db)
        mock_session_ctx.return_value.__aexit__ = AsyncMock(return_value=False)
        with patch("app.tasks.sync_tasks.SyncOrchestrator", return_value=mock_orch):
            from app.tasks.sync_tasks import _sync_team_of_week_for_tour
            result = await _sync_team_of_week_for_tour(100, 5)

    assert result["needs_retry"] is True


@pytest.mark.asyncio
async def test_tow_retry_on_partial_locale():
    """Only one locale synced → needs_retry=True."""
    mock_orch = AsyncMock()
    mock_orch.sync_team_of_week.return_value = {
        "tours_synced": 1, "tours_empty": 0, "tours_skipped": 1,
    }

    with patch("app.tasks.sync_tasks.AsyncSessionLocal") as mock_session_ctx:
        mock_db = AsyncMock()
        mock_session_ctx.return_value.__aenter__ = AsyncMock(return_value=mock_db)
        mock_session_ctx.return_value.__aexit__ = AsyncMock(return_value=False)
        with patch("app.tasks.sync_tasks.SyncOrchestrator", return_value=mock_orch):
            from app.tasks.sync_tasks import _sync_team_of_week_for_tour
            result = await _sync_team_of_week_for_tour(100, 5)

    assert result["needs_retry"] is True


@pytest.mark.asyncio
async def test_tow_disabled_season_skipped():
    """Disabled season → skipped=True, needs_retry=False."""
    mock_orch = AsyncMock()
    mock_orch.sync_team_of_week.return_value = {
        "skipped": True, "reason": "sync disabled for season",
    }

    with patch("app.tasks.sync_tasks.AsyncSessionLocal") as mock_session_ctx:
        mock_db = AsyncMock()
        mock_session_ctx.return_value.__aenter__ = AsyncMock(return_value=mock_db)
        mock_session_ctx.return_value.__aexit__ = AsyncMock(return_value=False)
        with patch("app.tasks.sync_tasks.SyncOrchestrator", return_value=mock_orch):
            from app.tasks.sync_tasks import _sync_team_of_week_for_tour
            result = await _sync_team_of_week_for_tour(100, 5)

    assert result["needs_retry"] is False
    assert result["skipped"] is True


# ---------------------------------------------------------------------------
# _dispatch_tow_sync_for_tours — Redis dedupe
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_tow_dedupe_initial():
    """Second dispatch with same season/tour/suffix is blocked by Redis NX."""
    mock_redis = MagicMock()
    # First call: set returns True (key didn't exist), second: False (already set)
    mock_redis.set.side_effect = [True, False]

    with patch("app.tasks.sync_tasks.redis_lib") as mock_redis_lib:
        mock_redis_lib.from_url.return_value = mock_redis
        with patch("app.tasks.sync_tasks.sync_team_of_week_for_tour") as mock_task:
            from app.tasks.sync_tasks import _dispatch_tow_sync_for_tours
            # First call — dispatched
            await _dispatch_tow_sync_for_tours(100, [5], "initial", countdown=60)
            assert mock_task.apply_async.call_count == 1

            # Second call — blocked by Redis NX
            await _dispatch_tow_sync_for_tours(100, [5], "initial", countdown=60)
            assert mock_task.apply_async.call_count == 1  # Still 1, not 2


@pytest.mark.asyncio
async def test_tow_dedupe_extended_separate():
    """Initial and extended use different keys, both pass."""
    mock_redis = MagicMock()
    mock_redis.set.return_value = True  # All NX succeed (different keys)

    with patch("app.tasks.sync_tasks.redis_lib") as mock_redis_lib:
        mock_redis_lib.from_url.return_value = mock_redis
        with patch("app.tasks.sync_tasks.sync_team_of_week_for_tour") as mock_task:
            from app.tasks.sync_tasks import _dispatch_tow_sync_for_tours
            await _dispatch_tow_sync_for_tours(100, [5], "initial", countdown=60)
            await _dispatch_tow_sync_for_tours(100, [5], "extended")

            # Both dispatched — initial via apply_async, extended via delay
            assert mock_task.apply_async.call_count == 1
            assert mock_task.delay.call_count == 1

            # Verify different Redis keys were used
            set_calls = mock_redis.set.call_args_list
            keys = [c[0][0] for c in set_calls]
            assert "qfl:tow_sync:100:5:initial" in keys
            assert "qfl:tow_sync:100:5:extended" in keys


# ---------------------------------------------------------------------------
# Celery task: sync_team_of_week_for_tour — Telegram + retry
# ---------------------------------------------------------------------------

def test_tow_telegram_only_on_final_success():
    """Telegram sent only on final success."""
    with patch(
        "app.tasks.sync_tasks._sync_team_of_week_for_tour",
        new=AsyncMock(return_value={"needs_retry": False, "tours_synced": 2}),
    ) as mock_sync:
        with patch(
            "app.tasks.sync_tasks.send_telegram_message",
            new=AsyncMock(),
        ) as mock_telegram:
            from app.tasks.sync_tasks import sync_team_of_week_for_tour

            result = sync_team_of_week_for_tour.run(100, 5)

    assert result["final_status"] == "success"
    mock_sync.assert_awaited_once_with(100, 5)
    mock_telegram.assert_awaited_once_with(
        "⚽ Team of week synced: season 100 tour 5"
    )


def test_tow_telegram_not_on_retry():
    """No Telegram on retry — self.retry raises, only sync run_async called."""
    from app.tasks.sync_tasks import sync_team_of_week_for_tour

    with patch(
        "app.tasks.sync_tasks._sync_team_of_week_for_tour",
        new=AsyncMock(return_value={"needs_retry": True, "tours_synced": 1}),
    ) as mock_sync:
        with patch(
            "app.tasks.sync_tasks.send_telegram_message",
            new=AsyncMock(),
        ) as mock_telegram:
            with patch.object(
                sync_team_of_week_for_tour, "retry", side_effect=Exception("retry")
            ):
                with pytest.raises(Exception, match="retry"):
                    sync_team_of_week_for_tour.run(100, 5)

    mock_sync.assert_awaited_once_with(100, 5)
    mock_telegram.assert_not_awaited()


def test_tow_telegram_on_max_retries_exceeded():
    """Telegram error sent when max retries exceeded."""
    from app.tasks.sync_tasks import sync_team_of_week_for_tour

    with patch(
        "app.tasks.sync_tasks._sync_team_of_week_for_tour",
        new=AsyncMock(return_value={"needs_retry": True, "tours_synced": 0}),
    ) as mock_sync:
        with patch(
            "app.tasks.sync_tasks.send_telegram_message",
            new=AsyncMock(),
        ) as mock_telegram:
            with patch.object(
                sync_team_of_week_for_tour,
                "retry",
                side_effect=sync_team_of_week_for_tour.MaxRetriesExceededError("max"),
            ):
                result = sync_team_of_week_for_tour.run(100, 5)

    assert result["final_status"] == "failed"
    mock_sync.assert_awaited_once_with(100, 5)
    mock_telegram.assert_awaited_once_with(
        "❌ Team-of-week sync failed after retries: season 100 tour 5"
    )


# ---------------------------------------------------------------------------
# _post_finish_followup — tour complete → dispatch
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_tow_dispatch_on_tour_complete():
    """All games in tour finished → dispatch with countdown=60, suffix=initial."""
    mock_redis = AsyncMock()
    mock_redis.set.return_value = True  # All dedup keys succeed

    mock_db = AsyncMock()
    mock_game = MagicMock()
    mock_game.season_id = 100
    mock_game.tour = 5
    mock_game.sota_id = "123"
    mock_game.sync_disabled = False
    mock_game.finished_at = None
    mock_db.get.return_value = mock_game

    # Tour completion query: all 4 games completed
    mock_result = MagicMock()
    mock_result.one.return_value = _make_game_row(total=4, completed=4)
    mock_db.execute.return_value = mock_result

    mock_dispatch = AsyncMock()

    with patch("app.utils.live_flag.get_redis", return_value=mock_redis):
        with patch("app.tasks.live_tasks.AsyncSessionLocal") as mock_session:
            mock_session.return_value.__aenter__ = AsyncMock(return_value=mock_db)
            mock_session.return_value.__aexit__ = AsyncMock(return_value=False)
            with patch("app.tasks.live_tasks.LiveSyncService"):
                with patch("app.tasks.live_tasks.get_sota_client"):
                    with patch("app.tasks.sync_tasks.check_tour_completion"):
                        with patch(
                            "app.tasks.sync_tasks._dispatch_tow_sync_for_tours",
                            mock_dispatch,
                        ):
                            from app.tasks.live_tasks import _post_finish_followup
                            await _post_finish_followup(1)
                            mock_dispatch.assert_called_once_with(
                                100, [5], "initial", countdown=60
                            )


@pytest.mark.asyncio
async def test_tow_no_dispatch_partial_tour():
    """3/4 games finished → dispatch NOT called."""
    mock_redis = AsyncMock()
    mock_redis.set.return_value = True

    mock_db = AsyncMock()
    mock_game = MagicMock()
    mock_game.season_id = 100
    mock_game.tour = 5
    mock_game.sota_id = "123"
    mock_game.sync_disabled = False
    mock_game.finished_at = None
    mock_db.get.return_value = mock_game

    # 3/4 completed
    mock_result = MagicMock()
    mock_result.one.return_value = _make_game_row(total=4, completed=3)
    mock_db.execute.return_value = mock_result

    mock_dispatch = AsyncMock()

    with patch("app.utils.live_flag.get_redis", return_value=mock_redis):
        with patch("app.tasks.live_tasks.AsyncSessionLocal") as mock_session:
            mock_session.return_value.__aenter__ = AsyncMock(return_value=mock_db)
            mock_session.return_value.__aexit__ = AsyncMock(return_value=False)
            with patch("app.tasks.live_tasks.LiveSyncService"):
                with patch("app.tasks.live_tasks.get_sota_client"):
                    with patch("app.tasks.sync_tasks.check_tour_completion"):
                        with patch(
                            "app.tasks.sync_tasks._dispatch_tow_sync_for_tours",
                            mock_dispatch,
                        ):
                            from app.tasks.live_tasks import _post_finish_followup
                            await _post_finish_followup(2)
                            mock_dispatch.assert_not_called()


@pytest.mark.asyncio
async def test_tow_dispatch_with_technical_defeat():
    """3 finished + 1 technical_defeat → dispatch called (all terminal)."""
    mock_redis = AsyncMock()
    mock_redis.set.return_value = True

    mock_db = AsyncMock()
    mock_game = MagicMock()
    mock_game.season_id = 100
    mock_game.tour = 5
    mock_game.sota_id = "123"
    mock_game.sync_disabled = False
    mock_game.finished_at = None
    mock_db.get.return_value = mock_game

    # All 4 games completed (including technical_defeat)
    mock_result = MagicMock()
    mock_result.one.return_value = _make_game_row(total=4, completed=4)
    mock_db.execute.return_value = mock_result

    mock_dispatch = AsyncMock()

    with patch("app.utils.live_flag.get_redis", return_value=mock_redis):
        with patch("app.tasks.live_tasks.AsyncSessionLocal") as mock_session:
            mock_session.return_value.__aenter__ = AsyncMock(return_value=mock_db)
            mock_session.return_value.__aexit__ = AsyncMock(return_value=False)
            with patch("app.tasks.live_tasks.LiveSyncService"):
                with patch("app.tasks.live_tasks.get_sota_client"):
                    with patch("app.tasks.sync_tasks.check_tour_completion"):
                        with patch(
                            "app.tasks.sync_tasks._dispatch_tow_sync_for_tours",
                            mock_dispatch,
                        ):
                            from app.tasks.live_tasks import _post_finish_followup
                            await _post_finish_followup(3)
                            mock_dispatch.assert_called_once_with(
                                100, [5], "initial", countdown=60
                            )


# ---------------------------------------------------------------------------
# Extended aggregate paths — extended dispatch
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_tow_extended_dispatch_in_sync_aggregate_bundle():
    """Sync aggregate bundle dispatches extended team-of-week sync for marked tours."""
    mock_db1 = AsyncMock()
    mock_db2 = AsyncMock()

    mock_orch = AsyncMock()
    mock_orch.sync_team_season_stats.return_value = 12
    mock_orch.sync_player_stats.return_value = 34
    mock_orch.sync_player_tour_stats.return_value = 56

    mock_mark = AsyncMock()
    mock_revalidate = AsyncMock()
    mock_dispatch = AsyncMock()

    with patch("app.tasks.sync_tasks.AsyncSessionLocal") as mock_session:
        mock_session.return_value.__aenter__ = AsyncMock(
            side_effect=[mock_db1, mock_db2]
        )
        mock_session.return_value.__aexit__ = AsyncMock(return_value=False)
        with patch("app.tasks.sync_tasks.SyncOrchestrator", return_value=mock_orch):
            with patch("app.tasks.tour_readiness.mark_tour_synced", mock_mark):
                with patch(
                    "app.tasks.tour_readiness.maybe_trigger_tour_revalidation",
                    mock_revalidate,
                ):
                    with patch(
                        "app.tasks.sync_tasks._dispatch_tow_sync_for_tours",
                        mock_dispatch,
                    ):
                        from app.tasks.sync_tasks import _sync_extended_aggregate_bundle

                        result = await _sync_extended_aggregate_bundle(100, {5})

    assert result["tour_stats"] == {5: 56}
    mock_mark.assert_awaited_once_with(mock_db2, 100, 5)
    mock_revalidate.assert_awaited_once_with(mock_db2, 100, 5)
    mock_dispatch.assert_awaited_once_with(100, [5], "extended")


@pytest.mark.asyncio
async def test_tow_extended_dispatch_in_live_aggregate_path():
    """Live aggregate path dispatches extended team-of-week sync for marked tours."""
    mock_db = AsyncMock()

    mock_orch = AsyncMock()
    mock_orch.sync_team_season_stats.return_value = 12
    mock_orch.sync_player_stats.return_value = 34
    mock_orch.sync_player_tour_stats.return_value = 56

    mock_mark = AsyncMock()
    mock_revalidate = AsyncMock()
    mock_dispatch = AsyncMock()

    with patch("app.tasks.live_tasks.AsyncSessionLocal") as mock_session:
        mock_session.return_value.__aenter__ = AsyncMock(return_value=mock_db)
        mock_session.return_value.__aexit__ = AsyncMock(return_value=False)
        with patch("app.services.sync.SyncOrchestrator", return_value=mock_orch):
            with patch("app.tasks.tour_readiness.mark_tour_synced", mock_mark):
                with patch(
                    "app.tasks.tour_readiness.maybe_trigger_tour_revalidation",
                    mock_revalidate,
                ):
                    with patch(
                        "app.tasks.sync_tasks._dispatch_tow_sync_for_tours",
                        mock_dispatch,
                    ):
                        from app.tasks.live_tasks import (
                            _sync_extended_aggregates_for_season,
                        )

                        result = await _sync_extended_aggregates_for_season(100, {5})

    assert result["tour_stats"] == {5: 56}
    mock_mark.assert_awaited_once_with(mock_db, 100, 5)
    mock_revalidate.assert_awaited_once_with(mock_db, 100, 5)
    mock_dispatch.assert_awaited_once_with(100, [5], "extended")
