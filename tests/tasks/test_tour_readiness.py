"""Integration test for maybe_trigger_tour_revalidation date-exclusion fix.

A future-dated `created` game must not block stats-page ISR revalidation for a
tour whose played games are all finished + extended-stats-synced. Before the
fix the future game inflated `total`, so `scored != total` → returned False.
"""

from datetime import date, time
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from app.models import Game, GameStatus
from app.models.tour_sync_status import TourSyncStatus
from app.tasks.tour_readiness import maybe_trigger_tour_revalidation
from app.utils.timestamps import utcnow


async def _add_game(session, season_id, teams, *, tour, status, gdate, scored=False, ext=False):
    session.add(
        Game(
            sota_id=uuid4(),
            date=gdate,
            time=time(17, 0),
            tour=tour,
            season_id=season_id,
            home_team_id=teams[0].id,
            away_team_id=teams[1].id,
            status=status,
            home_score=1 if scored else None,
            away_score=0 if scored else None,
            extended_stats_synced_at=utcnow() if ext else None,
        )
    )


@pytest.mark.asyncio
async def test_revalidation_not_blocked_by_future_created_game(
    test_session, sample_season, sample_teams
):
    # 3 played + fully synced games, all in the past.
    for _ in range(3):
        await _add_game(
            test_session, sample_season.id, sample_teams,
            tour=9, status=GameStatus.finished, gdate=date(2026, 6, 11),
            scored=True, ext=True,
        )
    # One game rescheduled to the future, still `created`.
    await _add_game(
        test_session, sample_season.id, sample_teams,
        tour=9, status=GameStatus.created, gdate=date(2026, 9, 4),
    )
    # Aggregate marker (condition 3) present.
    test_session.add(TourSyncStatus(season_id=sample_season.id, tour=9, synced_at=utcnow()))
    await test_session.commit()

    fake_redis = MagicMock()
    fake_redis.exists.return_value = 0
    fake_redis.set.return_value = True
    mock_trigger = MagicMock()

    with patch("app.tasks.tour_readiness.redis.from_url", return_value=fake_redis), \
         patch("app.tasks.sync_tasks.trigger_stats_revalidation", mock_trigger):
        result = await maybe_trigger_tour_revalidation(test_session, sample_season.id, 9)

    assert result is True
    mock_trigger.delay.assert_called_once_with(season_id=sample_season.id, tour=9)
