"""Tests for the public YouTube stats overview endpoint.

Regression coverage for the bug where unplayed matches (which get a YouTube
live URL assigned before kickoff) were counted in the summary, inflating
`games_with_live_url` / `total_live_views` and skewing season comparisons.
Only `status == finished` matches must be counted.
"""

from datetime import date, time
from uuid import uuid4

import pytest

from app.models import Game, GameStatus


@pytest.fixture
async def youtube_games(test_session, sample_season, sample_teams):
    """One finished match with a live URL + one not-yet-played match with a live URL."""
    home, away = sample_teams[0], sample_teams[1]
    finished = Game(
        sota_id=uuid4(),
        date=date(2025, 5, 1),
        time=time(18, 0),
        tour=1,
        season_id=sample_season.id,
        home_team_id=home.id,
        away_team_id=away.id,
        home_score=2,
        away_score=1,
        status=GameStatus.finished,
        youtube_live_url="https://youtu.be/finished",
        youtube_live_view_count=10000,
        video_review_url="https://youtu.be/finished-review",
        video_review_view_count=5000,
    )
    upcoming = Game(
        sota_id=uuid4(),
        date=date(2025, 5, 8),
        time=time(18, 0),
        tour=2,
        season_id=sample_season.id,
        home_team_id=home.id,
        away_team_id=away.id,
        status=GameStatus.created,
        youtube_live_url="https://youtu.be/upcoming",
        youtube_live_view_count=42,  # pre-roll views on an unplayed match
    )
    test_session.add_all([finished, upcoming])
    await test_session.commit()
    await test_session.refresh(finished)
    await test_session.refresh(upcoming)
    return {"finished": finished, "upcoming": upcoming}


async def test_overview_excludes_unplayed_matches(client, sample_season, youtube_games):
    resp = await client.get(
        "/api/v1/youtube-stats/overview", params={"season_id": sample_season.id}
    )
    assert resp.status_code == 200
    data = resp.json()

    # Only the finished match counts.
    assert data["summary"]["games_with_live_url"] == 1
    assert data["summary"]["games_with_review_url"] == 1
    assert data["summary"]["total_live_views"] == 10000
    assert data["summary"]["total_review_views"] == 5000

    # The unplayed fixture must not appear in the per-match table.
    game_ids = {row["game_id"] for row in data["games"]}
    assert youtube_games["finished"].id in game_ids
    assert youtube_games["upcoming"].id not in game_ids
