"""TDD tests for live-aware league table responses.

Covers:
- LiveMatchInline payload for teams currently in live matches
- include_live=false query parameter (Flashscore-style snapshot)
- has_live / live_team_ids flags
- position_change suppression when include_live=false
"""

from __future__ import annotations

from datetime import date, time
from uuid import uuid4

import pytest
from httpx import AsyncClient


pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.usefixtures("override_web_session_factory"),
]


async def _make_season_with_live(test_session, *, season_id: int = 305) -> tuple[int, int, int, int]:
    """Seed a season with two teams, ScoreTable rows and one live game.

    Returns (season_id, home_team_id, away_team_id, live_game_id).
    """
    from app.models import Championship, Game, GameStatus, ScoreTable, Season, Team

    championship = Championship(id=season_id // 10, name=f"Champ {season_id}")
    test_session.add(championship)
    await test_session.flush()

    season = Season(
        id=season_id,
        name=str(2025 + season_id % 5),
        championship_id=championship.id,
        date_start=date(2025, 3, 1),
        date_end=date(2025, 11, 30),
    )
    test_session.add(season)
    await test_session.flush()

    home = Team(id=season_id * 10 + 1, name="LiveHome")
    away = Team(id=season_id * 10 + 2, name="LiveAway")
    test_session.add_all([home, away])
    await test_session.flush()

    # Pre-live state: two finished games already played, equal points/GD,
    # so team order is decided alphabetically (i.e. LiveAway, LiveHome).
    finished_a = Game(
        sota_id=uuid4(),
        date=date(2025, 4, 1), time=time(18, 0), tour=1,
        season_id=season.id,
        home_team_id=home.id, away_team_id=away.id,
        home_score=1, away_score=1,
        status=GameStatus.finished,
    )
    finished_b = Game(
        sota_id=uuid4(),
        date=date(2025, 4, 8), time=time(18, 0), tour=2,
        season_id=season.id,
        home_team_id=away.id, away_team_id=home.id,
        home_score=0, away_score=0,
        status=GameStatus.finished,
    )

    # Live game: home leading 2-1 in 67th minute. After include_live this flips
    # the standings (home above away by points).
    live_game = Game(
        sota_id=uuid4(),
        date=date(2025, 5, 15), time=time(18, 0), tour=3,
        season_id=season.id,
        home_team_id=home.id, away_team_id=away.id,
        home_score=2, away_score=1,
        status=GameStatus.live,
        live_minute=67,
        live_half=2,
    )
    test_session.add_all([finished_a, finished_b, live_game])

    # ScoreTable rows so the team list is non-empty even before live.
    test_session.add_all([
        ScoreTable(
            season_id=season.id, team_id=home.id, position=1,
            games_played=2, wins=0, draws=2, losses=0,
            goals_scored=1, goals_conceded=1, points=2,
        ),
        ScoreTable(
            season_id=season.id, team_id=away.id, position=2,
            games_played=2, wins=0, draws=2, losses=0,
            goals_scored=1, goals_conceded=1, points=2,
        ),
    ])
    await test_session.commit()
    return season.id, home.id, away.id, live_game.id


class TestLeagueTableLive:
    async def test_no_live_response_has_null_live_match(
        self, client: AsyncClient, sample_season, sample_score_table
    ):
        """Sanity: a season without live games never returns live_match payload."""
        response = await client.get(f"/api/v1/seasons/{sample_season.id}/table")
        assert response.status_code == 200
        data = response.json()

        assert data["has_live"] is False
        assert data["live_team_ids"] == []
        for row in data["table"]:
            assert row.get("live_match") in (None, {}), row

    async def test_live_match_inline_payload_for_both_teams(
        self, client: AsyncClient, test_session
    ):
        """Live match must surface as live_match on home and away rows."""
        season_id, home_id, away_id, live_game_id = await _make_season_with_live(
            test_session, season_id=305
        )

        response = await client.get(f"/api/v1/seasons/{season_id}/table")
        assert response.status_code == 200
        data = response.json()

        assert data["has_live"] is True
        assert sorted(data["live_team_ids"]) == sorted([home_id, away_id])

        rows_by_team = {row["team_id"]: row for row in data["table"]}

        home_live = rows_by_team[home_id].get("live_match")
        assert home_live is not None
        assert home_live["match_id"] == live_game_id
        assert home_live["opponent_id"] == away_id
        assert home_live["is_home"] is True
        assert home_live["score_for"] == 2
        assert home_live["score_against"] == 1
        assert home_live["minute"] == 67
        assert home_live["half"] == 2

        away_live = rows_by_team[away_id].get("live_match")
        assert away_live is not None
        assert away_live["match_id"] == live_game_id
        assert away_live["opponent_id"] == home_id
        assert away_live["is_home"] is False
        assert away_live["score_for"] == 1
        assert away_live["score_against"] == 2

    async def test_include_live_false_excludes_live_from_standings(
        self, client: AsyncClient, test_session
    ):
        """include_live=false must compute the table without live game contribution."""
        season_id, home_id, away_id, _ = await _make_season_with_live(
            test_session, season_id=315
        )

        # With live counted: home (2-1 lead) gets +3 points → home above away.
        with_live = await client.get(f"/api/v1/seasons/{season_id}/table")
        assert with_live.status_code == 200
        with_live_data = with_live.json()
        with_live_rows = {r["team_id"]: r for r in with_live_data["table"]}
        assert with_live_rows[home_id]["points"] > with_live_rows[away_id]["points"]

        # Without live: only the two finished draws count → equal points.
        without_live = await client.get(
            f"/api/v1/seasons/{season_id}/table?include_live=false"
        )
        assert without_live.status_code == 200
        without_live_data = without_live.json()
        without_live_rows = {r["team_id"]: r for r in without_live_data["table"]}
        assert (
            without_live_rows[home_id]["points"]
            == without_live_rows[away_id]["points"]
        )
        # Live flags still surface so the UI can offer the toggle.
        assert without_live_data["has_live"] is True
        assert sorted(without_live_data["live_team_ids"]) == sorted([home_id, away_id])

    async def test_include_live_false_clears_live_match_field(
        self, client: AsyncClient, test_session
    ):
        """include_live=false hides per-row live_match — UI shows pure pre-live table."""
        season_id, *_ = await _make_season_with_live(test_session, season_id=320)

        response = await client.get(
            f"/api/v1/seasons/{season_id}/table?include_live=false"
        )
        assert response.status_code == 200
        data = response.json()

        for row in data["table"]:
            assert row.get("live_match") in (None, {}), row

    async def test_include_live_false_clears_position_change(
        self, client: AsyncClient, test_session
    ):
        """position_change has no meaning when live is excluded — must be null."""
        season_id, *_ = await _make_season_with_live(test_session, season_id=325)

        response = await client.get(
            f"/api/v1/seasons/{season_id}/table?include_live=false"
        )
        assert response.status_code == 200
        data = response.json()

        for row in data["table"]:
            assert row.get("position_change") is None, row
