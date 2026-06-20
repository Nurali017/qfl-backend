"""Tests for the week-based league-performance (season dynamics) endpoint.

The chart used to bucket standings by matchweek (tour). That breaks when a
fixture is postponed and replayed in a far-off tour slot. The endpoint now
buckets by the ISO calendar week of each match's date, so it is chronological by
construction and a high-tour makeup game lands in the week it was actually
played.
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


async def _seed_season(test_session, *, season_id: int = 405) -> tuple[int, dict[str, int]]:
    """Seed 4 teams across 3 ISO weeks, including a postponed makeup (tour=25)
    that was actually played in week 2. Returns (season_id, {name: team_id})."""
    from app.models import Championship, Game, GameStatus, Season, Team

    champ = Championship(id=season_id // 10, name=f"Champ {season_id}")
    test_session.add(champ)
    await test_session.flush()

    season = Season(
        id=season_id,
        name="2026",
        championship_id=champ.id,
        date_start=date(2026, 3, 1),
        date_end=date(2026, 11, 30),
    )
    test_session.add(season)
    await test_session.flush()

    teams = {name: Team(id=season_id * 10 + i, name=name) for i, name in enumerate("ABCD", 1)}
    test_session.add_all(teams.values())
    await test_session.flush()
    A, B, C, D = (teams[n].id for n in "ABCD")

    def g(d: date, tour: int, home: int, away: int, hs: int, as_: int) -> "Game":
        return Game(
            sota_id=uuid4(), date=d, time=time(18, 0), tour=tour, season_id=season.id,
            home_team_id=home, away_team_id=away, home_score=hs, away_score=as_,
            status=GameStatus.finished,
        )

    test_session.add_all([
        # Week 1 (Mon 2026-03-02 .. Sun 2026-03-08)
        g(date(2026, 3, 7), 1, A, B, 1, 0),
        g(date(2026, 3, 8), 1, C, D, 1, 0),
        # Week 2 (Mon 2026-03-09 .. Sun 2026-03-15) — the D-B game is a postponed
        # makeup carrying a far-off tour number (25) but played this week.
        g(date(2026, 3, 14), 2, A, C, 0, 0),
        g(date(2026, 3, 15), 25, D, B, 3, 0),
        # Week 3 (Mon 2026-03-30 .. Sun 2026-04-05)
        g(date(2026, 4, 4), 3, A, D, 2, 1),
        g(date(2026, 4, 5), 3, B, C, 1, 1),
    ])
    await test_session.commit()
    return season.id, {"A": A, "B": B, "C": C, "D": D}


class TestLeaguePerformanceWeeks:
    async def test_buckets_by_calendar_week_not_tour(
        self, client: AsyncClient, test_session
    ):
        season_id, ids = await _seed_season(test_session, season_id=405)

        resp = await client.get(f"/api/v1/seasons/{season_id}/league-performance?lang=ru")
        assert resp.status_code == 200
        data = resp.json()

        # 3 calendar weeks had matches — NOT 25 (the makeup game's tour slot).
        assert data["week_count"] == 3
        assert data["max_tour"] == 3  # legacy alias kept for the chart
        assert [w["start"] for w in data["weeks"]] == [
            "2026-03-07", "2026-03-14", "2026-04-04",
        ]

        # Every team has exactly one position per week.
        assert len(data["teams"]) == 4
        assert all(len(t["positions"]) == 3 for t in data["teams"])

    async def test_makeup_game_counts_in_its_calendar_week(
        self, client: AsyncClient, test_session
    ):
        """The tour-25 makeup (D beats B 3-0) is played in week 2, so by the end
        of week 2 D must sit above B — proving date-week bucketing, not tour."""
        season_id, ids = await _seed_season(test_session, season_id=415)

        resp = await client.get(f"/api/v1/seasons/{season_id}/league-performance?lang=ru")
        assert resp.status_code == 200
        positions = {t["team_id"]: t["positions"] for t in resp.json()["teams"]}

        # Week 1: B lost, D lost — B is listed before D (insertion order).
        assert positions[ids["B"]][0] < positions[ids["D"]][0]
        # Week 2: D won the makeup 3-0 → D jumps above B.
        assert positions[ids["D"]][1] < positions[ids["B"]][1]

    async def test_half_entered_game_does_not_create_a_week(
        self, client: AsyncClient, test_session
    ):
        """A game with only one score filled in must not invent a week point."""
        from app.models import Championship, Game, GameStatus, Season, Team

        season_id = 425
        champ = Championship(id=season_id // 10, name=f"Champ {season_id}")
        test_session.add(champ)
        await test_session.flush()
        season = Season(
            id=season_id, name="2026", championship_id=champ.id,
            date_start=date(2026, 3, 1), date_end=date(2026, 11, 30),
        )
        test_session.add(season)
        await test_session.flush()
        a = Team(id=season_id * 10 + 1, name="A")
        b = Team(id=season_id * 10 + 2, name="B")
        test_session.add_all([a, b])
        await test_session.flush()

        test_session.add_all([
            # Week 1: a real, fully-played game.
            Game(
                sota_id=uuid4(), date=date(2026, 3, 7), time=time(18, 0), tour=1,
                season_id=season.id, home_team_id=a.id, away_team_id=b.id,
                home_score=2, away_score=1, status=GameStatus.finished,
            ),
            # Week 2: only a half-entered game (away_score is None) → ignored.
            Game(
                sota_id=uuid4(), date=date(2026, 3, 14), time=time(18, 0), tour=2,
                season_id=season.id, home_team_id=b.id, away_team_id=a.id,
                home_score=1, away_score=None, status=GameStatus.finished,
            ),
        ])
        await test_session.commit()

        resp = await client.get(f"/api/v1/seasons/{season_id}/league-performance?lang=ru")
        assert resp.status_code == 200
        data = resp.json()

        # The half-entered week 2 must NOT appear.
        assert data["week_count"] == 1
        assert [w["start"] for w in data["weeks"]] == ["2026-03-07"]
        assert all(len(t["positions"]) == 1 for t in data["teams"])
