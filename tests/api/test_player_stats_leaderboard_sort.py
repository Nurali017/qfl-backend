"""Regression test for the season player-stats leaderboard ordering.

`/api/v1/seasons/{id}/player-stats` sorts goal / goal_pass / dry_match by SOTA's
``*_rank`` column but renders the raw metric value. ``best_players`` — the only
task that writes the rank columns — is skipped while any game is live, so on
match days the rank lags the freshly-synced counts for the few minutes between
the post-match stats sync and the next ``best_players`` run. A pure rank sort
then shows the new counts in the stale rank order (the prod 7, 6, 7, 6, 7 seen on
2026-06-21). The endpoint must order by the displayed metric first and use the
rank only as a tie-break, so the visible column is always monotonic.
"""

import pytest
from httpx import AsyncClient

from app.models import Player, PlayerSeasonStats


@pytest.mark.asyncio
class TestLeaderboardSortByValue:
    async def _seed(self, session, season_id, team_id, rows):
        players = [
            Player(first_name="P", last_name=last_name, player_type="offence", top_role="CF")
            for last_name, _goal, _rank in rows
        ]
        session.add_all(players)
        await session.flush()
        session.add_all(
            [
                PlayerSeasonStats(
                    player_id=player.id,
                    season_id=season_id,
                    team_id=team_id,
                    goal=goal,
                    goal_rank=rank,
                )
                for player, (_last, goal, rank) in zip(players, rows)
            ]
        )
        await session.commit()

    async def test_goal_leaderboard_is_monotonic_with_rank_tiebreak(
        self,
        client: AsyncClient,
        test_session,
        sample_season,
        sample_teams,
    ):
        # (last_name, goal, goal_rank) — ranks deliberately disagree with goals,
        # exactly the stale-rank state observed on prod after full time.
        rows = [
            ("Amir", 6, 1),       # stale rank floats a 6-goal player to the top
            ("Satpaev", 7, 2),
            ("DaCosta", 7, 3),
            ("Murtazaev", 7, 4),
            ("Anuarbekov", 6, 5),
            ("Ghost", 9, None),   # unranked: excluded though it has the most goals
        ]
        await self._seed(test_session, sample_season.id, sample_teams[0].id, rows)

        resp = await client.get(
            f"/api/v1/seasons/{sample_season.id}/player-stats",
            params={"sort_by": "goal", "limit": 10, "lang": "ru"},
        )
        assert resp.status_code == 200
        data = resp.json()

        names = [item["last_name"] for item in data["items"]]
        goals = [item["goal"] for item in data["items"]]

        # Unranked player is filtered out (goal_rank IS NOT NULL).
        assert "Ghost" not in names
        assert data["total"] == 5

        # The displayed goal column is non-increasing — never 7, 6, 7, 6, 7.
        assert goals == sorted(goals, reverse=True), goals
        assert goals == [7, 7, 7, 6, 6]

        # Within equal goals, SOTA's rank order is preserved (ascending).
        assert names == ["Satpaev", "DaCosta", "Murtazaev", "Amir", "Anuarbekov"]
