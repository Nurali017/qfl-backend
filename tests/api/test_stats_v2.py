from datetime import date, time
from uuid import uuid4

import pytest
from httpx import AsyncClient

from app.models import Game, Player, PlayerSeasonStats, PlayerTeam, TeamSeasonStats
from app.services.stats_v2 import PLAYER_V2_METRICS, TEAM_V2_METRICS


@pytest.mark.asyncio
class TestStatsV2API:
    async def test_get_stats_catalog_v2_returns_canonical_groups(
        self,
        client: AsyncClient,
    ):
        response = await client.get("/api/v2/stats/catalog")
        assert response.status_code == 200
        data = response.json()

        assert data["players"]["key_stats"] == [
            "games_played",
            "time_on_field_total",
            "goal",
            "goal_pass",
            "goal_and_assist",
        ]
        assert data["players"]["groups"]["attacking"] == [
            "dribble",
            "dribble_success",
            "dribble_per_90",
            "corner",
            "offside",
        ]
        assert "goalkeeping" in data["players"]["groups"]
        assert "goalkeeping" not in data["teams"]["groups"]
        assert data["teams"]["key_stats"] == [
            "games_played",
            "win",
            "draw",
            "match_loss",
            "goal",
            "goals_conceded",
            "goal_difference",
            "points",
        ]
        assert data["players"]["metrics"]["games_played"] == {
            "group": "key_stats",
            "rankable": True,
            "rank_order": "desc",
        }
        assert data["players"]["metrics"]["goal"] == {
            "group": "goals",
            "rankable": True,
            "rank_order": "desc",
        }
        assert data["players"]["metrics"]["red_cards"]["rank_order"] == "asc"
        assert data["teams"]["metrics"]["points"]["group"] == "key_stats"
        assert data["teams"]["metrics"]["match_loss"]["rank_order"] == "asc"

    async def test_get_player_stats_v2_returns_normalized_payload(
        self,
        client: AsyncClient,
        test_session,
        sample_player,
        sample_season,
        sample_teams,
    ):
        other_players = [
            Player(first_name="Other", last_name="One", player_type="offence", top_role="CF"),
            Player(first_name="Other", last_name="Two", player_type="offence", top_role="RW"),
        ]
        test_session.add_all(other_players)
        await test_session.flush()

        test_session.add_all(
            [
                PlayerSeasonStats(
                    player_id=sample_player.id,
                    season_id=sample_season.id,
                    team_id=sample_teams[0].id,
                    games_played=12,
                    time_on_field_total=900,
                    goal=5,
                    goal_pass=3,
                    goal_and_assist=None,
                    xg=1.7,
                    pass_ratio=82.5,
                    yellow_cards=2,
                    second_yellow_cards=1,
                    red_cards=1,
                    extra_stats={"interception": 7},
                ),
                PlayerSeasonStats(
                    player_id=other_players[0].id,
                    season_id=sample_season.id,
                    team_id=sample_teams[1].id,
                    games_played=10,
                    time_on_field_total=850,
                    goal=7,
                    goal_pass=3,
                    goal_and_assist=None,
                    xg=2.0,
                    yellow_cards=0,
                    second_yellow_cards=0,
                    red_cards=0,
                    interception=10,
                ),
                PlayerSeasonStats(
                    player_id=other_players[1].id,
                    season_id=sample_season.id,
                    team_id=sample_teams[2].id,
                    games_played=11,
                    time_on_field_total=700,
                    goal=5,
                    goal_pass=1,
                    goal_and_assist=None,
                    xg=1.2,
                    yellow_cards=2,
                    second_yellow_cards=0,
                    red_cards=1,
                    interception=2,
                ),
            ]
        )
        await test_session.commit()

        response = await client.get(
            f"/api/v2/players/{sample_player.id}/stats?season_id={sample_season.id}"
        )
        assert response.status_code == 200
        data = response.json()

        assert data["player_id"] == sample_player.id
        assert data["goal_and_assist"] == 8
        assert data["interception"] == 7
        assert data["red_cards"] == 1
        assert data["second_yellow_cards"] == 1
        assert set(data["ranks"]) == {
            key for key, definition in PLAYER_V2_METRICS.items() if definition.rankable
        }
        assert data["ranks"]["goal"] == 2
        assert data["ranks"]["goal_pass"] == 1
        assert data["ranks"]["goal_and_assist"] == 2
        assert data["ranks"]["xg"] == 2
        assert data["ranks"]["interception"] == 2
        # sample has yellow_cards=2, others are [0, 2] — asc ranking with
        # zero excluded yields [2, 2] tied at rank 1.
        assert data["ranks"]["yellow_cards"] == 1
        # sample has red_cards=1, others are [0, 1] — same story.
        assert data["ranks"]["red_cards"] == 1
        assert "extra_stats" not in data

    async def test_get_player_stats_table_v2_matches_canonical_metrics(
        self,
        client: AsyncClient,
        test_session,
        sample_player,
        sample_season,
        sample_teams,
    ):
        test_session.add(
            PlayerTeam(
                player_id=sample_player.id,
                team_id=sample_teams[0].id,
                season_id=sample_season.id,
                number=10,
                amplua=3,
            )
        )
        test_session.add(
            PlayerSeasonStats(
                player_id=sample_player.id,
                season_id=sample_season.id,
                team_id=sample_teams[0].id,
                games_played=12,
                time_on_field_total=900,
                goal=4,
                goal_pass=2,
                goal_and_assist=6,
                tackle=9,
                red_cards=1,
                second_yellow_cards=2,
            )
        )
        await test_session.commit()

        response = await client.get(
            f"/api/v2/seasons/{sample_season.id}/player-stats?sort_by=goal&lang=ru"
        )
        assert response.status_code == 200
        data = response.json()

        assert data["season_id"] == sample_season.id
        assert data["sort_by"] == "goal"
        assert data["total"] == 1
        assert data["items"][0]["goal_and_assist"] == 6
        assert data["items"][0]["red_cards"] == 1
        assert data["items"][0]["second_yellow_cards"] == 2
        assert data["items"][0]["position_code"] == "MID"

    async def test_get_team_stats_v2_returns_derived_metrics(
        self,
        client: AsyncClient,
        test_session,
        sample_season,
        sample_teams,
    ):
        test_session.add(
            TeamSeasonStats(
                team_id=sample_teams[0].id,
                season_id=sample_season.id,
                games_played=4,
                win=3,
                draw=1,
                match_loss=0,
                goal=9,
                goals_conceded=2,
                goals_difference=999,
                points=10,
                shot=20,
                shots_on_goal=11,
                passes=120,
                foul=8,
            )
        )
        test_session.add_all(
            [
                TeamSeasonStats(
                    team_id=sample_teams[1].id,
                    season_id=sample_season.id,
                    games_played=4,
                    win=4,
                    draw=0,
                    match_loss=0,
                    goal=7,
                    goals_conceded=4,
                    goals_difference=3,
                    points=12,
                    shot=18,
                    shots_on_goal=7,
                    passes=110,
                    foul=4,
                ),
                TeamSeasonStats(
                    team_id=sample_teams[2].id,
                    season_id=sample_season.id,
                    games_played=4,
                    win=3,
                    draw=1,
                    match_loss=0,
                    goal=6,
                    goals_conceded=2,
                    goals_difference=4,
                    points=10,
                    shot=16,
                    shots_on_goal=6,
                    passes=100,
                    foul=12,
                ),
            ]
        )
        test_session.add_all(
            [
                Game(
                    sota_id=uuid4(),
                    date=date(2025, 5, 20),
                    time=time(18, 0),
                    tour=2,
                    season_id=sample_season.id,
                    home_team_id=sample_teams[0].id,
                    away_team_id=sample_teams[1].id,
                    home_score=2,
                    away_score=0,
                    has_stats=True,
                ),
                Game(
                    sota_id=uuid4(),
                    date=date(2025, 5, 27),
                    time=time(18, 0),
                    tour=3,
                    season_id=sample_season.id,
                    home_team_id=sample_teams[2].id,
                    away_team_id=sample_teams[0].id,
                    home_score=0,
                    away_score=1,
                    has_stats=True,
                ),
            ]
        )
        await test_session.commit()

        response = await client.get(
            f"/api/v2/teams/{sample_teams[0].id}/stats?season_id={sample_season.id}"
        )
        assert response.status_code == 200
        data = response.json()

        assert data["goal_difference"] == 7
        assert data["goals_per_match"] == 2.25
        assert data["shot_accuracy"] == 55.0
        assert data["pass_per_match"] == 30.0
        assert data["foul_per_match"] == 2.0
        assert data["clean_sheets"] == 2
        assert set(data["ranks"]) == {
            key for key, definition in TEAM_V2_METRICS.items() if definition.rankable
        }
        assert data["ranks"]["points"] == 2
        assert data["ranks"]["goal_difference"] == 1
        assert data["ranks"]["goals_conceded"] == 1
        # All three fixture teams have match_loss=0 — asc ranking with zero
        # excluded gives every team a null rank for this metric.
        assert data["ranks"]["match_loss"] is None
        assert "extra_stats" not in data

    async def test_get_team_stats_table_v2_sorts_by_goal_difference(
        self,
        client: AsyncClient,
        test_session,
        sample_season,
        sample_teams,
    ):
        test_session.add_all(
            [
                TeamSeasonStats(
                    team_id=sample_teams[0].id,
                    season_id=sample_season.id,
                    games_played=5,
                    goal=10,
                    goals_conceded=3,
                    goals_difference=0,
                    points=11,
                ),
                TeamSeasonStats(
                    team_id=sample_teams[1].id,
                    season_id=sample_season.id,
                    games_played=5,
                    goal=7,
                    goals_conceded=5,
                    goals_difference=100,
                    points=12,
                ),
            ]
        )
        await test_session.commit()

        response = await client.get(
            f"/api/v2/seasons/{sample_season.id}/team-stats?sort_by=goal_difference&lang=ru"
        )
        assert response.status_code == 200
        data = response.json()

        assert data["total"] == 2
        assert data["items"][0]["team_id"] == sample_teams[0].id
        assert data["items"][0]["goal_difference"] == 7
        assert data["items"][1]["goal_difference"] == 2
