import pytest
from httpx import AsyncClient
from uuid import uuid4
from datetime import date, time


@pytest.mark.asyncio
class TestSeasonsAPI:
    """Tests for /api/v1/seasons endpoints."""

    async def test_get_seasons_empty(self, client: AsyncClient):
        """Test getting seasons when database is empty."""
        response = await client.get("/api/v1/seasons")
        assert response.status_code == 200
        data = response.json()
        assert data["items"] == []
        assert data["total"] == 0

    async def test_get_seasons_with_data(self, client: AsyncClient, sample_season):
        """Test getting seasons with data."""
        response = await client.get("/api/v1/seasons")
        assert response.status_code == 200
        data = response.json()
        assert len(data["items"]) == 1
        assert data["items"][0]["id"] == 61
        assert data["items"][0]["name"] == "2025"
        assert data["total"] == 1

    async def test_get_season_by_id(self, client: AsyncClient, sample_season):
        """Test getting a specific season."""
        response = await client.get("/api/v1/seasons/61")
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == 61
        assert data["name"] == "2025"

    async def test_get_season_not_found(self, client: AsyncClient):
        """Test 404 for non-existent season."""
        response = await client.get("/api/v1/seasons/99999")
        assert response.status_code == 404
        assert response.json()["detail"] == "Season not found"

    async def test_get_season_table_empty(self, client: AsyncClient, sample_season):
        """Test getting empty season table."""
        response = await client.get("/api/v1/seasons/61/table")
        assert response.status_code == 200
        data = response.json()
        assert data["season_id"] == 61
        assert data["table"] == []

    async def test_get_season_table_with_data(
        self, client: AsyncClient, sample_season, sample_score_table
    ):
        """Test getting season table with data."""
        response = await client.get("/api/v1/seasons/61/table")
        assert response.status_code == 200
        data = response.json()
        assert data["season_id"] == 61
        assert len(data["table"]) == 3
        assert data["table"][0]["position"] == 1

    async def test_get_season_games(self, client: AsyncClient, sample_season, sample_game):
        """Test getting games for a season."""
        response = await client.get("/api/v1/seasons/61/games")
        assert response.status_code == 200
        data = response.json()
        assert len(data["items"]) == 1
        assert data["total"] == 1

    async def test_get_season_games_by_tour(
        self, client: AsyncClient, sample_season, sample_game
    ):
        """Test filtering games by tour."""
        response = await client.get("/api/v1/seasons/61/games?tour=1")
        assert response.status_code == 200
        data = response.json()
        assert len(data["items"]) == 1

        response = await client.get("/api/v1/seasons/61/games?tour=99")
        assert response.status_code == 200
        data = response.json()
        assert len(data["items"]) == 0

    async def test_get_player_stats_includes_position_code(
        self, client: AsyncClient, test_session, sample_season, sample_teams, sample_player
    ):
        """Player stats table includes player_type/top_role/position_code fields."""
        from app.models import PlayerSeasonStats

        entry = PlayerSeasonStats(
            player_id=sample_player.id,
            season_id=sample_season.id,
            team_id=sample_teams[0].id,
            games_played=1,
            minutes_played=90,
            goals=1,
            assists=0,
        )
        test_session.add(entry)
        await test_session.commit()

        response = await client.get("/api/v1/seasons/61/player-stats?lang=ru&limit=1")
        assert response.status_code == 200
        data = response.json()

        assert data["total"] == 1
        assert len(data["items"]) == 1
        item = data["items"][0]

        assert item["player_type"] == "halfback"
        assert item["top_role"] == "AM (attacking midfielder)"
        assert item["position_code"] == "MID"

    async def test_get_player_stats_team_id_filter(
        self, client: AsyncClient, test_session, sample_season, sample_teams, sample_player
    ):
        """Player stats table supports filtering by team_id."""
        from app.models import Player, PlayerSeasonStats

        other_player = Player(
            first_name="Other",
            last_name="Player",
            player_type="goalkeeper",
            top_role="Goalkeeper",
        )
        test_session.add(other_player)
        await test_session.commit()

        entry1 = PlayerSeasonStats(
            player_id=sample_player.id,
            season_id=sample_season.id,
            team_id=sample_teams[0].id,
            games_played=1,
            minutes_played=90,
            goals=1,
            assists=0,
        )
        entry2 = PlayerSeasonStats(
            player_id=other_player.id,
            season_id=sample_season.id,
            team_id=sample_teams[1].id,
            games_played=1,
            minutes_played=90,
            goals=0,
            assists=0,
            save_shot=5,
            dry_match=1,
        )
        test_session.add_all([entry1, entry2])
        await test_session.commit()

        response = await client.get(
            f"/api/v1/seasons/{sample_season.id}/player-stats?lang=ru&team_id={sample_teams[0].id}"
        )
        assert response.status_code == 200
        data = response.json()

        assert data["total"] == 1
        assert len(data["items"]) == 1
        assert data["items"][0]["team_id"] == sample_teams[0].id
        assert data["items"][0]["player_id"] == sample_player.id

    async def test_get_player_stats_nationality_filter_kz(
        self, client: AsyncClient, test_session, sample_season, sample_teams
    ):
        """Player stats table supports filtering by Kazakhstan citizenship."""
        from app.models import Country, Player, PlayerSeasonStats

        kz_country = Country(code="KZ", name="Казахстан")
        foreign_country = Country(code="BR", name="Бразилия")
        test_session.add_all([kz_country, foreign_country])
        await test_session.commit()

        kz_player = Player(
            first_name="Local",
            last_name="Player",
            player_type="midfielder",
            top_role="CM",
            country_id=kz_country.id,
        )
        foreign_player = Player(
            first_name="Foreign",
            last_name="Player",
            player_type="forward",
            top_role="FW",
            country_id=foreign_country.id,
        )
        test_session.add_all([kz_player, foreign_player])
        await test_session.commit()

        test_session.add_all(
            [
                PlayerSeasonStats(
                    player_id=kz_player.id,
                    season_id=sample_season.id,
                    team_id=sample_teams[0].id,
                    games_played=1,
                    minutes_played=90,
                    goals=2,
                ),
                PlayerSeasonStats(
                    player_id=foreign_player.id,
                    season_id=sample_season.id,
                    team_id=sample_teams[1].id,
                    games_played=1,
                    minutes_played=90,
                    goals=3,
                ),
            ]
        )
        await test_session.commit()

        response = await client.get(
            f"/api/v1/seasons/{sample_season.id}/player-stats?lang=ru&nationality=kz&limit=10"
        )
        assert response.status_code == 200
        data = response.json()

        assert data["total"] == 1
        assert len(data["items"]) == 1
        assert data["items"][0]["player_id"] == kz_player.id
        assert data["items"][0]["country"]["code"] == "KZ"

    async def test_get_player_stats_nationality_filter_foreign(
        self, client: AsyncClient, test_session, sample_season, sample_teams
    ):
        """Player stats table supports filtering by foreign players."""
        from app.models import Country, Player, PlayerSeasonStats

        kz_country = Country(code="KZ", name="Казахстан")
        foreign_country = Country(code="RS", name="Сербия")
        test_session.add_all([kz_country, foreign_country])
        await test_session.commit()

        kz_player = Player(
            first_name="Local",
            last_name="One",
            player_type="midfielder",
            top_role="CM",
            country_id=kz_country.id,
        )
        foreign_player = Player(
            first_name="Foreign",
            last_name="One",
            player_type="forward",
            top_role="FW",
            country_id=foreign_country.id,
        )
        no_country_player = Player(
            first_name="Unknown",
            last_name="Country",
            player_type="defender",
            top_role="CB",
            country_id=None,
        )
        test_session.add_all([kz_player, foreign_player, no_country_player])
        await test_session.commit()

        test_session.add_all(
            [
                PlayerSeasonStats(
                    player_id=kz_player.id,
                    season_id=sample_season.id,
                    team_id=sample_teams[0].id,
                    games_played=1,
                    minutes_played=90,
                    goals=1,
                ),
                PlayerSeasonStats(
                    player_id=foreign_player.id,
                    season_id=sample_season.id,
                    team_id=sample_teams[1].id,
                    games_played=1,
                    minutes_played=90,
                    goals=2,
                ),
                PlayerSeasonStats(
                    player_id=no_country_player.id,
                    season_id=sample_season.id,
                    team_id=sample_teams[2].id,
                    games_played=1,
                    minutes_played=90,
                    goals=3,
                ),
            ]
        )
        await test_session.commit()

        response = await client.get(
            f"/api/v1/seasons/{sample_season.id}/player-stats?lang=ru&nationality=foreign&limit=10"
        )
        assert response.status_code == 200
        data = response.json()

        assert data["total"] == 1
        assert len(data["items"]) == 1
        assert data["items"][0]["player_id"] == foreign_player.id
        assert data["items"][0]["country"]["code"] == "RS"

    async def test_get_player_stats_position_code_filter_gk(
        self, client: AsyncClient, test_session, sample_season, sample_teams, sample_player
    ):
        """Player stats table supports filtering by computed position_code."""
        from app.models import Player, PlayerSeasonStats

        gk_player = Player(
            first_name="GK",
            last_name="One",
            player_type="goalkeeper",
            top_role="Goalkeeper",
        )
        test_session.add(gk_player)
        await test_session.commit()

        entries = [
            PlayerSeasonStats(
                player_id=sample_player.id,
                season_id=sample_season.id,
                team_id=sample_teams[0].id,
                games_played=1,
                minutes_played=90,
                goals=1,
                assists=0,
            ),
            PlayerSeasonStats(
                player_id=gk_player.id,
                season_id=sample_season.id,
                team_id=sample_teams[0].id,
                games_played=1,
                minutes_played=90,
                goals=0,
                assists=0,
                save_shot=7,
                dry_match=1,
            ),
        ]
        test_session.add_all(entries)
        await test_session.commit()

        response = await client.get(
            f"/api/v1/seasons/{sample_season.id}/player-stats?lang=ru&position_code=GK&limit=10"
        )
        assert response.status_code == 200
        data = response.json()

        assert data["total"] == 1
        assert len(data["items"]) == 1
        item = data["items"][0]
        assert item["player_id"] == gk_player.id
        assert item["position_code"] == "GK"

    async def test_get_player_stats_position_code_sort_by_save_shot(
        self, client: AsyncClient, test_session, sample_season, sample_teams
    ):
        """Position filter path sorts correctly by save_shot."""
        from app.models import Player, PlayerSeasonStats

        gk1 = Player(
            first_name="GK",
            last_name="A",
            player_type="goalkeeper",
            top_role="Goalkeeper",
        )
        gk2 = Player(
            first_name="GK",
            last_name="B",
            player_type="goalkeeper",
            top_role="Goalkeeper",
        )
        test_session.add_all([gk1, gk2])
        await test_session.commit()

        entry1 = PlayerSeasonStats(
            player_id=gk1.id,
            season_id=sample_season.id,
            team_id=sample_teams[0].id,
            games_played=1,
            minutes_played=90,
            save_shot=10,
            dry_match=1,
        )
        entry2 = PlayerSeasonStats(
            player_id=gk2.id,
            season_id=sample_season.id,
            team_id=sample_teams[0].id,
            games_played=1,
            minutes_played=90,
            save_shot=5,
            dry_match=0,
        )
        test_session.add_all([entry1, entry2])
        await test_session.commit()

        response = await client.get(
            f"/api/v1/seasons/{sample_season.id}/player-stats?lang=ru&position_code=GK&sort_by=save_shot&limit=10"
        )
        assert response.status_code == 200
        data = response.json()

        assert data["total"] == 2
        assert len(data["items"]) == 2
        assert data["items"][0]["player_id"] == gk1.id
        assert data["items"][0]["save_shot"] == 10
        assert data["items"][1]["player_id"] == gk2.id
        assert data["items"][1]["save_shot"] == 5

    async def test_get_player_stats_sanitizes_nan_metrics(
        self, client: AsyncClient, test_session, sample_season, sample_teams, sample_player
    ):
        """Player stats table should not return non-JSON NaN numeric values."""
        from app.models import PlayerSeasonStats

        entry = PlayerSeasonStats(
            player_id=sample_player.id,
            season_id=sample_season.id,
            team_id=sample_teams[0].id,
            games_played=1,
            minutes_played=90,
            goals=1,
            assists=0,
            xg=float("nan"),
            pass_accuracy=float("nan"),
        )
        test_session.add(entry)
        await test_session.commit()

        response = await client.get(
            f"/api/v1/seasons/{sample_season.id}/player-stats?lang=ru&sort_by=goals&limit=10"
        )
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert data["items"][0]["xg"] is None
        assert data["items"][0]["pass_accuracy"] is None

    async def test_get_team_stats_fallback_from_score_table(
        self, client: AsyncClient, sample_season, sample_teams, sample_score_table
    ):
        """Team stats table falls back to score_table when TeamSeasonStats is empty."""
        response = await client.get("/api/v1/seasons/61/team-stats?lang=ru")
        assert response.status_code == 200
        data = response.json()

        assert data["season_id"] == 61
        assert data["sort_by"] == "points"
        assert data["total"] == 3
        assert len(data["items"]) == 3

        for item in data["items"]:
            assert item["team_id"] in {91, 13, 90}
            assert item["points"] is not None
            assert item["goals_per_match"] is not None

    async def test_get_team_stats_fallback_from_games(
        self, client: AsyncClient, sample_season, sample_teams, sample_game
    ):
        """Team stats table falls back to finished games when no score_table exists."""
        response = await client.get("/api/v1/seasons/61/team-stats?lang=ru")
        assert response.status_code == 200
        data = response.json()

        assert data["season_id"] == 61
        assert data["sort_by"] == "points"
        assert data["total"] == 2
        assert len(data["items"]) == 2

        # Sample game is 2-1 for home team; home team should be first with 3 points.
        assert data["items"][0]["team_id"] == sample_teams[0].id
        assert data["items"][0]["points"] == 3

    async def test_get_season_teams_fallback_from_score_table(
        self, client: AsyncClient, sample_season, sample_score_table
    ):
        """Season teams endpoint falls back to score_table when TeamTournament is empty."""
        response = await client.get("/api/v1/seasons/61/teams?lang=ru")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 3
        assert {item["team_id"] for item in data["items"]} == {91, 13, 90}

    async def test_get_season_teams_backfills_partial_team_tournament(
        self, client: AsyncClient, test_session, sample_season, sample_teams, sample_score_table
    ):
        """Season teams endpoint should include fallback teams beyond TeamTournament rows."""
        from app.models import TeamTournament

        test_session.add(TeamTournament(team_id=sample_teams[0].id, season_id=sample_season.id, group_name="A"))
        await test_session.commit()

        response = await client.get("/api/v1/seasons/61/teams?lang=ru")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 3
        assert {item["team_id"] for item in data["items"]} == {
            sample_teams[0].id,
            sample_teams[1].id,
            sample_teams[2].id,
        }
        grouped = {item["team_id"]: item["group_name"] for item in data["items"]}
        assert grouped[sample_teams[0].id] == "A"

    async def test_get_team_stats_backfills_missing_participants_for_partial_stats(
        self,
        client: AsyncClient,
        test_session,
        sample_season,
        sample_teams,
        sample_score_table,
    ):
        """Team stats endpoint should include teams missing from TeamSeasonStats rows."""
        from app.models import TeamSeasonStats

        test_session.add(
            TeamSeasonStats(
                team_id=sample_teams[0].id,
                season_id=sample_season.id,
                games_played=10,
                wins=6,
                draws=2,
                losses=2,
                goals_scored=18,
                goals_conceded=9,
                goals_difference=9,
                points=20,
            )
        )
        await test_session.commit()

        response = await client.get("/api/v1/seasons/61/team-stats?lang=ru")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 3
        assert {item["team_id"] for item in data["items"]} == {
            sample_teams[0].id,
            sample_teams[1].id,
            sample_teams[2].id,
        }

    async def test_get_goals_by_period_season_not_found(self, client: AsyncClient):
        """Goals-by-period endpoint returns 404 for unknown season."""
        response = await client.get("/api/v1/seasons/99999/goals-by-period")
        assert response.status_code == 404
        assert response.json()["detail"] == "Season not found"

    async def test_get_goals_by_period_empty_events(
        self, client: AsyncClient, sample_season, sample_game
    ):
        """Goals-by-period returns empty buckets and zero coverage when no goal events exist."""
        response = await client.get("/api/v1/seasons/61/goals-by-period")
        assert response.status_code == 200
        data = response.json()

        assert data["season_id"] == 61
        assert data["period_size_minutes"] == 15
        assert len(data["periods"]) == 6
        assert [p["period"] for p in data["periods"]] == [
            "0-15",
            "16-30",
            "31-45+",
            "46-60",
            "61-75",
            "76-90+",
        ]
        assert all(p["goals"] == 0 for p in data["periods"])
        assert all(p["home"] == 0 for p in data["periods"])
        assert all(p["away"] == 0 for p in data["periods"])
        assert data["meta"] == {
            "matches_played": 1,
            "matches_with_goal_events": 0,
            "coverage_pct": 0.0,
        }

    async def test_get_goals_by_period_bucketing_and_coverage(
        self, client: AsyncClient, test_session, sample_season, sample_teams, sample_game
    ):
        """Goals-by-period supports 6 buckets, home/away split, stoppage, and coverage metadata."""
        from app.models import Game, GameEvent, GameEventType

        second_finished_game = Game(
            sota_id=uuid4(),
            date=date(2025, 5, 22),
            time=time(19, 0),
            tour=2,
            season_id=sample_season.id,
            home_team_id=sample_teams[1].id,
            away_team_id=sample_teams[2].id,
            home_score=0,
            away_score=0,
            has_stats=True,
            stadium="Central Stadium",
            visitors=9000,
        )

        goal_events = [
            GameEvent(
                game_id=sample_game.id,
                half=1,
                minute=10,
                event_type=GameEventType.goal,
                team_id=sample_teams[0].id,
            ),
            GameEvent(
                game_id=sample_game.id,
                half=1,
                minute=20,
                event_type=GameEventType.goal,
                team_id=sample_teams[1].id,
            ),
            # First-half stoppage time should stay in 31-45+ bucket.
            GameEvent(
                game_id=sample_game.id,
                half=1,
                minute=47,
                event_type=GameEventType.goal,
                team_id=sample_teams[0].id,
            ),
            GameEvent(
                game_id=sample_game.id,
                half=2,
                minute=50,
                event_type=GameEventType.goal,
                team_id=sample_teams[1].id,
            ),
            GameEvent(
                game_id=sample_game.id,
                half=2,
                minute=70,
                event_type=GameEventType.goal,
                team_id=sample_teams[0].id,
            ),
            # Second-half stoppage time should stay in 76-90+ bucket.
            GameEvent(
                game_id=sample_game.id,
                half=2,
                minute=95,
                event_type=GameEventType.goal,
                team_id=sample_teams[1].id,
            ),
        ]

        test_session.add(second_finished_game)
        test_session.add_all(goal_events)
        await test_session.commit()

        response = await client.get(f"/api/v1/seasons/{sample_season.id}/goals-by-period")
        assert response.status_code == 200
        data = response.json()

        periods = {item["period"]: item for item in data["periods"]}
        assert periods["0-15"] == {"period": "0-15", "goals": 1, "home": 1, "away": 0}
        assert periods["16-30"] == {"period": "16-30", "goals": 1, "home": 0, "away": 1}
        assert periods["31-45+"] == {"period": "31-45+", "goals": 1, "home": 1, "away": 0}
        assert periods["46-60"] == {"period": "46-60", "goals": 1, "home": 0, "away": 1}
        assert periods["61-75"] == {"period": "61-75", "goals": 1, "home": 1, "away": 0}
        assert periods["76-90+"] == {"period": "76-90+", "goals": 1, "home": 0, "away": 1}

        assert data["meta"] == {
            "matches_played": 2,
            "matches_with_goal_events": 1,
            "coverage_pct": 50.0,
        }
