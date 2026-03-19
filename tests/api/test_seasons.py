import pytest
from httpx import AsyncClient
from uuid import uuid4
from datetime import date, time, datetime


async def _seed_score_table(
    test_session,
    *,
    season_id: int,
    start_team_id: int,
    team_count: int,
):
    from app.models import ScoreTable, Team

    teams = [
        Team(id=start_team_id + index, name=f"Team {start_team_id + index}")
        for index in range(team_count)
    ]
    test_session.add_all(teams)
    await test_session.flush()

    entries = []
    for index, team in enumerate(teams, 1):
        entries.append(
            ScoreTable(
                season_id=season_id,
                team_id=team.id,
                position=index,
                games_played=20,
                wins=max(0, 20 - index),
                draws=0,
                losses=index - 1,
                goals_scored=max(0, 30 - index),
                goals_conceded=index,
                goal_difference=max(0, 30 - index) - index,
                points=100 - index,
            )
        )

    test_session.add_all(entries)
    await test_session.commit()


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

    async def test_get_seasons_excludes_hidden(self, client: AsyncClient, test_session, sample_championship):
        from app.models import Season

        visible = Season(
            id=61,
            name="2025",
            championship_id=sample_championship.id,
            date_start=date(2025, 3, 1),
            date_end=date(2025, 11, 30),
            is_visible=True,
        )
        hidden = Season(
            id=62,
            name="2024",
            championship_id=sample_championship.id,
            date_start=date(2024, 3, 1),
            date_end=date(2024, 11, 30),
            is_visible=False,
        )
        test_session.add_all([visible, hidden])
        await test_session.commit()

        response = await client.get("/api/v1/seasons")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert data["items"][0]["id"] == 61

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

    async def test_get_hidden_season_returns_404(self, client: AsyncClient, sample_season, test_session):
        sample_season.is_visible = False
        await test_session.commit()

        response = await client.get(f"/api/v1/seasons/{sample_season.id}")
        assert response.status_code == 404
        assert response.json()["detail"] == "Season not found"

    async def test_hidden_season_subresource_returns_404(
        self, client: AsyncClient, sample_season, test_session
    ):
        sample_season.is_visible = False
        await test_session.commit()

        response = await client.get(f"/api/v1/seasons/{sample_season.id}/table")
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

    async def test_get_season_table_includes_zone_field_and_champion_euro_defaults(
        self, client: AsyncClient, sample_season, sample_score_table, test_session
    ):
        sample_season.champion_spots = 1
        sample_season.euro_cup_spots = 2
        sample_season.relegation_spots = 0
        await test_session.commit()

        response = await client.get("/api/v1/seasons/61/table")
        assert response.status_code == 200
        data = response.json()

        assert data["table"][0]["zone"] == "champion"
        assert data["table"][1]["zone"] == "euro_cups"
        assert data["table"][2]["zone"] == "euro_cups"

    async def test_get_season_table_relegation_zone_for_season_61_marks_only_last_team(
        self, client: AsyncClient, sample_season, test_session
    ):
        sample_season.champion_spots = 1
        sample_season.euro_cup_spots = 2
        sample_season.relegation_spots = 1
        await test_session.commit()

        await _seed_score_table(
            test_session,
            season_id=sample_season.id,
            start_team_id=6100,
            team_count=6,
        )

        response = await client.get("/api/v1/seasons/61/table")
        assert response.status_code == 200
        data = response.json()

        relegation_positions = [row["position"] for row in data["table"] if row["zone"] == "relegation"]
        assert relegation_positions == [6]

    async def test_get_season_table_relegation_zone_for_season_200_marks_last_two_teams(
        self, client: AsyncClient, sample_championship, test_session
    ):
        from app.models import Season

        season_200 = Season(
            id=200,
            name="2026",
            championship_id=sample_championship.id,
            date_start=date(2026, 3, 1),
            date_end=date(2026, 11, 30),
            champion_spots=1,
            euro_cup_spots=2,
            relegation_spots=2,
        )
        test_session.add(season_200)
        await test_session.flush()

        await _seed_score_table(
            test_session,
            season_id=season_200.id,
            start_team_id=6200,
            team_count=6,
        )

        response = await client.get("/api/v1/seasons/200/table")
        assert response.status_code == 200
        data = response.json()

        relegation_positions = [row["position"] for row in data["table"] if row["zone"] == "relegation"]
        assert relegation_positions == [5, 6]

    async def test_get_season_table_with_zero_relegation_spots_has_no_relegation_zone(
        self, client: AsyncClient, sample_championship, test_session
    ):
        from app.models import Season

        season_201 = Season(
            id=201,
            name="2027",
            championship_id=sample_championship.id,
            date_start=date(2027, 3, 1),
            date_end=date(2027, 11, 30),
            champion_spots=1,
            euro_cup_spots=2,
            relegation_spots=0,
        )
        test_session.add(season_201)
        await test_session.flush()

        await _seed_score_table(
            test_session,
            season_id=season_201.id,
            start_team_id=6300,
            team_count=6,
        )

        response = await client.get("/api/v1/seasons/201/table")
        assert response.status_code == 200
        data = response.json()

        relegation_positions = [row["position"] for row in data["table"] if row["zone"] == "relegation"]
        assert relegation_positions == []

    async def test_get_season_table_group_filter_uses_group_participants(
        self, client: AsyncClient, test_session, sample_season, sample_teams, sample_game
    ):
        """Table group filter should be calculated dynamically from group matches."""
        from app.models import SeasonParticipant

        test_session.add_all(
            [
                SeasonParticipant(team_id=sample_teams[0].id, season_id=sample_season.id, group_name="A"),
                SeasonParticipant(team_id=sample_teams[1].id, season_id=sample_season.id, group_name="A"),
                SeasonParticipant(team_id=sample_teams[2].id, season_id=sample_season.id, group_name="B"),
            ]
        )
        await test_session.commit()

        response = await client.get(f"/api/v1/seasons/{sample_season.id}/table?group=A&lang=ru")
        assert response.status_code == 200
        data = response.json()

        assert {row["team_id"] for row in data["table"]} == {
            sample_teams[0].id,
            sample_teams[1].id,
        }
        assert all(row["team_id"] != sample_teams[2].id for row in data["table"])

    async def test_get_season_table_final_filter_uses_final_stage_ids(
        self, client: AsyncClient, test_session, sample_season, sample_teams, sample_game
    ):
        """Table final filter should include only games from season.final_stage_ids."""
        from app.models import Game, Stage

        regular_stage = Stage(season_id=sample_season.id, name="Regular Stage", sort_order=1)
        final_stage = Stage(season_id=sample_season.id, name="Final Stage", sort_order=2)
        test_session.add_all([regular_stage, final_stage])
        await test_session.flush()

        sample_game.stage_id = regular_stage.id
        sample_season.final_stage_ids = [final_stage.id]

        final_game = Game(
            sota_id=uuid4(),
            date=date(2025, 5, 20),
            time=time(20, 0),
            tour=2,
            season_id=sample_season.id,
            home_team_id=sample_teams[1].id,
            away_team_id=sample_teams[2].id,
            home_score=1,
            away_score=0,
            stage_id=final_stage.id,
            has_stats=True,
            visitors=12000,
        )
        test_session.add(final_game)
        await test_session.commit()

        response = await client.get(f"/api/v1/seasons/{sample_season.id}/table?final=true&lang=ru")
        assert response.status_code == 200
        data = response.json()

        assert len(data["table"]) == 2
        assert {row["team_id"] for row in data["table"]} == {
            sample_teams[1].id,
            sample_teams[2].id,
        }
        assert all(row["team_id"] != sample_teams[0].id for row in data["table"])

    async def test_get_season_table_group_and_final_are_mutually_exclusive(
        self, client: AsyncClient, sample_season
    ):
        response = await client.get(f"/api/v1/seasons/{sample_season.id}/table?group=A&final=true")
        assert response.status_code == 400
        assert "mutually exclusive" in response.json()["detail"]

    async def test_get_results_grid_final_filter_uses_final_stage_ids(
        self, client: AsyncClient, test_session, sample_season, sample_teams, sample_score_table, sample_game
    ):
        """Results grid final filter should include only final-stage participants."""
        from app.models import Game, Stage

        regular_stage = Stage(season_id=sample_season.id, name="Regular Stage", sort_order=1)
        final_stage = Stage(season_id=sample_season.id, name="Final Stage", sort_order=2)
        test_session.add_all([regular_stage, final_stage])
        await test_session.flush()

        sample_game.stage_id = regular_stage.id
        sample_season.final_stage_ids = [final_stage.id]

        final_game = Game(
            sota_id=uuid4(),
            date=date(2025, 5, 20),
            time=time(20, 0),
            tour=2,
            season_id=sample_season.id,
            home_team_id=sample_teams[1].id,
            away_team_id=sample_teams[2].id,
            home_score=1,
            away_score=0,
            stage_id=final_stage.id,
            has_stats=True,
            visitors=12000,
        )
        test_session.add(final_game)
        await test_session.commit()

        response = await client.get(f"/api/v1/seasons/{sample_season.id}/results-grid?final=true&lang=ru")
        assert response.status_code == 200
        data = response.json()

        assert {row["team_id"] for row in data["teams"]} == {
            sample_teams[1].id,
            sample_teams[2].id,
        }
        assert all(row["team_id"] != sample_teams[0].id for row in data["teams"])

    async def test_get_results_grid_group_and_final_are_mutually_exclusive(
        self, client: AsyncClient, sample_season
    ):
        response = await client.get(f"/api/v1/seasons/{sample_season.id}/results-grid?group=A&final=true")
        assert response.status_code == 400
        assert "mutually exclusive" in response.json()["detail"]

    async def test_get_season_games(self, client: AsyncClient, sample_season, sample_game):
        """Test getting games for a season."""
        response = await client.get("/api/v1/seasons/61/games")
        assert response.status_code == 200
        data = response.json()
        assert len(data["items"]) == 1
        assert data["total"] == 1
        assert data["items"][0]["is_schedule_tentative"] is False

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

    async def test_get_season_games_includes_schedule_tentative_flag_when_true(
        self, client: AsyncClient, test_session, sample_game
    ):
        sample_game.is_schedule_tentative = True
        await test_session.commit()

        response = await client.get("/api/v1/seasons/61/games")
        assert response.status_code == 200
        data = response.json()
        assert data["items"][0]["is_schedule_tentative"] is True

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

    async def test_get_season_teams_returns_empty_without_season_participants(
        self, client: AsyncClient, sample_season, sample_score_table
    ):
        """Season teams endpoint should be strict and not fallback to score_table."""
        response = await client.get("/api/v1/seasons/61/teams?lang=ru")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 0
        assert data["items"] == []

    async def test_get_season_teams_does_not_backfill_partial_season_participants(
        self, client: AsyncClient, test_session, sample_season, sample_teams, sample_score_table
    ):
        """Season teams endpoint should return only explicit season_participants."""
        from app.models import SeasonParticipant

        test_session.add(SeasonParticipant(team_id=sample_teams[0].id, season_id=sample_season.id, group_name="A"))
        await test_session.commit()

        response = await client.get("/api/v1/seasons/61/teams?lang=ru")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert {item["team_id"] for item in data["items"]} == {sample_teams[0].id}
        grouped = {item["team_id"]: item["group_name"] for item in data["items"]}
        assert grouped[sample_teams[0].id] == "A"

    async def test_get_team_stats_partial_stats_without_season_participants_returns_partial(
        self,
        client: AsyncClient,
        test_session,
        sample_season,
        sample_teams,
        sample_score_table,
    ):
        """Team stats endpoint should not synthesize missing teams without season_participants."""
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
        assert data["total"] == 1
        assert {item["team_id"] for item in data["items"]} == {sample_teams[0].id}

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
            visitors=9000,
            extended_stats_synced_at=datetime(2025, 5, 22, 22, 0, 0),
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

    # ── effective_max_round / _compute_stats_scope tests ──────────────

    async def test_statistics_round_robin_no_completed_tour_shows_zero(
        self, client: AsyncClient, test_session, sample_championship, sample_teams,
    ):
        """Round-robin season with no fully completed tour → matches_played=0, all stats zero."""
        from app.models import Season, Game
        from app.services.season_visibility import invalidate_season_cache

        season = Season(
            id=300,
            name="2026 RR empty",
            championship_id=sample_championship.id,
            date_start=date(2025, 3, 1),
            date_end=date(2025, 11, 30),
            tournament_format="round_robin",
            has_table=True,
            is_visible=True,
        )
        test_session.add(season)
        await test_session.flush()

        # Tour 1: only 1 of 2 games finished → tour not complete
        test_session.add_all([
            Game(
                sota_id=uuid4(),
                date=date(2025, 4, 1),
                time=time(18, 0),
                tour=1,
                season_id=300,
                home_team_id=sample_teams[0].id,
                away_team_id=sample_teams[1].id,
                home_score=1,
                away_score=0,
            ),
            Game(
                sota_id=uuid4(),
                date=date(2025, 4, 1),
                time=time(18, 0),
                tour=1,
                season_id=300,
                home_team_id=sample_teams[1].id,
                away_team_id=sample_teams[2].id,
                home_score=None,
                away_score=None,
            ),
        ])
        await test_session.commit()
        invalidate_season_cache()

        response = await client.get("/api/v1/seasons/300/statistics")
        assert response.status_code == 200
        data = response.json()

        assert data["matches_played"] == 0
        assert data["total_goals"] == 0
        assert data["max_completed_round"] is None

    async def test_statistics_pass_accuracy_null_when_no_matches_in_scope(
        self, client: AsyncClient, test_session, sample_championship, sample_teams,
    ):
        """pass_accuracy must be null when matches_played=0, even if TeamSeasonStats has data."""
        from app.models import Season, Game, TeamSeasonStats
        from app.services.season_visibility import invalidate_season_cache

        season = Season(
            id=310,
            name="2026 PA leak",
            championship_id=sample_championship.id,
            date_start=date(2025, 3, 1),
            date_end=date(2025, 11, 30),
            tournament_format="round_robin",
            has_table=True,
            is_visible=True,
        )
        test_session.add(season)
        await test_session.flush()

        # No TourSyncStatus → effective_max_round=0 → matches_played=0
        test_session.add(Game(
            sota_id=uuid4(),
            date=date(2025, 4, 1),
            time=time(18, 0),
            tour=1,
            season_id=310,
            home_team_id=sample_teams[0].id,
            away_team_id=sample_teams[1].id,
            home_score=1,
            away_score=0,
            extended_stats_synced_at=datetime(2025, 4, 2, 10, 0, 0),
        ))
        # TeamSeasonStats with pass_ratio — this must NOT leak into the response
        test_session.add_all([
            TeamSeasonStats(season_id=310, team_id=sample_teams[0].id, games_played=1, pass_ratio=77.9),
            TeamSeasonStats(season_id=310, team_id=sample_teams[1].id, games_played=1, pass_ratio=68.2),
        ])
        await test_session.commit()
        invalidate_season_cache()

        response = await client.get("/api/v1/seasons/310/statistics")
        assert response.status_code == 200
        data = response.json()

        assert data["matches_played"] == 0
        assert data["pass_accuracy"] is None

    async def test_statistics_pass_accuracy_null_when_low_coverage_round_scoped(
        self, client: AsyncClient, test_session, sample_championship, sample_teams,
    ):
        """Round-scoped: GameTeamStats lacks pass_accuracy (0/2 coverage) → null, no fallback."""
        from app.models import Season, Game, GameTeamStats, TeamSeasonStats
        from app.models.tour_sync_status import TourSyncStatus
        from app.services.season_visibility import invalidate_season_cache

        season = Season(
            id=311,
            name="2026 PA low cov",
            championship_id=sample_championship.id,
            date_start=date(2025, 3, 1),
            date_end=date(2025, 11, 30),
            tournament_format="round_robin",
            has_table=True,
            is_visible=True,
        )
        test_session.add(season)
        await test_session.flush()

        g = Game(
            sota_id=uuid4(),
            date=date(2025, 4, 1),
            time=time(18, 0),
            tour=1,
            season_id=311,
            home_team_id=sample_teams[0].id,
            away_team_id=sample_teams[1].id,
            home_score=1,
            away_score=0,
            visitors=5000,
            extended_stats_synced_at=datetime(2025, 4, 2, 10, 0, 0),
        )
        test_session.add(g)
        await test_session.flush()

        # GameTeamStats WITHOUT pass_accuracy — 0/2 = 0% coverage
        test_session.add_all([
            GameTeamStats(game_id=g.id, team_id=sample_teams[0].id, fouls=3, pass_accuracy=None),
            GameTeamStats(game_id=g.id, team_id=sample_teams[1].id, fouls=2, pass_accuracy=None),
        ])
        # TeamSeasonStats WITH pass_ratio — must NOT be used (round-scoped, low coverage)
        test_session.add_all([
            TeamSeasonStats(season_id=311, team_id=sample_teams[0].id, games_played=1, pass_ratio=80.0),
            TeamSeasonStats(season_id=311, team_id=sample_teams[1].id, games_played=1, pass_ratio=70.0),
        ])
        test_session.add(TourSyncStatus(season_id=311, tour=1, synced_at=datetime(2025, 4, 2, 12, 0, 0)))
        await test_session.commit()
        invalidate_season_cache()

        response = await client.get("/api/v1/seasons/311/statistics")
        assert response.status_code == 200
        data = response.json()

        assert data["matches_played"] == 1
        assert data["pass_accuracy"] is None  # round-scoped + low coverage → null

    async def test_statistics_pass_accuracy_fallback_full_season(
        self, client: AsyncClient, test_session, sample_championship, sample_teams,
    ):
        """Full-season (cup): GameTeamStats lacks pass_accuracy → fallback to TeamSeasonStats."""
        from app.models import Season, Game, GameTeamStats, TeamSeasonStats
        from app.services.season_visibility import invalidate_season_cache

        season = Season(
            id=312,
            name="Cup 2026 PA fallback",
            championship_id=sample_championship.id,
            date_start=date(2025, 3, 1),
            date_end=date(2025, 11, 30),
            tournament_format="knockout",
            has_table=False,
            is_visible=True,
        )
        test_session.add(season)
        await test_session.flush()

        g = Game(
            sota_id=uuid4(),
            date=date(2025, 4, 1),
            time=time(18, 0),
            tour=1,
            season_id=312,
            home_team_id=sample_teams[0].id,
            away_team_id=sample_teams[1].id,
            home_score=1,
            away_score=0,
            visitors=5000,
            extended_stats_synced_at=datetime(2025, 4, 2, 10, 0, 0),
        )
        test_session.add(g)
        await test_session.flush()

        # GameTeamStats WITHOUT pass_accuracy — 0/2 coverage
        test_session.add_all([
            GameTeamStats(game_id=g.id, team_id=sample_teams[0].id, fouls=3, pass_accuracy=None),
            GameTeamStats(game_id=g.id, team_id=sample_teams[1].id, fouls=2, pass_accuracy=None),
        ])
        # TeamSeasonStats WITH pass_ratio — should be used as full-season fallback
        test_session.add_all([
            TeamSeasonStats(season_id=312, team_id=sample_teams[0].id, games_played=1, pass_ratio=80.0),
            TeamSeasonStats(season_id=312, team_id=sample_teams[1].id, games_played=1, pass_ratio=70.0),
        ])
        await test_session.commit()
        invalidate_season_cache()

        response = await client.get("/api/v1/seasons/312/statistics")
        assert response.status_code == 200
        data = response.json()

        assert data["matches_played"] == 1
        assert data["pass_accuracy"] == 75.0  # full-season fallback: avg(80.0, 70.0)

    async def test_statistics_pass_accuracy_high_coverage(
        self, client: AsyncClient, test_session, sample_championship, sample_teams,
    ):
        """Round-scoped: GameTeamStats has pass_accuracy on all rows (100% coverage) → uses it."""
        from app.models import Season, Game, GameTeamStats
        from app.models.tour_sync_status import TourSyncStatus
        from app.services.season_visibility import invalidate_season_cache

        season = Season(
            id=313,
            name="2026 PA high cov",
            championship_id=sample_championship.id,
            date_start=date(2025, 3, 1),
            date_end=date(2025, 11, 30),
            tournament_format="round_robin",
            has_table=True,
            is_visible=True,
        )
        test_session.add(season)
        await test_session.flush()

        g = Game(
            sota_id=uuid4(),
            date=date(2025, 4, 1),
            time=time(18, 0),
            tour=1,
            season_id=313,
            home_team_id=sample_teams[0].id,
            away_team_id=sample_teams[1].id,
            home_score=1,
            away_score=0,
            visitors=5000,
            extended_stats_synced_at=datetime(2025, 4, 2, 10, 0, 0),
        )
        test_session.add(g)
        await test_session.flush()

        # GameTeamStats WITH pass_accuracy — 2/2 = 100% coverage
        test_session.add_all([
            GameTeamStats(game_id=g.id, team_id=sample_teams[0].id, fouls=3, pass_accuracy=82.5),
            GameTeamStats(game_id=g.id, team_id=sample_teams[1].id, fouls=2, pass_accuracy=71.3),
        ])
        test_session.add(TourSyncStatus(season_id=313, tour=1, synced_at=datetime(2025, 4, 2, 12, 0, 0)))
        await test_session.commit()
        invalidate_season_cache()

        response = await client.get("/api/v1/seasons/313/statistics")
        assert response.status_code == 200
        data = response.json()

        assert data["matches_played"] == 1
        assert data["pass_accuracy"] == 76.9  # avg(82.5, 71.3) = 76.9

    async def test_statistics_round_robin_caps_to_completed_tour(
        self, client: AsyncClient, test_session, sample_championship, sample_teams,
    ):
        """Round-robin season caps stats to highest fully completed tour."""
        from app.models import Season, Game, GameTeamStats
        from app.models.tour_sync_status import TourSyncStatus
        from app.services.season_visibility import invalidate_season_cache

        season = Season(
            id=301,
            name="2026 RR capped",
            championship_id=sample_championship.id,
            date_start=date(2025, 3, 1),
            date_end=date(2025, 11, 30),
            tournament_format="round_robin",
            has_table=True,
            is_visible=True,
        )
        test_session.add(season)
        await test_session.flush()

        # Tour 1: fully complete with TourSyncStatus marker
        game_t1 = Game(
            sota_id=uuid4(),
            date=date(2025, 4, 1),
            time=time(18, 0),
            tour=1,
            season_id=301,
            home_team_id=sample_teams[0].id,
            away_team_id=sample_teams[1].id,
            home_score=2,
            away_score=1,
            visitors=5000,
            extended_stats_synced_at=datetime(2025, 4, 2, 10, 0, 0),
        )
        # Tour 2: partially complete (1 scored, 1 not) — no TourSyncStatus
        game_t2a = Game(
            sota_id=uuid4(),
            date=date(2025, 5, 1),
            time=time(18, 0),
            tour=2,
            season_id=301,
            home_team_id=sample_teams[0].id,
            away_team_id=sample_teams[2].id,
            home_score=3,
            away_score=0,
        )
        game_t2b = Game(
            sota_id=uuid4(),
            date=date(2025, 5, 1),
            time=time(18, 0),
            tour=2,
            season_id=301,
            home_team_id=sample_teams[1].id,
            away_team_id=sample_teams[2].id,
            home_score=None,
            away_score=None,
        )
        test_session.add_all([game_t1, game_t2a, game_t2b])
        await test_session.flush()

        # Add GameTeamStats for game_t1 so team_stats_query doesn't return NULL
        test_session.add_all([
            GameTeamStats(game_id=game_t1.id, team_id=sample_teams[0].id, yellow_cards=1, fouls=5),
            GameTeamStats(game_id=game_t1.id, team_id=sample_teams[1].id, yellow_cards=2, fouls=3),
        ])
        # TourSyncStatus marker for tour 1 only
        test_session.add(TourSyncStatus(season_id=301, tour=1, synced_at=datetime(2025, 4, 2, 12, 0, 0)))
        await test_session.commit()
        invalidate_season_cache()

        response = await client.get("/api/v1/seasons/301/statistics")
        assert response.status_code == 200
        data = response.json()

        # Only tour 1 counts
        assert data["max_completed_round"] == 1
        assert data["matches_played"] == 1
        assert data["total_goals"] == 3  # 2 + 1

    async def test_statistics_cup_season_uncapped(
        self, client: AsyncClient, test_session, sample_championship, sample_teams,
    ):
        """Cup/knockout season (no round_robin, no has_table) → all scored games count."""
        from app.models import Season, Game, GameTeamStats
        from app.services.season_visibility import invalidate_season_cache

        season = Season(
            id=302,
            name="Cup 2026",
            championship_id=sample_championship.id,
            date_start=date(2025, 3, 1),
            date_end=date(2025, 11, 30),
            tournament_format="knockout",
            has_table=False,
            is_visible=True,
        )
        test_session.add(season)
        await test_session.flush()

        # Two games in different tours, both scored + extended_stats_synced
        g1 = Game(
            sota_id=uuid4(),
            date=date(2025, 4, 1),
            time=time(18, 0),
            tour=1,
            season_id=302,
            home_team_id=sample_teams[0].id,
            away_team_id=sample_teams[1].id,
            home_score=1,
            away_score=0,
            extended_stats_synced_at=datetime(2025, 4, 2, 10, 0, 0),
        )
        g2 = Game(
            sota_id=uuid4(),
            date=date(2025, 5, 1),
            time=time(18, 0),
            tour=2,
            season_id=302,
            home_team_id=sample_teams[0].id,
            away_team_id=sample_teams[2].id,
            home_score=2,
            away_score=2,
            extended_stats_synced_at=datetime(2025, 5, 2, 10, 0, 0),
        )
        test_session.add_all([g1, g2])
        await test_session.flush()

        test_session.add_all([
            GameTeamStats(game_id=g1.id, team_id=sample_teams[0].id, fouls=1),
            GameTeamStats(game_id=g1.id, team_id=sample_teams[1].id, fouls=1),
            GameTeamStats(game_id=g2.id, team_id=sample_teams[0].id, fouls=1),
            GameTeamStats(game_id=g2.id, team_id=sample_teams[2].id, fouls=1),
        ])
        await test_session.commit()
        invalidate_season_cache()

        response = await client.get("/api/v1/seasons/302/statistics")
        assert response.status_code == 200
        data = response.json()

        # Both games counted — cup has no tour cap
        assert data["matches_played"] == 2
        assert data["total_goals"] == 5  # 1+0 + 2+2

    async def test_statistics_xg_high_coverage_from_game_player_stats(
        self, client: AsyncClient, test_session, sample_championship, sample_teams, sample_player,
    ):
        """When GamePlayerStats rows have xg in extra_stats with high coverage, use them."""
        from app.models import Season, Game, GameTeamStats, GamePlayerStats
        from app.models.tour_sync_status import TourSyncStatus
        from app.services.season_visibility import invalidate_season_cache

        season = Season(
            id=303,
            name="2026 xG test",
            championship_id=sample_championship.id,
            date_start=date(2025, 3, 1),
            date_end=date(2025, 11, 30),
            tournament_format="round_robin",
            has_table=True,
            is_visible=True,
        )
        test_session.add(season)
        await test_session.flush()

        # Create a second player for the away team
        from app.models import Player
        player2 = Player(first_name="Away", last_name="Player", country_id=sample_player.country_id)
        test_session.add(player2)
        await test_session.flush()

        g = Game(
            sota_id=uuid4(),
            date=date(2025, 4, 1),
            time=time(18, 0),
            tour=1,
            season_id=303,
            home_team_id=sample_teams[0].id,
            away_team_id=sample_teams[1].id,
            home_score=2,
            away_score=1,
            extended_stats_synced_at=datetime(2025, 4, 2, 10, 0, 0),
        )
        test_session.add(g)
        await test_session.flush()

        test_session.add_all([
            GameTeamStats(game_id=g.id, team_id=sample_teams[0].id, fouls=0),
            GameTeamStats(game_id=g.id, team_id=sample_teams[1].id, fouls=0),
        ])

        # GamePlayerStats with xg in extra_stats — 2/2 rows = 100% coverage
        test_session.add_all([
            GamePlayerStats(
                game_id=g.id, player_id=sample_player.id, team_id=sample_teams[0].id,
                started=True, extra_stats={"xg": "1.5"},
            ),
            GamePlayerStats(
                game_id=g.id, player_id=player2.id, team_id=sample_teams[1].id,
                started=True, extra_stats={"xg": "1.5"},
            ),
        ])
        test_session.add(TourSyncStatus(season_id=303, tour=1, synced_at=datetime(2025, 4, 2, 12, 0, 0)))
        await test_session.commit()
        invalidate_season_cache()

        response = await client.get("/api/v1/seasons/303/statistics")
        assert response.status_code == 200
        data = response.json()

        assert data["max_completed_round"] == 1
        assert data["matches_played"] == 1
        # total_xg = 1.5 + 1.5 = 3.0, matches = 1 → avg = 3.0
        assert data["avg_xg_per_match"] == 3.0

    async def test_statistics_xg_null_when_low_coverage_round_scoped(
        self, client: AsyncClient, test_session, sample_championship, sample_teams,
    ):
        """When no GamePlayerStats rows exist (low coverage) and round-scoped, return null."""
        from app.models import Season, Game, GameTeamStats
        from app.models.tour_sync_status import TourSyncStatus
        from app.services.season_visibility import invalidate_season_cache

        season = Season(
            id=304,
            name="2026 xG low coverage",
            championship_id=sample_championship.id,
            date_start=date(2025, 3, 1),
            date_end=date(2025, 11, 30),
            tournament_format="round_robin",
            has_table=True,
            is_visible=True,
        )
        test_session.add(season)
        await test_session.flush()

        g = Game(
            sota_id=uuid4(),
            date=date(2025, 4, 1),
            time=time(18, 0),
            tour=1,
            season_id=304,
            home_team_id=sample_teams[0].id,
            away_team_id=sample_teams[1].id,
            home_score=1,
            away_score=0,
            extended_stats_synced_at=datetime(2025, 4, 2, 10, 0, 0),
        )
        test_session.add(g)
        await test_session.flush()

        test_session.add_all([
            GameTeamStats(game_id=g.id, team_id=sample_teams[0].id, fouls=2),
            GameTeamStats(game_id=g.id, team_id=sample_teams[1].id, fouls=3),
        ])

        # No GamePlayerStats rows → total_rows=0 → 0% xg coverage → null for round-scoped
        test_session.add(TourSyncStatus(season_id=304, tour=1, synced_at=datetime(2025, 4, 2, 12, 0, 0)))
        await test_session.commit()
        invalidate_season_cache()

        response = await client.get("/api/v1/seasons/304/statistics")
        assert response.status_code == 200
        data = response.json()

        assert data["max_completed_round"] == 1
        assert data["matches_played"] == 1
        assert data["avg_xg_per_match"] is None

    async def test_statistics_xg_fallback_full_season_low_coverage(
        self, client: AsyncClient, test_session, sample_championship, sample_teams,
    ):
        """Full-season (cup), low xG coverage → fallback to TeamSeasonStats."""
        from app.models import Season, Game, GameTeamStats, TeamSeasonStats
        from app.services.season_visibility import invalidate_season_cache

        season = Season(
            id=306,
            name="2026 Cup xG fallback",
            championship_id=sample_championship.id,
            date_start=date(2025, 3, 1),
            date_end=date(2025, 11, 30),
            tournament_format="cup",
            has_table=False,
            is_visible=True,
        )
        test_session.add(season)
        await test_session.flush()

        g = Game(
            sota_id=uuid4(),
            date=date(2025, 4, 1),
            time=time(18, 0),
            season_id=306,
            home_team_id=sample_teams[0].id,
            away_team_id=sample_teams[1].id,
            home_score=1,
            away_score=1,
            extended_stats_synced_at=datetime(2025, 4, 2, 10, 0, 0),
        )
        test_session.add(g)
        await test_session.flush()

        test_session.add_all([
            GameTeamStats(game_id=g.id, team_id=sample_teams[0].id, fouls=2),
            GameTeamStats(game_id=g.id, team_id=sample_teams[1].id, fouls=3),
        ])

        # No GamePlayerStats rows → total_rows=0 → 0% xg coverage → fallback to TeamSeasonStats
        # TeamSeasonStats as fallback source
        test_session.add_all([
            TeamSeasonStats(season_id=306, team_id=sample_teams[0].id, games_played=1, xg=1.5),
            TeamSeasonStats(season_id=306, team_id=sample_teams[1].id, games_played=1, xg=0.8),
        ])
        await test_session.commit()
        invalidate_season_cache()

        response = await client.get("/api/v1/seasons/306/statistics")
        assert response.status_code == 200
        data = response.json()

        assert data["matches_played"] == 1
        # Fallback: total_xg = 1.5+0.8 = 2.3, total_team_games = 2, avg = 2.3 / (2/2) = 2.3
        assert data["avg_xg_per_match"] == 2.3

    async def test_goals_by_period_respects_round_cap(
        self, client: AsyncClient, test_session, sample_championship, sample_teams,
    ):
        """Goals-by-period for round-robin only includes goals from completed tours."""
        from app.models import Season, Game, GameEvent, GameEventType
        from app.models.tour_sync_status import TourSyncStatus
        from app.services.season_visibility import invalidate_season_cache

        season = Season(
            id=305,
            name="2026 GBP capped",
            championship_id=sample_championship.id,
            date_start=date(2025, 3, 1),
            date_end=date(2025, 11, 30),
            tournament_format="round_robin",
            has_table=True,
            is_visible=True,
        )
        test_session.add(season)
        await test_session.flush()

        # Tour 1: complete with TourSyncStatus marker
        g1 = Game(
            sota_id=uuid4(),
            date=date(2025, 4, 1),
            time=time(18, 0),
            tour=1,
            season_id=305,
            home_team_id=sample_teams[0].id,
            away_team_id=sample_teams[1].id,
            home_score=1,
            away_score=0,
            extended_stats_synced_at=datetime(2025, 4, 2, 10, 0, 0),
        )
        # Tour 2: incomplete (only 1 of 2 games scored) — no TourSyncStatus
        g2 = Game(
            sota_id=uuid4(),
            date=date(2025, 5, 1),
            time=time(18, 0),
            tour=2,
            season_id=305,
            home_team_id=sample_teams[0].id,
            away_team_id=sample_teams[2].id,
            home_score=2,
            away_score=1,
        )
        g3 = Game(
            sota_id=uuid4(),
            date=date(2025, 5, 1),
            time=time(18, 0),
            tour=2,
            season_id=305,
            home_team_id=sample_teams[1].id,
            away_team_id=sample_teams[2].id,
            home_score=None,
            away_score=None,
        )
        test_session.add_all([g1, g2, g3])
        await test_session.flush()

        test_session.add_all([
            GameEvent(game_id=g1.id, half=1, minute=10, event_type=GameEventType.goal, team_id=sample_teams[0].id),
            GameEvent(game_id=g2.id, half=1, minute=20, event_type=GameEventType.goal, team_id=sample_teams[0].id),
            GameEvent(game_id=g2.id, half=2, minute=55, event_type=GameEventType.goal, team_id=sample_teams[0].id),
            GameEvent(game_id=g2.id, half=2, minute=70, event_type=GameEventType.goal, team_id=sample_teams[2].id),
        ])
        # TourSyncStatus marker for tour 1 only
        test_session.add(TourSyncStatus(season_id=305, tour=1, synced_at=datetime(2025, 4, 2, 12, 0, 0)))
        await test_session.commit()
        invalidate_season_cache()

        response = await client.get("/api/v1/seasons/305/goals-by-period")
        assert response.status_code == 200
        data = response.json()

        # Only tour 1 games counted
        assert data["meta"]["matches_played"] == 1
        total_goals = sum(p["goals"] for p in data["periods"])
        assert total_goals == 1  # only the goal from g1

    async def test_get_season_attendance_hidden_season_returns_404(
        self, client: AsyncClient, sample_season, test_session
    ):
        sample_season.is_visible = False
        await test_session.commit()

        response = await client.get(f"/api/v1/seasons/{sample_season.id}/attendance")
        assert response.status_code == 404
        assert response.json()["detail"] == "Season not found"

    async def test_get_season_attendance_round_robin_caps_to_completed_tour(
        self, client: AsyncClient, test_session, sample_championship, sample_teams,
    ):
        from app.models import Season, Game, GameStatus
        from app.models.tour_sync_status import TourSyncStatus
        from app.services.season_visibility import invalidate_season_cache

        season = Season(
            id=320,
            name="2026 attendance capped",
            championship_id=sample_championship.id,
            date_start=date(2025, 3, 1),
            date_end=date(2025, 11, 30),
            tournament_format="round_robin",
            has_table=True,
            is_visible=True,
        )
        test_session.add(season)
        await test_session.flush()

        test_session.add_all([
            Game(
                sota_id=uuid4(),
                date=date(2025, 4, 1),
                time=time(18, 0),
                tour=1,
                season_id=320,
                home_team_id=sample_teams[0].id,
                away_team_id=sample_teams[1].id,
                home_score=2,
                away_score=1,
                visitors=5000,
                status=GameStatus.finished,
            ),
            Game(
                sota_id=uuid4(),
                date=date(2025, 5, 1),
                time=time(18, 0),
                tour=2,
                season_id=320,
                home_team_id=sample_teams[0].id,
                away_team_id=sample_teams[2].id,
                home_score=1,
                away_score=0,
                visitors=9000,
                status=GameStatus.finished,
            ),
        ])
        test_session.add(TourSyncStatus(season_id=320, tour=1, synced_at=datetime(2025, 4, 2, 12, 0, 0)))
        await test_session.commit()
        invalidate_season_cache()

        response = await client.get("/api/v1/seasons/320/attendance?lang=ru")
        assert response.status_code == 200
        data = response.json()

        assert data["max_completed_round"] == 1
        assert data["summary"] == {
            "total_matches": 1,
            "total_attendance": 5000,
            "average_attendance": 5000.0,
        }
        assert len(data["top_matches"]) == 1
        assert data["top_matches"][0]["visitors"] == 5000
        assert data["by_tour"] == [
            {
                "tour": 1,
                "matches": 1,
                "total_attendance": 5000,
                "average_attendance": 5000.0,
            }
        ]

    async def test_get_season_attendance_respects_explicit_max_round(
        self, client: AsyncClient, test_session, sample_championship, sample_teams,
    ):
        from app.models import Season, Game, GameStatus
        from app.services.season_visibility import invalidate_season_cache

        season = Season(
            id=321,
            name="2026 attendance explicit round",
            championship_id=sample_championship.id,
            date_start=date(2025, 3, 1),
            date_end=date(2025, 11, 30),
            tournament_format="round_robin",
            has_table=True,
            is_visible=True,
        )
        test_session.add(season)
        await test_session.flush()

        test_session.add_all([
            Game(
                sota_id=uuid4(),
                date=date(2025, 4, 1),
                time=time(18, 0),
                tour=1,
                season_id=321,
                home_team_id=sample_teams[0].id,
                away_team_id=sample_teams[1].id,
                home_score=2,
                away_score=1,
                visitors=4000,
                status=GameStatus.finished,
            ),
            Game(
                sota_id=uuid4(),
                date=date(2025, 5, 1),
                time=time(18, 0),
                tour=2,
                season_id=321,
                home_team_id=sample_teams[1].id,
                away_team_id=sample_teams[2].id,
                home_score=1,
                away_score=1,
                visitors=7000,
                status=GameStatus.finished,
            ),
        ])
        await test_session.commit()
        invalidate_season_cache()

        response = await client.get("/api/v1/seasons/321/attendance?lang=ru&max_round=1")
        assert response.status_code == 200
        data = response.json()

        assert data["max_completed_round"] is None
        assert data["summary"] == {
            "total_matches": 1,
            "total_attendance": 4000,
            "average_attendance": 4000.0,
        }
        assert [item["tour"] for item in data["by_tour"]] == [1]

    async def test_get_season_attendance_cup_season_uncapped(
        self, client: AsyncClient, test_session, sample_championship, sample_teams,
    ):
        from app.models import Season, Game, GameStatus
        from app.services.season_visibility import invalidate_season_cache

        season = Season(
            id=322,
            name="Cup 2026 attendance",
            championship_id=sample_championship.id,
            date_start=date(2025, 3, 1),
            date_end=date(2025, 11, 30),
            tournament_format="knockout",
            has_table=False,
            is_visible=True,
        )
        test_session.add(season)
        await test_session.flush()

        test_session.add_all([
            Game(
                sota_id=uuid4(),
                date=date(2025, 4, 1),
                time=time(18, 0),
                tour=1,
                season_id=322,
                home_team_id=sample_teams[0].id,
                away_team_id=sample_teams[1].id,
                home_score=1,
                away_score=0,
                visitors=6000,
                status=GameStatus.finished,
            ),
            Game(
                sota_id=uuid4(),
                date=date(2025, 5, 1),
                time=time(18, 0),
                tour=2,
                season_id=322,
                home_team_id=sample_teams[0].id,
                away_team_id=sample_teams[2].id,
                home_score=2,
                away_score=2,
                visitors=8000,
                status=GameStatus.finished,
            ),
        ])
        await test_session.commit()
        invalidate_season_cache()

        response = await client.get("/api/v1/seasons/322/attendance?lang=ru")
        assert response.status_code == 200
        data = response.json()

        assert data["max_completed_round"] is None
        assert data["summary"] == {
            "total_matches": 2,
            "total_attendance": 14000,
            "average_attendance": 7000.0,
        }
        assert [item["tour"] for item in data["by_tour"]] == [1, 2]
