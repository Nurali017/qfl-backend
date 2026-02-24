import pytest
from datetime import date, timedelta, time
from uuid import uuid4

from httpx import AsyncClient

from app.models import (
    Coach,
    CoachRole,
    Game,
    Player,
    PlayerSeasonStats,
    PlayerTeam,
    TeamCoach,
    TeamSeasonStats,
    SeasonParticipant,
)
from app.utils.error_messages import get_error_message


@pytest.mark.asyncio
class TestTeamsAPI:
    """Tests for /api/v1/teams endpoints."""

    async def test_get_teams_empty(self, client: AsyncClient):
        """Test getting teams when database is empty."""
        response = await client.get("/api/v1/teams")
        assert response.status_code == 200
        data = response.json()
        assert data["items"] == []
        assert data["total"] == 0

    async def test_get_teams_with_data(self, client: AsyncClient, sample_teams):
        """Test getting all teams."""
        response = await client.get("/api/v1/teams")
        assert response.status_code == 200
        data = response.json()
        assert len(data["items"]) == 3
        assert data["total"] == 3

    async def test_get_teams_by_season_uses_team_tournament_membership(
        self,
        client: AsyncClient,
        test_session,
        sample_teams,
        sample_season,
        sample_player,
    ):
        """Season filter should use season_participants, not player_teams."""
        test_session.add_all(
            [
                SeasonParticipant(team_id=sample_teams[0].id, season_id=sample_season.id),
                SeasonParticipant(team_id=sample_teams[1].id, season_id=sample_season.id),
                PlayerTeam(
                    player_id=sample_player.id,
                    team_id=sample_teams[2].id,
                    season_id=sample_season.id,
                    number=99,
                ),
            ]
        )
        await test_session.commit()

        response = await client.get(f"/api/v1/teams?season_id={sample_season.id}&lang=ru")
        assert response.status_code == 200
        data = response.json()

        team_ids = [item["id"] for item in data["items"]]
        assert set(team_ids) == {sample_teams[0].id, sample_teams[1].id}
        assert sample_teams[2].id not in team_ids
        assert data["total"] == 2

    async def test_get_teams_by_season_without_team_tournament_returns_409(
        self, client: AsyncClient, sample_season
    ):
        """Season filter should fail fast when season_participants are missing."""
        response = await client.get(f"/api/v1/teams?season_id={sample_season.id}&lang=ru")
        assert response.status_code == 409
        assert response.json()["detail"] == get_error_message(
            "season_teams_not_configured", "ru"
        )

    async def test_get_teams_by_season_requires_season_participants_even_with_score_table(
        self,
        client: AsyncClient,
        test_session,
        sample_teams,
        sample_season,
    ):
        """Season teams endpoint should stay strict and ignore score_table fallback."""
        # Keep at least one non-participant data source populated.
        from app.models import ScoreTable

        test_session.add(
            ScoreTable(
                season_id=sample_season.id,
                team_id=sample_teams[0].id,
                position=1,
                games_played=1,
                wins=1,
                draws=0,
                losses=0,
                goals_scored=2,
                goals_conceded=0,
                points=3,
            )
        )
        await test_session.commit()

        response = await client.get(f"/api/v1/teams?season_id={sample_season.id}&lang=ru")
        assert response.status_code == 409
        assert response.json()["detail"] == get_error_message(
            "season_teams_not_configured", "ru"
        )

    async def test_get_teams_by_season_does_not_backfill_partial_season_participants(
        self,
        client: AsyncClient,
        test_session,
        sample_teams,
        sample_season,
    ):
        """Only explicitly configured season_participants should be returned."""
        test_session.add(
            SeasonParticipant(team_id=sample_teams[0].id, season_id=sample_season.id)
        )
        await test_session.commit()

        response = await client.get(f"/api/v1/teams?season_id={sample_season.id}&lang=ru")
        assert response.status_code == 200
        data = response.json()
        assert {item["id"] for item in data["items"]} == {sample_teams[0].id}
        assert data["total"] == 1

    async def test_get_team_by_id(self, client: AsyncClient, sample_teams):
        """Test getting team by ID."""
        response = await client.get("/api/v1/teams/91")
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "Astana"
        assert data["id"] == 91

    async def test_get_team_seasons_includes_frontend_code_and_season_year(
        self,
        client: AsyncClient,
        test_session,
        sample_season,
        sample_game,
    ):
        """Team seasons endpoint should expose frontend_code and season_year."""
        sample_season.frontend_code = "pl"
        await test_session.commit()

        response = await client.get("/api/v1/teams/91/seasons?lang=ru")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert data["items"][0]["frontend_code"] == "pl"
        assert data["items"][0]["season_year"] == 2025

    async def test_get_team_seasons_season_year_fallbacks_to_name_when_date_start_missing(
        self,
        client: AsyncClient,
        test_session,
        sample_season,
        sample_game,
    ):
        """When date_start is missing, season_year should be parsed from season name."""
        sample_season.date_start = None
        sample_season.name = "Сезон 2024"
        await test_session.commit()

        response = await client.get("/api/v1/teams/91/seasons?lang=ru")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert data["items"][0]["season_year"] == 2024

    async def test_get_team_seasons_excludes_hidden_seasons(
        self,
        client: AsyncClient,
        test_session,
        sample_championship,
        sample_season,
        sample_teams,
        sample_game,
    ):
        """Team seasons endpoint should not return hidden seasons."""
        from app.models import Game, Season

        sample_season.is_visible = False

        visible_season = Season(
            id=62,
            name="2026",
            championship_id=sample_championship.id,
            date_start=date(2026, 3, 1),
            date_end=date(2026, 11, 30),
            frontend_code="pl",
            is_visible=True,
        )
        test_session.add(visible_season)
        await test_session.flush()

        test_session.add(
            Game(
                sota_id=uuid4(),
                date=date(2026, 4, 10),
                time=time(18, 0),
                tour=1,
                season_id=visible_season.id,
                home_team_id=sample_teams[0].id,
                away_team_id=sample_teams[1].id,
                home_score=1,
                away_score=0,
                has_stats=True,
            )
        )
        await test_session.commit()

        response = await client.get(f"/api/v1/teams/{sample_teams[0].id}/seasons?lang=ru")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert data["items"][0]["season_id"] == visible_season.id

    async def test_get_team_not_found(self, client: AsyncClient):
        """Test 404 for non-existent team."""
        response = await client.get("/api/v1/teams/99999")
        assert response.status_code == 404
        # Error message may be localized (ru/kz/en)
        assert "detail" in response.json()

    async def test_get_team_players_empty(self, client: AsyncClient, sample_teams, sample_season):
        """Test getting team players when no players assigned."""
        response = await client.get(f"/api/v1/teams/91/players?season_id={sample_season.id}")
        assert response.status_code == 200
        data = response.json()
        assert data["items"] == []
        assert data["total"] == 0

    async def test_get_team_games(
        self, client: AsyncClient, sample_teams, sample_season, sample_game
    ):
        """Test getting team games."""
        response = await client.get("/api/v1/teams/91/games?season_id=61")
        assert response.status_code == 200
        data = response.json()
        assert len(data["items"]) == 1

    async def test_get_team_stats(
        self, client: AsyncClient, sample_teams, sample_season
    ):
        """Test getting team statistics returns 404 when no stats exist."""
        # TeamSeasonStats table is empty, so API returns 404
        response = await client.get("/api/v1/teams/91/stats?season_id=61")
        assert response.status_code == 404
        assert "detail" in response.json()

    async def test_get_team_overview_not_found(self, client: AsyncClient):
        """Overview endpoint should return 404 for missing team."""
        response = await client.get("/api/v1/teams/99999/overview?season_id=61")
        assert response.status_code == 404
        assert "detail" in response.json()

    async def test_get_team_overview_fallback_summary(
        self, client: AsyncClient, sample_teams, sample_season, sample_game
    ):
        """Overview summary should fallback to games when team season stats are missing."""
        response = await client.get("/api/v1/teams/13/overview?season_id=61&lang=ru")
        assert response.status_code == 200
        data = response.json()

        assert data["summary"]["games_played"] == 1
        assert data["summary"]["losses"] == 1
        assert data["summary"]["goals_scored"] == 1
        assert data["summary"]["goals_conceded"] == 2
        assert data["recent_match"] is not None
        assert len(data["form_last5"]) == 1

    async def test_get_team_overview_fallback_standings_from_games(
        self, client: AsyncClient, sample_teams, sample_season, sample_game
    ):
        """Overview standings should fallback to games when score table is missing."""
        response = await client.get("/api/v1/teams/13/overview?season_id=61&lang=ru")
        assert response.status_code == 200
        data = response.json()

        assert len(data["standings_window"]) >= 2
        assert any(row["team_id"] == 13 for row in data["standings_window"])

    async def test_get_team_overview_full_shape(
        self,
        client: AsyncClient,
        test_session,
        sample_teams,
        sample_season,
        sample_game,
        sample_score_table,
    ):
        """Overview endpoint should return full aggregated shape when data exists."""
        # Team season stats
        test_session.add(
            TeamSeasonStats(
                team_id=13,
                season_id=61,
                games_played=12,
                wins=8,
                draws=2,
                losses=2,
                goals_scored=23,
                goals_conceded=11,
                goals_difference=12,
                points=26,
            )
        )

        # Upcoming game
        test_session.add(
            Game(
                sota_id=uuid4(),
                date=date.today() + timedelta(days=5),
                time=time(18, 30),
                tour=13,
                season_id=61,
                home_team_id=13,
                away_team_id=90,
                home_score=None,
                away_score=None,
                has_stats=False,
            )
        )

        # Players + season stats
        player_1 = Player(
            first_name="Dastan",
            last_name="Satpayev",
            player_type="forward",
            age=20,
            top_role="CF",
        )
        player_2 = Player(
            first_name="Georgi",
            last_name="Zaria",
            player_type="midfielder",
            age=27,
            top_role="CM",
        )
        test_session.add_all([player_1, player_2])
        await test_session.flush()

        test_session.add_all(
            [
                PlayerSeasonStats(
                    player_id=player_1.id,
                    season_id=61,
                    team_id=13,
                    games_played=12,
                    goals=11,
                    assists=4,
                    passes=240,
                    save_shot=0,
                    dry_match=0,
                    red_cards=1,
                ),
                PlayerSeasonStats(
                    player_id=player_2.id,
                    season_id=61,
                    team_id=13,
                    games_played=12,
                    goals=3,
                    assists=7,
                    passes=510,
                    save_shot=0,
                    dry_match=0,
                    red_cards=0,
                ),
            ]
        )

        # Staff preview
        coach = Coach(first_name="Vladimir", last_name="Petrov")
        test_session.add(coach)
        await test_session.flush()
        test_session.add(
            TeamCoach(
                team_id=13,
                coach_id=coach.id,
                season_id=61,
                role=CoachRole.head_coach,
                is_active=True,
            )
        )

        await test_session.commit()

        response = await client.get("/api/v1/teams/13/overview?season_id=61&lang=ru")
        assert response.status_code == 200
        data = response.json()

        assert data["team"]["id"] == 13
        assert data["summary"]["points"] == 26
        assert len(data["upcoming_matches"]) == 1
        assert len(data["standings_window"]) > 0
        assert data["leaders"]["top_scorer"] is not None
        assert len(data["leaders"]["goals_table"]) > 0
        assert len(data["leaders"]["assists_table"]) > 0
        assert len(data["staff_preview"]) == 1

    async def test_get_team_overview_localization(
        self, client: AsyncClient, test_session, sample_teams, sample_season, sample_game
    ):
        """Overview should return localized team names for KZ language."""
        team = sample_teams[1]  # id=13
        team.name_kz = "Қайрат"
        await test_session.commit()

        response = await client.get("/api/v1/teams/13/overview?season_id=61&lang=kz")
        assert response.status_code == 200
        data = response.json()
        assert data["team"]["name"] == "Қайрат"

    async def test_head_to_head_uses_logo_fallback_in_form_guide_table_and_meetings(
        self,
        client: AsyncClient,
        test_session,
        sample_season,
        sample_score_table,
    ):
        """H2H response should expose fallback logo URLs when teams.logo_url is empty."""
        h2h_game = Game(
            sota_id=uuid4(),
            date=date(2025, 6, 1),
            time=time(18, 0),
            tour=5,
            season_id=sample_season.id,
            home_team_id=13,
            away_team_id=90,
            home_score=3,
            away_score=1,
            has_stats=True,
            stadium="Central Stadium",
            visitors=12000,
        )
        test_session.add(h2h_game)
        await test_session.commit()

        response = await client.get("/api/v1/teams/13/vs/90/head-to-head?season_id=61&lang=ru")
        assert response.status_code == 200
        data = response.json()

        team1_form = data["form_guide"]["team1"]["matches"]
        team2_form = data["form_guide"]["team2"]["matches"]
        assert any(
            match["opponent_id"] == 90
            and match["opponent_logo_url"] == "/api/v1/files/teams/tobol/logo"
            for match in team1_form
        )
        assert any(
            match["opponent_id"] == 13
            and match["opponent_logo_url"] == "/api/v1/files/teams/kairat/logo"
            for match in team2_form
        )

        table_by_team_id = {entry["team_id"]: entry for entry in data["season_table"]}
        assert table_by_team_id[13]["logo_url"] == "/api/v1/files/teams/kairat/logo"
        assert table_by_team_id[90]["logo_url"] == "/api/v1/files/teams/tobol/logo"

        assert any(
            meeting["home_team_id"] == 13
            and meeting["away_team_id"] == 90
            and meeting["home_team_logo"] == "/api/v1/files/teams/kairat/logo"
            and meeting["away_team_logo"] == "/api/v1/files/teams/tobol/logo"
            for meeting in data["previous_meetings"]
        )

    async def test_head_to_head_fun_facts_biggest_results_use_whole_tournament(
        self,
        client: AsyncClient,
        test_session,
        sample_season,
        sample_game,
    ):
        """Biggest win/loss in H2H fun facts should be taken from full season games, not only mutual meetings."""
        h2h_game = Game(
            sota_id=uuid4(),
            date=date(2025, 6, 1),
            time=time(18, 0),
            tour=5,
            season_id=sample_season.id,
            home_team_id=13,
            away_team_id=90,
            home_score=1,
            away_score=0,
            has_stats=True,
            stadium="H2H Stadium",
            visitors=12000,
        )
        team1_big_win_game = Game(
            sota_id=uuid4(),
            date=date(2025, 6, 5),
            time=time(18, 0),
            tour=6,
            season_id=sample_season.id,
            home_team_id=13,
            away_team_id=91,
            home_score=5,
            away_score=1,
            has_stats=True,
            stadium="Big Win Stadium",
            visitors=14000,
        )
        team1_worst_defeat_game = Game(
            sota_id=uuid4(),
            date=date(2025, 6, 10),
            time=time(18, 0),
            tour=7,
            season_id=sample_season.id,
            home_team_id=91,
            away_team_id=13,
            home_score=3,
            away_score=0,
            has_stats=True,
            stadium="Worst Defeat Stadium",
            visitors=13000,
        )
        team2_big_win_game = Game(
            sota_id=uuid4(),
            date=date(2025, 6, 12),
            time=time(18, 0),
            tour=8,
            season_id=sample_season.id,
            home_team_id=90,
            away_team_id=91,
            home_score=4,
            away_score=0,
            has_stats=True,
            stadium="Team2 Big Win Stadium",
            visitors=11000,
        )
        team2_worst_defeat_game = Game(
            sota_id=uuid4(),
            date=date(2025, 6, 14),
            time=time(18, 0),
            tour=9,
            season_id=sample_season.id,
            home_team_id=91,
            away_team_id=90,
            home_score=4,
            away_score=1,
            has_stats=True,
            stadium="Team2 Worst Defeat Stadium",
            visitors=9000,
        )
        test_session.add_all(
            [
                h2h_game,
                team1_big_win_game,
                team1_worst_defeat_game,
                team2_big_win_game,
                team2_worst_defeat_game,
            ]
        )
        await test_session.commit()

        response = await client.get("/api/v1/teams/13/vs/90/head-to-head?season_id=61&lang=ru")
        assert response.status_code == 200
        data = response.json()

        assert data["overall"]["total_matches"] == 1

        team1_biggest_win = data["fun_facts"]["team1_biggest_win"]
        team2_biggest_win = data["fun_facts"]["team2_biggest_win"]
        team1_worst_defeat = data["fun_facts"]["team1_worst_defeat"]
        team2_worst_defeat = data["fun_facts"]["team2_worst_defeat"]

        assert team1_biggest_win["game_id"] == team1_big_win_game.id
        assert team1_biggest_win["score"] == "5-1"
        assert team1_biggest_win["goal_difference"] == 4
        assert team1_biggest_win["game_id"] != h2h_game.id

        assert team2_biggest_win["game_id"] == team2_big_win_game.id
        assert team2_biggest_win["score"] == "4-0"
        assert team2_biggest_win["goal_difference"] == 4
        assert team2_biggest_win["game_id"] != h2h_game.id

        assert team1_worst_defeat["game_id"] == team1_worst_defeat_game.id
        assert team1_worst_defeat["score"] == "0-3"
        assert team1_worst_defeat["goal_difference"] == 3

        assert team2_worst_defeat["game_id"] == team2_worst_defeat_game.id
        assert team2_worst_defeat["score"] == "1-4"
        assert team2_worst_defeat["goal_difference"] == 3
