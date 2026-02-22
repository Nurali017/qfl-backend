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

    async def test_get_team_not_found(self, client: AsyncClient):
        """Test 404 for non-existent team."""
        response = await client.get("/api/v1/teams/99999")
        assert response.status_code == 404
        # Error message may be localized (ru/kz/en)
        assert "detail" in response.json()

    async def test_get_team_players_empty(self, client: AsyncClient, sample_teams):
        """Test getting team players when no players assigned."""
        response = await client.get("/api/v1/teams/91/players")
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
