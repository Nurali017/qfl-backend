import pytest
from datetime import date, time, timedelta
from uuid import uuid4

from httpx import AsyncClient

from app.models import Season, Team, Game, Stage, SeasonParticipant, Championship
from app.models.game import GameStatus


@pytest.fixture
async def cup_championship(test_session) -> Championship:
    champ = Championship(id=1, name="Кубок Казахстана", name_kz="Қазақстан Кубогы", name_en="Kazakhstan Cup")
    test_session.add(champ)
    await test_session.commit()
    await test_session.refresh(champ)
    return champ


@pytest.fixture
async def cup_season(test_session, cup_championship) -> Season:
    s = Season(
        id=71, name="Кубок 2025", name_kz="Кубок 2025", name_en="Cup 2025",
        championship_id=cup_championship.id,
        date_start=date(2025, 3, 1), date_end=date(2025, 11, 30),
    )
    test_session.add(s)
    await test_session.commit()
    await test_session.refresh(s)
    return s


@pytest.fixture
async def cup_teams(test_session) -> list[Team]:
    teams = [
        Team(id=201, name="FC Alpha", name_kz="ФК Альфа", city="Almaty"),
        Team(id=202, name="FC Beta", name_kz="ФК Бета", city="Astana"),
        Team(id=203, name="FC Gamma", name_kz="ФК Гамма", city="Shymkent"),
        Team(id=204, name="FC Delta", name_kz="ФК Дельта", city="Karaganda"),
    ]
    test_session.add_all(teams)
    await test_session.commit()
    for t in teams:
        await test_session.refresh(t)
    return teams


@pytest.fixture
async def cup_stages(test_session, cup_season) -> list[Stage]:
    stages = [
        Stage(id=1, season_id=cup_season.id, name="1/4 финала", name_kz="1/4 финал", sort_order=1),
        Stage(id=2, season_id=cup_season.id, name="1/2 финала", name_kz="1/2 финал", sort_order=2),
        Stage(id=3, season_id=cup_season.id, name="Финал", name_kz="Финал", sort_order=3),
    ]
    test_session.add_all(stages)
    await test_session.commit()
    for s in stages:
        await test_session.refresh(s)
    return stages


@pytest.fixture
async def cup_games(test_session, cup_season, cup_teams, cup_stages) -> list[Game]:
    today = date.today()
    games = [
        # QF: finished
        Game(
            id=1001, sota_id=uuid4(), date=today - timedelta(days=10), time=time(18, 0),
            season_id=cup_season.id, stage_id=cup_stages[0].id,
            home_team_id=cup_teams[0].id, away_team_id=cup_teams[1].id,
            home_score=2, away_score=1, status=GameStatus.finished,
        ),
        Game(
            id=1002, sota_id=uuid4(), date=today - timedelta(days=9), time=time(18, 0),
            season_id=cup_season.id, stage_id=cup_stages[0].id,
            home_team_id=cup_teams[2].id, away_team_id=cup_teams[3].id,
            home_score=1, away_score=0, status=GameStatus.finished,
        ),
        # SF: one finished, one upcoming
        Game(
            id=1003, sota_id=uuid4(), date=today - timedelta(days=3), time=time(19, 0),
            season_id=cup_season.id, stage_id=cup_stages[1].id,
            home_team_id=cup_teams[0].id, away_team_id=cup_teams[2].id,
            home_score=3, away_score=2, status=GameStatus.finished,
        ),
        Game(
            id=1004, sota_id=uuid4(), date=today + timedelta(days=5), time=time(19, 0),
            season_id=cup_season.id, stage_id=cup_stages[1].id,
            home_team_id=cup_teams[1].id, away_team_id=cup_teams[3].id,
            home_score=None, away_score=None,
        ),
        # Final: upcoming
        Game(
            id=1005, sota_id=uuid4(), date=today + timedelta(days=20), time=time(20, 0),
            season_id=cup_season.id, stage_id=cup_stages[2].id,
            home_team_id=None, away_team_id=None,
            home_score=None, away_score=None,
        ),
    ]
    test_session.add_all(games)
    await test_session.commit()
    for g in games:
        await test_session.refresh(g)
    return games


@pytest.fixture
async def cup_groups(test_session, cup_season, cup_teams) -> list[SeasonParticipant]:
    entries = [
        SeasonParticipant(team_id=cup_teams[0].id, season_id=cup_season.id, group_name="A"),
        SeasonParticipant(team_id=cup_teams[1].id, season_id=cup_season.id, group_name="A"),
        SeasonParticipant(team_id=cup_teams[2].id, season_id=cup_season.id, group_name="B"),
        SeasonParticipant(team_id=cup_teams[3].id, season_id=cup_season.id, group_name="B"),
    ]
    test_session.add_all(entries)
    await test_session.commit()
    return entries


@pytest.mark.asyncio
class TestCupOverview:
    """Tests for GET /api/v1/cup/{season_id}/overview."""

    async def test_season_not_found(self, client: AsyncClient):
        response = await client.get("/api/v1/cup/99999/overview")
        assert response.status_code == 404

    async def test_empty_season(self, client: AsyncClient, cup_season):
        response = await client.get(f"/api/v1/cup/{cup_season.id}/overview")
        assert response.status_code == 200
        data = response.json()
        assert data["season_id"] == cup_season.id
        assert data["season_name"] == "Кубок 2025"
        assert data["championship_name"] == "Қазақстан Кубогы"
        assert data["rounds"] == []
        assert data["recent_results"] == []
        assert data["upcoming_games"] == []

    async def test_overview_with_games(
        self, client: AsyncClient, cup_season, cup_games, cup_stages
    ):
        response = await client.get(f"/api/v1/cup/{cup_season.id}/overview")
        assert response.status_code == 200
        data = response.json()

        # Should have 3 rounds in nav
        assert len(data["rounds"]) == 3

        # Recent results (finished games, most recent first)
        assert len(data["recent_results"]) > 0
        for r in data["recent_results"]:
            assert r["status"] == "finished"

        # Upcoming games
        assert len(data["upcoming_games"]) > 0
        for u in data["upcoming_games"]:
            assert u["status"] == "upcoming"

        # Current round should be the semi-final (first incomplete)
        cr = data["current_round"]
        assert cr is not None
        assert cr["round_key"] == "1_2"

    async def test_overview_localization(
        self, client: AsyncClient, cup_season, cup_games, cup_stages
    ):
        response = await client.get(f"/api/v1/cup/{cup_season.id}/overview?lang=kz")
        assert response.status_code == 200
        data = response.json()
        assert data["season_name"] == "Кубок 2025"
        assert data["championship_name"] == "Қазақстан Кубогы"

    async def test_overview_with_groups(
        self, client: AsyncClient, cup_season, cup_games, cup_stages, cup_groups
    ):
        response = await client.get(f"/api/v1/cup/{cup_season.id}/overview")
        assert response.status_code == 200
        data = response.json()
        assert data["groups"] is not None
        assert len(data["groups"]) == 2
        group_names = [g["group_name"] for g in data["groups"]]
        assert "A" in group_names
        assert "B" in group_names

    async def test_overview_with_bracket(
        self, client: AsyncClient, cup_season, cup_games, cup_stages
    ):
        response = await client.get(f"/api/v1/cup/{cup_season.id}/overview")
        assert response.status_code == 200
        data = response.json()
        assert data["bracket"] is not None
        assert data["bracket"]["season_id"] == cup_season.id
        round_names = [round_item["round_name"] for round_item in data["bracket"]["rounds"]]
        assert "1_4" in round_names
        assert "1_2" in round_names
        assert "final" in round_names

    async def test_overview_recent_limit(
        self, client: AsyncClient, cup_season, cup_games, cup_stages
    ):
        response = await client.get(f"/api/v1/cup/{cup_season.id}/overview?recent_limit=1")
        assert response.status_code == 200
        data = response.json()
        assert len(data["recent_results"]) <= 1

    async def test_overview_upcoming_limit(
        self, client: AsyncClient, cup_season, cup_games, cup_stages
    ):
        response = await client.get(f"/api/v1/cup/{cup_season.id}/overview?upcoming_limit=1")
        assert response.status_code == 200
        data = response.json()
        assert len(data["upcoming_games"]) <= 1


@pytest.mark.asyncio
class TestCupSchedule:
    """Tests for GET /api/v1/cup/{season_id}/schedule."""

    async def test_season_not_found(self, client: AsyncClient):
        response = await client.get("/api/v1/cup/99999/schedule")
        assert response.status_code == 404

    async def test_empty_schedule(self, client: AsyncClient, cup_season):
        response = await client.get(f"/api/v1/cup/{cup_season.id}/schedule")
        assert response.status_code == 200
        data = response.json()
        assert data["season_id"] == cup_season.id
        assert data["rounds"] == []
        assert data["total_games"] == 0

    async def test_full_schedule(
        self, client: AsyncClient, cup_season, cup_games, cup_stages
    ):
        response = await client.get(f"/api/v1/cup/{cup_season.id}/schedule")
        assert response.status_code == 200
        data = response.json()
        assert data["total_games"] == 5
        assert len(data["rounds"]) == 3

        # Each round should have games
        for r in data["rounds"]:
            assert len(r["games"]) > 0
            assert r["total_games"] == len(r["games"])

    async def test_filter_by_round_key(
        self, client: AsyncClient, cup_season, cup_games, cup_stages
    ):
        response = await client.get(f"/api/v1/cup/{cup_season.id}/schedule?round_key=1_4")
        assert response.status_code == 200
        data = response.json()
        assert len(data["rounds"]) == 1
        assert data["rounds"][0]["round_key"] == "1_4"
        assert data["total_games"] == 2

    async def test_filter_nonexistent_round(
        self, client: AsyncClient, cup_season, cup_games, cup_stages
    ):
        response = await client.get(f"/api/v1/cup/{cup_season.id}/schedule?round_key=1_16")
        assert response.status_code == 200
        data = response.json()
        assert data["rounds"] == []
        assert data["total_games"] == 0

    async def test_game_details_in_schedule(
        self, client: AsyncClient, cup_season, cup_games, cup_stages
    ):
        response = await client.get(f"/api/v1/cup/{cup_season.id}/schedule?round_key=1_4")
        assert response.status_code == 200
        data = response.json()
        game = data["rounds"][0]["games"][0]
        assert "id" in game
        assert "date" in game
        assert "home_team" in game
        assert "away_team" in game
        assert "status" in game
        assert game["home_team"]["name"] is not None


@pytest.mark.asyncio
class TestSeasonBracketFromStages:
    async def test_season_bracket_is_built_from_games_and_stages(
        self, client: AsyncClient, cup_season, cup_games, cup_stages
    ):
        response = await client.get(f"/api/v1/seasons/{cup_season.id}/bracket")
        assert response.status_code == 200

        data = response.json()
        assert data["season_id"] == cup_season.id
        round_names = [round_item["round_name"] for round_item in data["rounds"]]
        assert "1_4" in round_names
        assert "1_2" in round_names
        assert "final" in round_names

        final_round = next(round_item for round_item in data["rounds"] if round_item["round_name"] == "final")
        assert len(final_round["entries"]) >= 1
        assert final_round["entries"][0]["game"] is not None


@pytest.mark.asyncio
async def test_admin_playoff_routes_are_not_available(client: AsyncClient):
    response = await client.get("/api/v1/admin/playoff-brackets?season_id=71")
    assert response.status_code == 404
