import pytest
from httpx import AsyncClient
from uuid import uuid4
from datetime import datetime, date

from app.models import Championship, GameLineup, LineupType, Player
from app.main import app


@pytest.mark.asyncio
class TestGamesAPI:
    """Tests for /api/v1/games endpoints."""

    async def test_get_games_empty(self, client: AsyncClient):
        """Test getting games when database is empty."""
        response = await client.get("/api/v1/games")
        assert response.status_code == 404
        assert response.json()["detail"] == "Season not found"

    async def test_get_games_with_data(
        self, client: AsyncClient, sample_season, sample_game
    ):
        """Test getting all games."""
        response = await client.get("/api/v1/games?season_id=61")
        assert response.status_code == 200
        data = response.json()
        assert len(data["items"]) == 1
        assert data["total"] == 1
        assert data["items"][0]["is_schedule_tentative"] is False

    async def test_get_games_filter_by_season(
        self, client: AsyncClient, sample_season, sample_game
    ):
        """Test filtering games by season."""
        response = await client.get("/api/v1/games?season_id=61")
        assert response.status_code == 200
        data = response.json()
        assert len(data["items"]) == 1

        response = await client.get("/api/v1/games?season_id=999")
        assert response.status_code == 404
        assert response.json()["detail"] == "Season not found"

    async def test_get_games_hidden_season_returns_404(
        self, client: AsyncClient, sample_season, sample_game, test_session
    ):
        sample_season.is_visible = False
        await test_session.commit()

        response = await client.get(f"/api/v1/games?season_id={sample_season.id}")
        assert response.status_code == 404
        assert response.json()["detail"] == "Season not found"

    async def test_get_games_filter_by_month_without_year(
        self, client: AsyncClient, test_session, sample_season, sample_teams, sample_game
    ):
        """Month filter without year should match all years within season scope."""
        from app.models import Game

        may_2024_game = Game(
            sota_id=uuid4(),
            date=date(2024, 5, 12),
            time=datetime.strptime("18:30", "%H:%M").time(),
            tour=2,
            season_id=sample_season.id,
            home_team_id=sample_teams[1].id,
            away_team_id=sample_teams[2].id,
            home_score=1,
            away_score=0,
            has_stats=True,
            stadium="May Stadium 2024",
            visitors=6000,
        )
        june_2025_game = Game(
            sota_id=uuid4(),
            date=date(2025, 6, 1),
            time=datetime.strptime("20:00", "%H:%M").time(),
            tour=3,
            season_id=sample_season.id,
            home_team_id=sample_teams[2].id,
            away_team_id=sample_teams[0].id,
            home_score=2,
            away_score=2,
            has_stats=True,
            stadium="June Stadium 2025",
            visitors=7200,
        )
        test_session.add_all([may_2024_game, june_2025_game])
        await test_session.commit()

        response = await client.get(f"/api/v1/games?season_id={sample_season.id}&month=5")
        assert response.status_code == 200
        data = response.json()

        returned_ids = {item["id"] for item in data["items"]}
        assert sample_game.id in returned_ids  # 2025-05 fixture
        assert may_2024_game.id in returned_ids
        assert june_2025_game.id not in returned_ids

    async def test_get_games_filter_by_month_and_year(
        self, client: AsyncClient, test_session, sample_season, sample_teams, sample_game
    ):
        """Month + year should keep strict year filtering."""
        from app.models import Game

        may_2024_game = Game(
            sota_id=uuid4(),
            date=date(2024, 5, 18),
            time=datetime.strptime("16:00", "%H:%M").time(),
            tour=4,
            season_id=sample_season.id,
            home_team_id=sample_teams[2].id,
            away_team_id=sample_teams[1].id,
            home_score=0,
            away_score=1,
            has_stats=True,
            stadium="May Stadium 2024",
            visitors=5100,
        )
        test_session.add(may_2024_game)
        await test_session.commit()

        response = await client.get(
            f"/api/v1/games?season_id={sample_season.id}&month=5&year=2025"
        )
        assert response.status_code == 200
        data = response.json()

        returned_ids = {item["id"] for item in data["items"]}
        assert sample_game.id in returned_ids  # 2025-05 fixture
        assert may_2024_game.id not in returned_ids

    async def test_get_games_group_filter(
        self, client: AsyncClient, test_session, sample_season, sample_teams, sample_game
    ):
        """Group filter should include only games where both teams are in the selected group."""
        from app.models import Game, SeasonParticipant

        test_session.add_all(
            [
                SeasonParticipant(team_id=sample_teams[0].id, season_id=sample_season.id, group_name="A"),
                SeasonParticipant(team_id=sample_teams[1].id, season_id=sample_season.id, group_name="A"),
                SeasonParticipant(team_id=sample_teams[2].id, season_id=sample_season.id, group_name="B"),
            ]
        )

        cross_group_game = Game(
            sota_id=uuid4(),
            date=date(2025, 5, 18),
            time=datetime.strptime("19:00", "%H:%M").time(),
            tour=2,
            season_id=sample_season.id,
            home_team_id=sample_teams[0].id,
            away_team_id=sample_teams[2].id,
            home_score=1,
            away_score=1,
            has_stats=True,
            stadium="Central Stadium",
            visitors=9000,
        )
        test_session.add(cross_group_game)
        await test_session.commit()

        response = await client.get(f"/api/v1/games?season_id={sample_season.id}&group=A")
        assert response.status_code == 200
        data = response.json()

        assert data["total"] == 1
        assert data["items"][0]["id"] == sample_game.id

    async def test_get_games_final_filter(
        self, client: AsyncClient, test_session, sample_season, sample_teams, sample_game
    ):
        """Final filter should include only games from configured final stages."""
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
            time=datetime.strptime("20:00", "%H:%M").time(),
            tour=3,
            season_id=sample_season.id,
            home_team_id=sample_teams[1].id,
            away_team_id=sample_teams[2].id,
            home_score=2,
            away_score=0,
            stage_id=final_stage.id,
            has_stats=True,
            stadium="Final Stadium",
            visitors=14000,
        )
        test_session.add(final_game)
        await test_session.commit()

        response = await client.get(f"/api/v1/games?season_id={sample_season.id}&final=true")
        assert response.status_code == 200
        data = response.json()

        assert data["total"] == 1
        assert data["items"][0]["id"] == final_game.id

    async def test_get_games_group_and_final_are_mutually_exclusive(
        self, client: AsyncClient, sample_season
    ):
        response = await client.get(f"/api/v1/games?season_id={sample_season.id}&group=A&final=true")
        assert response.status_code == 400
        assert "mutually exclusive" in response.json()["detail"]

    async def test_get_game_by_id(self, client: AsyncClient, sample_game):
        """Test getting game by int ID."""
        response = await client.get(f"/api/v1/games/{sample_game.id}")
        assert response.status_code == 200
        data = response.json()
        assert data["home_score"] == 2
        assert data["away_score"] == 1
        assert data["protocol_url"] is None
        assert data["is_schedule_tentative"] is False

    async def test_get_game_by_id_uses_backend_logo_fallback_for_kairat(
        self,
        client: AsyncClient,
        sample_game,
    ):
        """Game detail should return fallback API logo URL when team.logo_url is empty."""
        response = await client.get(f"/api/v1/games/{sample_game.id}")
        assert response.status_code == 200

        data = response.json()
        assert data["away_team"]["id"] == 13
        assert data["away_team"]["logo_url"] == "/api/v1/files/teams/kairat/logo"
        assert data["away_team"]["logo_url"] is not None

    async def test_get_game_by_id_keeps_explicit_team_logo_url(
        self,
        client: AsyncClient,
        test_session,
        sample_game,
        sample_teams,
    ):
        """Explicit team.logo_url should not be overwritten by fallback."""
        sample_teams[1].logo_url = "https://cdn.example.com/kairat.png"
        await test_session.commit()

        response = await client.get(f"/api/v1/games/{sample_game.id}")
        assert response.status_code == 200

        data = response.json()
        assert data["away_team"]["id"] == 13
        assert data["away_team"]["logo_url"] == "https://cdn.example.com/kairat.png"

    async def test_get_game_by_id_includes_protocol_url(
        self,
        client: AsyncClient,
        test_session,
        sample_game,
    ):
        """Test game detail returns protocol_url when present."""
        sample_game.protocol_url = "document/match_protocols/test-game.pdf"
        await test_session.commit()

        response = await client.get(f"/api/v1/games/{sample_game.id}")
        assert response.status_code == 200

        data = response.json()
        assert data["protocol_url"] == "document/match_protocols/test-game.pdf"

    async def test_get_game_not_found(self, client: AsyncClient):
        """Test 404 for non-existent game."""
        response = await client.get("/api/v1/games/999999")
        assert response.status_code == 404
        assert response.json()["detail"] == "Game not found"

    async def test_get_game_stats(self, client: AsyncClient, sample_game):
        """Test getting game statistics."""
        response = await client.get(f"/api/v1/games/{sample_game.id}/stats")
        assert response.status_code == 200
        data = response.json()
        assert data["game_id"] == sample_game.id
        assert data["team_stats"] == []
        assert data["player_stats"] == []

    async def test_get_games_list_includes_protocol_url(
        self,
        client: AsyncClient,
        test_session,
        sample_season,
        sample_game,
    ):
        """Test games list returns protocol_url field."""
        sample_game.protocol_url = "document/match_protocols/list-protocol.pdf"
        await test_session.commit()

        response = await client.get("/api/v1/games?season_id=61")
        assert response.status_code == 200
        data = response.json()
        assert len(data["items"]) == 1
        assert (
            data["items"][0]["protocol_url"]
            == "document/match_protocols/list-protocol.pdf"
        )

    async def test_get_games_grouped_includes_protocol_url(
        self,
        client: AsyncClient,
        test_session,
        sample_season,
        sample_game,
    ):
        """Test grouped games format includes protocol_url field."""
        sample_game.protocol_url = "document/match_protocols/group-protocol.pdf"
        await test_session.commit()

        response = await client.get("/api/v1/games?season_id=61&group_by_date=true")
        assert response.status_code == 200

        data = response.json()
        assert len(data["groups"]) == 1
        group_games = data["groups"][0]["games"]
        assert len(group_games) == 1
        assert (
            group_games[0]["protocol_url"]
            == "document/match_protocols/group-protocol.pdf"
        )
        assert group_games[0]["is_schedule_tentative"] is False

    async def test_get_games_includes_schedule_tentative_flag_when_true(
        self,
        client: AsyncClient,
        test_session,
        sample_game,
    ):
        """List/grouped/detail endpoints should expose is_schedule_tentative flag."""
        sample_game.is_schedule_tentative = True
        await test_session.commit()

        list_response = await client.get("/api/v1/games?season_id=61")
        assert list_response.status_code == 200
        list_data = list_response.json()
        assert list_data["items"][0]["is_schedule_tentative"] is True

        grouped_response = await client.get("/api/v1/games?season_id=61&group_by_date=true")
        assert grouped_response.status_code == 200
        grouped_data = grouped_response.json()
        assert grouped_data["groups"][0]["games"][0]["is_schedule_tentative"] is True

        detail_response = await client.get(f"/api/v1/games/{sample_game.id}")
        assert detail_response.status_code == 200
        detail_data = detail_response.json()
        assert detail_data["is_schedule_tentative"] is True

    async def test_get_game_lineup_orders_starters_by_position_order(
        self,
        client: AsyncClient,
        test_session,
        sample_game,
        sample_player,
    ):
        """Test /games/{id}/lineup returns starters sorted by amplua + field_position."""
        def_left = Player(sota_id=uuid4(), first_name="Def", last_name="Left")
        mid_center = Player(sota_id=uuid4(), first_name="Mid", last_name="Center")
        fwd_center = Player(sota_id=uuid4(), first_name="Fwd", last_name="Center")
        test_session.add_all([def_left, mid_center, fwd_center])
        await test_session.commit()
        await test_session.refresh(def_left)
        await test_session.refresh(mid_center)
        await test_session.refresh(fwd_center)

        # Insert in mixed order to verify backend sorting logic.
        test_session.add_all(
            [
                GameLineup(
                    game_id=sample_game.id,
                    team_id=sample_game.home_team_id,
                    player_id=fwd_center.id,
                    lineup_type=LineupType.starter,
                    shirt_number=9,
                    amplua="F",
                    field_position="C",
                ),
                GameLineup(
                    game_id=sample_game.id,
                    team_id=sample_game.home_team_id,
                    player_id=def_left.id,
                    lineup_type=LineupType.starter,
                    shirt_number=3,
                    amplua="D",
                    field_position="L",
                ),
                GameLineup(
                    game_id=sample_game.id,
                    team_id=sample_game.home_team_id,
                    player_id=sample_player.id,
                    lineup_type=LineupType.starter,
                    shirt_number=1,
                    amplua="Gk",
                    field_position="C",
                ),
                GameLineup(
                    game_id=sample_game.id,
                    team_id=sample_game.home_team_id,
                    player_id=mid_center.id,
                    lineup_type=LineupType.starter,
                    shirt_number=8,
                    amplua="M",
                    field_position="C",
                ),
            ]
        )
        await test_session.commit()

        response = await client.get(f"/api/v1/games/{sample_game.id}/lineup")
        assert response.status_code == 200

        data = response.json()
        home_starters = data["lineups"]["home_team"]["starters"]
        starter_ids = [player["player_id"] for player in home_starters]

        assert starter_ids == [
            sample_player.id,  # Gk C
            def_left.id,       # D L
            mid_center.id,     # M C
            fwd_center.id,     # F C
        ]

    async def test_get_game_lineup_derives_amplua_and_field_position_from_top_role(
        self,
        client: AsyncClient,
        test_session,
        sample_game,
    ):
        gk_player = Player(
            sota_id=uuid4(),
            first_name="Goal",
            last_name="Keeper",
            top_role="ВР (вратарь)",
        )
        center_back = Player(
            sota_id=uuid4(),
            first_name="Center",
            last_name="Back",
            top_role="ЦЗ (центральный защитник)",
        )
        holding_mid = Player(
            sota_id=uuid4(),
            first_name="Holding",
            last_name="Mid",
            top_role="ОП (опорный полузащитник)",
        )
        left_mid = Player(
            sota_id=uuid4(),
            first_name="Left",
            last_name="Mid",
            top_role="ЛП (левый полузащитник)",
        )
        unknown_role = Player(
            sota_id=uuid4(),
            first_name="No",
            last_name="Role",
            top_role=None,
        )
        striker = Player(
            sota_id=uuid4(),
            first_name="Main",
            last_name="Striker",
            top_role="ЦН (центральный нападающий)",
        )

        test_session.add_all([gk_player, center_back, holding_mid, left_mid, unknown_role, striker])
        await test_session.commit()
        await test_session.refresh(gk_player)
        await test_session.refresh(center_back)
        await test_session.refresh(holding_mid)
        await test_session.refresh(left_mid)
        await test_session.refresh(unknown_role)
        await test_session.refresh(striker)

        test_session.add_all(
            [
                GameLineup(
                    game_id=sample_game.id,
                    team_id=sample_game.home_team_id,
                    player_id=gk_player.id,
                    lineup_type=LineupType.starter,
                    shirt_number=1,
                ),
                GameLineup(
                    game_id=sample_game.id,
                    team_id=sample_game.home_team_id,
                    player_id=center_back.id,
                    lineup_type=LineupType.starter,
                    shirt_number=4,
                ),
                GameLineup(
                    game_id=sample_game.id,
                    team_id=sample_game.home_team_id,
                    player_id=holding_mid.id,
                    lineup_type=LineupType.starter,
                    shirt_number=6,
                ),
                GameLineup(
                    game_id=sample_game.id,
                    team_id=sample_game.home_team_id,
                    player_id=left_mid.id,
                    lineup_type=LineupType.starter,
                    shirt_number=8,
                ),
                GameLineup(
                    game_id=sample_game.id,
                    team_id=sample_game.home_team_id,
                    player_id=unknown_role.id,
                    lineup_type=LineupType.starter,
                    shirt_number=11,
                ),
                GameLineup(
                    game_id=sample_game.id,
                    team_id=sample_game.home_team_id,
                    player_id=striker.id,
                    lineup_type=LineupType.starter,
                    shirt_number=9,
                ),
            ]
        )
        await test_session.commit()

        response = await client.get(f"/api/v1/games/{sample_game.id}/lineup")
        assert response.status_code == 200

        data = response.json()
        by_player_id = {
            player["player_id"]: player
            for player in data["lineups"]["home_team"]["starters"]
        }

        assert by_player_id[gk_player.id]["amplua"] == "Gk"
        assert by_player_id[gk_player.id]["field_position"] == "C"

        assert by_player_id[center_back.id]["amplua"] == "D"
        assert by_player_id[center_back.id]["field_position"] == "C"

        assert by_player_id[holding_mid.id]["amplua"] == "DM"
        assert by_player_id[holding_mid.id]["field_position"] == "C"

        assert by_player_id[left_mid.id]["amplua"] == "M"
        assert by_player_id[left_mid.id]["field_position"] == "L"

        assert by_player_id[unknown_role.id]["amplua"] == "M"
        assert by_player_id[unknown_role.id]["field_position"] == "C"

        assert by_player_id[striker.id]["amplua"] == "F"
        assert by_player_id[striker.id]["field_position"] == "C"

    async def test_get_game_lineup_prefers_persisted_sota_formation_over_detected(
        self,
        client: AsyncClient,
        test_session,
        sample_game,
        sample_player,
    ):
        """
        Lineup endpoint must return formation persisted from SOTA /em,
        even when amplua-based detection suggests another shape.
        """
        sample_game.home_formation = "3-6-1"

        extra_players: list[Player] = []
        for idx in range(10):
            extra_players.append(
                Player(
                    sota_id=uuid4(),
                    first_name=f"Starter{idx}",
                    last_name="Home",
                )
            )
        test_session.add_all(extra_players)
        await test_session.commit()
        for p in extra_players:
            await test_session.refresh(p)

        detected_442_roles = [
            ("D", "L"),
            ("D", "LC"),
            ("D", "RC"),
            ("D", "R"),
            ("M", "L"),
            ("M", "LC"),
            ("M", "RC"),
            ("M", "R"),
            ("F", "LC"),
            ("F", "RC"),
        ]

        starters = [
            GameLineup(
                game_id=sample_game.id,
                team_id=sample_game.home_team_id,
                player_id=sample_player.id,
                lineup_type=LineupType.starter,
                shirt_number=1,
                amplua="Gk",
                field_position="C",
            )
        ]
        for shirt_number, (player, (amplua, field_pos)) in enumerate(
            zip(extra_players, detected_442_roles),
            start=2,
        ):
            starters.append(
                GameLineup(
                    game_id=sample_game.id,
                    team_id=sample_game.home_team_id,
                    player_id=player.id,
                    lineup_type=LineupType.starter,
                    shirt_number=shirt_number,
                    amplua=amplua,
                    field_position=field_pos,
                )
            )

        test_session.add_all(starters)
        await test_session.commit()

        response = await client.get(f"/api/v1/games/{sample_game.id}/lineup")
        assert response.status_code == 200
        data = response.json()

        assert data["lineups"]["home_team"]["formation"] == "3-6-1"

    async def test_get_game_lineup_rendering_mode_field_when_rules_and_positions_are_valid(
        self,
        client: AsyncClient,
        test_session,
        sample_game,
        sample_championship,
    ):
        sample_championship.legacy_id = 1
        sample_game.date = date(2025, 6, 15)
        await test_session.commit()

        role_pairs = [
            ("Gk", "C"),
            ("D", "L"),
            ("D", "LC"),
            ("D", "C"),
            ("D", "RC"),
            ("DM", "C"),
            ("M", "L"),
            ("M", "C"),
            ("AM", "RC"),
            ("F", "LC"),
            ("F", "R"),
        ]

        players: list[Player] = []
        for idx in range(22):
            players.append(
                Player(
                    sota_id=uuid4(),
                    first_name=f"Starter{idx}",
                    last_name="Player",
                )
            )
        test_session.add_all(players)
        await test_session.commit()
        for player in players:
            await test_session.refresh(player)

        lineups = []
        for idx, (amplua, field_position) in enumerate(role_pairs):
            lineups.append(
                GameLineup(
                    game_id=sample_game.id,
                    team_id=sample_game.home_team_id,
                    player_id=players[idx].id,
                    lineup_type=LineupType.starter,
                    shirt_number=idx + 1,
                    amplua=amplua,
                    field_position=field_position,
                )
            )
        for idx, (amplua, field_position) in enumerate(role_pairs):
            lineups.append(
                GameLineup(
                    game_id=sample_game.id,
                    team_id=sample_game.away_team_id,
                    player_id=players[idx + 11].id,
                    lineup_type=LineupType.starter,
                    shirt_number=idx + 1,
                    amplua=amplua,
                    field_position=field_position,
                )
            )

        test_session.add_all(lineups)
        await test_session.commit()

        response = await client.get(f"/api/v1/games/{sample_game.id}/lineup")
        assert response.status_code == 200

        data = response.json()
        assert data["has_lineup"] is True
        assert data["rendering"]["mode"] == "field"
        assert data["rendering"]["field_allowed_by_rules"] is True
        assert data["rendering"]["field_data_valid"] is True

    async def test_get_game_lineup_rendering_mode_list_when_positions_invalid(
        self,
        client: AsyncClient,
        test_session,
        sample_game,
        sample_championship,
    ):
        sample_championship.legacy_id = 2
        sample_game.date = date(2025, 7, 1)

        home_player = Player(sota_id=uuid4(), first_name="Home", last_name="NoPos")
        away_player = Player(sota_id=uuid4(), first_name="Away", last_name="NoPos")
        test_session.add_all([home_player, away_player])
        await test_session.commit()
        await test_session.refresh(home_player)
        await test_session.refresh(away_player)

        test_session.add_all(
            [
                GameLineup(
                    game_id=sample_game.id,
                    team_id=sample_game.home_team_id,
                    player_id=home_player.id,
                    lineup_type=LineupType.starter,
                    shirt_number=4,
                    amplua=None,
                    field_position=None,
                ),
                GameLineup(
                    game_id=sample_game.id,
                    team_id=sample_game.away_team_id,
                    player_id=away_player.id,
                    lineup_type=LineupType.starter,
                    shirt_number=5,
                    amplua=None,
                    field_position=None,
                ),
            ]
        )
        await test_session.commit()

        response = await client.get(f"/api/v1/games/{sample_game.id}/lineup")
        assert response.status_code == 200

        data = response.json()
        assert data["has_lineup"] is True
        assert data["rendering"]["mode"] == "list"
        assert data["rendering"]["field_allowed_by_rules"] is True
        assert data["rendering"]["field_data_valid"] is False

    async def test_get_game_lineup_rendering_mode_hidden_when_no_lineup_data(
        self,
        client: AsyncClient,
        test_session,
        sample_game,
    ):
        sample_game.has_lineup = False
        await test_session.commit()

        response = await client.get(f"/api/v1/games/{sample_game.id}/lineup")
        assert response.status_code == 200

        data = response.json()
        assert data["has_lineup"] is False
        assert data["rendering"]["mode"] == "hidden"
