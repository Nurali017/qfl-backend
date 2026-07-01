import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
class TestPlayersAPI:
    """Tests for /api/v1/players endpoints."""

    async def test_get_players_empty(self, client: AsyncClient):
        """Test getting players when database is empty."""
        response = await client.get("/api/v1/players")
        assert response.status_code == 200
        data = response.json()
        assert data["items"] == []
        assert data["total"] == 0

    async def test_get_players_with_data(self, client: AsyncClient, sample_player):
        """Test getting all players."""
        response = await client.get("/api/v1/players")
        assert response.status_code == 200
        data = response.json()
        assert len(data["items"]) == 1
        assert data["total"] == 1

    async def test_get_players_pagination(self, client: AsyncClient, sample_player):
        """Test player pagination."""
        response = await client.get("/api/v1/players?limit=1&offset=0")
        assert response.status_code == 200
        data = response.json()
        assert len(data["items"]) == 1
        assert data["total"] == 1

    async def test_get_player_by_id(self, client: AsyncClient, sample_player):
        """Test getting player by id."""
        player_id = sample_player.id
        response = await client.get(f"/api/v1/players/{player_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["first_name"] == "Test"
        assert data["last_name"] == "Player"

    async def test_get_player_not_found(self, client: AsyncClient):
        """Test 404 for non-existent player."""
        response = await client.get("/api/v1/players/999999")
        assert response.status_code == 404
        # Error message may be localized (ru/kz/en)
        assert "detail" in response.json()

    async def test_get_player_invalid_id(self, client: AsyncClient):
        """Test invalid player id format."""
        response = await client.get("/api/v1/players/not-a-number")
        assert response.status_code == 422

    async def test_get_player_stats(self, client: AsyncClient, sample_player, sample_season):
        """Test getting player stats returns 404 when no stats exist."""
        player_id = sample_player.id
        response = await client.get(
            f"/api/v1/players/{player_id}/stats?season_id={sample_season.id}"
        )
        # PlayerSeasonStats table is empty, so API returns 404
        assert response.status_code == 404
        assert "detail" in response.json()

    async def test_get_player_stats_sanitizes_nan_metrics(
        self,
        client: AsyncClient,
        test_session,
        sample_player,
        sample_season,
        sample_teams,
    ):
        """Player stats endpoint should replace non-JSON NaN values with null."""
        from app.models import PlayerSeasonStats

        stats = PlayerSeasonStats(
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
        test_session.add(stats)
        await test_session.commit()

        response = await client.get(
            f"/api/v1/players/{sample_player.id}/stats?season_id={sample_season.id}"
        )
        assert response.status_code == 200
        data = response.json()
        assert data["player_id"] == sample_player.id
        assert data["xg"] is None
        assert data["pass_accuracy"] is None

    async def test_get_player_games_empty(self, client: AsyncClient, sample_player, sample_season):
        """Test getting player games when no games played."""
        player_id = sample_player.id
        response = await client.get(
            f"/api/v1/players/{player_id}/games?season_id={sample_season.id}"
        )
        assert response.status_code == 200
        data = response.json()
        assert data["items"] == []
        assert data["total"] == 0

    async def test_top_role_from_player_column(
        self,
        client: AsyncClient,
        sample_player,
    ):
        """When Player.top_role is set, it should be returned as-is."""
        # sample_player fixture already sets top_role="AM (attacking midfielder)"
        response = await client.get(f"/api/v1/players/{sample_player.id}")
        assert response.status_code == 200
        assert response.json()["top_role"] == "AM (attacking midfielder)"

    async def test_top_role_fallback_to_player_team_position(
        self,
        client: AsyncClient,
        test_session,
        sample_season,
        sample_teams,
    ):
        """When Player.top_role is NULL, fall back to the latest PlayerTeam.position."""
        from uuid import uuid4
        from datetime import date
        from app.models.player import Player
        from app.models.player_team import PlayerTeam

        player = Player(
            sota_id=uuid4(),
            first_name="No",
            last_name="Role",
            birthday=date(1998, 3, 10),
            player_type="forward",
            top_role=None,
        )
        test_session.add(player)
        await test_session.commit()
        await test_session.refresh(player)

        pt = PlayerTeam(
            player_id=player.id,
            team_id=sample_teams[0].id,
            season_id=sample_season.id,
            position_ru="Нападающий",
            position_kz="Шабуылшы",
            position_en="Forward",
            number=9,
            is_active=True,
            is_hidden=False,
        )
        test_session.add(pt)
        await test_session.commit()

        response = await client.get(f"/api/v1/players/{player.id}?lang=ru")
        assert response.status_code == 200
        assert response.json()["top_role"] == "Нападающий"

        response_kz = await client.get(f"/api/v1/players/{player.id}?lang=kz")
        assert response_kz.json()["top_role"] == "Шабуылшы"

    async def test_get_player_ignores_hidden_team_when_visible_one_exists(
        self,
        client: AsyncClient,
        test_session,
        sample_season,
        sample_teams,
    ):
        """A hidden contract (e.g. an old team kept for historical stats after a
        transfer) must never be picked as "the" team over a visible contract in
        the same season — this is what player.teams[0] on the frontend uses to
        render the player's team badge/link."""
        from uuid import uuid4
        from datetime import date
        from app.models.player import Player
        from app.models.player_team import PlayerTeam

        player = Player(
            sota_id=uuid4(),
            first_name="Transferred",
            last_name="Player",
            birthday=date(1999, 4, 20),
            player_type="halfback",
        )
        test_session.add(player)
        await test_session.commit()
        await test_session.refresh(player)

        old_team, new_team = sample_teams[0], sample_teams[1]

        # Old contract inserted first, so it would sort first in the relationship
        # by primary key — and is marked hidden after the transfer.
        old_pt = PlayerTeam(
            player_id=player.id,
            team_id=old_team.id,
            season_id=sample_season.id,
            number=9,
            is_active=True,
            is_hidden=True,
        )
        test_session.add(old_pt)
        await test_session.commit()

        new_pt = PlayerTeam(
            player_id=player.id,
            team_id=new_team.id,
            season_id=sample_season.id,
            number=7,
            is_active=True,
            is_hidden=False,
        )
        test_session.add(new_pt)
        await test_session.commit()

        response = await client.get(
            f"/api/v1/players/{player.id}?season_id={sample_season.id}"
        )
        assert response.status_code == 200
        data = response.json()
        assert data["teams"] == [new_team.id]
        assert data["jersey_number"] == 7

    async def test_top_role_none_when_no_player_team(
        self,
        client: AsyncClient,
        test_session,
    ):
        """When Player.top_role is NULL and there are no PlayerTeam rows, return None."""
        from uuid import uuid4
        from datetime import date
        from app.models.player import Player

        player = Player(
            sota_id=uuid4(),
            first_name="Lone",
            last_name="Wolf",
            birthday=date(2000, 6, 1),
            player_type=None,
            top_role=None,
        )
        test_session.add(player)
        await test_session.commit()
        await test_session.refresh(player)

        response = await client.get(f"/api/v1/players/{player.id}")
        assert response.status_code == 200
        assert response.json()["top_role"] is None


@pytest.mark.asyncio
class TestPlayerTournamentsCurrentLeague:
    """Tests for current_season_id on GET /players/{id}/tournaments.

    The bar on the player page reflects the league where the player plays NOW,
    derived from the active contract in the current season (with playtime as the
    tie-break when several active contracts exist).
    """

    async def _setup(self, test_session):
        """Create a player, a championship, and PL/1L current seasons + an old PL season."""
        from uuid import uuid4
        from datetime import date
        from app.models.championship import Championship
        from app.models.player import Player
        from app.models.season import Season
        from app.models.team import Team
        from app.models.player_team import PlayerTeam
        from app.models.player_season_stats import PlayerSeasonStats

        champ = Championship(id=500, name="Test Champ")
        pl_now = Season(
            id=200, name="ПЛ 2026", championship_id=500, frontend_code="pl",
            date_start=date(2026, 3, 1), is_current=True, is_visible=True,
        )
        l1_now = Season(
            id=204, name="Первая 2026", championship_id=500, frontend_code="1l",
            date_start=date(2026, 3, 1), is_current=True, is_visible=True,
        )
        pl_old = Season(
            id=61, name="ПЛ 2025", championship_id=500, frontend_code="pl",
            date_start=date(2025, 3, 1), is_current=False, is_visible=True,
        )
        teams = [Team(id=701, name="A"), Team(id=702, name="B")]
        player = Player(
            sota_id=uuid4(), first_name="Young", last_name="Talent",
            birthday=date(2006, 1, 1), player_type="forward",
        )
        test_session.add_all([champ, pl_now, l1_now, pl_old, *teams, player])
        await test_session.commit()
        await test_session.refresh(player)
        return player

    async def test_current_league_from_active_first_league_contract(
        self, client: AsyncClient, test_session
    ):
        """Active contract in current First League, stats only in old PL → current = 1l."""
        from app.models.player_team import PlayerTeam
        from app.models.player_season_stats import PlayerSeasonStats

        player = await self._setup(test_session)
        # Active contract in current First League season (no stats yet).
        test_session.add(PlayerTeam(
            player_id=player.id, team_id=702, season_id=204,
            is_active=True, is_hidden=False,
        ))
        # Historical PL stats (old season) — should NOT drive the current league.
        test_session.add(PlayerSeasonStats(
            player_id=player.id, season_id=61, team_id=701,
            games_played=20, time_on_field_total=1800,
        ))
        await test_session.commit()

        resp = await client.get(f"/api/v1/players/{player.id}/tournaments")
        assert resp.status_code == 200
        body = resp.json()
        # Season 204 is the First League season.
        assert body["current_season_id"] == 204

    async def test_dual_registration_picks_league_with_more_playtime(
        self, client: AsyncClient, test_session
    ):
        """Active in both PL and 1L; more minutes in 1L → current = 1l season (not priority)."""
        from app.models.player_team import PlayerTeam
        from app.models.player_season_stats import PlayerSeasonStats

        player = await self._setup(test_session)
        test_session.add_all([
            PlayerTeam(player_id=player.id, team_id=701, season_id=200,
                       is_active=True, is_hidden=False),
            PlayerTeam(player_id=player.id, team_id=702, season_id=204,
                       is_active=True, is_hidden=False),
            # Barely plays in PL, plays a lot in First League.
            PlayerSeasonStats(player_id=player.id, season_id=200, team_id=701,
                              games_played=1, time_on_field_total=20),
            PlayerSeasonStats(player_id=player.id, season_id=204, team_id=702,
                              games_played=15, time_on_field_total=1300),
        ])
        await test_session.commit()

        resp = await client.get(f"/api/v1/players/{player.id}/tournaments")
        assert resp.status_code == 200
        assert resp.json()["current_season_id"] == 204  # First League

    async def test_dual_registration_no_playtime_falls_back_to_priority(
        self, client: AsyncClient, test_session
    ):
        """Active in both PL and 1L, no playtime yet → priority pl > 1l → current = pl season."""
        from app.models.player_team import PlayerTeam

        player = await self._setup(test_session)
        test_session.add_all([
            PlayerTeam(player_id=player.id, team_id=701, season_id=200,
                       is_active=True, is_hidden=False),
            PlayerTeam(player_id=player.id, team_id=702, season_id=204,
                       is_active=True, is_hidden=False),
        ])
        await test_session.commit()

        resp = await client.get(f"/api/v1/players/{player.id}/tournaments")
        assert resp.status_code == 200
        assert resp.json()["current_season_id"] == 200  # Premier League

    async def test_no_active_current_contract_falls_back_to_default(
        self, client: AsyncClient, test_session
    ):
        """No active contract in a current season → fall back to default_season_id's code."""
        from app.models.player_season_stats import PlayerSeasonStats

        player = await self._setup(test_session)
        # Only historical PL stats, no active current-season contract.
        test_session.add(PlayerSeasonStats(
            player_id=player.id, season_id=61, team_id=701,
            games_played=20, time_on_field_total=1800,
        ))
        await test_session.commit()

        resp = await client.get(f"/api/v1/players/{player.id}/tournaments")
        assert resp.status_code == 200
        body = resp.json()
        assert body["current_season_id"] == body["default_season_id"] == 61
