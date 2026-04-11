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
