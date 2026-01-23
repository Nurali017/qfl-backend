import httpx
from datetime import datetime, timedelta
from typing import Any

from app.config import get_settings

settings = get_settings()


class SotaClient:
    """Client for SOTA API (https://sota.id/api)"""

    BASE_URL = settings.sota_api_base_url

    def __init__(self):
        self.email = settings.sota_api_email
        self.password = settings.sota_api_password
        self.access_token: str | None = None
        self.refresh_token: str | None = None
        self.token_expires_at: datetime | None = None

    async def authenticate(self) -> None:
        """Authenticate and get JWT tokens."""
        async with httpx.AsyncClient(follow_redirects=True) as client:
            response = await client.post(
                f"{self.BASE_URL}/auth/token/",
                json={"email": self.email, "password": self.password},
                timeout=30.0,
            )
            response.raise_for_status()
            data = response.json()
            self.access_token = data["access"]
            self.refresh_token = data.get("refresh")
            self.token_expires_at = datetime.now() + timedelta(hours=23)

    async def ensure_authenticated(self) -> None:
        """Ensure we have a valid access token."""
        if not self.access_token or not self.token_expires_at:
            await self.authenticate()
        elif datetime.now() >= self.token_expires_at:
            await self.authenticate()

    def get_headers(self, language: str = "ru") -> dict[str, str]:
        """Get headers with authorization and language localization."""
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Accept-Language": language,
        }

    async def _get_paginated(
        self, url: str, headers: dict | None = None, params: dict | None = None
    ) -> list[dict[str, Any]]:
        """Fetch all pages from a paginated endpoint."""
        results = []
        async with httpx.AsyncClient(follow_redirects=True) as client:
            while url:
                response = await client.get(
                    url, headers=headers, params=params, timeout=30.0
                )
                response.raise_for_status()
                data = response.json()

                # Handle both paginated and non-paginated responses
                if isinstance(data, list):
                    # Direct list response
                    results.extend(data)
                    url = None
                elif isinstance(data, dict):
                    # Paginated response with results
                    results.extend(data.get("results", data.get("data", [])))
                    url = data.get("next")
                else:
                    url = None

                params = None  # Params are included in next URL
        return results

    # ==================== Endpoints requiring authentication ====================

    async def get_tournaments(self, language: str = "ru") -> list[dict[str, Any]]:
        """Get all tournaments."""
        await self.ensure_authenticated()
        return await self._get_paginated(
            f"{self.BASE_URL}/public/v1/tournaments/", headers=self.get_headers(language)
        )

    async def get_seasons(self, language: str = "ru") -> list[dict[str, Any]]:
        """Get all seasons."""
        await self.ensure_authenticated()
        return await self._get_paginated(
            f"{self.BASE_URL}/public/v1/seasons/", headers=self.get_headers(language)
        )

    async def get_teams(self, season_id: int | None = None, language: str = "ru") -> list[dict[str, Any]]:
        """Get teams, optionally filtered by season."""
        await self.ensure_authenticated()
        params = {"season_id": season_id} if season_id else None
        return await self._get_paginated(
            f"{self.BASE_URL}/public/v1/teams/", headers=self.get_headers(language), params=params
        )

    async def get_players(
        self, season_id: int, team_id: int | None = None, language: str = "ru"
    ) -> list[dict[str, Any]]:
        """Get players for a season, optionally filtered by team."""
        await self.ensure_authenticated()
        params = {"season_id": season_id}
        if team_id:
            params["team_id"] = team_id
        return await self._get_paginated(
            f"{self.BASE_URL}/public/v1/players/", headers=self.get_headers(language), params=params
        )

    async def get_games(self, season_id: int, language: str = "ru") -> list[dict[str, Any]]:
        """Get all games for a season."""
        await self.ensure_authenticated()
        return await self._get_paginated(
            f"{self.BASE_URL}/public/v1/games/",
            headers=self.get_headers(language),
            params={"season_id": season_id},
        )

    async def get_score_table(self, season_id: int, language: str = "ru") -> dict[str, Any]:
        """Get league table for a season."""
        await self.ensure_authenticated()
        async with httpx.AsyncClient(follow_redirects=True) as client:
            response = await client.get(
                f"{self.BASE_URL}/public/v1/seasons/{season_id}/score_table/",
                headers=self.get_headers(language),
                timeout=30.0,
            )
            response.raise_for_status()
            return response.json()

    async def get_team_season_stats(
        self, team_id: int, season_id: int, language: str = "ru"
    ) -> dict[str, Any]:
        """Get team statistics for a season (v1 - basic stats)."""
        await self.ensure_authenticated()
        async with httpx.AsyncClient(follow_redirects=True) as client:
            response = await client.get(
                f"{self.BASE_URL}/public/v1/teams/{team_id}/season_stats/",
                headers=self.get_headers(language),
                params={"season_id": season_id},
                timeout=30.0,
            )
            response.raise_for_status()
            return response.json()

    async def get_team_season_stats_v2(
        self, team_id: int, season_id: int, language: str = "ru"
    ) -> dict[str, Any]:
        """
        Get detailed team statistics for a season (v2 - 92 metrics).

        Returns stats as key-value dict including:
        - xg, opponent_xg, xg_per_match
        - possession_percent_average
        - pass_ratio, duel_ratio, dribble_ratio
        - And many more...
        """
        await self.ensure_authenticated()
        async with httpx.AsyncClient(follow_redirects=True) as client:
            response = await client.get(
                f"{self.BASE_URL}/public/v2/teams/{team_id}/season_stats/",
                headers=self.get_headers(language),
                params={"season_id": season_id},
                timeout=30.0,
            )
            response.raise_for_status()
            data = response.json()

            # Convert array of {key, value, name} to dict {key: value}
            stats_list = data.get("data", {}).get("stats", [])
            stats_dict = {s["key"]: s["value"] for s in stats_list if "key" in s}
            return stats_dict

    async def get_player_season_stats(
        self, player_id: str, season_id: int, language: str = "ru"
    ) -> dict[str, Any]:
        """
        Get player statistics for a season (v2 - 50+ metrics).

        Returns stats as key-value dict including:
        - xg, xg_per_90
        - duels, aerial_duel, ground_duel
        - dribble, tackle, interception
        - pass_ratio, key_pass, pass_forward
        - And more...
        """
        await self.ensure_authenticated()
        async with httpx.AsyncClient(follow_redirects=True) as client:
            response = await client.get(
                f"{self.BASE_URL}/public/v2/players/{player_id}/season_stats/",
                headers=self.get_headers(language),
                params={"season_id": season_id},
                timeout=30.0,
            )
            response.raise_for_status()
            data = response.json()

            # Convert array of {key, value, name} to dict {key: value}
            stats_list = data.get("data", {}).get("stats", [])
            stats_dict = {s["key"]: s["value"] for s in stats_list if "key" in s}

            # Add player info
            player_data = data.get("data", {})
            stats_dict["first_name"] = player_data.get("first_name")
            stats_dict["last_name"] = player_data.get("last_name")

            return stats_dict

    # ==================== Game stats endpoints ====================

    async def get_game_player_stats(self, game_id: str, language: str = "ru") -> list[dict[str, Any]]:
        """Get player statistics for a game."""
        await self.ensure_authenticated()
        async with httpx.AsyncClient(follow_redirects=True) as client:
            response = await client.get(
                f"{self.BASE_URL}/public/v1/games/{game_id}/players/",
                headers=self.get_headers(language),
                timeout=30.0,
            )
            response.raise_for_status()
            data = response.json()
            if isinstance(data, list):
                return data
            # Handle nested response: {"result": "...", "data": {"players": [...]}}
            if isinstance(data, dict):
                inner = data.get("data", {})
                if isinstance(inner, dict):
                    return inner.get("players", inner.get("results", []))
                return data.get("results", [])
            return []

    async def get_game_team_stats(self, game_id: str, language: str = "ru") -> list[dict[str, Any]]:
        """Get team statistics for a game."""
        await self.ensure_authenticated()
        async with httpx.AsyncClient(follow_redirects=True) as client:
            response = await client.get(
                f"{self.BASE_URL}/public/v1/games/{game_id}/teams/",
                headers=self.get_headers(language),
                timeout=30.0,
            )
            response.raise_for_status()
            data = response.json()
            if isinstance(data, list):
                return data
            # Handle nested response: {"result": "...", "data": {"teams": [...]}}
            if isinstance(data, dict):
                inner = data.get("data", {})
                if isinstance(inner, dict):
                    return inner.get("teams", inner.get("results", []))
                return data.get("results", [])
            return []

    async def get_game_stats(self, game_id: str, language: str = "ru") -> dict[str, Any]:
        """Get full game statistics."""
        player_stats = await self.get_game_player_stats(game_id, language)
        team_stats = await self.get_game_team_stats(game_id, language)
        return {"players": player_stats, "teams": team_stats}

    async def get_pre_game_lineup(self, game_id: str, language: str = "ru") -> dict[str, Any]:
        """
        Get pre-game lineup data including referees, coaches, and player lineups.

        Returns dict with:
        - referees: list of referee assignments with roles
        - coaches: dict with home_team and away_team lists of coaches
        - lineups: dict with home_team and away_team player lists

        Example response structure:
        {
            "referees": [
                {"id": 123, "first_name": "...", "last_name": "...", "role": "main"},
                ...
            ],
            "coaches": {
                "home_team": [{"id": 1, "first_name": "...", "role": "head_coach"}],
                "away_team": [...]
            },
            "lineups": {
                "home_team": [{"player_id": "uuid", "shirt_number": 10, "is_captain": true, "lineup_type": "starter"}],
                "away_team": [...]
            }
        }
        """
        await self.ensure_authenticated()
        async with httpx.AsyncClient(follow_redirects=True) as client:
            response = await client.get(
                f"{self.BASE_URL}/public/v1/games/{game_id}/pre_game_lineup/",
                headers=self.get_headers(language),
                timeout=30.0,
            )
            response.raise_for_status()
            return response.json()

    # ==================== Live match endpoints (/em/) ====================

    async def get_live_team_lineup(self, game_id: str, side: str) -> list[dict[str, Any]]:
        """
        Get live team lineup from /em/ endpoint.

        Args:
            game_id: Game UUID
            side: 'home' or 'away'

        Returns list of players with:
            - number: shirt number or special markers (TEAM, FORMATION, COACH, MAIN, ОСНОВНЫЕ, ЗАПАСНЫЕ)
            - first_name, last_name, full_name
            - gk: bool (goalkeeper)
            - capitan: bool
            - amplua: position category (Gk, D, DM, M, AM, F)
            - position: field position (C, L, R, LC, RC)
            - id: player UUID
            - bas_image_path: player photo path
        """
        await self.ensure_authenticated()
        url = f"https://sota.id/em/{game_id}-team-{side}.json"
        async with httpx.AsyncClient(follow_redirects=True) as client:
            response = await client.get(
                url,
                params={"access_token": self.access_token},
                timeout=30.0,
            )
            response.raise_for_status()
            return response.json()

    async def get_live_match_events(self, game_id: str) -> list[dict[str, Any]]:
        """
        Get live match events from /em/ endpoint.

        Returns list of events with:
            - half: 1 or 2
            - time: minute
            - action: ГОЛ, ГОЛЕВОЙ ПАС, ЖК, КК, ЗАМЕНА
            - number1, first_name1, last_name1, team1: primary player
            - number2, first_name2, last_name2, team2: secondary player (assist/sub)
            - standard: null or set piece type
        """
        await self.ensure_authenticated()
        url = f"https://sota.id/em/{game_id}-list.json"
        async with httpx.AsyncClient(follow_redirects=True) as client:
            response = await client.get(
                url,
                params={"access_token": self.access_token},
                timeout=30.0,
            )
            response.raise_for_status()
            return response.json()

    async def get_live_match_data(self, game_id: str) -> dict[str, Any]:
        """
        Get all live match data: both lineups and events.

        Returns dict with:
            - home_lineup: list of home team players
            - away_lineup: list of away team players
            - events: list of match events
        """
        home_lineup = await self.get_live_team_lineup(game_id, "home")
        away_lineup = await self.get_live_team_lineup(game_id, "away")
        events = await self.get_live_match_events(game_id)
        return {
            "home_lineup": home_lineup,
            "away_lineup": away_lineup,
            "events": events,
        }


# Singleton instance
_sota_client: SotaClient | None = None


def get_sota_client() -> SotaClient:
    global _sota_client
    if _sota_client is None:
        _sota_client = SotaClient()
    return _sota_client
