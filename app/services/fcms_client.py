"""HTTP client for FCMS (FIFA CMS) API with cookie-based authentication."""

import asyncio
import logging
import time

import httpx
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

from app.config import get_settings

settings = get_settings()
logger = logging.getLogger(__name__)

RETRYABLE_EXCEPTIONS = (
    httpx.ConnectTimeout,
    httpx.ReadTimeout,
    httpx.ConnectError,
    httpx.RemoteProtocolError,
)

# Cookie TTL ~30 min, refresh conservatively at 25 min
_COOKIE_TTL_SECONDS = 25 * 60


class FcmsClient:
    """Client for FCMS API (https://api-standard.fcms.ma.services)."""

    def __init__(self):
        self._client: httpx.AsyncClient | None = None
        self._auth_lock = asyncio.Lock()
        self._cookie_obtained_at: float = 0.0

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                follow_redirects=True,
                timeout=httpx.Timeout(30.0),
                limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
            )
        return self._client

    def _is_cookie_valid(self) -> bool:
        if self._cookie_obtained_at == 0.0:
            return False
        return (time.monotonic() - self._cookie_obtained_at) < _COOKIE_TTL_SECONDS

    async def authenticate(self) -> None:
        """Authenticate with FCMS and store BEARER cookie."""
        async with self._auth_lock:
            # Double-check after acquiring lock
            if self._is_cookie_valid():
                return

            client = await self._get_client()
            resp = await client.post(
                settings.fcms_auth_url,
                json={
                    "username": settings.fcms_email,
                    "password": settings.fcms_password,
                },
                headers={"x-customer-code": settings.fcms_customer_code},
            )
            resp.raise_for_status()
            self._cookie_obtained_at = time.monotonic()
            logger.info("FCMS authentication successful")

    async def _ensure_authenticated(self) -> None:
        if not self._is_cookie_valid():
            await self.authenticate()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict | None = None,
        accept: str = "application/json",
        _retry_auth: bool = True,
    ) -> httpx.Response:
        """Make an authenticated request to FCMS API."""
        await self._ensure_authenticated()
        client = await self._get_client()

        url = f"{settings.fcms_base_url}{path}"
        headers = {
            "x-customer-code": settings.fcms_customer_code,
            "Accept": accept,
            "Accept-Language": "ru",
        }

        resp = await client.request(method, url, headers=headers, params=params)

        # Handle 401 by re-authenticating once
        if resp.status_code == 401 and _retry_auth:
            self._cookie_obtained_at = 0.0
            await self.authenticate()
            return await self._request(method, path, params=params, accept=accept, _retry_auth=False)

        resp.raise_for_status()
        return resp

    async def get_match(self, match_id: int) -> dict:
        """Get match details."""
        resp = await self._request("GET", f"/v1/matches/{match_id}")
        return resp.json()

    async def get_competition_competitors(self, competition_id: int) -> list[dict]:
        """Get all teams registered in a competition (заявки команд)."""
        resp = await self._request(
            "GET",
            f"/v1/competitions/{competition_id}/competitors",
            params={"limit": 1000},
        )
        data = resp.json()
        return data.get("_embedded", {}).get("competitors", [])

    async def get_competitor_players(self, competition_id: int, team_id: int) -> list[dict]:
        """Get player roster for a team in a competition (заявка игроков)."""
        resp = await self._request(
            "GET",
            f"/v1/competitions/{competition_id}/competitors/{team_id}/players",
            params={"limit": 100},
        )
        data = resp.json()
        return data.get("_embedded", {}).get("competitorPlayers", [])

    async def get_competitor_officials(self, competition_id: int, team_id: int) -> list[dict]:
        """Get coaching staff for a team in a competition (тренерский штаб)."""
        resp = await self._request(
            "GET",
            f"/v1/competitions/{competition_id}/competitors/{team_id}/teamOfficials",
            params={"limit": 50},
        )
        data = resp.json()
        return data.get("_embedded", {}).get("competitorTeamOfficials", [])

    async def get_pre_match_report_pdf(self, match_id: int) -> bytes | None:
        """Download pre-match report PDF (contains lineups).

        Returns PDF bytes or None if not available yet.
        """
        try:
            resp = await self._request(
                "GET",
                f"/v1/matches/{match_id}/reports",
                params={"reportType": "PRE_MATCH_REPORT", "reportOutputType": "pdf"},
                accept="application/pdf",
            )
            if resp.headers.get("content-type", "").startswith("application/pdf"):
                return resp.content
        except httpx.HTTPStatusError as e:
            if e.response.status_code in (400, 404):
                logger.debug("FCMS match %d pre-match report not available: %d", match_id, e.response.status_code)
                return None
            raise
        return None

    async def get_match_report_pdf(self, match_id: int) -> bytes | None:
        """Download match report PDF (post-match protocol with attendance).

        Returns PDF bytes or None if not available yet.
        """
        try:
            resp = await self._request(
                "GET",
                f"/v1/matches/{match_id}/reports",
                params={"reportType": "MATCH_REPORT", "reportOutputType": "pdf"},
                accept="application/pdf",
            )
            if resp.headers.get("content-type", "").startswith("application/pdf"):
                return resp.content
        except httpx.HTTPStatusError as e:
            if e.response.status_code in (400, 404):
                logger.debug("FCMS match %d match report not available: %d", match_id, e.response.status_code)
                return None
            raise
        return None

    async def get_match_events(self, match_id: int) -> list[dict]:
        """Get match events (goals, cards, substitutions)."""
        resp = await self._request(
            "GET",
            f"/v1/matches/{match_id}/matchEvents",
            params={"limit": 200},
        )
        return resp.json().get("_embedded", {}).get("matchEvents", [])

    async def get_match_players(self, match_id: int, competitor_id: int) -> list[dict]:
        """Get match player list for a competitor (maps matchPlayerId → player details)."""
        resp = await self._request(
            "GET",
            f"/v1/matches/{match_id}/competitors/{competitor_id}/matchPlayers",
            params={"limit": 100},
        )
        return resp.json().get("_embedded", {}).get("matchPlayers", [])

    async def get_match_official_allocations(self, match_id: int) -> list[dict]:
        """Get assigned match officials (referees, VAR, commissioner, inspector)."""
        resp = await self._request(
            "GET",
            f"/v1/matches/{match_id}/matchOfficialAllocations",
            params={"limit": 50},
        )
        return resp.json().get("_embedded", {}).get("matchOfficialAllocations", [])

    async def list_matches(
        self,
        group_id: int,
        page: int = 1,
        limit: int = 100,
    ) -> dict:
        """List matches for a group (competition round)."""
        resp = await self._request(
            "GET",
            "/v1/matches",
            params={
                "filter[matchGroup][]": group_id,
                "page": page,
                "limit": limit,
            },
        )
        return resp.json()

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None


# Singleton
_fcms_client: FcmsClient | None = None


def get_fcms_client() -> FcmsClient:
    global _fcms_client
    if _fcms_client is None:
        _fcms_client = FcmsClient()
    return _fcms_client
