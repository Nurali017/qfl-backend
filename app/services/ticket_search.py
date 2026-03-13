"""Ticket search service — finds ticket URLs via SerpAPI (Google Search)."""

import asyncio
import logging
import re
from datetime import date, datetime, timedelta
from urllib.parse import urlparse, unquote

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models import Game, GameStatus, Team

logger = logging.getLogger(__name__)

# Prioritized ticket platform domains
TICKET_DOMAINS = [
    "ticketon.kz",
    "sxodim.com",
    "portalbilet.kz",
    "shop.kaspi.kz",
    "iticket.kz",
    "zakazbiletov.kz",
    "kino.kz",
]

# Generic paths that don't point to a specific event — reject these
_GENERIC_PATHS = {
    "/", "/sports", "/tickets", "/sport", "/bilety",
    "/ru/page/bilety", "/kz/page/bilety",
}

SERPAPI_URL = "https://serpapi.com/search.json"

# Russian month names (genitive case) for query formatting
_MONTHS_RU = {
    1: "января", 2: "февраля", 3: "марта", 4: "апреля",
    5: "мая", 6: "июня", 7: "июля", 8: "августа",
    9: "сентября", 10: "октября", 11: "ноября", 12: "декабря",
}

# Known team name slugs used on ticket platforms (Cyrillic → Latin)
# Covers cases where standard transliteration doesn't match URL slugs
_TEAM_SLUG_OVERRIDES: dict[str, list[str]] = {
    "женис": ["zhenis", "jenis", "zhenis"],
    "иртыш": ["irtysh", "irtish", "ertis"],
    "кайрат": ["kairat", "qairat"],
    "кайсар": ["kaisar", "kaysar"],
    "жетысу": ["zhetysu", "jetisu", "zhetisu"],
    "окжетпес": ["okzhetpes", "okjetpes"],
    "елимай": ["elimai", "elimay"],
    "улытау": ["ulytau", "ulitau"],
    "кызылжар": ["kyzylzhar", "kyzyljar", "kyzylzar"],
    "тобыл": ["tobyl", "tobol"],
    "ордабасы": ["ordabasy", "ordabasi"],
    "актобе": ["aktobe", "aqtobe"],
    "атырау": ["atyrau", "atirau"],
    "астана": ["astana"],
    "каспий": ["kaspiy", "caspiy", "kaspi"],
}

# Standard transliteration map
_TRANSLIT = {
    "а": "a", "б": "b", "в": "v", "г": "g", "д": "d", "е": "e", "ё": "e",
    "ж": "zh", "з": "z", "и": "i", "й": "y", "к": "k", "л": "l", "м": "m",
    "н": "n", "о": "o", "п": "p", "р": "r", "с": "s", "т": "t", "у": "u",
    "ф": "f", "х": "kh", "ц": "ts", "ч": "ch", "ш": "sh", "щ": "shch",
    "ъ": "", "ы": "y", "ь": "", "э": "e", "ю": "yu", "я": "ya",
}


def _transliterate(text: str) -> str:
    """Transliterate Cyrillic to Latin for URL matching."""
    result = []
    for ch in text.lower():
        result.append(_TRANSLIT.get(ch, ch))
    return "".join(result)


def _normalize(text: str) -> str:
    """Lowercase, strip non-alphanumeric, for fuzzy matching."""
    return re.sub(r"[^a-zа-яё0-9]", "", text.lower())


def _team_matches_text(team_name: str, text: str) -> bool:
    """Check if team name appears in text (URL/title) in Cyrillic or Latin form."""
    text_norm = _normalize(text)
    name_lower = team_name.lower()
    # Direct Cyrillic match
    if _normalize(name_lower) in text_norm:
        return True
    # Known slug overrides (handles non-standard transliterations)
    overrides = _TEAM_SLUG_OVERRIDES.get(name_lower, [])
    for slug in overrides:
        if slug in text_norm:
            return True
    # Standard transliteration fallback
    translit = _transliterate(name_lower)
    if translit in text_norm:
        return True
    return False


def _format_date_ru(d: date) -> str:
    """Format date as '15 апреля' for Russian-language search queries."""
    return f"{d.day} {_MONTHS_RU[d.month]}"


def _build_search_query(home_name: str, away_name: str, game_date: date) -> str:
    """Build a Google search query for ticket URLs."""
    date_str = _format_date_ru(game_date)
    return f"билеты {home_name} {away_name} {date_str}"


# Phrases indicating free entry (case-insensitive)
_FREE_ENTRY_PHRASES = [
    "вход свободный",
    "вход бесплатный",
    "бесплатный вход",
    "свободный вход",
    "тегін кіру",       # Kazakh: free entry
    "кіру тегін",
    "кіру еркін",
    "free entry",
    "free admission",
]


def _detect_free_entry(organic_results: list[dict], home_name: str) -> bool:
    """Check if search results indicate free entry for the home team's match."""
    for result in organic_results:
        title = result.get("title", "")
        snippet = result.get("snippet", "")
        text = f"{title} {snippet}".lower()
        # Only consider results that mention the home team
        if not _team_matches_text(home_name, text):
            continue
        for phrase in _FREE_ENTRY_PHRASES:
            if phrase in text:
                return True
    return False


async def _check_team_website_free_entry(
    website: str, home_name: str, client: httpx.AsyncClient
) -> bool:
    """Fetch home team's website and check for free entry phrases."""
    # Normalize to root URL (some DB entries have /kk/ or /ru/ paths that 404)
    parsed = urlparse(website)
    root_url = f"{parsed.scheme}://{parsed.hostname}"
    for url in (website, root_url):
        try:
            resp = await client.get(url, follow_redirects=True, timeout=10)
            if resp.status_code != 200:
                continue
            text = resp.text.lower()
            if not _team_matches_text(home_name, text):
                continue
            for phrase in _FREE_ENTRY_PHRASES:
                if phrase in text:
                    return True
        except Exception:
            continue
    return False


def _extract_ticket_url(
    organic_results: list[dict],
    home_name: str,
    away_name: str,
) -> str | None:
    """Extract the first ticket URL matching allowed domains + team names."""
    for result in organic_results:
        link = result.get("link", "")
        if not link:
            continue
        # Normalize: add scheme if missing
        if not link.startswith(("http://", "https://")):
            link = "https://" + link
        try:
            parsed = urlparse(link)
            hostname = parsed.hostname
            if not hostname:
                continue
            # Reject generic pages
            path = parsed.path.rstrip("/") or "/"
            if path in _GENERIC_PATHS:
                continue
            # ticketon.kz: only accept /event/* links with both teams in URL
            is_ticketon = hostname == "ticketon.kz" or hostname.endswith(".ticketon.kz")
            if is_ticketon:
                if not path.startswith("/event/"):
                    continue
                # Reject old events with 6-digit date suffix (e.g. -300918 = 30.09.2018)
                slug = path.rsplit("/", 1)[-1]
                if re.search(r"-\d{6}$", slug):
                    continue
                # Both teams must be in the URL slug (ticketon always has both)
                if not _team_matches_text(home_name, unquote(path)):
                    continue
                if not _team_matches_text(away_name, unquote(path)):
                    continue
            # sxodim.com: reject /tag/ pages (club listings, not events)
            if hostname == "sxodim.com" or hostname.endswith(".sxodim.com"):
                if "/tag/" in path:
                    continue
            # Check domain allowlist
            domain_ok = False
            for domain in TICKET_DOMAINS:
                if hostname == domain or hostname.endswith(f".{domain}"):
                    domain_ok = True
                    break
            if not domain_ok:
                continue
            # Verify BOTH teams appear in URL/title/snippet
            match_text = unquote(link) + " " + result.get("title", "") + " " + result.get("snippet", "")
            if not _team_matches_text(home_name, match_text):
                continue
            if not _team_matches_text(away_name, match_text):
                continue
            return link
        except Exception:
            continue
    return None


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, httpx.TimeoutException):
        return True
    if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code >= 500:
        return True
    return False


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception(_is_retryable),
    reraise=True,
)
async def _search_serpapi(
    query: str, api_key: str, client: httpx.AsyncClient
) -> list[dict]:
    """Search Google via SerpAPI. Returns organic_results list."""
    resp = await client.get(
        SERPAPI_URL,
        params={
            "engine": "google",
            "q": query,
            "gl": "kz",
            "hl": "ru",
            "num": 10,
            "api_key": api_key,
        },
    )
    resp.raise_for_status()
    data = resp.json()
    return data.get("organic_results", [])


async def search_and_update_tickets(db: AsyncSession) -> dict:
    """Search for ticket URLs for upcoming games and store results."""
    from app.config import get_settings
    settings = get_settings()

    if not settings.ticket_search_enabled:
        return {"skipped": True, "reason": "ticket search disabled"}

    if not settings.serper_api_key:
        return {"skipped": True, "reason": "serper_api_key not set"}

    today = date.today()
    cutoff = today + timedelta(days=14)
    three_hours_ago = datetime.utcnow() - timedelta(hours=3)

    result = await db.execute(
        select(Game)
        .options(selectinload(Game.home_team), selectinload(Game.away_team))
        .where(
            Game.date >= today,
            Game.date <= cutoff,
            Game.status == GameStatus.created,
            Game.ticket_url.is_(None),
            Game.is_free_entry.is_(False),
        )
    )
    games = result.scalars().all()

    updated = 0
    free_entry = 0
    searched = 0
    skipped = 0
    errors = 0

    async with httpx.AsyncClient(timeout=15) as client:
        for game in games:
            # Skip if recently searched
            if game.ticket_url_fetched_at and game.ticket_url_fetched_at > three_hours_ago:
                skipped += 1
                continue

            home_team = game.home_team
            away_team = game.away_team
            if not home_team or not away_team:
                skipped += 1
                continue

            try:
                query = _build_search_query(home_team.name, away_team.name, game.date)
                organic = await _search_serpapi(query, settings.serper_api_key, client)

                # Check for free entry in Google results
                is_free = _detect_free_entry(organic, home_team.name)

                # If Google didn't detect free entry, check home team's website
                if not is_free and home_team.website:
                    is_free = await _check_team_website_free_entry(
                        home_team.website, home_team.name, client,
                    )

                if is_free:
                    game.is_free_entry = True
                    free_entry += 1
                    logger.info(
                        "Detected free entry for game %s (%s vs %s)",
                        game.id, home_team.name, away_team.name,
                    )
                else:
                    ticket_url = _extract_ticket_url(organic, home_team.name, away_team.name)
                    if ticket_url:
                        game.ticket_url = ticket_url
                        updated += 1
                        logger.info(
                            "Found ticket URL for game %s (%s vs %s): %s",
                            game.id, home_team.name, away_team.name, ticket_url,
                        )

                game.ticket_url_fetched_at = datetime.utcnow()
                searched += 1

                # Rate limit: SerpAPI allows ~1 req/sec on free plan
                await asyncio.sleep(1.0)

            except Exception:
                logger.warning(
                    "Ticket search failed for game %s (%s vs %s)",
                    game.id, home_team.name, away_team.name, exc_info=True,
                )
                errors += 1

    return {
        "updated": updated, "free_entry": free_entry,
        "searched": searched, "skipped": skipped, "errors": errors,
    }
