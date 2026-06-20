"""Ticket search service — finds ticket URLs via Serper (Google Search)."""

import asyncio
import logging
import re
from datetime import date, datetime, timedelta
from typing import NamedTuple
from urllib.parse import urlparse, unquote

import anthropic
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models import Game, GameStatus, Team
from app.models.season import Season
from app.services.telegram import send_telegram_message
from app.utils.timestamps import ensure_utc, utcnow

# Only search tickets for these championships (Premier League, Cup)
_TICKET_CHAMPIONSHIP_IDS = {1, 3}

logger = logging.getLogger(__name__)

# Prioritized ticket platform domains
TICKET_DOMAINS = [
    "ticketon.kz",
    "zakazbiletov.kz",
    "afisha.yandex.kz",
    "afisha.yandex.ru",  # widget.afisha.yandex.ru — клубы часто шарят виджет .ru-зоны
    "kino.kz",
]

# Generic paths that don't point to a specific event — reject these
_GENERIC_PATHS = {
    "/", "/sports", "/tickets", "/sport", "/bilety",
    "/ru/page/bilety", "/kz/page/bilety",
}

# Futsal/mini-football keywords — reject these (we only want football tickets)
_FUTSAL_KEYWORDS = [
    "futzal", "futsal", "futbol-zal", "mini-football",
    "мини-футбол", "футзал", "мини футбол",
    "mfk-",  # МФК = мини-футбольный клуб (ticketon URL slug prefix)
]

SERPER_URL = "https://google.serper.dev/search"
SERPER_SCRAPE_URL = "https://scrape.serper.dev"

# Russian month names (genitive case) for query formatting
_MONTHS_RU = {
    1: "января", 2: "февраля", 3: "марта", 4: "апреля",
    5: "мая", 6: "июня", 7: "июля", 8: "августа",
    9: "сентября", 10: "октября", 11: "ноября", 12: "декабря",
}

# Kazakh month names (nominative) for date detection in club posts
_MONTHS_KZ = {
    1: "қаңтар", 2: "ақпан", 3: "наурыз", 4: "сәуір",
    5: "мамыр", 6: "маусым", 7: "шілде", 8: "тамыз",
    9: "қыркүйек", 10: "қазан", 11: "қараша", 12: "желтоқсан",
}

# Kazakh-only letters mapped to closest Cyrillic match so _normalize and
# substring lookups treat "Қызылжар"/"Жетісу" as "Кызылжар"/"Жетису".
# Without this, _normalize strips them as non-[а-яё] and team-match fails.
_KAZAKH_TO_RU = str.maketrans({
    "қ": "к", "ғ": "г", "ң": "н", "ү": "у", "ұ": "у",
    "ө": "о", "һ": "х", "і": "и", "ә": "а",
})

# Known team name slugs used on ticket platforms (Cyrillic → Latin)
# Covers cases where standard transliteration doesn't match URL slugs
_TEAM_SLUG_OVERRIDES: dict[str, list[str]] = {
    "женис": ["zhenis", "jenis", "zhenis"],
    "иртыш": ["irtysh", "irtish", "ertis"],
    "кайрат": ["kairat", "qairat"],
    "кайсар": ["kaisar", "kaysar", "qaysar"],
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
    """Lowercase, fold Kazakh-only letters to Cyrillic, strip non-alphanumeric."""
    return re.sub(r"[^a-zа-яё0-9]", "", text.lower().translate(_KAZAKH_TO_RU))


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


def _team_position_in_text(team_name: str, text: str) -> int:
    """Earliest index where the team name appears (Cyrillic, slug, or translit), or -1.

    Used to verify home/away ordering in ticket URL slugs like
    "fk-aktobe-vs-fk-astana" where the home team must come first.
    """
    text_norm = _normalize(text)
    candidates: list[str] = []
    name_lower = team_name.lower()
    candidates.append(_normalize(name_lower))
    candidates.extend(_TEAM_SLUG_OVERRIDES.get(name_lower, []))
    candidates.append(_transliterate(name_lower))
    positions = [text_norm.find(c) for c in candidates if c]
    positions = [p for p in positions if p >= 0]
    return min(positions) if positions else -1


def _home_before_away_in_slug(home_name: str, away_name: str, path: str) -> bool:
    """True if home team appears before away team in a ticketon event slug.

    Ticketon slugs always list the home team first (e.g. "fk-aktobe-vs-fk-astana").
    Returns True when ordering can't be determined (one of them not found by
    position), leaving the existing presence checks as the gate.
    """
    text = unquote(path)
    home_pos = _team_position_in_text(home_name, text)
    away_pos = _team_position_in_text(away_name, text)
    if home_pos < 0 or away_pos < 0 or home_pos == away_pos:
        return True
    return home_pos < away_pos


def _format_date_ru(d: date) -> str:
    """Format date as '15 апреля' for Russian-language search queries."""
    return f"{d.day} {_MONTHS_RU[d.month]}"


def _build_search_query(home_name: str, away_name: str, game_date: date) -> str:
    """Build a Google search query for ticket URLs."""
    date_str = _format_date_ru(game_date)
    return f"билеты {home_name} {away_name} {date_str}"


def _build_instagram_query(home_name: str, away_name: str, game_date: date) -> str:
    """Build a Google search query targeting team Instagram posts."""
    date_str = _format_date_ru(game_date)
    return f"site:instagram.com {home_name} {away_name} {date_str}"


def _build_afisha_query(home_name: str, away_name: str) -> str:
    """Build a Google search targeting the indexed Yandex.Afisha event page.

    Clubs frequently sell tickets via an embedded Afisha widget
    (widget.afisha.yandex.kz/w/sessions/<opaque-id>) whose URL carries no team
    names and is never indexed by Google. The SAME sale, however, has a canonical
    indexed event page like afisha.yandex.kz/<city>/sport/football-<home>-<away>
    with both teams in the slug (home first). A site:-scoped search surfaces it
    without drowning in avia/rail "город→город" ticket noise. No date in the
    query — the slug has none; date is verified later via snippet + AI validation.
    """
    return f"site:afisha.yandex.kz {home_name} {away_name}"


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

# Words indicating partial free entry (only for specific groups, not general)
_FREE_ENTRY_EXCEPTIONS = [
    "детям",
    "до 14",
    "пенсионер",
    "инвалид",
    "балалар",      # Kazakh: children
    "зейнеткер",    # Kazakh: pensioner
]


def _snippet_mentions_date(text: str, game_date: date) -> bool:
    """Check if snippet/title mentions the game date (day + month in any format)."""
    day = str(game_date.day)
    month_ru = _MONTHS_RU[game_date.month]
    month_kz = _MONTHS_KZ[game_date.month]
    # "18 апреля", "18 мамыр", "18.04", "18/04"
    month_num = f"{game_date.month:02d}"
    if f"{day} {month_ru}" in text:
        return True
    if f"{day} {month_kz}" in text:
        return True
    if f"{day}.{month_num}" in text:
        return True
    if f"{day}/{month_num}" in text:
        return True
    return False


def _detect_free_entry(organic_results: list[dict], home_name: str, game_date: date) -> bool:
    """Check if search results indicate free entry for the home team's match."""
    for result in organic_results:
        title = result.get("title", "")
        snippet = result.get("snippet", "")
        text = f"{title} {snippet}".lower()
        # Only consider results that mention the home team
        if not _team_matches_text(home_name, text):
            continue
        # Must mention the correct date to avoid old results
        if not _snippet_mentions_date(text, game_date):
            continue
        for phrase in _FREE_ENTRY_PHRASES:
            if phrase in text:
                # Skip if free entry only applies to specific groups
                if any(exc in text for exc in _FREE_ENTRY_EXCEPTIONS):
                    logger.info(
                        "Skipped partial free entry '%s' (exception word found) in: %s",
                        phrase, snippet[:120] or title[:120],
                    )
                    continue
                logger.info(
                    "Detected free entry phrase '%s' in: %s",
                    phrase, snippet[:120] or title[:120],
                )
                return True
    return False


async def _check_team_website_free_entry(
    website: str, home_name: str, away_name: str, client: httpx.AsyncClient
) -> bool:
    """Fetch home team's website and check for free entry phrases.

    Requires BOTH team names to appear near the free entry phrase
    to avoid false positives from old news on the homepage.
    """
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
            if not _team_matches_text(away_name, text):
                continue
            for phrase in _FREE_ENTRY_PHRASES:
                if phrase in text:
                    return True
        except Exception:
            continue
    return False


# Pages on the club portal that are NOT a single-match sale — never link these.
_NON_MATCH_TICKET_PATHS = ("subscription", "abonement", "abonen", "season")


async def _find_ticket_url_on_website(
    website: str,
    home_name: str,
    away_name: str,
    game_date: date,
    client: httpx.AsyncClient,
) -> str | None:
    """Scan the home team's official site for a link to its own ticket portal.

    Many clubs sell tickets only through their own portal (e.g.
    tickets.fcelimai.kz) whose landing page carries no team names — so Serper
    can't find it and the URL can't be validated on its own. Instead we anchor
    on the CLUB SITE: only when both team names AND the game date appear on the
    page do we treat the featured match as this one, then extract the club's
    ticketing link. The link may be a generic portal landing — acceptable, since
    it is the club's official sales channel for their home match.
    """
    parsed = urlparse(website)
    if not parsed.hostname:
        return None
    root_url = f"{parsed.scheme}://{parsed.hostname}"
    # Own ticket subdomain like tickets.fcelimai.kz — derived from the site host
    own_ticket_host = f"tickets.{parsed.hostname.removeprefix('www.')}"
    for url in dict.fromkeys([website, root_url]):
        try:
            resp = await client.get(url, follow_redirects=True, timeout=12)
            if resp.status_code != 200:
                continue
            html = resp.text
            text = html.lower()
            # Anchor: the page must feature THIS match (both teams + date)
            if not _team_matches_text(home_name, text):
                continue
            if not _team_matches_text(away_name, text):
                continue
            if not _snippet_mentions_date(text, game_date):
                continue
            for link in re.findall(r'https?://[^\s"\'<>]+', html):
                low = link.lower()
                host = urlparse(low).hostname or ""
                is_ticket_link = (
                    host == own_ticket_host
                    or host.startswith("tickets.")
                    or "/tickets" in low
                    or "/bilety" in low
                    or host.endswith("ticketon.kz")
                    or "afisha.yandex" in host
                )
                if not is_ticket_link:
                    continue
                if any(p in low for p in _NON_MATCH_TICKET_PATHS):
                    continue
                clean = link.rstrip('",);')
                logger.info(
                    "Found club ticket portal on %s for %s vs %s: %s",
                    url, home_name, away_name, clean,
                )
                return clean
        except Exception:
            continue
    return None


def _snippet_has_wrong_year(snippet: str, title: str, game_year: int) -> bool:
    """Reject if snippet/title mentions a specific year that doesn't match the game year."""
    text = f"{title} {snippet}"
    # Find all 4-digit years (2020-2030 range)
    years_found = re.findall(r'\b(20[2-3]\d)\b', text)
    if not years_found:
        return False  # No year mentioned — can't reject
    # If ANY mentioned year matches game year, accept
    return all(int(y) != game_year for y in years_found)


class TicketMatch(NamedTuple):
    url: str
    title: str
    snippet: str


def _extract_ticket_urls(
    organic_results: list[dict],
    home_name: str,
    away_name: str,
    game_date: date | None = None,
) -> list[TicketMatch]:
    """Extract all ticket URLs matching allowed domains + team names, in result order.

    Returns every candidate (deduped by URL) so the caller can AI-validate them one
    by one — rejecting the first candidate must not abort the search for a game.
    """
    matches: list[TicketMatch] = []
    seen: set[str] = set()
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
                if not ("/event/" in path or path.startswith("/show/")):
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
                # Home team must come first in the slug (ticketon lists home first).
                # Rejects the mirror fixture, e.g. accepting "astana-vs-aktobe"
                # for an Aktobe home game.
                if not _home_before_away_in_slug(home_name, away_name, path):
                    logger.info(
                        "Rejected ticket URL (home/away reversed in slug): %s",
                        link,
                    )
                    continue
            # afisha.yandex.{kz,ru}: only accept specific sport EVENT pages with
            # both teams in the slug (home first). Rejects city roots
            # (/aktobe), venue schedules (/sport/places/...), concerts,
            # selections, and the opaque widget (/w/sessions/...) which has no
            # teams to validate against.
            is_afisha = (
                hostname == "afisha.yandex.kz" or hostname.endswith(".afisha.yandex.kz")
                or hostname == "afisha.yandex.ru" or hostname.endswith(".afisha.yandex.ru")
            )
            if is_afisha:
                seg = [p for p in path.split("/") if p]
                if "sport" not in seg or "places" in seg or "selections" in seg:
                    continue
                slug = seg[-1]
                if slug in ("sport", "places", "schedule"):
                    continue
                slug_text = unquote(slug)
                if not _team_matches_text(home_name, slug_text):
                    continue
                if not _team_matches_text(away_name, slug_text):
                    continue
                # Home team must come first in the slug (afisha lists home first).
                # Rejects the mirror fixture, e.g. .../sport/football-astana-aktobe.
                if not _home_before_away_in_slug(home_name, away_name, slug):
                    logger.info(
                        "Rejected afisha URL (home/away reversed in slug): %s", link,
                    )
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
            snippet = result.get("snippet", "")
            title = result.get("title", "")
            match_text = unquote(link) + " " + title + " " + snippet
            if not _team_matches_text(home_name, match_text):
                continue
            if not _team_matches_text(away_name, match_text):
                continue
            # Reject results mentioning a different year than the game
            if game_date and _snippet_has_wrong_year(snippet, title, game_date.year):
                logger.info(
                    "Rejected ticket URL (wrong year in snippet): %s — %s",
                    link, snippet[:120],
                )
                continue
            # Reject futsal/mini-football events
            combined_lower = (link + " " + title + " " + snippet).lower()
            if any(kw in combined_lower for kw in _FUTSAL_KEYWORDS):
                logger.info("Rejected futsal ticket URL: %s — %s", link, title[:100])
                continue
            if link in seen:
                continue
            seen.add(link)
            logger.info("Matched ticket URL: %s (title: %s)", link, title[:100])
            matches.append(TicketMatch(url=link, title=title, snippet=snippet))
        except Exception:
            continue
    return matches


async def _ai_validate_ticket_url(
    url: str,
    title: str,
    snippet: str,
    home_name: str,
    away_name: str,
    game_date: date,
) -> bool:
    """Use Claude Haiku to validate whether a ticket URL is for this specific match."""
    from app.config import get_settings
    settings = get_settings()

    if not settings.anthropic_api_key:
        return True  # Skip AI validation if no API key

    date_str = _format_date_ru(game_date)
    # Build transliteration hints so AI can match Cyrillic team names in Latin URLs
    home_lower = home_name.lower()
    away_lower = away_name.lower()
    home_slugs = _TEAM_SLUG_OVERRIDES.get(home_lower, [_transliterate(home_lower)])
    away_slugs = _TEAM_SLUG_OVERRIDES.get(away_lower, [_transliterate(away_lower)])
    slug_hint = (
        f"Транслитерация: {home_name} = {', '.join(home_slugs)}; "
        f"{away_name} = {', '.join(away_slugs)}"
    )
    prompt = (
        f"Это ссылка на билеты на ФУТБОЛЬНЫЙ матч где {home_name} играет ДОМА против {away_name}, "
        f"дата: {date_str} {game_date.year}?\n\n"
        f"URL: {url}\n"
        f"Заголовок: {title}\n"
        f"Описание: {snippet}\n"
        f"{slug_hint}\n\n"
        "Правила:\n"
        "- Оба названия команд должны быть в URL/заголовке (в любой транслитерации) — иначе НЕТ\n"
        f"- Хозяин поля = первая команда в URL/заголовке. Должна быть {home_name}; если в ссылке хозяин другая — НЕТ\n"
        "- Физический стадион игнорируй: команды могут играть «дома» на нейтральном поле\n"
        "- Если это ФУТЗАЛ/мини-футбол — НЕТ\n"
        "- Если дата не совпадает — НЕТ\n\n"
        "Ответь только 'да' или 'нет'."
    )

    try:
        client = anthropic.AsyncAnthropic(
            api_key=settings.anthropic_api_key,
            max_retries=1,
            timeout=10,
        )
        response = await client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=8,
            messages=[{"role": "user", "content": prompt}],
        )
        answer = response.content[0].text.strip().lower()
        is_valid = answer.startswith("да") or answer.startswith("yes")
        logger.info(
            "AI validation for game ticket (%s vs %s): %s → %s",
            home_name, away_name, url, "accepted" if is_valid else "rejected",
        )
        return is_valid
    except Exception:
        logger.warning("AI ticket validation failed, accepting URL as fallback", exc_info=True)
        return True


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, httpx.TimeoutException):
        return True
    if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code >= 500:
        return True
    return False


class SerperAuthError(Exception):
    """Raised when Serper API returns 401/403 — API key invalid or expired."""
    pass


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception(_is_retryable),
    reraise=True,
)
async def _search_serper(
    query: str, api_key: str, client: httpx.AsyncClient
) -> list[dict]:
    """Search Google via Serper.dev. Returns organic results list."""
    resp = await client.post(
        SERPER_URL,
        headers={"X-API-KEY": api_key, "Content-Type": "application/json"},
        json={"q": query, "gl": "kz", "hl": "ru", "num": 10},
    )
    if resp.status_code in (401, 403):
        body = resp.text[:200]
        logger.error("Serper API auth failed (%s): %s", resp.status_code, body)
        raise SerperAuthError(f"Serper API {resp.status_code}: {body}")
    resp.raise_for_status()
    data = resp.json()
    return data.get("organic", [])


async def _scrape_caption(url: str, api_key: str, client: httpx.AsyncClient) -> str:
    """Deep-read a page's full text via Serper scrape (renders JS).

    Google's organic snippet truncates Instagram captions to the first line, so
    free-entry phrases («Кіру тегін») and ticket links deeper in the caption are
    invisible to snippet-only detection. The scrape endpoint returns the full
    rendered text + og:description. It's slow and flaky for instagram.com
    (JS-heavy, occasional 5xx/timeout), so we retry briefly and degrade to "".
    """
    for _ in range(2):
        try:
            resp = await client.post(
                SERPER_SCRAPE_URL,
                headers={"X-API-KEY": api_key, "Content-Type": "application/json"},
                json={"url": url},
                timeout=35,
            )
            if resp.status_code in (401, 403):
                raise SerperAuthError(f"Serper scrape {resp.status_code}: {resp.text[:200]}")
            if resp.status_code != 200:
                continue
            data = resp.json()
            meta = data.get("metadata") or {}
            parts = [str(meta.get("og:description") or ""), data.get("text") or ""]
            caption = "\n".join(p for p in parts if p).strip()
            if caption:
                return caption
        except SerperAuthError:
            raise
        except Exception:
            continue
    return ""


async def _enrich_with_instagram_captions(
    all_organic: list[dict],
    home_name: str,
    api_key: str,
    client: httpx.AsyncClient,
    max_posts: int,
) -> list[dict]:
    """Scrape full captions of the most relevant Instagram posts and return them
    as synthetic result dicts (caption as snippet, plus any embedded URLs as
    separate results so ticket extraction sees links buried in the caption).

    Only posts whose short snippet already mentions the home team are scraped,
    to bound Serper credit spend.
    """
    candidates: list[str] = []
    for r in all_organic:
        link = r.get("link", "") or ""
        if "instagram.com/p/" not in link and "instagram.com/reel/" not in link:
            continue
        if link in candidates:
            continue
        blurb = f"{r.get('title', '')} {r.get('snippet', '')}"
        if not _team_matches_text(home_name, blurb):
            continue
        candidates.append(link)
        if len(candidates) >= max_posts:
            break

    enriched: list[dict] = []
    for link in candidates:
        caption = await _scrape_caption(link, api_key, client)
        await asyncio.sleep(1.0)  # rate limit between scrape calls
        if not caption:
            continue
        logger.info("Scraped IG caption for %s (%d chars)", link, len(caption))
        # The caption itself — feeds free-entry detection + team/date presence.
        enriched.append({"link": link, "title": "", "snippet": caption})
        # Any ticket links embedded in the caption — feeds ticket extraction.
        for u in set(re.findall(r'https?://[^\s"\'<>]+', caption)):
            enriched.append({"link": u.rstrip('",);'), "title": "", "snippet": caption})
    return enriched


def _build_telegram_query(home_name: str, away_name: str) -> str:
    """Build a Google search to discover the club's Telegram channel.

    No date — Google rarely indexes fresh posts, but the channel root is stable
    and its live t.me/s/ feed always shows the latest posts once discovered.
    """
    return f"t.me {home_name} {away_name}"


# Extract the channel handle from a t.me link (handles /s/ feed form and ?query).
_TELEGRAM_LINK_RE = re.compile(
    r"t\.me/(?:s/)?([A-Za-z0-9_]{4,32})", re.IGNORECASE
)
# Pull individual messages (post id + text) out of a t.me/s/<channel> feed page.
_TG_MESSAGE_RE = re.compile(
    r'data-post="([^"]+)".*?'
    r'tgme_widget_message_text[^>]*>(.*?)</div>',
    re.IGNORECASE | re.DOTALL,
)


def _strip_html(fragment: str) -> str:
    """Convert a Telegram message HTML fragment to plain text (keep <a> hrefs)."""
    import html as _html
    # Surface link targets so embedded ticket URLs survive tag stripping
    fragment = re.sub(r'<a[^>]*href="([^"]+)"[^>]*>', r" \1 ", fragment)
    fragment = re.sub(r"<br\s*/?>", "\n", fragment)
    return _html.unescape(re.sub(r"<[^>]+>", " ", fragment)).strip()


async def _scrape_telegram_channel(
    channel: str, client: httpx.AsyncClient
) -> list[tuple[str, str]]:
    """Fetch the public t.me/s/<channel> feed → list of (post_url, plain_text).

    Public Telegram channels render their latest ~20 posts (full text + links)
    without auth, so a plain GET is reliable — unlike Instagram, which blocks
    scraping. Free-entry phrases and ticket links in club posts live here.
    """
    try:
        resp = await client.get(
            f"https://t.me/s/{channel}",
            follow_redirects=True,
            timeout=15,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        if resp.status_code != 200:
            return []
        out: list[tuple[str, str]] = []
        for post_id, frag in _TG_MESSAGE_RE.findall(resp.text):
            text = _strip_html(frag)
            if text:
                out.append((f"https://t.me/{post_id}", text))
        return out
    except Exception:
        return []


async def _enrich_with_telegram(
    all_organic: list[dict],
    home_name: str,
    away_name: str,
    serper_key: str,
    client: httpx.AsyncClient,
    game_date: date,
    max_channels: int,
) -> list[dict]:
    """Discover the club's Telegram channel and return posts mentioning BOTH
    teams as synthetic result dicts (text as snippet + embedded URLs as results).
    """
    # Discover channels: a dedicated t.me search + any t.me links already present.
    channels: list[str] = []

    def _collect(text: str) -> None:
        for handle in _TELEGRAM_LINK_RE.findall(text or ""):
            h = handle.lower()
            if h in ("s", "share", "joinchat", "addemoji", "iv") or h in channels:
                continue
            channels.append(h)

    try:
        tg_query = _build_telegram_query(home_name, away_name)
        tg_results = await _search_serper(tg_query, serper_key, client)
        await asyncio.sleep(1.0)
        for r in tg_results:
            _collect(r.get("link", ""))
    except SerperAuthError:
        raise
    except Exception:
        logger.warning("Telegram discovery search failed", exc_info=True)
    for r in all_organic:
        _collect(r.get("link", ""))

    enriched: list[dict] = []
    for channel in channels[:max_channels]:
        posts = await _scrape_telegram_channel(channel, client)
        for post_url, text in posts:
            # A club's own channel names only the OPPONENT + city for a home game
            # (it omits its own name), so requiring both teams is too strict.
            # Anchor on the match DATE + at least one team — strong enough given
            # the channel was surfaced by a "<home> <away>" search.
            if not _snippet_mentions_date(text.lower(), game_date):
                continue
            if not (
                _team_matches_text(home_name, text)
                or _team_matches_text(away_name, text)
            ):
                continue
            logger.info("Telegram post matched fixture (%s): %s", channel, post_url)
            # Prepend both team names so downstream home/away presence checks pass
            # regardless of which side the club named; real text follows verbatim
            # (preserves free-entry phrases + embedded ticket URLs).
            snippet = f"{home_name} {away_name}\n{text}"
            enriched.append({"link": post_url, "title": "", "snippet": snippet})
            for u in set(re.findall(r'https?://[^\s"\'<>]+', text)):
                enriched.append({"link": u.rstrip('",);'), "title": "", "snippet": snippet})
    return enriched


async def search_and_update_tickets(db: AsyncSession) -> dict:
    """Search for ticket URLs for upcoming games and store results."""
    from app.config import get_settings
    settings = get_settings()

    if not settings.ticket_search_enabled:
        return {"skipped": True, "reason": "ticket search disabled"}

    if not settings.serper_api_key:
        return {"skipped": True, "reason": "serper_api_key not set"}

    today = date.today()
    # +5/+3 — найти билеты заранее; +2/+1/+0 — догнать матчи, по которым ссылка
    # появилась только в последний момент (часто так с виджетами Яндекс.Афиши,
    # Ticketon новые события создаёт за день/в день матча)
    search_dates = {
        today + timedelta(days=5),
        today + timedelta(days=3),
        today + timedelta(days=2),
        today + timedelta(days=1),
        today,
    }

    result = await db.execute(
        select(Game)
        .join(Season, Game.season_id == Season.id)
        .options(selectinload(Game.home_team), selectinload(Game.away_team))
        .where(
            Game.date.in_(search_dates),
            Game.status == GameStatus.created,
            Game.ticket_url.is_(None),
            Game.is_free_entry.is_(False),
            Season.championship_id.in_(_TICKET_CHAMPIONSHIP_IDS),
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
            fetched_at = ensure_utc(game.ticket_url_fetched_at)
            if fetched_at and fetched_at.date() == today:
                skipped += 1
                continue

            home_team = game.home_team
            away_team = game.away_team
            if not home_team or not away_team:
                skipped += 1
                continue

            try:
                query = _build_search_query(home_team.name, away_team.name, game.date)
                organic = await _search_serper(query, settings.serper_api_key, client)

                # Also search team Instagram posts for ticket/free entry info
                ig_query = _build_instagram_query(home_team.name, away_team.name, game.date)
                await asyncio.sleep(1.0)  # rate limit between requests
                ig_organic = await _search_serper(ig_query, settings.serper_api_key, client)

                # Also search the indexed Yandex.Afisha event page — catches matches
                # sold only via the embedded Afisha widget (opaque, non-indexed URL).
                afisha_query = _build_afisha_query(home_team.name, away_team.name)
                await asyncio.sleep(1.0)  # rate limit between requests
                afisha_organic = await _search_serper(
                    afisha_query, settings.serper_api_key, client
                )

                # Merge results: general search + Instagram + Afisha event page
                all_organic = organic + ig_organic + afisha_organic

                # Deep-read full Instagram captions — Google snippets truncate the
                # free-entry phrase / ticket link that lives deeper in the caption.
                if settings.ticket_ig_scrape_enabled:
                    enriched = await _enrich_with_instagram_captions(
                        all_organic, home_team.name, settings.serper_api_key,
                        client, settings.ticket_ig_scrape_max_posts,
                    )
                    all_organic = all_organic + enriched

                # Deep-read the club's Telegram channel — many clubs post free-entry
                # and ticket info there only; t.me/s/ is reliably scrapable.
                if settings.ticket_telegram_scrape_enabled:
                    tg_enriched = await _enrich_with_telegram(
                        all_organic, home_team.name, away_team.name,
                        settings.serper_api_key, client, game.date,
                        settings.ticket_telegram_scrape_max_channels,
                    )
                    all_organic = all_organic + tg_enriched

                # Check for free entry in all results
                is_free = _detect_free_entry(all_organic, home_team.name, game.date)

                # If search didn't detect free entry, check home team's website
                if not is_free and home_team.website:
                    is_free = await _check_team_website_free_entry(
                        home_team.website, home_team.name, away_team.name, client,
                    )

                if is_free:
                    game.is_free_entry = True
                    free_entry += 1
                    logger.info(
                        "Detected free entry for game %s (%s vs %s)",
                        game.id, home_team.name, away_team.name,
                    )
                    await send_telegram_message(
                        "\U0001f3df Свободный вход\n\n"
                        f"\u26bd Матч: {home_team.name} — {away_team.name}\n"
                        f"\U0001f4c5 Дата: {game.date}"
                    )
                else:
                    candidates = _extract_ticket_urls(
                        all_organic, home_team.name, away_team.name, game.date,
                    )
                    # AI-validate candidates in result order; take the first accepted.
                    # Rejecting one must not abort the search — keep trying the rest.
                    match = None
                    # cap AI checks per game — Serper rarely returns >a few real ticket pages
                    for cand in candidates[:6]:
                        is_valid = await _ai_validate_ticket_url(
                            cand.url, cand.title, cand.snippet,
                            home_team.name, away_team.name, game.date,
                        )
                        if is_valid:
                            match = cand
                            break
                        logger.info(
                            "AI rejected ticket URL for game %s: %s",
                            game.id, cand.url,
                        )
                    # Fallback: no search candidate — scan the club's own site for
                    # its ticket portal (catches clubs selling only via tickets.<club>).
                    if match is None and home_team.website:
                        portal_url = await _find_ticket_url_on_website(
                            home_team.website, home_team.name, away_team.name,
                            game.date, client,
                        )
                        if portal_url:
                            game.ticket_url = portal_url
                            updated += 1
                            logger.info(
                                "Found club ticket portal for game %s (%s vs %s): %s",
                                game.id, home_team.name, away_team.name, portal_url,
                            )
                            await send_telegram_message(
                                "\U0001f3df Билеты найдены (сайт клуба)\n\n"
                                f"⚽ Матч: {home_team.name} — {away_team.name}\n"
                                f"\U0001f4c5 Дата: {game.date}\n"
                                f'\U0001f517 Ссылка: <a href="{portal_url}">Купить билеты</a>'
                            )
                            game.ticket_url_fetched_at = utcnow()
                            searched += 1
                            await asyncio.sleep(1.0)
                            continue
                    if match is None and candidates and not game.ticket_url_fetched_at:
                        # All candidates rejected — notify once (avoid spam every 3h)
                        first = candidates[0]
                        await send_telegram_message(
                            "\u274c <b>AI отверг билет</b>\n\n"
                            f"\u26bd Матч: {home_team.name} — {away_team.name}\n"
                            f"\U0001f4c5 Дата: {game.date}\n"
                            f"\U0001f517 URL: {first.url}\n"
                            f"\U0001f4dd {first.title}\n"
                            f"(проверено кандидатов: {len(candidates)})"
                        )
                    if match:
                        game.ticket_url = match.url
                        updated += 1
                        logger.info(
                            "Found ticket URL for game %s (%s vs %s): %s",
                            game.id, home_team.name, away_team.name, match.url,
                        )
                        await send_telegram_message(
                            "\U0001f3df Билеты найдены\n\n"
                            f"\u26bd Матч: {home_team.name} — {away_team.name}\n"
                            f"\U0001f4c5 Дата: {game.date}\n"
                            f'\U0001f517 Ссылка: <a href="{match.url}">Купить билеты</a>'
                        )

                game.ticket_url_fetched_at = utcnow()
                searched += 1

                # Rate limit: Serper allows ~1 req/sec
                await asyncio.sleep(1.0)

            except SerperAuthError as e:
                logger.error(
                    "Serper API key invalid — aborting ticket search for all remaining games"
                )
                await send_telegram_message(
                    "\u26a0\ufe0f <b>Serper API сломан</b>\n\n"
                    f"Ошибка: {e}\n"
                    "Поиск билетов остановлен. Нужно обновить API ключ."
                )
                errors += len(games) - searched - skipped
                break
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
