"""Weather service — orchestrates METAR observations + Open-Meteo forecasts.

Strategy:
  - Live or imminent (≤24h to kickoff) games → METAR via the city's nearest
    airport. METAR is real, observed weather and tracks fronts that forecast
    models miss.
  - Upcoming games further out → Open-Meteo hourly forecast.
  - METAR failures fall back to Open-Meteo automatically.

Public API (kept stable for callers):
  * :func:`format_weather` — render the temperature+condition string.
  * :func:`fetch_and_update_weather` — beat task entry, refreshes upcoming + live.
  * :func:`fetch_and_update_live_weather` — fast loop entry, only live games.
  * :data:`WEATHER_CONDITIONS` — localized labels per condition key.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.data.icao_mapping import icao_for_city
from app.models import Game, GameStatus, Stadium
from app.utils.timestamps import ensure_utc, utcnow

from .metar import fetch_metar
from .openmeteo import fetch_forecast, geocode_city

logger = logging.getLogger(__name__)

# Condition key → localized labels (kept here as the canonical source).
WEATHER_CONDITIONS: dict[str, dict[str, str]] = {
    "clear": {"kz": "Ашық", "ru": "Ясно", "en": "Clear"},
    "clouds": {"kz": "Бұлтты", "ru": "Облачно", "en": "Cloudy"},
    "rain": {"kz": "Жаңбырлы", "ru": "Дождь", "en": "Rain"},
    "drizzle": {"kz": "Бүркіт жаңбыр", "ru": "Морось", "en": "Drizzle"},
    "thunderstorm": {"kz": "Найзағай", "ru": "Гроза", "en": "Thunderstorm"},
    "snow": {"kz": "Қарлы", "ru": "Снег", "en": "Snow"},
    "fog": {"kz": "Тұманды", "ru": "Туман", "en": "Fog"},
}

# How "live or imminent" is defined — game starts within this delta from now.
_METAR_WINDOW = timedelta(hours=24)
# Stale threshold for the periodic 3h beat (skip if updated within this window).
_FORECAST_FRESH_FOR = timedelta(hours=3)
# Stale threshold for the live 15min beat (skip if updated within this window).
_LIVE_FRESH_FOR = timedelta(minutes=10)


def format_weather(temp: int | None, condition: str | None, lang: str) -> str | None:
    """Format weather string like ``+15°C, Ясно``. Returns None if no data."""
    if temp is None or condition is None:
        return None
    sign = "+" if temp > 0 else ""
    labels = WEATHER_CONDITIONS.get(condition, {})
    label = labels.get(lang, labels.get("en", condition))
    return f"{sign}{temp}°C, {label}"


def _city_for_stadium(stadium: Stadium) -> str | None:
    """Pick the most informative city name from the stadium row."""
    return stadium.city_en or stadium.city or stadium.city_ru or stadium.city_kz


def _kickoff_at(game: Game) -> datetime | None:
    """Return the game kickoff as a naive UTC datetime (``date`` + ``time``)."""
    if not game.date or not game.time:
        return None
    # Game date/time is local Asia/Almaty; we just need a rough magnitude for
    # the 24h window check, so naive comparison via UTC offset of +5 is fine.
    local_dt = datetime.combine(game.date, game.time)
    return local_dt - timedelta(hours=5)  # Asia/Almaty → UTC


def _within_metar_window(game: Game, now_utc: datetime) -> bool:
    kickoff = _kickoff_at(game)
    if kickoff is None:
        return False
    delta = abs(kickoff - now_utc)
    return delta <= _METAR_WINDOW


async def _refresh_one(
    game: Game,
    *,
    client: httpx.AsyncClient,
    now_utc: datetime,
    prefer_metar: bool,
) -> str:
    """Refresh weather for a single game; returns 'updated' | 'skipped' | 'error'."""
    stadium: Stadium | None = game.stadium_rel
    if not stadium:
        return "skipped"

    city = _city_for_stadium(stadium)
    if not city:
        return "skipped"

    weather: tuple[int, str] | None = None
    source: str | None = None

    if prefer_metar:
        icao = icao_for_city(city) or icao_for_city(stadium.city or "")
        if icao:
            try:
                weather = await fetch_metar(icao, client)
                if weather:
                    source = f"metar:{icao}"
            except Exception:
                logger.warning(
                    "METAR fetch failed for game %s (icao=%s)", game.id, icao,
                    exc_info=True,
                )

    if weather is None:
        try:
            coords = await geocode_city(city, client)
            if not coords:
                logger.warning("Geocoding failed for city=%s (game %s)", city, game.id)
                return "skipped"
            lat, lon = coords
            weather = await fetch_forecast(lat, lon, game.date, game.time, client)
            if weather:
                source = "open-meteo"
        except Exception:
            logger.warning(
                "Forecast fetch failed for game %s, city=%s", game.id, city,
                exc_info=True,
            )
            return "error"

    if weather is None:
        return "skipped"

    temp, condition = weather
    game.weather_temp = temp
    game.weather_condition = condition
    game.weather_fetched_at = utcnow()
    logger.debug(
        "Weather updated game=%s city=%s source=%s temp=%s condition=%s",
        game.id, city, source, temp, condition,
    )
    return "updated"


async def fetch_and_update_weather(db: AsyncSession) -> dict:
    """Refresh weather for upcoming and live games (3h beat).

    For games inside the 24h kickoff window, METAR is preferred and
    falls back to Open-Meteo forecast. Outside the window, Open-Meteo
    forecast is used directly. Skips games whose row was updated less
    than 3h ago.
    """
    from app.config import get_settings
    settings = get_settings()

    if not settings.weather_enabled:
        return {"skipped": True, "reason": "weather disabled"}

    today = date.today()
    cutoff = today + timedelta(days=16)
    now_utc = datetime.now(timezone.utc)
    fresh_threshold = now_utc - _FORECAST_FRESH_FOR

    result = await db.execute(
        select(Game)
        .options(selectinload(Game.stadium_rel))
        .where(
            Game.date >= today,
            Game.date <= cutoff,
            Game.status.in_([GameStatus.created, GameStatus.live]),
            Game.stadium_id.isnot(None),
        )
    )
    games = result.scalars().all()

    counts = {"updated": 0, "skipped": 0, "errors": 0}

    async with httpx.AsyncClient(timeout=10) as client:
        for game in games:
            fetched = ensure_utc(game.weather_fetched_at)
            # Always allow live games to refresh on this beat too.
            is_live = game.status == GameStatus.live
            if not is_live and fetched and fetched > fresh_threshold:
                counts["skipped"] += 1
                continue

            outcome = await _refresh_one(
                game,
                client=client,
                now_utc=now_utc,
                prefer_metar=is_live or _within_metar_window(game, now_utc),
            )
            if outcome == "updated":
                counts["updated"] += 1
            elif outcome == "error":
                counts["errors"] += 1
            else:
                counts["skipped"] += 1

    return counts


async def fetch_and_update_live_weather(db: AsyncSession) -> dict:
    """Refresh weather for live games only (15min beat) using METAR first."""
    from app.config import get_settings
    settings = get_settings()

    if not settings.weather_enabled:
        return {"skipped": True, "reason": "weather disabled"}

    now_utc = datetime.now(timezone.utc)
    fresh_threshold = now_utc - _LIVE_FRESH_FOR

    result = await db.execute(
        select(Game)
        .options(selectinload(Game.stadium_rel))
        .where(
            Game.status == GameStatus.live,
            Game.stadium_id.isnot(None),
        )
    )
    games = result.scalars().all()

    counts = {"updated": 0, "skipped": 0, "errors": 0}

    async with httpx.AsyncClient(timeout=10) as client:
        for game in games:
            fetched = ensure_utc(game.weather_fetched_at)
            if fetched and fetched > fresh_threshold:
                counts["skipped"] += 1
                continue

            outcome = await _refresh_one(
                game, client=client, now_utc=now_utc, prefer_metar=True,
            )
            if outcome == "updated":
                counts["updated"] += 1
            elif outcome == "error":
                counts["errors"] += 1
            else:
                counts["skipped"] += 1

    return counts


__all__ = [
    "WEATHER_CONDITIONS",
    "fetch_and_update_live_weather",
    "fetch_and_update_weather",
    "format_weather",
]
