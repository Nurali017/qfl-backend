"""Weather service — fetches weather from Open-Meteo (free, no API key)."""

import logging
from datetime import datetime, date, time, timedelta

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models import Game, GameStatus, Stadium
from app.utils.timestamps import ensure_utc, utcnow

logger = logging.getLogger(__name__)

# WMO weather code → condition key
_WMO_TO_CONDITION: dict[int, str] = {
    0: "clear",
    1: "clear", 2: "clouds", 3: "clouds",
    45: "fog", 48: "fog",
    51: "drizzle", 53: "drizzle", 55: "drizzle",
    56: "drizzle", 57: "drizzle",
    61: "rain", 63: "rain", 65: "rain",
    66: "rain", 67: "rain",
    71: "snow", 73: "snow", 75: "snow", 77: "snow",
    80: "rain", 81: "rain", 82: "rain",
    85: "snow", 86: "snow",
    95: "thunderstorm", 96: "thunderstorm", 99: "thunderstorm",
}

# Condition key → localized labels
WEATHER_CONDITIONS: dict[str, dict[str, str]] = {
    "clear": {"kz": "Ашық", "ru": "Ясно", "en": "Clear"},
    "clouds": {"kz": "Бұлтты", "ru": "Облачно", "en": "Cloudy"},
    "rain": {"kz": "Жаңбырлы", "ru": "Дождь", "en": "Rain"},
    "drizzle": {"kz": "Бүркіт жаңбыр", "ru": "Морось", "en": "Drizzle"},
    "thunderstorm": {"kz": "Найзағай", "ru": "Гроза", "en": "Thunderstorm"},
    "snow": {"kz": "Қарлы", "ru": "Снег", "en": "Snow"},
    "fog": {"kz": "Тұманды", "ru": "Туман", "en": "Fog"},
}

# In-memory geocoding cache: city_name → (lat, lon)
_geocode_cache: dict[str, tuple[float, float] | None] = {}


def format_weather(temp: int | None, condition: str | None, lang: str) -> str | None:
    """Format weather string like '+15°C, Ясно'. Returns None if no data."""
    if temp is None or condition is None:
        return None
    sign = "+" if temp > 0 else ""
    labels = WEATHER_CONDITIONS.get(condition, {})
    label = labels.get(lang, labels.get("en", condition))
    return f"{sign}{temp}°C, {label}"


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
async def _geocode_city(city: str, client: httpx.AsyncClient) -> tuple[float, float] | None:
    """Geocode city name via Open-Meteo geocoding API. Returns (lat, lon) or None."""
    if city in _geocode_cache:
        return _geocode_cache[city]

    resp = await client.get(
        "https://geocoding-api.open-meteo.com/v1/search",
        params={"name": city, "count": 1, "language": "en"},
    )
    resp.raise_for_status()
    data = resp.json()
    results = data.get("results")
    if not results:
        _geocode_cache[city] = None
        return None

    lat, lon = results[0]["latitude"], results[0]["longitude"]
    _geocode_cache[city] = (lat, lon)
    return lat, lon


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception(_is_retryable),
    reraise=True,
)
async def fetch_weather(
    lat: float, lon: float, game_date: date, game_time: time | None,
    client: httpx.AsyncClient,
) -> tuple[int, str] | None:
    """Fetch weather from Open-Meteo. Returns (temp_celsius, condition_key) or None."""
    resp = await client.get(
        "https://api.open-meteo.com/v1/forecast",
        params={
            "latitude": lat,
            "longitude": lon,
            "hourly": "temperature_2m,weather_code",
            "forecast_days": 16,
            "timezone": "Asia/Almaty",
        },
    )
    resp.raise_for_status()
    data = resp.json()

    hourly = data.get("hourly", {})
    times = hourly.get("time", [])
    temps = hourly.get("temperature_2m", [])
    codes = hourly.get("weather_code", [])

    if not times:
        return None

    # Build target datetime
    target_hour = game_time.hour if game_time else 15
    target = datetime.combine(game_date, time(target_hour, 0))

    # Find closest hourly slot
    best_idx = None
    best_diff = None
    for i, t_str in enumerate(times):
        dt = datetime.fromisoformat(t_str)
        diff = abs((dt - target).total_seconds())
        if best_diff is None or diff < best_diff:
            best_diff = diff
            best_idx = i

    if best_idx is not None and best_idx < len(temps) and best_idx < len(codes):
        temp = round(temps[best_idx])
        wmo_code = codes[best_idx]
        condition = _WMO_TO_CONDITION.get(wmo_code, "clouds")
        return temp, condition

    return None


async def fetch_and_update_weather(db: AsyncSession) -> dict:
    """Fetch weather for upcoming games and store results."""
    from app.config import get_settings
    settings = get_settings()

    if not settings.weather_enabled:
        return {"skipped": True, "reason": "weather disabled"}

    today = date.today()
    cutoff = today + timedelta(days=16)
    three_hours_ago = utcnow() - timedelta(hours=3)

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

    updated = 0
    errors = 0
    skipped = 0

    async with httpx.AsyncClient(timeout=10) as client:
        for game in games:
            weather_fetched_at = ensure_utc(game.weather_fetched_at)
            if weather_fetched_at and weather_fetched_at > three_hours_ago:
                skipped += 1
                continue

            stadium: Stadium | None = game.stadium_rel
            if not stadium:
                skipped += 1
                continue

            city = stadium.city_en or stadium.city
            if not city:
                skipped += 1
                continue

            try:
                coords = await _geocode_city(city, client)
                if not coords:
                    logger.warning("Geocoding failed for city=%s (game %s)", city, game.id)
                    skipped += 1
                    continue

                lat, lon = coords
                weather = await fetch_weather(lat, lon, game.date, game.time, client)
                if weather:
                    temp, condition = weather
                    game.weather_temp = temp
                    game.weather_condition = condition
                    game.weather_fetched_at = utcnow()
                    updated += 1
                else:
                    skipped += 1
            except Exception:
                logger.warning("Weather fetch failed for game %s, city=%s", game.id, city, exc_info=True)
                errors += 1

    return {"updated": updated, "skipped": skipped, "errors": errors}
