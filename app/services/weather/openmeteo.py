"""Open-Meteo provider — geocoding + hourly forecast.

Used for upcoming matches outside the METAR window (>24h before kickoff)
and as a fallback when METAR is unavailable.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, time

import httpx
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

# WMO weather code → condition key (matches keys in `format.WEATHER_CONDITIONS`).
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

# Bounded geocoding cache: city_name → (lat, lon) | None. Max 128 entries.
_geocode_cache: dict[str, tuple[float, float] | None] = {}
_GEOCODE_CACHE_MAX = 128


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
async def geocode_city(city: str, client: httpx.AsyncClient) -> tuple[float, float] | None:
    """Geocode a city name via Open-Meteo geocoding API. Returns (lat, lon) or None."""
    if city in _geocode_cache:
        return _geocode_cache[city]

    resp = await client.get(
        "https://geocoding-api.open-meteo.com/v1/search",
        params={"name": city, "count": 1, "language": "en"},
    )
    resp.raise_for_status()
    data = resp.json()
    results = data.get("results")

    if len(_geocode_cache) >= _GEOCODE_CACHE_MAX:
        _geocode_cache.clear()

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
async def fetch_forecast(
    lat: float,
    lon: float,
    game_date: date,
    game_time: time | None,
    client: httpx.AsyncClient,
) -> tuple[int, str] | None:
    """Fetch weather from Open-Meteo for the given match's start hour.

    Returns (temp_celsius, condition_key) or None if no slot found.
    """
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

    target_hour = game_time.hour if game_time else 15
    target = datetime.combine(game_date, time(target_hour, 0))

    best_idx: int | None = None
    best_diff: float | None = None
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
