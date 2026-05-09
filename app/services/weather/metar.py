"""METAR provider — fetches actual airport weather observations.

Source: NOAA aviationweather.gov (free, no key, JSON). METAR is an ICAO
standard reported by aerodromes every 30 minutes (or sooner via SPECI).
This is observation data, not a forecast — much more accurate for live
matches than any model-based provider when a major airport is nearby.
"""

from __future__ import annotations

import logging

import httpx
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

_METAR_URL = "https://aviationweather.gov/api/data/metar"


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, httpx.TimeoutException):
        return True
    if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code >= 500:
        return True
    return False


def parse_condition(wx_string: str | None, cover: str | None) -> str:
    """Map a METAR `wxString` and cloud `cover` to our condition key.

    METAR wxString examples:
      - ``""`` / ``None``   — no significant phenomenon (use cloud cover)
      - ``"-RA"``           — light rain
      - ``"+SHRA"``         — heavy rain shower
      - ``"-SHRA VCTS"``    — light rain shower, thunderstorm in vicinity
      - ``"TSRA"``          — thunderstorm with rain
      - ``"+TSRAGR"``       — heavy thunderstorm with rain and hail
      - ``"-SN"``, ``"BLSN"`` — snow / blowing snow
      - ``"FG"``, ``"BR"``  — fog / mist
      - ``"VCSH"``          — showers in vicinity

    Cloud cover (``cover`` in JSON / cloud groups SKC, CLR, NSC, FEW, SCT, BKN, OVC):
      - SKC/CLR/NSC/NCD     — clear sky
      - FEW/SCT             — partly clear, few/scattered (treat as clear)
      - BKN/OVC             — broken/overcast (treat as clouds)

    Priority: thunderstorm > snow > rain/drizzle > fog > clouds/clear.
    """
    wx = (wx_string or "").upper()

    if "TS" in wx:
        return "thunderstorm"
    if "SN" in wx or "SG" in wx or "PL" in wx:
        return "snow"
    if "RA" in wx or "SH" in wx:
        return "rain"
    if "DZ" in wx:
        return "drizzle"
    if "FG" in wx or "BR" in wx or "HZ" in wx:
        return "fog"

    cov = (cover or "").upper()
    if cov in {"SKC", "CLR", "NSC", "NCD", "CAVOK", "FEW", "SCT"}:
        return "clear"
    if cov in {"BKN", "OVC"}:
        return "clouds"
    return "clouds"


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception(_is_retryable),
    reraise=True,
)
async def fetch_metar(icao: str, client: httpx.AsyncClient) -> tuple[int, str] | None:
    """Fetch the most recent METAR for an ICAO airport.

    Returns (temp_celsius, condition_key) or None if no usable observation.
    """
    resp = await client.get(
        _METAR_URL,
        params={"ids": icao, "format": "json", "hours": 2},
    )
    resp.raise_for_status()
    data = resp.json()

    if not data:
        return None

    # The API returns reports newest-first; take the first one with usable temp.
    for report in data:
        temp = report.get("temp")
        if temp is None:
            continue
        wx = report.get("wxString")
        cover = report.get("cover")
        condition = parse_condition(wx, cover)
        return int(round(temp)), condition

    return None
