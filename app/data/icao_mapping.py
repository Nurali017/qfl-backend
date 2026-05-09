"""City → ICAO airport code mapping for weather (METAR) lookup.

Used to map a stadium city to the nearest international/regional airport
that publishes METAR observations. Coverage is intentionally conservative:
only Kazakhstan cities with confirmed major airports are included.
Cities not in this map fall back to Open-Meteo forecast in the orchestrator.

Keys are lowercased; the helper :func:`icao_for_city` accepts any case.
Multiple aliases (KZ/RU/EN spellings) point to the same ICAO code.
"""

from __future__ import annotations

# Lowercased city alias → ICAO code.
_ICAO_BY_CITY: dict[str, str] = {
    # Astana / Nur-Sultan — UACC
    "астана": "UACC",
    "astana": "UACC",
    "нур-султан": "UACC",
    "нурсултан": "UACC",
    # Almaty — UAAA
    "алматы": "UAAA",
    "almaty": "UAAA",
    "алма-ата": "UAAA",
    "алма ата": "UAAA",
    # Aktobe — UATT
    "ақтөбе": "UATT",
    "актобе": "UATT",
    "aktobe": "UATT",
    # Atyrau — UATG
    "атырау": "UATG",
    "atyrau": "UATG",
    # Aktau — UATE
    "ақтау": "UATE",
    "актау": "UATE",
    "aktau": "UATE",
    # Karaganda — UAKK (Sary-Arka)
    "қарағанды": "UAKK",
    "караганда": "UAKK",
    "karaganda": "UAKK",
    "qaraghandy": "UAKK",
    # Kostanay — UAUU
    "қостанай": "UAUU",
    "костанай": "UAUU",
    "kostanay": "UAUU",
    # Pavlodar — UASP
    "павлодар": "UASP",
    "pavlodar": "UASP",
    # Petropavl — UACP
    "петропавл": "UACP",
    "петропавловск": "UACP",
    "petropavl": "UACP",
    "petropavlovsk": "UACP",
    # Semey — UASS
    "семей": "UASS",
    "semey": "UASS",
    # Shymkent — UAII
    "шымкент": "UAII",
    "shymkent": "UAII",
    "chimkent": "UAII",
    # Taraz — UAAT
    "тараз": "UAAT",
    "taraz": "UAAT",
    # Oral / Uralsk — UARR
    "орал": "UARR",
    "уральск": "UARR",
    "oral": "UARR",
    "uralsk": "UARR",
    # Oskemen / Ust-Kamenogorsk — UASK
    "өскемен": "UASK",
    "усть-каменогорск": "UASK",
    "oskemen": "UASK",
    "ust-kamenogorsk": "UASK",
    # Kokshetau — UACK
    "көкшетау": "UACK",
    "кокшетау": "UACK",
    "kokshetau": "UACK",
    # Kyzylorda — UAOO (Korkyt-Ata)
    "қызылорда": "UAOO",
    "кызылорда": "UAOO",
    "kyzylorda": "UAOO",
}


def icao_for_city(city: str | None) -> str | None:
    """Return ICAO code for a Kazakhstani city, or None if not mapped."""
    if not city:
        return None
    return _ICAO_BY_CITY.get(city.strip().lower())
