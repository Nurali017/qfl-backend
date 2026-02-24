from __future__ import annotations

import re
from typing import Any


_TEAM_TRANSLATION_TABLE = str.maketrans(
    {
        "ё": "е",
        "ә": "а",
        "ғ": "г",
        "қ": "к",
        "ң": "н",
        "ө": "о",
        "ұ": "у",
        "ү": "у",
        "һ": "х",
        "і": "и",
        "й": "и",
    }
)
_WHITESPACE_RE = re.compile(r"\s+")
_PUNCT_RE = re.compile(r"[^\w\s]+", re.UNICODE)
_TEAM_LOGO_ROUTE_TEMPLATE = "/api/v1/files/teams/{slug}/logo"

# Canonical slug mapping by internal team id (covers current PL set).
_TEAM_LOGO_SLUG_BY_ID: dict[int, str] = {
    13: "kairat",
    45: "zhetysu",
    46: "shakhter",
    49: "atyrau",
    51: "aktobe",
    80: "turan",
    81: "ordabasy",
    87: "kyzylzhar",
    90: "tobol",
    91: "astana",
    92: "zhenis",
    93: "elimai",
    94: "kaysar",
    293: "ulytau",
    318: "okzhetpes",
}

_TEAM_LOGO_ALIASES: dict[str, tuple[str, ...]] = {
    "kairat": ("kairat", "кайрат", "қайрат", "kairat a", "кайрат а"),
    "astana": ("astana", "астана"),
    "tobol": ("tobol", "тобол", "тобыл"),
    "elimai": ("elimai", "елимай"),
    "aktobe": ("aktobe", "актобе"),
    "zhenis": ("zhenis", "zhenis", "женис", "женис"),
    "ordabasy": ("ordabasy", "ордабасы"),
    "okzhetpes": ("okzhetpes", "окжетпес", "өкжетпес"),
    "kyzylzhar": ("kyzylzhar", "qyzyljar", "кызылжар", "қызылжар"),
    "ulytau": ("ulytau", "ұлытау", "улытау"),
    "kaysar": ("kaysar", "кайсар", "қайсар"),
    "zhetysu": ("zhetysu", "jetisu", "жетысу", "жетісу", "жетису"),
    "atyrau": ("atyrau", "атырау"),
    "turan": ("turan", "туран"),
    "shakhter": ("shakhter", "шахтер", "шахтёр"),
}


def _normalize_team_name(value: str | None) -> str:
    if not value:
        return ""
    normalized = value.casefold().translate(_TEAM_TRANSLATION_TABLE)
    normalized = _PUNCT_RE.sub(" ", normalized)
    normalized = _WHITESPACE_RE.sub(" ", normalized).strip()

    tokens = normalized.split()
    if tokens and tokens[0] in {"fc", "fk", "фк"}:
        tokens = tokens[1:]
    return " ".join(tokens)


def _build_alias_lookup() -> dict[str, str]:
    aliases: dict[str, str] = {}
    for slug, values in _TEAM_LOGO_ALIASES.items():
        for value in values:
            normalized = _normalize_team_name(value)
            if normalized:
                aliases[normalized] = slug
    return aliases


_SLUG_BY_NORMALIZED_ALIAS = _build_alias_lookup()


def resolve_team_logo_slug(team: Any | None) -> str | None:
    if team is None:
        return None

    team_id = getattr(team, "id", None)
    if isinstance(team_id, int):
        slug_by_id = _TEAM_LOGO_SLUG_BY_ID.get(team_id)
        if slug_by_id:
            return slug_by_id

    for field in ("name", "name_kz", "name_en"):
        normalized = _normalize_team_name(getattr(team, field, None))
        if not normalized:
            continue

        direct_slug = _SLUG_BY_NORMALIZED_ALIAS.get(normalized)
        if direct_slug:
            return direct_slug

        # Handles forms like "Kairat A" / "Қайрат А".
        parts = normalized.split()
        if len(parts) > 1 and len(parts[-1]) == 1:
            trimmed = " ".join(parts[:-1])
            trimmed_slug = _SLUG_BY_NORMALIZED_ALIAS.get(trimmed)
            if trimmed_slug:
                return trimmed_slug

    return None


def resolve_team_logo_url(team: Any | None) -> str | None:
    if team is None:
        return None

    explicit_logo_url = getattr(team, "logo_url", None)
    if explicit_logo_url:
        return explicit_logo_url

    slug = resolve_team_logo_slug(team)
    if not slug:
        return None

    return _TEAM_LOGO_ROUTE_TEMPLATE.format(slug=slug)
