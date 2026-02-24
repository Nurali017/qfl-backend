"""Localization utilities for multilingual data."""

from typing import Any


def get_localized_field(obj: Any, field: str, lang: str = "kz") -> str | None:
    """
    Get localized field value from object.

    Args:
        obj: Object with localized fields (e.g., Team, Player)
        field: Base field name (e.g., 'name', 'first_name', 'city')
        lang: Language code ('kz', 'ru', 'en')

    Returns:
        Localized value or fallback to default field

    Example:
        get_localized_field(team, 'name', 'ru') -> team.name_ru or team.name
    """
    # Try localized field first
    localized_field = f"{field}_{lang}"
    value = getattr(obj, localized_field, None)

    if value:
        return value

    # Fallback to default field
    return getattr(obj, field, None)


def get_localized_name(obj: Any, lang: str = "kz") -> str | None:
    """Get localized name from object."""
    return get_localized_field(obj, "name", lang)


def get_localized_full_name(obj: Any, lang: str = "kz") -> str:
    """
    Get localized full name (first_name + last_name) from object.

    Args:
        obj: Object with first_name and last_name fields (Player, Referee, Coach)
        lang: Language code ('kz', 'ru', 'en')

    Returns:
        Full name string
    """
    first = get_localized_field(obj, "first_name", lang) or ""
    last = get_localized_field(obj, "last_name", lang) or ""
    return f"{first} {last}".strip()


def get_localized_city(obj: Any, lang: str = "kz") -> str | None:
    """Get localized city from object."""
    return get_localized_field(obj, "city", lang)


def get_localized_country_name(obj: Any, lang: str = "kz") -> str | None:
    """Get localized country name from object."""
    return get_localized_field(obj, "country_name", lang)


def localize_team(team: Any, lang: str = "kz") -> dict:
    """
    Create localized team dict.

    Returns dict with:
        - id
        - name (localized)
        - city (localized)
        - logo_url
    """
    return {
        "id": team.id,
        "name": get_localized_name(team, lang),
        "city": get_localized_city(team, lang),
        "logo_url": team.logo_url,
    }


def localize_player(player: Any, lang: str = "kz") -> dict:
    """
    Create localized player dict.

    Returns dict with localized first_name, last_name, country_name.
    """
    return {
        "id": player.id,
        "first_name": get_localized_field(player, "first_name", lang),
        "last_name": get_localized_field(player, "last_name", lang),
        "birthday": player.birthday,
        "player_type": player.player_type,
        "country_name": get_localized_country_name(player, lang),
        "country_code": player.country_code,
        "photo_url": player.photo_url,
        "age": player.age,
        "top_role": get_localized_field(player, "top_role", lang),
    }


def localize_season(season: Any, lang: str = "kz") -> dict:
    """Create localized season dict."""
    return {
        "id": season.id,
        "name": get_localized_name(season, lang),
        "championship_id": season.championship_id,
        "date_start": season.date_start,
        "date_end": season.date_end,
    }
