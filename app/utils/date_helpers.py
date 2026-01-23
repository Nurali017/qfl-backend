"""
Utility functions for date formatting and localization.
"""

from datetime import date
from typing import Any, Literal


# Weekday names in different languages (Monday = 0, Sunday = 6)
WEEKDAY_NAMES = {
    "ru": ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"],
    "en": ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"],
    "kz": ["Дүйсенбі", "Сейсенбі", "Сәрсенбі", "Бейсенбі", "Жұма", "Сенбі", "Жексенбі"],
}

# Month names in genitive case for date formatting (January = 0, December = 11)
MONTH_NAMES = {
    "ru": [
        "января", "февраля", "марта", "апреля", "мая", "июня",
        "июля", "августа", "сентября", "октября", "ноября", "декабря"
    ],
    "en": [
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December"
    ],
    "kz": [
        "қаңтар", "ақпан", "наурыз", "сәуір", "мамыр", "маусым",
        "шілде", "тамыз", "қыркүйек", "қазан", "қараша", "желтоқсан"
    ],
}


def format_match_date(
    game_date: date,
    language: Literal["kz", "ru", "en"] = "ru"
) -> str:
    """
    Format a date for match center display with weekday and month names.

    Args:
        game_date: The date to format
        language: Language code (kz, ru, or en)

    Returns:
        Formatted date string like "Пятница, 27 февраля 2026"

    Examples:
        >>> format_match_date(date(2026, 2, 27), "ru")
        "Пятница, 27 февраля 2026"
        >>> format_match_date(date(2026, 2, 27), "en")
        "Friday, 27 February 2026"
    """
    weekday_list = WEEKDAY_NAMES.get(language, WEEKDAY_NAMES["ru"])
    month_list = MONTH_NAMES.get(language, MONTH_NAMES["ru"])

    weekday = weekday_list[game_date.weekday()]
    month = month_list[game_date.month - 1]

    return f"{weekday}, {game_date.day} {month} {game_date.year}"


def get_localized_field(
    obj: Any,
    field_base: str,
    language: Literal["kz", "ru", "en"] = "ru"
) -> Any:
    """
    Get a localized field value from an object with fallback to default.

    Looks for field_{language} first, falls back to base field if not found or None.

    Args:
        obj: Object containing the fields
        field_base: Base field name (e.g., "name" for name/name_kz/name_en)
        language: Language code (kz, ru, or en)

    Returns:
        Localized field value or base field value as fallback

    Examples:
        >>> get_localized_field(team, "name", "kz")
        # Returns team.name_kz if exists and not None, otherwise team.name
        >>> get_localized_field(stadium, "city", "en")
        # Returns stadium.city_en if exists and not None, otherwise stadium.city
    """
    if language == "kz":
        localized_value = getattr(obj, f"{field_base}_kz", None)
        return localized_value if localized_value is not None else getattr(obj, field_base, None)
    elif language == "en":
        localized_value = getattr(obj, f"{field_base}_en", None)
        return localized_value if localized_value is not None else getattr(obj, field_base, None)
    else:  # ru or default
        # For Russian, check if there's a _ru suffix, otherwise use base field
        localized_value = getattr(obj, f"{field_base}_ru", None)
        if localized_value is not None:
            return localized_value
        return getattr(obj, field_base, None)
