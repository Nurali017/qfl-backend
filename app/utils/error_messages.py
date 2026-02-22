"""Localized error messages for API responses."""

ERROR_MESSAGES = {
    "team_not_found": {
        "ru": "Команда не найдена",
        "kz": "Команда табылмады",
        "en": "Team not found",
    },
    "player_not_found": {
        "ru": "Игрок не найден",
        "kz": "Ойыншы табылмады",
        "en": "Player not found",
    },
    "game_not_found": {
        "ru": "Матч не найден",
        "kz": "Матч табылмады",
        "en": "Game not found",
    },
    "season_not_found": {
        "ru": "Сезон не найден",
        "kz": "Маусым табылмады",
        "en": "Season not found",
    },
    "season_teams_not_configured": {
        "ru": "Для выбранного сезона не настроен состав команд. Запустите синхронизацию.",
        "kz": "Таңдалған маусым үшін командалар құрамы бапталмаған. Синхрондауды іске қосыңыз.",
        "en": "Teams are not configured for the selected season. Run synchronization first.",
    },
    "stats_not_found": {
        "ru": "Статистика не найдена. Сначала запустите синхронизацию.",
        "kz": "Статистика табылмады. Алдымен синхрондау іске қосыңыз.",
        "en": "Stats not found. Run sync first.",
    },
    "country_not_found": {
        "ru": "Страна не найдена",
        "kz": "Ел табылмады",
        "en": "Country not found",
    },
    "news_not_found": {
        "ru": "Новость не найдена",
        "kz": "Жаңалық табылмады",
        "en": "News not found",
    },
    "page_not_found": {
        "ru": "Страница не найдена",
        "kz": "Бет табылмады",
        "en": "Page not found",
    },
    "flag_not_found": {
        "ru": "Флаг не найден",
        "kz": "Жалау табылмады",
        "en": "Flag not found",
    },
    "file_not_found": {
        "ru": "Файл не найден",
        "kz": "Файл табылмады",
        "en": "File not found",
    },
    "invalid_sort_field": {
        "ru": "Недопустимое поле сортировки",
        "kz": "Жарамсыз сұрыптау өрісі",
        "en": "Invalid sort field",
    },
    "teams_not_found": {
        "ru": "Одна или обе команды не найдены",
        "kz": "Бір немесе екі команда да табылмады",
        "en": "One or both teams not found",
    },
    "country_code_exists": {
        "ru": "Код страны уже существует",
        "kz": "Ел коды бұрыннан бар",
        "en": "Country code already exists",
    },
    "tournament_not_found": {
        "ru": "Турнир не найден",
        "kz": "Турнир табылмады",
        "en": "Tournament not found",
    },
    "lineup_not_found": {
        "ru": "Состав не найден",
        "kz": "Құрам табылмады",
        "en": "Lineup not found",
    },
}


def get_error_message(error_key: str, lang: str = "en") -> str:
    """Get localized error message.

    Args:
        error_key: Key for the error message
        lang: Language code (ru, kz, en)

    Returns:
        Localized error message, falls back to English if not found
    """
    if error_key not in ERROR_MESSAGES:
        return error_key

    messages = ERROR_MESSAGES[error_key]
    return messages.get(lang, messages.get("en", error_key))
