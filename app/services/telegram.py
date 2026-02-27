import logging

import httpx

from app.config import get_settings

logger = logging.getLogger(__name__)

ROLE_LABELS: dict[int, str] = {
    1: "Игрок",
    2: "Тренер",
    3: "Сотрудник",
    4: "Администрация",
}

AMPLUA_LABELS: dict[int, str] = {
    1: "Вратарь",
    2: "Защитник",
    3: "Полузащитник",
    4: "Нападающий",
}


async def send_telegram_message(text: str) -> None:
    settings = get_settings()
    if not settings.telegram_notifications_enabled:
        return
    if not settings.telegram_bot_token or not settings.telegram_chat_id:
        logger.warning("Telegram token/chat_id not configured, skipping notification")
        return

    url = f"https://api.telegram.org/bot{settings.telegram_bot_token}/sendMessage"
    payload = {
        "chat_id": settings.telegram_chat_id,
        "text": text,
        "parse_mode": "HTML",
    }
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(url, json=payload)
            if resp.status_code != 200:
                logger.error("Telegram API error %s: %s", resp.status_code, resp.text[:200])
    except Exception:
        logger.exception("Failed to send Telegram notification")


def _format_contract_details(
    *,
    role: int | None = None,
    amplua: int | None = None,
    number: int | None = None,
    position_ru: str | None = None,
    position_kz: str | None = None,
    position_en: str | None = None,
    is_active: bool | None = None,
    is_hidden: bool | None = None,
) -> str:
    lines: list[str] = []
    if role is not None:
        lines.append(f"\U0001f465 Роль: {ROLE_LABELS.get(role, str(role))}")
    if amplua is not None:
        lines.append(f"\u26bd Амплуа: {AMPLUA_LABELS.get(amplua, str(amplua))}")
    if number is not None:
        lines.append(f"#\ufe0f\u20e3 Номер: {number}")
    pos_parts = [p for p in (position_ru, position_kz, position_en) if p]
    if pos_parts:
        lines.append(f"\U0001f4cc Должность: {' / '.join(pos_parts)}")
    if is_active is not None:
        lines.append(f"\u2705 Активен: {'да' if is_active else 'нет'}")
    if is_hidden is not None:
        lines.append(f"\U0001f648 Скрыт: {'да' if is_hidden else 'нет'}")
    return "\n".join(lines)


async def notify_contract_change(
    action: str,
    player_name: str,
    team_name: str,
    season_name: str,
    admin_email: str,
    contract_id: int,
    *,
    role: int | None = None,
    amplua: int | None = None,
    number: int | None = None,
    position_ru: str | None = None,
    position_kz: str | None = None,
    position_en: str | None = None,
    is_active: bool | None = None,
    is_hidden: bool | None = None,
) -> None:
    details = _format_contract_details(
        role=role,
        amplua=amplua,
        number=number,
        position_ru=position_ru,
        position_kz=position_kz,
        position_en=position_en,
        is_active=is_active,
        is_hidden=is_hidden,
    )
    text = (
        f"\U0001f4cb Контракт <b>{action}</b>\n\n"
        f"\U0001f464 Игрок: {player_name}\n"
        f"\U0001f3df Команда: {team_name}\n"
        f"\U0001f4c5 Сезон: {season_name}\n"
    )
    if details:
        text += details + "\n"
    text += (
        f"\n\U0001f468\u200d\U0001f4bc Админ: {admin_email}\n"
        f"\U0001f517 ID контракта: {contract_id}"
    )
    await send_telegram_message(text)


async def notify_contract_updated(
    player_name: str,
    team_name: str,
    season_name: str,
    admin_email: str,
    contract_id: int,
    changes: dict[str, tuple],
) -> None:
    """Send detailed notification about contract update with changed fields."""
    change_lines: list[str] = []
    field_labels = {
        "role": "Роль",
        "amplua": "Амплуа",
        "number": "Номер",
        "position_ru": "Должность (RU)",
        "position_kz": "Должность (KZ)",
        "position_en": "Должность (EN)",
        "is_active": "Активен",
        "is_hidden": "Скрыт",
        "photo_url": "Фото",
        "player_id": "Игрок ID",
        "team_id": "Команда ID",
        "season_id": "Сезон ID",
    }
    for field, (old_val, new_val) in changes.items():
        label = field_labels.get(field, field)
        # Pretty-print known enums
        if field == "role":
            old_display = ROLE_LABELS.get(old_val, str(old_val)) if old_val is not None else "—"
            new_display = ROLE_LABELS.get(new_val, str(new_val)) if new_val is not None else "—"
        elif field == "amplua":
            old_display = AMPLUA_LABELS.get(old_val, str(old_val)) if old_val is not None else "—"
            new_display = AMPLUA_LABELS.get(new_val, str(new_val)) if new_val is not None else "—"
        elif field in ("is_active", "is_hidden"):
            old_display = "да" if old_val else "нет"
            new_display = "да" if new_val else "нет"
        else:
            old_display = str(old_val) if old_val is not None else "—"
            new_display = str(new_val) if new_val is not None else "—"
        change_lines.append(f"  {label}: {old_display} \u2192 {new_display}")

    changes_text = "\n".join(change_lines) if change_lines else "  (нет данных)"

    text = (
        f"\U0001f4cb Контракт <b>изменён</b>\n\n"
        f"\U0001f464 Игрок: {player_name}\n"
        f"\U0001f3df Команда: {team_name}\n"
        f"\U0001f4c5 Сезон: {season_name}\n\n"
        f"\U0001f4dd Изменения:\n{changes_text}\n\n"
        f"\U0001f468\u200d\U0001f4bc Админ: {admin_email}\n"
        f"\U0001f517 ID контракта: {contract_id}"
    )
    await send_telegram_message(text)
