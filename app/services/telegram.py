import logging

import httpx

from app.config import get_settings

logger = logging.getLogger(__name__)


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


async def notify_contract_change(
    action: str,
    player_name: str,
    team_name: str,
    season_name: str,
    admin_email: str,
    contract_id: int,
) -> None:
    text = (
        f"\U0001f4cb Контракт <b>{action}</b>\n\n"
        f"\U0001f464 Игрок: {player_name}\n"
        f"\U0001f3df Команда: {team_name}\n"
        f"\U0001f4c5 Сезон: {season_name}\n"
        f"\U0001f468\u200d\U0001f4bc Админ: {admin_email}\n\n"
        f"\U0001f517 ID контракта: {contract_id}"
    )
    await send_telegram_message(text)
