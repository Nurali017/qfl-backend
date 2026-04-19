"""User-account Telegram client (Telethon) for public posts.

Unlike Bot API, user accounts with Premium can freely send custom_emoji
entities that reference packs added to their account (e.g. KazakhstanFootballClubs).

Reads credentials from env:
- TELETHON_API_ID, TELETHON_API_HASH — app credentials from my.telegram.org
- TELETHON_SESSION_PATH — path to .session file (already authorized)
"""
from __future__ import annotations

import asyncio
import html as _html_lib
import logging
import os
import re
from typing import Iterable

from telethon import TelegramClient, errors as tg_errors
from telethon.tl.types import (
    MessageEntityBold,
    MessageEntityCustomEmoji,
    MessageEntityItalic,
    MessageEntityTextUrl,
)

from app.config import get_settings


class TelegramTransientError(Exception):
    """Raised on network/timeout/flood/server errors — Celery should retry."""

logger = logging.getLogger(__name__)

# Supported tags:
#   <tg-emoji emoji-id="N">X</tg-emoji> → MessageEntityCustomEmoji
#   <a href="URL">text</a>              → MessageEntityTextUrl
#   <b>text</b>                         → MessageEntityBold
#   <i>text</i>                         → MessageEntityItalic
_TAG_RE = re.compile(
    r'<tg-emoji emoji-id="(?P<eid>\d+)">(?P<et>[^<]+)</tg-emoji>'
    r'|<a href="(?P<href>[^"]+)">(?P<at>[^<]+)</a>'
    r'|<b>(?P<bt>[^<]+)</b>'
    r'|<i>(?P<it>[^<]+)</i>'
)


def _parse_tg_emoji_html(text_html: str):
    """Strip tags and return (plain_text, entities).

    Offsets/lengths in UTF-16 code units (MTProto requirement).
    """
    out: list[str] = []
    entities: list = []
    last_end = 0
    for m in _TAG_RE.finditer(text_html):
        out.append(text_html[last_end:m.start()])
        prefix_plain = _html_lib.unescape("".join(out))
        utf16_off = len(prefix_plain.encode("utf-16-le")) // 2
        if m.group("eid"):
            content = m.group("et")
            utf16_len = len(content.encode("utf-16-le")) // 2
            entities.append(
                MessageEntityCustomEmoji(
                    offset=utf16_off, length=utf16_len, document_id=int(m.group("eid"))
                )
            )
        elif m.group("href"):
            content = m.group("at")
            utf16_len = len(content.encode("utf-16-le")) // 2
            entities.append(
                MessageEntityTextUrl(
                    offset=utf16_off, length=utf16_len, url=m.group("href")
                )
            )
        elif m.group("bt") is not None:
            content = m.group("bt")
            utf16_len = len(content.encode("utf-16-le")) // 2
            entities.append(MessageEntityBold(offset=utf16_off, length=utf16_len))
        else:
            content = m.group("it")
            utf16_len = len(content.encode("utf-16-le")) // 2
            entities.append(MessageEntityItalic(offset=utf16_off, length=utf16_len))
        out.append(content)
        last_end = m.end()
    out.append(text_html[last_end:])
    plain = _html_lib.unescape("".join(out))
    return plain, entities


def _resolve_chat_id(raw: str) -> int | str:
    try:
        return int(raw)
    except (TypeError, ValueError):
        return raw


async def send_public_user_photo(
    photo_path: str,
    caption_html: str = "",
    reply_to: int | None = None,
) -> int | None:
    """Send a photo. Returns Telegram message_id on success, None on failure."""
    settings = get_settings()
    if not settings.telegram_public_posts_enabled:
        return None
    chat_raw = settings.telegram_public_chat_id or settings.telegram_chat_id
    if not chat_raw:
        return None
    api_id = os.environ.get("TELETHON_API_ID")
    api_hash = os.environ.get("TELETHON_API_HASH")
    session = os.environ.get("TELETHON_SESSION_PATH", ".telethon_qfl_session")
    if not (api_id and api_hash):
        return None

    caption_text, entities = _parse_tg_emoji_html(caption_html or "")
    chat_id = _resolve_chat_id(chat_raw)
    client = TelegramClient(session, int(api_id), api_hash)
    try:
        await client.connect()
        if not await client.is_user_authorized():
            logger.error("Telethon session not authorized")
            return None
        msg = await client.send_file(
            chat_id,
            photo_path,
            caption=caption_text,
            formatting_entities=entities if entities else None,
            reply_to=reply_to,
        )
        return msg.id if msg else None
    except (
        tg_errors.FloodWaitError,
        tg_errors.ServerError,
        tg_errors.TimedOutError,
        tg_errors.NetworkMigrateError,
        tg_errors.PhoneMigrateError,
        tg_errors.RPCError,
        ConnectionError,
        OSError,
        asyncio.TimeoutError,
    ) as e:
        logger.warning("Telethon send_file transient error: %s", e)
        raise TelegramTransientError(str(e)) from e
    except Exception:
        logger.exception("Telethon send_file permanent failure")
        return None
    finally:
        await client.disconnect()


async def edit_public_user_message_media(
    message_id: int,
    file_path: str,
    caption_html: str = "",
) -> bool:
    """Edit an existing public message to attach media (e.g. a goal clip).

    Returns True on success. Used to upgrade a text-only goal post with its
    highlight video once the clip is uploaded to MinIO.
    """
    settings = get_settings()
    if not settings.telegram_public_posts_enabled:
        return False
    chat_raw = settings.telegram_public_chat_id or settings.telegram_chat_id
    if not chat_raw:
        return False
    api_id = os.environ.get("TELETHON_API_ID")
    api_hash = os.environ.get("TELETHON_API_HASH")
    session = os.environ.get("TELETHON_SESSION_PATH", ".telethon_qfl_session")
    if not (api_id and api_hash):
        return False

    caption_text, entities = _parse_tg_emoji_html(caption_html or "")
    chat_id = _resolve_chat_id(chat_raw)
    client = TelegramClient(session, int(api_id), api_hash)
    try:
        await client.connect()
        if not await client.is_user_authorized():
            logger.error("Telethon session not authorized")
            return False
        await client.edit_message(
            chat_id,
            message_id,
            caption_text,
            file=file_path,
            formatting_entities=entities if entities else None,
        )
        return True
    except (
        tg_errors.FloodWaitError,
        tg_errors.ServerError,
        tg_errors.TimedOutError,
        tg_errors.NetworkMigrateError,
        tg_errors.PhoneMigrateError,
        tg_errors.RPCError,
        ConnectionError,
        OSError,
        asyncio.TimeoutError,
    ) as e:
        logger.warning("Telethon edit_message transient error: %s", e)
        raise TelegramTransientError(str(e)) from e
    except Exception:
        logger.exception("Telethon edit_message permanent failure")
        return False
    finally:
        await client.disconnect()


async def send_public_user_message(
    text_html: str,
    reply_to: int | None = None,
) -> int | None:
    """Post via Telethon user-client. Returns message_id on success, None on failure.

    Gated by settings.telegram_public_posts_enabled.
    """
    settings = get_settings()
    if not settings.telegram_public_posts_enabled:
        return None

    chat_raw = settings.telegram_public_chat_id or settings.telegram_chat_id
    if not chat_raw:
        return None

    api_id = os.environ.get("TELETHON_API_ID")
    api_hash = os.environ.get("TELETHON_API_HASH")
    session = os.environ.get("TELETHON_SESSION_PATH", ".telethon_qfl_session")
    if not (api_id and api_hash):
        return None

    text, entities = _parse_tg_emoji_html(text_html)
    chat_id = _resolve_chat_id(chat_raw)

    client = TelegramClient(session, int(api_id), api_hash)
    try:
        await client.connect()
        if not await client.is_user_authorized():
            logger.error("Telethon session not authorized")
            return None
        msg = await client.send_message(
            chat_id,
            text,
            formatting_entities=entities if entities else None,
            reply_to=reply_to,
        )
        return msg.id if msg else None
    except (
        tg_errors.FloodWaitError,
        tg_errors.ServerError,
        tg_errors.TimedOutError,
        tg_errors.NetworkMigrateError,
        tg_errors.PhoneMigrateError,
        tg_errors.RPCError,
        ConnectionError,
        OSError,
        asyncio.TimeoutError,
    ) as e:
        logger.warning("Telethon send_message transient error: %s", e)
        raise TelegramTransientError(str(e)) from e
    except Exception:
        logger.exception("Telethon send_message permanent failure")
        return None
    finally:
        await client.disconnect()
