"""Service for translating news articles between Kazakh and Russian using OpenAI."""

from __future__ import annotations

import logging
from typing import Literal

from openai import AsyncOpenAI

from app.config import get_settings

logger = logging.getLogger(__name__)

LANG_NAMES = {"kz": "казахского", "ru": "русского"}
LANG_TARGETS = {"kz": "казахский", "ru": "русский"}

CONTEXT = (
    "Ты — профессиональный переводчик, работающий в пресс-службе "
    "Казахстанской федерации футбола (КФФ). "
    "Ты переводишь официальные новости, статьи, интервью и аналитические материалы "
    "для сайта КФФ Лиги (kffleague.kz). "
    "Используй спортивную терминологию, принятую в казахстанском и российском футболе. "
    "Сохраняй официальный, но живой журналистский стиль."
)


def _build_system_prompt(
    source_lang: str,
    target_lang: str,
    fmt: Literal["plain", "html"],
) -> str:
    src = LANG_NAMES.get(source_lang, source_lang)
    tgt = LANG_TARGETS.get(target_lang, target_lang)
    if fmt == "html":
        return (
            f"{CONTEXT}\n\n"
            f"Переведи следующий HTML-контент с {src} языка на {tgt}. "
            f"Сохрани ВСЕ HTML-теги, атрибуты и форматирование в точности. "
            f"Переводи только видимый текст между тегами. "
            f"Верни ТОЛЬКО переведённый HTML, без пояснений."
        )
    return (
        f"{CONTEXT}\n\n"
        f"Переведи следующий текст с {src} языка на {tgt}. "
        f"Верни ТОЛЬКО переведённый текст, без пояснений и комментариев."
    )


class NewsTranslatorService:
    def __init__(self) -> None:
        settings = get_settings()
        self._enabled = bool(settings.openai_api_key)
        self._model = settings.openai_model
        self._client: AsyncOpenAI | None = None
        if self._enabled:
            self._client = AsyncOpenAI(
                api_key=settings.openai_api_key,
                max_retries=settings.openai_max_retries,
                timeout=120,  # translations can be slow for long articles
            )

    @property
    def enabled(self) -> bool:
        return self._enabled

    async def translate(
        self,
        text: str,
        source_lang: str = "kz",
        target_lang: str = "ru",
        fmt: Literal["plain", "html"] = "plain",
    ) -> str:
        if not self._client or not self._enabled:
            raise RuntimeError("OpenAI API is not configured")

        system_prompt = _build_system_prompt(source_lang, target_lang, fmt)

        response = await self._client.chat.completions.create(
            model=self._model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": text},
            ],
            temperature=0.3,
            max_tokens=16000,
        )
        result = (response.choices[0].message.content or "").strip()
        if not result:
            raise RuntimeError("Empty translation response from OpenAI")
        return result
