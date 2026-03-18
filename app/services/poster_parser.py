"""Service for extracting broadcaster and stadium data from match schedule poster images.

Approach: send the poster image + DB context (games, stadiums, broadcasters)
to AI Vision — the model matches directly by IDs, no fuzzy matching needed.
"""

from __future__ import annotations

import base64
import json
import logging

import anthropic
from openai import AsyncOpenAI

from app.config import get_settings

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """\
You are analyzing a Kazakhstan Premier League (QFL) match schedule poster.

You will receive:
1. An image of the poster
2. A JSON context with the games, stadiums, and broadcasters from our database

Your task: for each match visible on the poster, determine:
- Which game from the DB it corresponds to (by matching team names — note names may be in Kazakh, Russian, or English on the poster vs DB)
- Which stadium is shown (match the poster stadium text to a DB stadium by name/city)
- Which broadcasters/TV channels are shown (match logos/text to DB broadcaster names)

IMPORTANT matching rules:
- Team names on poster may be in Kazakh (Ұлытау, Ертіс, Жетісу) while DB has Russian (Улытау, Иртыш, Жетысу) or vice versa — they are the same teams
- "FC" / "ФК" prefixes should be ignored
- Poster may show "YouTube" — this corresponds to the "KFF League" broadcaster (their YouTube channel)
- Stadium names on poster are often in Kazakh with "стадионы" suffix, DB has Russian names
- If you cannot confidently match a game/stadium/broadcaster, set its ID to null

Return ONLY valid JSON:
{
  "matches": [
    {
      "game_id": 123,
      "stadium_id": 7,
      "broadcaster_ids": [1, 4],
      "poster_home": "Ұлытау",
      "poster_away": "Оқжетпес",
      "poster_stadium": "Металлург стадионы",
      "poster_broadcasters": ["Кинопоиск", "KFF League"]
    }
  ]
}
"""

TEXT_SYSTEM_PROMPT = """\
You are analyzing a Kazakhstan Premier League (QFL) match schedule text (pasted from Telegram/WhatsApp).

You will receive:
1. A text message containing match schedule information
2. A JSON context with the games, stadiums, and broadcasters from our database

Your task: for each match visible in the text, determine:
- Which game from the DB it corresponds to (by matching team names — note names may be in Kazakh, Russian, or English in the text vs DB)
- Which stadium is mentioned (match the text stadium to a DB stadium by name/city)
- Which broadcasters/TV channels are mentioned (match text to DB broadcaster names)

IMPORTANT matching rules:
- Team names in text may be in Kazakh (Ұлытау, Ертіс, Жетісу) while DB has Russian (Улытау, Иртыш, Жетысу) or vice versa — they are the same teams
- "FC" / "ФК" prefixes should be ignored
- Text may mention "YouTube" — this corresponds to the "KFF League" broadcaster (their YouTube channel)
- Stadium names in text are often in Kazakh with "стадионы" suffix, DB has Russian names
- If you cannot confidently match a game/stadium/broadcaster, set its ID to null

Return ONLY valid JSON:
{
  "matches": [
    {
      "game_id": 123,
      "stadium_id": 7,
      "broadcaster_ids": [1, 4],
      "poster_home": "Ұлытау",
      "poster_away": "Оқжетпес",
      "poster_stadium": "Металлург стадионы",
      "poster_broadcasters": ["Кинопоиск", "KFF League"]
    }
  ]
}
"""


def build_context(games: list, stadiums: list, broadcasters: list) -> str:
    """Build JSON context string with DB data for the AI prompt."""
    ctx = {
        "games": [
            {
                "game_id": g.id,
                "home_team": g.home_team_names,
                "away_team": g.away_team_names,
            }
            for g in games
        ],
        "stadiums": [
            {
                "stadium_id": s.id,
                "names": [n for n in [s.name, s.name_kz, getattr(s, "name_ru", None), getattr(s, "name_en", None)] if n],
                "city": [c for c in [getattr(s, "city", None), getattr(s, "city_kz", None)] if c],
            }
            for s in stadiums
        ],
        "broadcasters": [
            {"broadcaster_id": b.id, "name": b.name}
            for b in broadcasters
        ],
    }
    return json.dumps(ctx, ensure_ascii=False, indent=2)


class PosterParserService:
    def __init__(self) -> None:
        settings = get_settings()
        self._provider: str | None = None
        self._anthropic: anthropic.AsyncAnthropic | None = None
        self._openai: AsyncOpenAI | None = None

        if settings.anthropic_api_key:
            self._provider = "anthropic"
            self._anthropic_model = settings.anthropic_model
            self._anthropic = anthropic.AsyncAnthropic(
                api_key=settings.anthropic_api_key,
                max_retries=3,
                timeout=120,
            )
        elif settings.openai_api_key:
            self._provider = "openai"
            self._openai_model = "gpt-4o"
            self._openai = AsyncOpenAI(
                api_key=settings.openai_api_key,
                max_retries=settings.openai_max_retries,
                timeout=120,
            )

    @property
    def enabled(self) -> bool:
        return self._provider is not None

    async def parse_poster(
        self,
        image_bytes: bytes,
        games: list,
        stadiums: list,
        broadcasters: list,
    ) -> dict:
        """Parse poster image with DB context. Returns matched results directly."""
        if not self._provider:
            raise RuntimeError("No AI API configured (set ANTHROPIC_API_KEY or OPENAI_API_KEY)")

        b64 = base64.b64encode(image_bytes).decode()
        context = build_context(games, stadiums, broadcasters)
        user_text = f"Here is the database context:\n\n{context}\n\nMatch the poster image to this data."

        if self._provider == "anthropic":
            raw = await self._call_anthropic(b64, user_text)
        else:
            raw = await self._call_openai(b64, user_text)

        raw = raw.strip()
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1] if "\n" in raw else raw[3:]
            if raw.endswith("```"):
                raw = raw[:-3].strip()

        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            logger.error("Failed to parse AI response: %s", raw)
            raise RuntimeError(f"Failed to parse AI response as JSON: {raw[:300]}")

    async def _call_anthropic(self, b64: str, user_text: str) -> str:
        assert self._anthropic is not None
        response = await self._anthropic.messages.create(
            model=self._anthropic_model,
            max_tokens=4000,
            system=SYSTEM_PROMPT,
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image",
                            "source": {
                                "type": "base64",
                                "media_type": "image/jpeg",
                                "data": b64,
                            },
                        },
                        {"type": "text", "text": user_text},
                    ],
                },
            ],
        )
        return response.content[0].text

    async def _call_openai(self, b64: str, user_text: str) -> str:
        assert self._openai is not None
        response = await self._openai.chat.completions.create(
            model=self._openai_model,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/jpeg;base64,{b64}",
                                "detail": "high",
                            },
                        },
                        {"type": "text", "text": user_text},
                    ],
                },
            ],
            temperature=0.1,
            max_tokens=4000,
        )
        return response.choices[0].message.content or ""

    # ---------- Text-only parsing ----------

    async def parse_poster_text(
        self,
        text: str,
        games: list,
        stadiums: list,
        broadcasters: list,
    ) -> dict:
        """Parse schedule text (from Telegram/WhatsApp) with DB context."""
        if not self._provider:
            raise RuntimeError("No AI API configured (set ANTHROPIC_API_KEY or OPENAI_API_KEY)")

        context = build_context(games, stadiums, broadcasters)
        user_text = (
            f"Here is the database context:\n\n{context}\n\n"
            f"Here is the schedule text to parse:\n\n{text}"
        )

        if self._provider == "anthropic":
            raw = await self._call_anthropic_text(user_text)
        else:
            raw = await self._call_openai_text(user_text)

        raw = raw.strip()
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1] if "\n" in raw else raw[3:]
            if raw.endswith("```"):
                raw = raw[:-3].strip()

        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            logger.error("Failed to parse AI response: %s", raw)
            raise RuntimeError(f"Failed to parse AI response as JSON: {raw[:300]}")

    async def _call_anthropic_text(self, user_text: str) -> str:
        assert self._anthropic is not None
        response = await self._anthropic.messages.create(
            model=self._anthropic_model,
            max_tokens=4000,
            system=TEXT_SYSTEM_PROMPT,
            messages=[
                {"role": "user", "content": user_text},
            ],
        )
        return response.content[0].text

    async def _call_openai_text(self, user_text: str) -> str:
        assert self._openai is not None
        response = await self._openai.chat.completions.create(
            model=self._openai_model,
            messages=[
                {"role": "system", "content": TEXT_SYSTEM_PROMPT},
                {"role": "user", "content": user_text},
            ],
            temperature=0.1,
            max_tokens=4000,
        )
        return response.choices[0].message.content or ""
