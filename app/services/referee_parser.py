"""Service for parsing referee assignment texts (Kazakh) and matching to DB entities.

Approach: send the pasted text + DB context (games, referees) to AI —
the model matches team/referee names directly by IDs.
"""

from __future__ import annotations

import json
import logging

import anthropic
from openai import AsyncOpenAI

from app.config import get_settings

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """\
You are parsing a Kazakhstan Premier League (QFL) referee assignment text written in Kazakh.

You will receive:
1. The referee assignment text (pasted from Telegram/WhatsApp)
2. A JSON context with the games and referees from our database

The text lists matches with their assigned referees. Each match block typically shows:
- Two team names (home — away)
- Referee roles in Kazakh with referee names and cities

Your task: for each match in the text, determine:
- Which game from the DB it corresponds to (match team names — they may be in Kazakh while DB has Russian/English variants or vice versa)
- Which referee from the DB each named person corresponds to (match by name variants)
- Which role each referee has

Kazakh role label → role enum mapping:
- Бас төреші → main
- Көмекші төреші (1st listed) → first_assistant
- Көмекші төреші (2nd listed) → second_assistant
- Қапталдағы төрешілер / Қапталдағы төреші (1st listed) → first_assistant
- Қапталдағы төрешілер / Қапталдағы төреші (2nd listed) → second_assistant
- Қосалқы төреші / Резервті төреші → fourth_referee
- VAR → var_main
- AVAR / АВАР → var_assistant
- Бас инспектор / Инспектор → match_inspector

IMPORTANT:
- Team names in text are usually in Kazakh (e.g. Ұлытау, Ертіс, Жетісу), DB may have Russian (Улытау, Иртыш, Жетысу) — they're the same teams
- "ФК" / "FC" prefixes should be ignored
- Referee names in text may differ slightly from DB (Kazakh vs Russian transliteration) — match by closest variant
- If you cannot confidently match a game, set game_id to null
- If you cannot confidently match a referee, set referee_id to null
- Always include the parsed name from the text for verification

Return ONLY valid JSON:
{
  "matches": [
    {
      "game_id": 123,
      "home_team_parsed": "Ұлытау",
      "away_team_parsed": "Оқжетпес",
      "referees": [
        {
          "role": "main",
          "parsed_name": "Иванов И.",
          "parsed_city": "Алматы",
          "referee_id": 45
        }
      ]
    }
  ]
}
"""


def build_context(games: list, referees: list) -> str:
    """Build JSON context string with DB data for the AI prompt."""
    ctx = {
        "games": [
            {
                "game_id": g["game_id"],
                "home_team": g["home_team"],
                "away_team": g["away_team"],
            }
            for g in games
        ],
        "referees": [
            {
                "referee_id": r["referee_id"],
                "names": r["names"],
            }
            for r in referees
        ],
    }
    return json.dumps(ctx, ensure_ascii=False, indent=2)


class RefereeParserService:
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

    async def parse_referees(
        self,
        text: str,
        games: list[dict],
        referees: list[dict],
    ) -> dict:
        """Parse referee assignment text with DB context. Returns matched results."""
        if not self._provider:
            raise RuntimeError("No AI API configured (set ANTHROPIC_API_KEY or OPENAI_API_KEY)")

        context = build_context(games, referees)
        user_text = (
            f"Here is the database context:\n\n{context}\n\n"
            f"Here is the referee assignment text to parse:\n\n{text}"
        )

        if self._provider == "anthropic":
            raw = await self._call_anthropic(user_text)
        else:
            raw = await self._call_openai(user_text)

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

    async def _call_anthropic(self, user_text: str) -> str:
        assert self._anthropic is not None
        response = await self._anthropic.messages.create(
            model=self._anthropic_model,
            max_tokens=4000,
            system=SYSTEM_PROMPT,
            messages=[
                {
                    "role": "user",
                    "content": [{"type": "text", "text": user_text}],
                },
            ],
        )
        return response.content[0].text

    async def _call_openai(self, user_text: str) -> str:
        assert self._openai is not None
        response = await self._openai.chat.completions.create(
            model=self._openai_model,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_text},
            ],
            temperature=0.1,
            max_tokens=4000,
        )
        return response.choices[0].message.content or ""
