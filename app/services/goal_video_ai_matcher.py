"""AI fallback for matching Drive video files to GameEvent rows.

Used when the deterministic matcher cannot pick a unique `GameEvent`:
either the filename parser failed, zero candidates, or multiple candidates.

The matcher only returns an event_id when the model is confident — a
low/medium confidence answer leaves the video unlinked for manual review.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Literal

import anthropic

from app.config import get_settings

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = (
    "You help match a football goal video clip (from Google Drive) to the correct "
    "goal event in our database. Given a filename and a list of candidate goals, "
    "return which event_id is the most likely match. Reply with strict JSON only, "
    "no prose. Schema: {\"event_id\": int|null, \"confidence\": \"high\"|\"medium\"|\"low\", "
    "\"reason\": string}. Return event_id=null if you are not confident. Use the "
    "filename (minute numbers, player surnames — possibly transliterated), the "
    "parent folder name (round hint), and the upload time (compared to goal minutes "
    "and game start) to decide."
)


@dataclass(frozen=True)
class CandidateGoal:
    event_id: int
    game_id: int
    game_label: str  # e.g. "Kairat vs Astana"
    minute: int
    half: int
    player_name: str | None
    team_name: str | None
    event_type: str


@dataclass(frozen=True)
class CandidateGame:
    game_id: int
    home: str
    away: str


@dataclass(frozen=True)
class AiMatchResult:
    event_id: int | None
    confidence: Literal["high", "medium", "low"]
    reason: str


@dataclass(frozen=True)
class AiFolderMatchResult:
    game_id: int | None
    confidence: Literal["high", "medium", "low"]
    reason: str


_FOLDER_SYSTEM_PROMPT = (
    "Given a football match video folder name (from Google Drive) and a list of today's "
    "active matches, return the game_id that best fits. Account for transliteration "
    "(Иртыш↔Ертис, Qairat↔Кайрат), abbreviations, mixed case, and dash separators. "
    "Reply with strict JSON only: "
    '{"game_id": int|null, "confidence": "high"|"medium"|"low", "reason": string}. '
    "Return game_id=null if not confident."
)


class GoalVideoAiMatcher:
    def __init__(self) -> None:
        settings = get_settings()
        self._enabled = (
            settings.goal_video_ai_fallback_enabled and bool(settings.anthropic_api_key)
        )
        if self._enabled:
            self._client = anthropic.AsyncAnthropic(
                api_key=settings.anthropic_api_key,
                max_retries=2,
                timeout=30,
            )
            self._model = settings.goal_video_ai_model
        else:
            self._client = None
            self._model = ""

    @property
    def enabled(self) -> bool:
        return self._enabled

    async def match(
        self,
        *,
        filename: str,
        parent_folder_name: str | None,
        drive_created_time: str | None,
        candidates: list[CandidateGoal],
    ) -> AiMatchResult | None:
        """Ask the model which candidate matches. Returns None if AI is disabled."""
        if not self._client or not candidates:
            return None

        payload = {
            "filename": filename,
            "parent_folder": parent_folder_name,
            "drive_uploaded_at": drive_created_time,
            "candidates": [
                {
                    "event_id": c.event_id,
                    "game_id": c.game_id,
                    "game": c.game_label,
                    "minute": c.minute,
                    "half": c.half,
                    "player": c.player_name,
                    "team": c.team_name,
                    "event_type": c.event_type,
                }
                for c in candidates
            ],
        }
        user_prompt = json.dumps(payload, ensure_ascii=False, indent=2)

        try:
            response = await self._client.messages.create(
                model=self._model,
                max_tokens=200,
                system=_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": user_prompt}],
            )
        except Exception:
            logger.exception("AI goal-video matcher call failed")
            return None

        try:
            text = response.content[0].text.strip()
            # Strip accidental Markdown fences.
            if text.startswith("```"):
                text = text.strip("`")
                if text.startswith("json"):
                    text = text[4:]
                text = text.strip()
            parsed = json.loads(text)
        except Exception:
            logger.warning("AI matcher returned non-JSON reply: %s", response.content if response else None)
            return None

        event_id = parsed.get("event_id")
        confidence = parsed.get("confidence", "low")
        reason = parsed.get("reason", "")

        if confidence not in ("high", "medium", "low"):
            confidence = "low"
        if event_id is not None:
            try:
                event_id = int(event_id)
            except (TypeError, ValueError):
                event_id = None

        return AiMatchResult(event_id=event_id, confidence=confidence, reason=reason)

    async def match_folder(
        self,
        *,
        folder_name: str,
        candidates: list[CandidateGame],
    ) -> AiFolderMatchResult | None:
        """Resolve a Drive folder label to a game_id when string-aliases didn't."""
        if not self._client or not candidates:
            return None

        payload = {
            "folder_name": folder_name,
            "today_matches": [
                {"game_id": c.game_id, "home": c.home, "away": c.away}
                for c in candidates
            ],
        }
        user_prompt = json.dumps(payload, ensure_ascii=False, indent=2)

        try:
            response = await self._client.messages.create(
                model=self._model,
                max_tokens=150,
                system=_FOLDER_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": user_prompt}],
            )
        except Exception:
            logger.exception("AI folder-matcher call failed")
            return None

        try:
            text = response.content[0].text.strip()
            if text.startswith("```"):
                text = text.strip("`")
                if text.startswith("json"):
                    text = text[4:]
                text = text.strip()
            parsed = json.loads(text)
        except Exception:
            logger.warning("AI folder-matcher returned non-JSON reply")
            return None

        game_id = parsed.get("game_id")
        confidence = parsed.get("confidence", "low")
        reason = parsed.get("reason", "")
        if confidence not in ("high", "medium", "low"):
            confidence = "low"
        if game_id is not None:
            try:
                game_id = int(game_id)
            except (TypeError, ValueError):
                game_id = None

        return AiFolderMatchResult(game_id=game_id, confidence=confidence, reason=reason)


_matcher: GoalVideoAiMatcher | None = None


def get_ai_matcher() -> GoalVideoAiMatcher:
    global _matcher
    if _matcher is None:
        _matcher = GoalVideoAiMatcher()
    return _matcher


def reset_ai_matcher() -> None:
    """Test helper."""
    global _matcher
    _matcher = None
