"""Unit tests for the AI fallback matcher (mocked Anthropic SDK)."""
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from app.services.goal_video_ai_matcher import (
    CandidateGame,
    CandidateGoal,
    GoalVideoAiMatcher,
    reset_ai_matcher,
)


@pytest.fixture(autouse=True)
def _reset():
    reset_ai_matcher()
    yield
    reset_ai_matcher()


def _make_matcher_with_reply(raw_text: str) -> GoalVideoAiMatcher:
    """Build a matcher whose Anthropic client returns *raw_text*."""
    with patch("app.services.goal_video_ai_matcher.get_settings") as get_settings:
        get_settings.return_value = SimpleNamespace(
            goal_video_ai_fallback_enabled=True,
            anthropic_api_key="test-key",
            goal_video_ai_model="claude-haiku-4-5-20251001",
        )
        matcher = GoalVideoAiMatcher()

    # Replace the underlying anthropic client with a mock.
    fake_message = SimpleNamespace(content=[SimpleNamespace(text=raw_text)])
    matcher._client = SimpleNamespace(
        messages=SimpleNamespace(create=AsyncMock(return_value=fake_message))
    )
    return matcher


def _candidate(event_id: int = 111, minute: int = 45) -> CandidateGoal:
    return CandidateGoal(
        event_id=event_id,
        game_id=1,
        game_label="Kairat vs Astana",
        minute=minute,
        half=1,
        player_name="Ivanov",
        team_name="Kairat",
        event_type="goal",
    )


@pytest.mark.asyncio
async def test_high_confidence_parsed():
    matcher = _make_matcher_with_reply(
        json.dumps({"event_id": 111, "confidence": "high", "reason": "minute & player match"})
    )
    result = await matcher.match(
        filename="45_Ivanov_1-0.mp4",
        parent_folder_name="6- Тур",
        drive_created_time="2026-04-19T15:00:00Z",
        candidates=[_candidate()],
    )
    assert result is not None
    assert result.event_id == 111
    assert result.confidence == "high"


@pytest.mark.asyncio
async def test_null_event_id_preserved():
    matcher = _make_matcher_with_reply(
        json.dumps({"event_id": None, "confidence": "low", "reason": "no match"})
    )
    result = await matcher.match(
        filename="mystery.mp4",
        parent_folder_name=None,
        drive_created_time=None,
        candidates=[_candidate()],
    )
    assert result is not None
    assert result.event_id is None
    assert result.confidence == "low"


@pytest.mark.asyncio
async def test_handles_markdown_fences():
    matcher = _make_matcher_with_reply(
        "```json\n" + json.dumps({"event_id": 222, "confidence": "medium", "reason": "x"}) + "\n```"
    )
    result = await matcher.match(
        filename="45_ivanov.mp4",
        parent_folder_name=None,
        drive_created_time=None,
        candidates=[_candidate(event_id=222)],
    )
    assert result is not None
    assert result.event_id == 222


@pytest.mark.asyncio
async def test_returns_none_on_invalid_json():
    matcher = _make_matcher_with_reply("totally not json")
    result = await matcher.match(
        filename="45.mp4",
        parent_folder_name=None,
        drive_created_time=None,
        candidates=[_candidate()],
    )
    assert result is None


@pytest.mark.asyncio
async def test_returns_none_when_no_candidates():
    matcher = _make_matcher_with_reply(json.dumps({"event_id": 1, "confidence": "high"}))
    result = await matcher.match(
        filename="45.mp4",
        parent_folder_name=None,
        drive_created_time=None,
        candidates=[],
    )
    assert result is None


@pytest.mark.asyncio
async def test_folder_match_high_confidence():
    matcher = _make_matcher_with_reply(
        json.dumps({"game_id": 927, "confidence": "high", "reason": "home+away match"})
    )
    result = await matcher.match_folder(
        folder_name="ЕЛИМАЙ АСТАНА",
        candidates=[
            CandidateGame(game_id=927, home="Елимай", away="Астана"),
            CandidateGame(game_id=928, home="Иртыш", away="Жетысу"),
        ],
    )
    assert result is not None
    assert result.game_id == 927
    assert result.confidence == "high"


@pytest.mark.asyncio
async def test_folder_match_invalid_json_returns_none():
    matcher = _make_matcher_with_reply("not valid json")
    result = await matcher.match_folder(
        folder_name="QWE",
        candidates=[CandidateGame(game_id=1, home="A", away="B")],
    )
    assert result is None


@pytest.mark.asyncio
async def test_folder_match_empty_candidates():
    matcher = _make_matcher_with_reply(json.dumps({"game_id": 1, "confidence": "high"}))
    result = await matcher.match_folder(folder_name="X", candidates=[])
    assert result is None


@pytest.mark.asyncio
async def test_disabled_when_no_api_key():
    with patch("app.services.goal_video_ai_matcher.get_settings") as get_settings:
        get_settings.return_value = SimpleNamespace(
            goal_video_ai_fallback_enabled=True,
            anthropic_api_key="",
            goal_video_ai_model="claude-haiku-4-5-20251001",
        )
        matcher = GoalVideoAiMatcher()
    assert matcher.enabled is False
    result = await matcher.match(
        filename="45.mp4",
        parent_folder_name=None,
        drive_created_time=None,
        candidates=[_candidate()],
    )
    assert result is None
