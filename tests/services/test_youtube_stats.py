"""Tests for YouTube view_count sync service."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from app.services.youtube_stats import _Target, fetch_view_counts


class _MockResponse:
    """Minimal mock that mimics httpx.Response."""

    def __init__(self, payload: dict, status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            import httpx

            raise httpx.HTTPStatusError("err", request=None, response=None)  # type: ignore[arg-type]

    def json(self) -> dict:
        return self._payload


@pytest.mark.asyncio
async def test_fetch_view_counts_empty_returns_empty() -> None:
    assert await fetch_view_counts([]) == {}


@pytest.mark.asyncio
async def test_fetch_view_counts_single_batch() -> None:
    payload = {
        "items": [
            {"id": "aaaaaaaaaaa", "statistics": {"viewCount": "1234"}},
            {"id": "bbbbbbbbbbb", "statistics": {"viewCount": "0"}},
        ]
    }

    async def _get(*args, **kwargs):
        return _MockResponse(payload)

    mock_client = AsyncMock()
    mock_client.get = _get
    mock_client.__aenter__.return_value = mock_client

    with (
        patch("app.services.youtube_stats.httpx.AsyncClient", return_value=mock_client),
        patch("app.services.youtube_stats.get_settings") as mock_settings,
    ):
        mock_settings.return_value.youtube_api_key = "test_key"
        result = await fetch_view_counts(["aaaaaaaaaaa", "bbbbbbbbbbb"])

    assert result == {"aaaaaaaaaaa": 1234, "bbbbbbbbbbb": 0}


@pytest.mark.asyncio
async def test_fetch_view_counts_skips_on_missing_api_key() -> None:
    with patch("app.services.youtube_stats.get_settings") as mock_settings:
        mock_settings.return_value.youtube_api_key = ""
        result = await fetch_view_counts(["aaaaaaaaaaa"])
    assert result == {}


@pytest.mark.asyncio
async def test_fetch_view_counts_handles_invalid_count() -> None:
    payload = {
        "items": [
            {"id": "aaaaaaaaaaa", "statistics": {"viewCount": "not-a-number"}},
            {"id": "bbbbbbbbbbb", "statistics": {"viewCount": "42"}},
            {"id": "ccccccccccc", "statistics": {}},  # missing viewCount
        ]
    }

    async def _get(*args, **kwargs):
        return _MockResponse(payload)

    mock_client = AsyncMock()
    mock_client.get = _get
    mock_client.__aenter__.return_value = mock_client

    with (
        patch("app.services.youtube_stats.httpx.AsyncClient", return_value=mock_client),
        patch("app.services.youtube_stats.get_settings") as mock_settings,
    ):
        mock_settings.return_value.youtube_api_key = "test_key"
        result = await fetch_view_counts(["aaaaaaaaaaa", "bbbbbbbbbbb", "ccccccccccc"])

    assert result == {"bbbbbbbbbbb": 42}


def test_target_is_frozen_dataclass() -> None:
    t = _Target(source="game_live", ref_id=1, yt_id="aaaaaaaaaaa")
    with pytest.raises(Exception):
        t.ref_id = 2  # type: ignore[misc]
