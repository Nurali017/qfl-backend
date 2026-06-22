"""Tests for club-website ticket-link extraction (asset-URL rejection)."""

from datetime import date

import pytest

from app.services.ticket_search import (
    _find_ticket_url_on_website,
    _is_asset_url,
)


class _FakeResponse:
    def __init__(self, text: str, status_code: int = 200):
        self.text = text
        self.status_code = status_code


class _FakeClient:
    """Minimal async client returning the same HTML for any GET."""

    def __init__(self, html: str):
        self._html = html

    async def get(self, url, **kwargs):
        return _FakeResponse(self._html)


def test_is_asset_url_rejects_plugin_css():
    # The exact URL the reported bug surfaced.
    assert _is_asset_url(
        "https://fcjetisu.kz/wp-content/plugins/event-tickets/build/css/tickets.css?ver=5.27.3"
    )
    assert _is_asset_url("https://fcjetisu.kz/static/app.js")
    assert _is_asset_url("https://fcjetisu.kz/img/logo.png")


def test_is_asset_url_allows_real_pages():
    assert not _is_asset_url("https://tickets.fcjetisu.kz/")
    assert not _is_asset_url("https://fcjetisu.kz/bilety/zhetysu-aktobe")


@pytest.mark.asyncio
async def test_plugin_css_not_returned_as_ticket_link():
    """The reported bug: tickets.css must not be returned as the ticket URL."""
    html = (
        "<html><head>"
        '<link rel="stylesheet" '
        'href="https://fcjetisu.kz/wp-content/plugins/event-tickets/build/css/tickets.css?ver=5.27.3">'
        "</head><body>"
        "Жетысу — Актобе, 27 июня 2026"
        "</body></html>"
    )
    url = await _find_ticket_url_on_website(
        "https://fcjetisu.kz", "Жетысу", "Актобе", date(2026, 6, 27),
        _FakeClient(html),
    )
    assert url is None


@pytest.mark.asyncio
async def test_real_ticket_link_still_found():
    """A genuine ticket link on the page is still extracted."""
    html = (
        "<html><head>"
        '<link rel="stylesheet" '
        'href="https://fcjetisu.kz/wp-content/plugins/event-tickets/build/css/tickets.css?ver=5.27.3">'
        "</head><body>"
        "Жетысу — Актобе, 27 июня 2026 "
        '<a href="https://tickets.fcjetisu.kz/event/zhetysu-aktobe">Купить билеты</a>'
        "</body></html>"
    )
    url = await _find_ticket_url_on_website(
        "https://fcjetisu.kz", "Жетысу", "Актобе", date(2026, 6, 27),
        _FakeClient(html),
    )
    assert url == "https://tickets.fcjetisu.kz/event/zhetysu-aktobe"
