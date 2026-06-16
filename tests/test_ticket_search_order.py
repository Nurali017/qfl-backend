"""Tests for home/away ordering in ticketon ticket URL extraction."""

from datetime import date

from app.services.ticket_search import (
    _extract_ticket_urls,
    _home_before_away_in_slug,
)


def test_home_before_away_basic():
    assert _home_before_away_in_slug("Актобе", "Астана", "/event/fk-aktobe-vs-fk-astana")
    assert not _home_before_away_in_slug("Актобе", "Астана", "/event/fk-astana-vs-fk-aktobe")


def test_reversed_fixture_rejected():
    """The reported bug: Aktobe home, but URL has Astana first → reject."""
    organic = [{
        "link": "https://ticketon.kz/event/fk-astana-vs-fk-aktobe",
        "title": "ФК Астана - ФК Актобе",
        "snippet": "21 июня 2026",
    }]
    matches = _extract_ticket_urls(organic, "Актобе", "Астана", date(2026, 6, 21))
    assert matches == []


def test_correct_fixture_accepted():
    organic = [{
        "link": "https://ticketon.kz/event/fk-aktobe-vs-fk-astana",
        "title": "ФК Актобе - ФК Астана",
        "snippet": "21 июня 2026",
    }]
    matches = _extract_ticket_urls(organic, "Актобе", "Астана", date(2026, 6, 21))
    assert len(matches) == 1
