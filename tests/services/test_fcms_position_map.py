"""Tests for FCMS playerPositionId → position name mapping."""
from __future__ import annotations

import logging

import pytest

from app.services.fcms_roster_sync import _FCMS_POSITION_MAP, _resolve_position


def test_resolve_goalkeeper():
    assert _resolve_position(117) == ("Вратарь", "Қақпашы", "Goalkeeper")


def test_resolve_defender():
    assert _resolve_position(118) == ("Защитник", "Қорғаушы", "Defender")


def test_resolve_midfielder():
    assert _resolve_position(119) == ("Полузащитник", "Жартылай қорғаушы", "Midfielder")


def test_resolve_forward():
    assert _resolve_position(120) == ("Нападающий", "Шабуылшы", "Forward")


def test_resolve_none_returns_none():
    assert _resolve_position(None) is None


def test_resolve_unknown_logs_warning_and_returns_none(caplog):
    with caplog.at_level(logging.WARNING):
        result = _resolve_position(999)
    assert result is None
    assert any("Unknown FCMS playerPositionId=999" in r.message for r in caplog.records)


def test_map_covers_all_football_positions():
    """Sanity check: map must cover all 4 football positions from FCMS."""
    assert set(_FCMS_POSITION_MAP.keys()) == {117, 118, 119, 120}
