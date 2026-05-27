"""Tests for FCMS roster deregistration detection (_is_deregistered)."""
from __future__ import annotations

from app.services.fcms_roster_sync import _is_deregistered


def test_active_player_with_number_is_kept():
    fp = {"jerseyNumber": "9", "isActiveInTeam": True}
    assert _is_deregistered(fp) is False


def test_missing_jersey_number_is_deregistered():
    fp = {"jerseyNumber": "", "isActiveInTeam": True}
    assert _is_deregistered(fp) is True


def test_none_jersey_number_is_deregistered():
    fp = {"isActiveInTeam": True}
    assert _is_deregistered(fp) is True


def test_inactive_in_team_with_number_is_deregistered():
    # Real case: Еркен kept in Кайсар list with №19 after transfer to Туран.
    fp = {"jerseyNumber": "19", "isActiveInTeam": False}
    assert _is_deregistered(fp) is True


def test_active_flag_absent_keeps_numbered_player():
    # Older FCMS rows may omit isActiveInTeam; absence must not deregister.
    fp = {"jerseyNumber": "7"}
    assert _is_deregistered(fp) is False
