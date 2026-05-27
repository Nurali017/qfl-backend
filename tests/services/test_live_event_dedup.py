"""Dedup logic for live-event sync.

Regression tests for the bug where the post-match SOTA sync created duplicate
events on top of operator-entered (manual) ones because the matcher (a) only
compared against existing SOTA events and (b) required an exact minute, while
operators frequently type a minute that is off by one from SOTA.
"""

from app.services.live_sync_service import (
    LiveSyncService,
    MANUAL_MINUTE_TOLERANCE,
)


def _sig(event_type, half, minute, player_id, player_name=""):
    return LiveSyncService._event_signature(event_type, half, minute, player_id, player_name)


def test_exact_match_by_player_id():
    a = _sig("goal", 1, 50, 645)
    b = _sig("goal", 1, 50, 645)
    assert LiveSyncService._signatures_match(a, b)


def test_no_tolerance_keeps_sota_minute_exact():
    """SOTA-to-SOTA reconciliation must still require an exact minute."""
    a = _sig("yellow_card", 1, 15, 1094)
    b = _sig("yellow_card", 1, 16, 1094)
    assert not LiveSyncService._signatures_match(a, b)


def test_manual_off_by_one_matches_with_tolerance():
    """Operator typed 15', SOTA reports 16' — same player → must match."""
    manual = _sig("yellow_card", 1, 15, 1094, "Шмидт Дмитрий")
    sota = _sig("yellow_card", 1, 16, 1094, "Дмитрий Шмидт")
    assert LiveSyncService._signatures_match(
        sota, manual, minute_tolerance=MANUAL_MINUTE_TOLERANCE
    )


def test_tolerance_does_not_match_far_apart_minutes():
    a = _sig("yellow_card", 1, 15, 1094)
    b = _sig("yellow_card", 1, 20, 1094)
    assert not LiveSyncService._signatures_match(
        a, b, minute_tolerance=MANUAL_MINUTE_TOLERANCE
    )


def test_different_players_same_minute_do_not_match():
    """Two different players carded in the same minute must stay distinct."""
    a = _sig("yellow_card", 1, 45, 381)
    b = _sig("yellow_card", 1, 45, 1515)
    assert not LiveSyncService._signatures_match(
        a, b, minute_tolerance=MANUAL_MINUTE_TOLERANCE
    )


def test_different_half_never_matches():
    a = _sig("goal", 1, 45, 465)
    b = _sig("goal", 2, 45, 465)
    assert not LiveSyncService._signatures_match(
        a, b, minute_tolerance=MANUAL_MINUTE_TOLERANCE
    )


def test_different_event_type_never_matches():
    a = _sig("goal", 1, 45, 465)
    b = _sig("penalty", 1, 45, 465)
    assert not LiveSyncService._signatures_match(
        a, b, minute_tolerance=MANUAL_MINUTE_TOLERANCE
    )


def test_falls_back_to_name_when_player_id_missing():
    """One side lacks player_id but names are identical → match by name."""
    a = _sig("goal", 1, 29, None, "Климович Владислав")
    b = _sig("goal", 1, 29, 2600, "климович владислав")
    assert LiveSyncService._signatures_match(a, b)


def test_no_match_when_player_id_missing_and_names_differ():
    """Both anchors weak (no shared id, different name) → must NOT merge."""
    a = _sig("goal", 1, 29, None, "Смитх Медина Ариагнер Стивен")
    b = _sig("goal", 1, 29, None, "Ариагнер Стивен Смит Медина")
    assert not LiveSyncService._signatures_match(
        a, b, minute_tolerance=MANUAL_MINUTE_TOLERANCE
    )
