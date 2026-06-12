"""Duplicate guard for fcms_bulk_import game creation.

Regression for game 1788 (2026-06-11): FCMS operator created the Turan—Kaspiy
women's match with a wrong round number (1101538, tour 2), our sync saw no game
on that date and created a duplicate, then the operator deleted the FCMS match
and re-created it as 1101562. The guard must catch a same-pairing game nearby
instead of creating a twin.
"""

from datetime import date
from types import SimpleNamespace

from scripts.fcms_bulk_import import DUPLICATE_GUARD_WINDOW_DAYS, find_duplicate_twin

SEASON = 205
TURAN = 665
KASPIY = 5059


def _game(home_id, away_id, game_date, season_id=SEASON, game_id=1):
    return SimpleNamespace(
        id=game_id,
        season_id=season_id,
        home_team_id=home_id,
        away_team_id=away_id,
        date=game_date,
    )


def test_same_pairing_same_date_is_twin():
    existing = _game(TURAN, KASPIY, date(2026, 6, 29), game_id=1664)
    twin = find_duplicate_twin([existing], SEASON, TURAN, KASPIY, date(2026, 6, 29))
    assert twin is existing


def test_same_pairing_within_window_is_twin():
    # The real incident: existing tour-8 game still on 06-15, FCMS twin on 06-29
    existing = _game(TURAN, KASPIY, date(2026, 6, 15), game_id=1664)
    twin = find_duplicate_twin([existing], SEASON, TURAN, KASPIY, date(2026, 6, 29))
    assert twin is existing


def test_same_pairing_outside_window_is_not_twin():
    beyond = date(2026, 6, 29 - (DUPLICATE_GUARD_WINDOW_DAYS + 1))
    existing = _game(TURAN, KASPIY, beyond, game_id=1664)
    assert find_duplicate_twin([existing], SEASON, TURAN, KASPIY, date(2026, 6, 29)) is None


def test_reverse_fixture_is_not_twin():
    # Second-leg pairing (Kaspiy home) is a legitimate separate fixture
    existing = _game(KASPIY, TURAN, date(2026, 6, 29), game_id=1744)
    assert find_duplicate_twin([existing], SEASON, TURAN, KASPIY, date(2026, 6, 29)) is None


def test_other_season_is_not_twin():
    existing = _game(TURAN, KASPIY, date(2026, 6, 29), season_id=200)
    assert find_duplicate_twin([existing], SEASON, TURAN, KASPIY, date(2026, 6, 29)) is None


def test_game_without_date_is_skipped():
    existing = _game(TURAN, KASPIY, None)
    assert find_duplicate_twin([existing], SEASON, TURAN, KASPIY, date(2026, 6, 29)) is None
