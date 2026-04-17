"""Tests for pick_default_season — player/team default season resolution."""

from app.services.default_season import pick_default_season


def test_empty_returns_none():
    assert pick_default_season([]) is None


def test_single_entry():
    assert pick_default_season([(200, 2026, "pl")]) == 200


def test_pl_2025_vs_1l_2026_picks_1l_2026():
    # Player was in PL 2025, moved to 1L 2026 — default should be the newer 1L season.
    entries = [(61, 2025, "pl"), (204, 2026, "1l")]
    assert pick_default_season(entries) == 204


def test_pl_2026_wins_over_cup_2026():
    # Within the same year, league beats cup by priority.
    entries = [(202, 2026, "cup"), (200, 2026, "pl")]
    assert pick_default_season(entries) == 200


def test_priority_order_within_year():
    # pl > 1l > 2l > el > cup
    entries = [
        (205, 2026, "el"),
        (203, 2026, "2l"),
        (204, 2026, "1l"),
        (202, 2026, "cup"),
        (200, 2026, "pl"),
    ]
    assert pick_default_season(entries) == 200

    # Without pl: 1l wins
    entries_no_pl = [
        (205, 2026, "el"),
        (203, 2026, "2l"),
        (204, 2026, "1l"),
        (202, 2026, "cup"),
    ]
    assert pick_default_season(entries_no_pl) == 204

    # Without pl/1l: 2l wins
    entries_no_1l = [
        (205, 2026, "el"),
        (203, 2026, "2l"),
        (202, 2026, "cup"),
    ]
    assert pick_default_season(entries_no_1l) == 203

    # Cup only if nothing else
    entries_cup_only = [(202, 2026, "cup")]
    assert pick_default_season(entries_cup_only) == 202


def test_unknown_frontend_code_goes_last():
    # Unknown codes (e.g., supercup/null) rank after known tournaments.
    entries = [(300, 2026, "sc"), (200, 2026, "pl")]
    assert pick_default_season(entries) == 200


def test_null_frontend_code_goes_last():
    entries = [(300, 2026, None), (200, 2026, "pl")]
    assert pick_default_season(entries) == 200


def test_year_beats_priority():
    # Newer year wins even if tournament has lower priority.
    entries = [(61, 2025, "pl"), (205, 2026, "el")]
    assert pick_default_season(entries) == 205


def test_null_year_treated_as_very_old():
    # A season without a year is the lowest priority — known-year seasons win.
    entries = [(61, 2025, "pl"), (999, None, "pl")]
    assert pick_default_season(entries) == 61


def test_tiebreak_by_season_id_desc():
    # Same year, same priority — newer id wins.
    entries = [(61, 2025, "pl"), (60, 2025, "pl")]
    assert pick_default_season(entries) == 61
