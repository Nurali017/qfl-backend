"""Tests for TeamNameMatcher, focused on women's/regional gender-marker
tolerance.

Broadcast feeds (SOTA) append a one-letter gender marker to women's sides —
"Тұран Ә", "Каспий Ә" — while the stored team name may omit it ("Туран") or
spell it with a different letter ("Каспий Ж"). Without reconciliation the live
event sync cannot resolve team_id from the team name and falls back to player
resolution alone; any player-name mismatch then leaves the goal's team_id NULL
and the goal renders on the wrong side. See match 1664 (Туран Ж — Каспий Ж).
"""
from types import SimpleNamespace

from app.utils.team_name_matcher import TeamNameMatcher, normalize_team_name


def _game(home_name, away_name, *, home_id=665, away_id=5059,
          home_kz=None, away_kz=None):
    return SimpleNamespace(
        home_team_id=home_id,
        away_team_id=away_id,
        home_team=SimpleNamespace(name=home_name, name_kz=home_kz, name_en=None),
        away_team=SimpleNamespace(name=away_name, name_kz=away_kz, name_en=None),
    )


def test_women_marker_resolves_home_without_stored_marker():
    """Match 1664: stored home "Туран", SOTA sends "Тұран Ә"."""
    matcher = TeamNameMatcher.from_game(_game("Туран", "Каспий Ж"))
    assert matcher.match("Тұран Ә") == 665


def test_women_marker_resolves_away_with_different_stored_marker():
    """Stored away "Каспий Ж" (Ж), SOTA sends "Каспий Ә" (Ә)."""
    matcher = TeamNameMatcher.from_game(_game("Туран", "Каспий Ж"))
    assert matcher.match("Каспий Ә") == 5059


def test_exact_name_still_matches():
    matcher = TeamNameMatcher.from_game(_game("Туран", "Каспий Ж"))
    assert matcher.match("Туран") == 665


def test_stored_marker_alias_still_matches():
    """Stored-side trailing-marker stripping (the pre-existing behavior)."""
    matcher = TeamNameMatcher.from_game(_game("Туран", "Каспий Ж"))
    assert matcher.match("Каспий") == 5059


def test_fc_prefix_alias_still_matches():
    matcher = TeamNameMatcher.from_game(_game("ФК Туран", "Каспий Ж"))
    assert matcher.match("Туран") == 665


def test_unknown_team_returns_none():
    matcher = TeamNameMatcher.from_game(_game("Туран", "Каспий Ж"))
    assert matcher.match("Кайрат Ә") is None


def test_marker_collision_is_ambiguous():
    """Both sides reduce to the same stripped form -> refuse to guess."""
    matcher = TeamNameMatcher.from_game(_game("Барыс Ә", "Барыс М"))
    assert matcher.match("Барыс Ж") is None


def test_normalize_folds_kazakh_letters():
    # ұ->у, ә->а, and й->и are all folded by the translation table.
    assert normalize_team_name("Тұран Ә") == "туран а"
    assert normalize_team_name("Каспий Ж") == "каспии ж"
