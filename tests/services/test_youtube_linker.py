"""Tests for YouTube auto-linker service."""

from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from app.services.youtube_linker import (
    classify_video,
    parse_video_title,
    _get_match_date,
    _roman_to_int,
    ParsedTitle,
    PendingGameIndex,
)


def _make_team(name: str, name_kz: str | None = None, name_en: str | None = None):
    return SimpleNamespace(name=name, name_kz=name_kz or name, name_en=name_en)


def _make_game(
    id: int,
    home_name: str,
    away_name: str,
    game_date: date,
    tour: int | None = None,
    youtube_live_url: str | None = None,
    video_review_url: str | None = None,
    home_name_kz: str | None = None,
    away_name_kz: str | None = None,
    home_name_en: str | None = None,
    away_name_en: str | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        id=id,
        date=game_date,
        tour=tour,
        home_team=_make_team(home_name, home_name_kz, home_name_en),
        away_team=_make_team(away_name, away_name_kz, away_name_en),
        youtube_live_url=youtube_live_url,
        video_review_url=video_review_url,
    )


# ── Classification tests ──


class TestClassifyVideo:
    def test_upcoming_is_live(self):
        snippet = {"liveBroadcastContent": "upcoming", "title": "Match"}
        assert classify_video(snippet, {"scheduledStartTime": "2026-03-20T15:00:00Z"}) == "live"

    def test_active_live_is_live(self):
        snippet = {"liveBroadcastContent": "live", "title": "Match"}
        assert classify_video(snippet, {"actualStartTime": "2026-03-20T15:00:00Z"}) == "live"

    def test_completed_broadcast_is_replay(self):
        snippet = {"liveBroadcastContent": "none", "title": "Match"}
        lsd = {"actualStartTime": "2026-03-20T15:00:00Z", "actualEndTime": "2026-03-20T17:00:00Z"}
        assert classify_video(snippet, lsd) == "replay"

    def test_regular_with_review_keyword_ru(self):
        snippet = {"liveBroadcastContent": "none", "title": "ОБЗОР | Ертіс - Ұлытау"}
        assert classify_video(snippet, None) == "review"

    def test_regular_with_review_keyword_kz(self):
        snippet = {"liveBroadcastContent": "none", "title": "Шолу | Ертіс - Ұлытау"}
        assert classify_video(snippet, None) == "review"

    def test_regular_with_highlights(self):
        snippet = {"liveBroadcastContent": "none", "title": "Highlights | Team A vs Team B"}
        assert classify_video(snippet, None) == "review"

    def test_regular_no_keyword_skipped(self):
        snippet = {"liveBroadcastContent": "none", "title": "Channel trailer 2026"}
        assert classify_video(snippet, None) is None

    def test_missing_broadcast_content_defaults_none(self):
        snippet = {"title": "Some video"}
        assert classify_video(snippet, None) is None


# ── Roman numeral tests ──


class TestRomanToInt:
    def test_basic(self):
        assert _roman_to_int("I") == 1
        assert _roman_to_int("II") == 2
        assert _roman_to_int("III") == 3
        assert _roman_to_int("IV") == 4
        assert _roman_to_int("V") == 5
        assert _roman_to_int("IX") == 9
        assert _roman_to_int("X") == 10
        assert _roman_to_int("XII") == 12

    def test_case_insensitive(self):
        assert _roman_to_int("iii") == 3
        assert _roman_to_int("Iv") == 4

    def test_invalid(self):
        assert _roman_to_int("") is None
        assert _roman_to_int("ABC") is None


# ── Title parsing tests ──


class TestParseVideoTitle:
    def test_standard_with_tour(self):
        result = parse_video_title("Ертіс - Ұлытау | Тур 5")
        assert result is not None
        assert result.team_a == "Ертіс"
        assert result.team_b == "Ұлытау"
        assert result.tour == 5

    def test_review_prefix(self):
        result = parse_video_title("ОБЗОР | Ertis vs Ulytau")
        assert result is not None
        assert result.team_a == "Ertis"
        assert result.team_b == "Ulytau"
        assert result.tour is None

    def test_em_dash_separator(self):
        result = parse_video_title("Team A — Team B")
        assert result is not None
        assert result.team_a == "Team A"
        assert result.team_b == "Team B"

    def test_en_dash_separator(self):
        result = parse_video_title("Team A – Team B | Тур 3")
        assert result is not None
        assert result.team_a == "Team A"
        assert result.team_b == "Team B"
        assert result.tour == 3

    def test_tour_first(self):
        result = parse_video_title("Тур 12 | Кайрат - Астана")
        assert result is not None
        assert result.team_a == "Кайрат"
        assert result.team_b == "Астана"
        assert result.tour == 12

    def test_kz_tour(self):
        result = parse_video_title("Тұр 7 | Атырау - Ордабасы")
        assert result is not None
        assert result.tour == 7

    def test_no_teams_returns_none(self):
        result = parse_video_title("Тур 5 highlights")
        assert result is None

    def test_single_word_returns_none(self):
        result = parse_video_title("Trailer")
        assert result is None

    def test_empty_string(self):
        result = parse_video_title("")
        assert result is None

    def test_multiple_pipes_with_extra_segments(self):
        result = parse_video_title("ПФЛ | Ертіс - Ұлытау | Тур 5 | 2026")
        assert result is not None
        assert result.team_a == "Ертіс"
        assert result.team_b == "Ұлытау"
        assert result.tour == 5

    def test_vs_case_insensitive(self):
        result = parse_video_title("Team A VS Team B")
        assert result is not None
        assert result.team_a == "Team A"
        assert result.team_b == "Team B"

    # ── Real KFF League title formats ──

    def test_real_kpl_format_with_roman_tour(self):
        """АТЫРАУ VS ЖЕҢІС | ҚПЛ - 2026 | III тур"""
        result = parse_video_title("АТЫРАУ VS ЖЕҢІС | ҚПЛ - 2026 | III тур")
        assert result is not None
        assert result.team_a == "АТЫРАУ"
        assert result.team_b == "ЖЕҢІС"
        assert result.tour == 3

    def test_real_kpl_astana_tobyl(self):
        result = parse_video_title("АСТАНА VS ТОБЫЛ | ҚПЛ - 2026 | III тур")
        assert result is not None
        assert result.team_a == "АСТАНА"
        assert result.team_b == "ТОБЫЛ"
        assert result.tour == 3

    def test_real_kpl_ordabasy_kaisar(self):
        result = parse_video_title("ОРДАБАСЫ VS ҚАЙСАР | ҚПЛ - 2026 | III тур")
        assert result is not None
        assert result.team_a == "ОРДАБАСЫ"
        assert result.team_b == "ҚАЙСАР"
        assert result.tour == 3

    def test_real_kpl_tour_2(self):
        result = parse_video_title("КАСПИЙ VS ОҚЖЕТПЕС | ҚПЛ - 2026 | II тур")
        assert result is not None
        assert result.team_a == "КАСПИЙ"
        assert result.team_b == "ОҚЖЕТПЕС"
        assert result.tour == 2

    def test_real_review_with_kazakh_separator(self):
        """Шолу І Каспий - Оқжетпес І ҚПЛ II - тур"""
        result = parse_video_title("Шолу \u0406 Каспий - Оқжетпес \u0406 ҚПЛ II - тур")
        assert result is not None
        assert result.team_a == "Каспий"
        assert result.team_b == "Оқжетпес"
        assert result.tour == 2

    def test_real_live_upcoming(self):
        result = parse_video_title("АЛТАЙ ӨСКЕМЕН VS КАСПИЙ | ҚПЛ - 2026 | III тур")
        assert result is not None
        assert result.team_a == "АЛТАЙ ӨСКЕМЕН"
        assert result.team_b == "КАСПИЙ"
        assert result.tour == 3

    def test_goal_clip_not_parsed(self):
        """Goal clips should not parse as match titles."""
        assert parse_video_title("Алибек Касымның голы!") is None
        assert parse_video_title("Гол! Иван Башич есеп ашты!") is None

    def test_best_goals_compilation_no_teams(self):
        """Best goals of tour - no team separator."""
        result = parse_video_title("II турдың үздік голдары | ҚПЛ - 2026")
        assert result is None

    # ── Pipe-separated team names (fallback) ──

    def test_pipe_separated_review_with_cup_suffix(self):
        """Шолу | Желаев Нан | Алтай Өскемен ҚАЗАҚСТАН КУБОГЫ | 1/16 ФИНАЛ"""
        result = parse_video_title(
            "Шолу | Желаев Нан | Алтай Өскемен ҚАЗАҚСТАН КУБОГЫ | 1/16 ФИНАЛ"
        )
        assert result is not None
        assert result.team_a == "Желаев Нан"
        assert result.team_b == "Алтай Өскемен"
        assert result.tour is None

    def test_pipe_separated_review_simple(self):
        """Обзор | Team A | Team B"""
        result = parse_video_title("Обзор | Team A | Team B")
        assert result is not None
        assert result.team_a == "Team A"
        assert result.team_b == "Team B"

    def test_cup_match_with_round(self):
        """ЖЕЛАЕВ НАН VS АЛТАЙ ӨСКЕМЕН | OLIMPBET ҚАЗАҚСТАН КУБОГЫ 2026 | 1/16 ФИНАЛ"""
        result = parse_video_title(
            "ЖЕЛАЕВ НАН VS АЛТАЙ ӨСКЕМЕН | OLIMPBET ҚАЗАҚСТАН КУБОГЫ 2026 | 1/16 ФИНАЛ"
        )
        assert result is not None
        assert result.team_a == "ЖЕЛАЕВ НАН"
        assert result.team_b == "АЛТАЙ ӨСКЕМЕН"
        assert result.tour is None

    def test_first_league_format(self):
        """КАСПИЙ М VS ҚАЙРАТ Ж | БІРІНШІ ЛИГА - 2026 | II тур"""
        result = parse_video_title(
            "КАСПИЙ М VS ҚАЙРАТ Ж | БІРІНШІ ЛИГА - 2026 | II тур"
        )
        assert result is not None
        assert result.team_a == "КАСПИЙ М"
        assert result.team_b == "ҚАЙРАТ Ж"
        assert result.tour == 2


# ── Date extraction tests ──


class TestGetMatchDate:
    def test_live_uses_scheduled_start(self):
        lsd = {"scheduledStartTime": "2026-03-20T15:00:00Z"}
        snippet = {"publishedAt": "2026-03-19T10:00:00Z"}
        from datetime import date
        assert _get_match_date("live", snippet, lsd) == date(2026, 3, 20)

    def test_replay_prefers_actual_start(self):
        lsd = {
            "scheduledStartTime": "2026-03-20T15:00:00Z",
            "actualStartTime": "2026-03-20T15:30:00Z",
        }
        snippet = {"publishedAt": "2026-03-19T10:00:00Z"}
        from datetime import date
        assert _get_match_date("replay", snippet, lsd) == date(2026, 3, 20)

    def test_review_uses_published_at(self):
        snippet = {"publishedAt": "2026-03-21T08:00:00Z"}
        from datetime import date
        assert _get_match_date("review", snippet, None) == date(2026, 3, 21)

    def test_fallback_to_published_at(self):
        snippet = {"publishedAt": "2026-03-20T10:00:00Z"}
        from datetime import date
        assert _get_match_date("live", snippet, None) == date(2026, 3, 20)

    def test_no_date_available(self):
        assert _get_match_date("live", {}, None) is None


# ── PendingGameIndex tests ──


class TestPendingGameIndex:
    """Tests for the game-first matching index."""

    def _game(self, **kw):
        defaults = dict(
            id=1, home_name="Тобыл", away_name="Кайрат",
            game_date=date(2026, 4, 11), tour=5,
        )
        defaults.update(kw)
        return _make_game(**defaults)

    # ── build() ──

    def test_build_entry_count(self):
        games = [self._game(id=1), self._game(id=2)]
        index = PendingGameIndex.build(games)
        assert len(index._entries) == 2

    def test_build_empty(self):
        index = PendingGameIndex.build([])
        assert len(index._entries) == 0

    def test_build_needs_flags_all_null(self):
        g = self._game()
        index = PendingGameIndex.build([g])
        e = index._entries[0]
        assert e.needs_live is True
        assert e.needs_review is True

    def test_build_needs_flags_all_set(self):
        g = self._game(
            youtube_live_url="https://...",
            video_review_url="https://...",
        )
        index = PendingGameIndex.build([g])
        e = index._entries[0]
        assert e.needs_live is False
        assert e.needs_review is False

    def test_build_contains_normalized_names(self):
        g = self._game(home_name="Тобыл", home_name_kz="Тобыл")
        index = PendingGameIndex.build([g])
        e = index._entries[0]
        # "тобыл" after normalize (й→и not relevant here)
        assert "тобыл" in e.home_names

    def test_build_contains_compact_form(self):
        g = self._game(away_name="Кызыл Жар", away_name_kz="Қызыл Жар")
        index = PendingGameIndex.build([g])
        e = index._entries[0]
        # compact: remove spaces
        assert any(" " not in n and "жар" in n for n in e.away_names)

    def test_build_contains_abbreviation(self):
        g = self._game(away_name="Кайрат-Жастар", away_name_kz="Қайрат-Жастар")
        index = PendingGameIndex.build([g])
        e = index._entries[0]
        # "каират жастар" → abbreviation "каират ж"
        assert "каират ж" in e.away_names

    # ── find_match() — team matching ──

    def test_find_match_forward(self):
        """team_a=home, team_b=away → match."""
        g = self._game(home_name="Тобыл", away_name="Кайрат", away_name_kz="Қайрат")
        index = PendingGameIndex.build([g])
        parsed = ParsedTitle(team_a="Тобыл", team_b="Қайрат", tour=None)
        result = index.find_match(parsed, date(2026, 4, 11), "live")
        assert result is not None
        assert result.id == g.id

    def test_find_match_reverse(self):
        """team_a=away, team_b=home → still match."""
        g = self._game(home_name="Тобыл", away_name="Кайрат", away_name_kz="Қайрат")
        index = PendingGameIndex.build([g])
        parsed = ParsedTitle(team_a="Қайрат", team_b="Тобыл", tour=None)
        result = index.find_match(parsed, date(2026, 4, 11), "live")
        assert result is not None
        assert result.id == g.id

    def test_find_match_abbreviation(self):
        """'ҚАЙРАТ Ж' matches 'Кайрат-Жастар' via abbreviation."""
        g = self._game(
            away_name="Кайрат-Жастар", away_name_kz="Қайрат-Жастар",
            home_name="Каспий М",
        )
        index = PendingGameIndex.build([g])
        parsed = ParsedTitle(team_a="КАСПИЙ М", team_b="ҚАЙРАТ Ж", tour=None)
        result = index.find_match(parsed, date(2026, 4, 11), "live")
        assert result is not None
        assert result.id == g.id

    def test_find_match_compact(self):
        """'кызылжар' matches 'Кызыл Жар' via compact form."""
        g = self._game(home_name="Кызыл Жар", away_name="Женис")
        index = PendingGameIndex.build([g])
        parsed = ParsedTitle(team_a="Кызылжар", team_b="Женис", tour=None)
        result = index.find_match(parsed, date(2026, 4, 11), "live")
        assert result is not None

    def test_find_match_word_set_reorder(self):
        """'Академия Онтустик' matches 'Онтустик Академия'."""
        g = self._game(home_name="Онтустик Академия", away_name="Тобыл")
        index = PendingGameIndex.build([g])
        parsed = ParsedTitle(team_a="Академия Онтустик", team_b="Тобыл", tour=None)
        result = index.find_match(parsed, date(2026, 4, 11), "live")
        assert result is not None

    def test_find_match_no_team_match(self):
        g = self._game(home_name="Тобыл", away_name="Кайрат")
        index = PendingGameIndex.build([g])
        parsed = ParsedTitle(team_a="Астана", team_b="Ордабасы", tour=None)
        result = index.find_match(parsed, date(2026, 4, 11), "live")
        assert result is None

    def test_find_match_empty_index(self):
        index = PendingGameIndex.build([])
        parsed = ParsedTitle(team_a="Тобыл", team_b="Кайрат", tour=None)
        assert index.find_match(parsed, date(2026, 4, 11), "live") is None

    # ── find_match() — date tolerance ──

    def test_find_match_date_within_tolerance_live(self):
        """±1 day for live → match at +1."""
        g = self._game(game_date=date(2026, 4, 11))
        index = PendingGameIndex.build([g])
        parsed = ParsedTitle(team_a="Тобыл", team_b="Кайрат", tour=None)
        result = index.find_match(parsed, date(2026, 4, 12), "live")
        assert result is not None

    def test_find_match_date_outside_tolerance_live(self):
        """±1 day for live → no match at +2."""
        g = self._game(game_date=date(2026, 4, 11))
        index = PendingGameIndex.build([g])
        parsed = ParsedTitle(team_a="Тобыл", team_b="Кайрат", tour=None)
        result = index.find_match(parsed, date(2026, 4, 13), "live")
        assert result is None

    def test_find_match_date_within_tolerance_review(self):
        """±2 days for review → match at +2."""
        g = self._game(game_date=date(2026, 4, 11))
        index = PendingGameIndex.build([g])
        parsed = ParsedTitle(team_a="Тобыл", team_b="Кайрат", tour=None)
        result = index.find_match(parsed, date(2026, 4, 13), "review")
        assert result is not None

    def test_find_match_date_outside_tolerance_review(self):
        """±2 days for review → no match at +3."""
        g = self._game(game_date=date(2026, 4, 11))
        index = PendingGameIndex.build([g])
        parsed = ParsedTitle(team_a="Тобыл", team_b="Кайрат", tour=None)
        result = index.find_match(parsed, date(2026, 4, 14), "review")
        assert result is None

    # ── find_match() — needs_type filtering ──

    def test_find_match_skip_live_when_already_set(self):
        g = self._game(youtube_live_url="https://already.set")
        index = PendingGameIndex.build([g])
        parsed = ParsedTitle(team_a="Тобыл", team_b="Кайрат", tour=None)
        assert index.find_match(parsed, date(2026, 4, 11), "live") is None

    def test_find_match_skip_replay_when_live_already_set(self):
        """Replay sets youtube_live_url → skip if already set."""
        g = self._game(youtube_live_url="https://already.set")
        index = PendingGameIndex.build([g])
        parsed = ParsedTitle(team_a="Тобыл", team_b="Кайрат", tour=None)
        assert index.find_match(parsed, date(2026, 4, 11), "replay") is None

    def test_find_match_skip_review_when_already_set(self):
        g = self._game(video_review_url="https://already.set")
        index = PendingGameIndex.build([g])
        parsed = ParsedTitle(team_a="Тобыл", team_b="Кайрат", tour=None)
        assert index.find_match(parsed, date(2026, 4, 11), "review") is None

    def test_find_match_allows_review_when_live_set(self):
        """Game has live URL but needs review → match for review type."""
        g = self._game(youtube_live_url="https://live.set")
        index = PendingGameIndex.build([g])
        parsed = ParsedTitle(team_a="Тобыл", team_b="Кайрат", tour=None)
        result = index.find_match(parsed, date(2026, 4, 11), "review")
        assert result is not None

    # ── find_match() — tour disambiguation ──

    def test_find_match_tour_disambiguates(self):
        """Two games same teams, different tours → tour selects the right one."""
        g1 = self._game(id=1, tour=4, game_date=date(2026, 4, 11))
        g2 = self._game(id=2, tour=5, game_date=date(2026, 4, 11))
        index = PendingGameIndex.build([g1, g2])
        parsed = ParsedTitle(team_a="Тобыл", team_b="Кайрат", tour=5)
        result = index.find_match(parsed, date(2026, 4, 11), "live")
        assert result is not None
        assert result.id == 2

    def test_find_match_single_candidate_ignores_tour_mismatch(self):
        """One candidate, tour mismatch → still match (YouTube title may be wrong)."""
        g = self._game(tour=5)
        index = PendingGameIndex.build([g])
        parsed = ParsedTitle(team_a="Тобыл", team_b="Кайрат", tour=4)
        result = index.find_match(parsed, date(2026, 4, 11), "live")
        assert result is not None
        assert result.id == g.id

    def test_find_match_ambiguous_no_tour(self):
        """Two candidates, no tour info → None (ambiguous)."""
        g1 = self._game(id=1, game_date=date(2026, 4, 11))
        g2 = self._game(id=2, game_date=date(2026, 4, 12))
        index = PendingGameIndex.build([g1, g2])
        parsed = ParsedTitle(team_a="Тобыл", team_b="Кайрат", tour=None)
        result = index.find_match(parsed, date(2026, 4, 11), "live")
        assert result is None
