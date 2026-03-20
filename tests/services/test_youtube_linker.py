"""Tests for YouTube auto-linker service."""

import pytest

from app.services.youtube_linker import (
    classify_video,
    parse_video_title,
    _get_match_date,
    _roman_to_int,
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
