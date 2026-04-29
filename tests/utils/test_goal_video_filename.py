"""Unit tests for goal video filename parser.

Test cases mirror real filenames observed in Google Drive for matches on
2026-04-18 (6- Тур).
"""
import pytest

from app.utils.goal_video_filename import parse_goal_filename


class TestRealFilenames:
    def test_abraev_goal(self):
        result = parse_goal_filename(
            "АБРАЕВ ГОЛ - 1 - Camera1 АБРАЕВ [18-06-24] [18-07-33].mp4"
        )
        assert result is not None
        assert result.wall_time == (18, 6, 24)
        assert result.player_hint == "АБРАЕВ"

    def test_elimay_goal(self):
        result = parse_goal_filename(
            "ГОЛ елимай - 1 - Camera1 ГОЛ ЕЛИМАЙ [17-28-20] [17-29-26].mp4"
        )
        assert result is not None
        # Filename only references the team (no scorer), so player_hint should
        # be None once team stopwords are filtered out.
        assert result.wall_time == (17, 28, 20)
        assert result.player_hint is None

    def test_zhorzhinho(self):
        result = parse_goal_filename(
            "жоржиньо гол - Camera1 жоржиньо [19-41-02] [19-41-32].mp4"
        )
        assert result is not None
        assert result.wall_time == (19, 41, 2)
        assert result.player_hint is not None
        assert result.player_hint.lower() == "жоржиньо"

    def test_sergey_malyy(self):
        result = parse_goal_filename(
            "СЕРГЕЙ МАЛЫЙ - 1 - Camera1 - [20-20-45] [20-22-06].mp4"
        )
        assert result is not None
        assert result.wall_time == (20, 20, 45)
        # Picks the longer token "СЕРГЕЙ" (6 chars) — not ideal, but acceptable
        # because the matcher uses fuzzy `contains` against event.player_name
        # (e.g. "Сергей Малый"), so a match on "СЕРГЕЙ" still resolves correctly.
        assert result.player_hint in ("СЕРГЕЙ", "МАЛЫЙ")

    def test_toktybay(self):
        result = parse_goal_filename(
            "Токтыбай - 1 - Camera1 - [20-49-43] [20-51-12].mp4"
        )
        assert result is not None
        assert result.wall_time == (20, 49, 43)
        assert result.player_hint == "Токтыбай"

    def test_de_assunsao(self):
        result = parse_goal_filename(
            "Де ассунсау - 1 - Camera1 - [20-47-24] [20-48-39].mp4"
        )
        assert result is not None
        # "ассунсау" is longer than "Де"; accept either.
        assert result.player_hint in ("ассунсау", "Де", "Ассунсау")

    def test_bare_goal_astana(self):
        # "ГОЛ АСТАНА.mp4" — both tokens are stopwords and there's no walltime,
        # so parser returns None (AI fallback + folder context pick up the slack).
        assert parse_goal_filename("ГОЛ АСТАНА.mp4") is None

    def test_empty_returns_none(self):
        assert parse_goal_filename("") is None
        assert parse_goal_filename(".mp4") is None

    def test_unknown_extension_kept(self):
        # Should not drop random tail tokens for unknown extensions.
        result = parse_goal_filename("45_Ivanov.xyz")
        assert result is None or result.player_hint in ("Ivanov", "xyz")

    @pytest.mark.parametrize("name", [
        "[18-00-00] Kairat goal Ivanov.mp4",
        "Ivanov gol [18-00-00].webm",
    ])
    def test_walltime_position_tolerant(self, name):
        result = parse_goal_filename(name)
        assert result is not None
        assert result.wall_time == (18, 0, 0)


class TestMinuteHint:
    def test_apostrophe(self):
        result = parse_goal_filename("Дүйсенбекұлы 22'.mp4")
        assert result is not None
        assert result.minute_hint == 22

    def test_backtick(self):
        result = parse_goal_filename("Илья Кругляков 22`.mp4")
        assert result is not None
        assert result.minute_hint == 22

    def test_trailing_no_apostrophe(self):
        # Real Drive sample (game 1358 / event 17730):
        # "Әділет Омарбек 78.mp4" — minute without apostrophe.
        result = parse_goal_filename("Әділет Омарбек 78.mp4")
        assert result is not None
        assert result.minute_hint == 78
        # Picker prefers the longer token. Either part of the full name works
        # because the matcher uses fuzzy `contains` against event.player_name.
        assert result.player_hint in ("Әділет", "Омарбек")

    def test_camera_index_not_picked_as_minute(self):
        # "- 1 -" is a camera index, not a minute. Tail is "Camera1" / "[ts]"
        # — no trailing bare digits after bracket strip.
        result = parse_goal_filename(
            "СЕРГЕЙ МАЛЫЙ - 1 - Camera1 - [20-20-45] [20-22-06].mp4"
        )
        assert result is not None
        assert result.minute_hint is None

    def test_minute_out_of_range(self):
        # 200 > 120 → ignored.
        result = parse_goal_filename("Игрок 200.mp4")
        assert result is None or result.minute_hint is None
