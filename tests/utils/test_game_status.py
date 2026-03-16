"""Tests for compute_game_status.

Detail context (for_list=False, default):
  finished/technical_defeat → always return actual status

List context (for_list=True):
  game.date == today     → upcoming (stay in Келесі)
  game.date == yesterday → upcoming (ещё 1 день)
  game.date == 2 days ago → finished
"""

from datetime import date, timedelta
from unittest.mock import MagicMock

import pytest

from app.models.game import GameStatus
from app.utils.game_status import compute_game_status

TODAY = date.today()
YESTERDAY = TODAY - timedelta(days=1)
TWO_DAYS_AGO = TODAY - timedelta(days=2)
TOMORROW = TODAY + timedelta(days=1)


def _make_game(status: GameStatus, home_score=None, away_score=None, game_date=None):
    game = MagicMock()
    game.status = status
    game.home_score = home_score
    game.away_score = away_score
    game.date = game_date
    return game


class TestCreatedGames:
    """created всегда → upcoming."""

    def test_created_no_scores(self):
        game = _make_game(GameStatus.created, game_date=TOMORROW)
        assert compute_game_status(game) == "upcoming"

    def test_created_with_scores_today(self):
        game = _make_game(GameStatus.created, home_score=1, away_score=0, game_date=TODAY)
        assert compute_game_status(game) == "upcoming"

    def test_created_with_scores_yesterday(self):
        game = _make_game(GameStatus.created, home_score=2, away_score=1, game_date=YESTERDAY)
        assert compute_game_status(game) == "upcoming"

    def test_created_only_home_score(self):
        game = _make_game(GameStatus.created, home_score=1, game_date=TODAY)
        assert compute_game_status(game) == "upcoming"


class TestFinishedGamesDetail:
    """Detail context: finished → always actual status."""

    def test_finished_today(self):
        game = _make_game(GameStatus.finished, home_score=2, away_score=1, game_date=TODAY)
        assert compute_game_status(game) == "finished"

    def test_finished_yesterday(self):
        game = _make_game(GameStatus.finished, home_score=2, away_score=1, game_date=YESTERDAY)
        assert compute_game_status(game) == "finished"

    def test_finished_two_days_ago(self):
        game = _make_game(GameStatus.finished, home_score=2, away_score=1, game_date=TWO_DAYS_AGO)
        assert compute_game_status(game) == "finished"

    def test_technical_defeat_today(self):
        game = _make_game(GameStatus.technical_defeat, home_score=3, away_score=0, game_date=TODAY)
        assert compute_game_status(game) == "technical_defeat"

    def test_technical_defeat_yesterday(self):
        game = _make_game(GameStatus.technical_defeat, home_score=3, away_score=0, game_date=YESTERDAY)
        assert compute_game_status(game) == "technical_defeat"


class TestFinishedGamesForList:
    """List context: today/yesterday finished → upcoming."""

    def test_finished_today_is_upcoming(self):
        game = _make_game(GameStatus.finished, home_score=2, away_score=1, game_date=TODAY)
        assert compute_game_status(game, for_list=True) == "upcoming"

    def test_finished_yesterday_is_upcoming(self):
        game = _make_game(GameStatus.finished, home_score=2, away_score=1, game_date=YESTERDAY)
        assert compute_game_status(game, for_list=True) == "upcoming"

    def test_finished_two_days_ago_is_finished(self):
        game = _make_game(GameStatus.finished, home_score=2, away_score=1, game_date=TWO_DAYS_AGO)
        assert compute_game_status(game, for_list=True) == "finished"

    def test_technical_defeat_today_is_upcoming(self):
        game = _make_game(GameStatus.technical_defeat, home_score=3, away_score=0, game_date=TODAY)
        assert compute_game_status(game, for_list=True) == "upcoming"

    def test_technical_defeat_yesterday_is_upcoming(self):
        game = _make_game(GameStatus.technical_defeat, home_score=3, away_score=0, game_date=YESTERDAY)
        assert compute_game_status(game, for_list=True) == "upcoming"


class TestLiveGames:
    """live всегда → live."""

    def test_live_today(self):
        game = _make_game(GameStatus.live, home_score=1, away_score=0, game_date=TODAY)
        assert compute_game_status(game) == "live"

    def test_live_no_date(self):
        game = _make_game(GameStatus.live, home_score=0, away_score=0)
        assert compute_game_status(game) == "live"


class TestPostponedCancelled:
    """postponed/cancelled — всегда как есть."""

    def test_postponed_today(self):
        game = _make_game(GameStatus.postponed, game_date=TODAY)
        assert compute_game_status(game) == "postponed"

    def test_cancelled_today(self):
        game = _make_game(GameStatus.cancelled, game_date=TODAY)
        assert compute_game_status(game) == "cancelled"

    def test_postponed_yesterday(self):
        game = _make_game(GameStatus.postponed, game_date=YESTERDAY)
        assert compute_game_status(game) == "postponed"
