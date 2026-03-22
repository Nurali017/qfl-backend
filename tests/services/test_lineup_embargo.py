"""Tests for lineup embargo: public API visibility timing.

Telegram notification is handled by FCMS sync (prematch PDF), not by this module.
"""

from datetime import date, time, timedelta
from unittest.mock import MagicMock

from app.models.game import Game, GameStatus


def _make_game(
    *,
    game_date: date = date(2026, 5, 15),
    game_time: time | None = time(18, 0),
    status: GameStatus = GameStatus.created,
) -> Game:
    game = Game(
        id=100,
        date=game_date,
        time=game_time,
        status=status,
    )
    return game


class TestIsLineupEmbargoed:
    """Test dynamic embargo computation: kickoff - 60 min."""

    def test_embargoed_75min_before(self):
        from app.services.lineup_embargo import is_lineup_embargoed
        from app.utils.timestamps import combine_almaty_local_to_utc

        game = _make_game()
        kickoff = combine_almaty_local_to_utc(date(2026, 5, 15), time(18, 0))
        now = kickoff - timedelta(minutes=75)

        assert is_lineup_embargoed(game, now_utc=now) is True

    def test_not_embargoed_55min_before(self):
        from app.services.lineup_embargo import is_lineup_embargoed
        from app.utils.timestamps import combine_almaty_local_to_utc

        game = _make_game()
        kickoff = combine_almaty_local_to_utc(date(2026, 5, 15), time(18, 0))
        now = kickoff - timedelta(minutes=55)

        assert is_lineup_embargoed(game, now_utc=now) is False

    def test_not_embargoed_exactly_60min(self):
        """Edge case: exactly 60 min before kickoff — should NOT be embargoed."""
        from app.services.lineup_embargo import is_lineup_embargoed
        from app.utils.timestamps import combine_almaty_local_to_utc

        game = _make_game()
        kickoff = combine_almaty_local_to_utc(date(2026, 5, 15), time(18, 0))
        now = kickoff - timedelta(minutes=60)

        assert is_lineup_embargoed(game, now_utc=now) is False

    def test_not_embargoed_live_game(self):
        from app.services.lineup_embargo import is_lineup_embargoed
        from app.utils.timestamps import combine_almaty_local_to_utc

        game = _make_game(status=GameStatus.live)
        kickoff = combine_almaty_local_to_utc(date(2026, 5, 15), time(18, 0))
        now = kickoff - timedelta(minutes=120)

        assert is_lineup_embargoed(game, now_utc=now) is False

    def test_not_embargoed_finished_game(self):
        from app.services.lineup_embargo import is_lineup_embargoed
        from app.utils.timestamps import combine_almaty_local_to_utc

        game = _make_game(status=GameStatus.finished)
        kickoff = combine_almaty_local_to_utc(date(2026, 5, 15), time(18, 0))
        now = kickoff - timedelta(minutes=120)

        assert is_lineup_embargoed(game, now_utc=now) is False

    def test_not_embargoed_no_time(self):
        from app.services.lineup_embargo import is_lineup_embargoed

        game = _make_game(game_time=None)
        assert is_lineup_embargoed(game) is False
