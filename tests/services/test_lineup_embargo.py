"""Tests for lineup embargo: Telegram notification + public API visibility timing.

TDD: these tests are written BEFORE the implementation.
"""

import hashlib
import html
from datetime import date, time, datetime, timedelta
from unittest.mock import AsyncMock, patch, MagicMock
from uuid import uuid4

import pytest

from app.models.game import Game, GameStatus
from app.models.game_lineup import GameLineup, LineupType


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_game(
    *,
    game_date: date = date(2026, 5, 15),
    game_time: time | None = time(18, 0),
    status: GameStatus = GameStatus.created,
    home_team_id: int = 1,
    away_team_id: int = 2,
    home_formation: str | None = "4-4-2",
    away_formation: str | None = "4-3-3",
) -> Game:
    game = Game(
        id=100,
        date=game_date,
        time=game_time,
        status=status,
        home_team_id=home_team_id,
        away_team_id=away_team_id,
        home_formation=home_formation,
        away_formation=away_formation,
    )
    # Mock team relationships for message formatting
    home = MagicMock()
    home.name = "FC Astana"
    away = MagicMock()
    away.name = "FC Kairat"
    game.home_team = home
    game.away_team = away
    return game


def _make_lineup_entry(
    *,
    game_id: int = 100,
    team_id: int = 1,
    player_id: int = 10,
    lineup_type: LineupType = LineupType.starter,
    shirt_number: int | None = 7,
    is_captain: bool = False,
    amplua: str | None = "M",
    field_position: str | None = "C",
    first_name: str = "Тест",
    last_name: str = "Игрок",
) -> GameLineup:
    entry = GameLineup(
        game_id=game_id,
        team_id=team_id,
        player_id=player_id,
        lineup_type=lineup_type,
        shirt_number=shirt_number,
        is_captain=is_captain,
        amplua=amplua,
        field_position=field_position,
    )
    # Mock player relationship
    player = MagicMock()
    player.first_name = first_name
    player.last_name = last_name
    player.id = player_id
    entry.player = player
    return entry


# ===========================================================================
# TestFormatLineupTelegramMessage
# ===========================================================================


class TestFormatLineupTelegramMessage:
    """Test Telegram message formatting for lineup notifications."""

    def test_basic_format_includes_team_names(self):
        from app.services.lineup_embargo import format_lineup_telegram_message

        game = _make_game()
        home = [_make_lineup_entry(team_id=1, player_id=1)]
        away = [_make_lineup_entry(team_id=2, player_id=2)]

        msg = format_lineup_telegram_message(game, home, away)
        assert "FC Astana" in msg
        assert "FC Kairat" in msg

    def test_shows_formation(self):
        from app.services.lineup_embargo import format_lineup_telegram_message

        game = _make_game(home_formation="4-4-2", away_formation="4-3-3")
        home = [_make_lineup_entry(team_id=1, player_id=1)]
        away = [_make_lineup_entry(team_id=2, player_id=2)]

        msg = format_lineup_telegram_message(game, home, away)
        assert "4-4-2" in msg
        assert "4-3-3" in msg

    def test_shows_shirt_number_and_position(self):
        from app.services.lineup_embargo import format_lineup_telegram_message

        game = _make_game()
        home = [_make_lineup_entry(team_id=1, player_id=1, shirt_number=7, amplua="M")]
        away = []

        msg = format_lineup_telegram_message(game, home, away)
        assert "#7" in msg

    def test_captain_marked(self):
        from app.services.lineup_embargo import format_lineup_telegram_message

        game = _make_game()
        home = [_make_lineup_entry(team_id=1, player_id=1, is_captain=True)]
        away = []

        msg = format_lineup_telegram_message(game, home, away)
        assert "©" in msg or "(C)" in msg or "капитан" in msg.lower()

    def test_update_flag_adds_updated_header(self):
        from app.services.lineup_embargo import format_lineup_telegram_message

        game = _make_game()
        home = [_make_lineup_entry(team_id=1, player_id=1)]
        away = []

        msg_normal = format_lineup_telegram_message(game, home, away, is_update=False)
        msg_update = format_lineup_telegram_message(game, home, away, is_update=True)

        assert "ОБНОВЛЕН" in msg_update.upper() or "UPDATED" in msg_update.upper()
        assert "ОБНОВЛЕН" not in msg_normal.upper() and "UPDATED" not in msg_normal.upper()

    def test_separates_starters_and_subs(self):
        from app.services.lineup_embargo import format_lineup_telegram_message

        game = _make_game()
        home = [
            _make_lineup_entry(team_id=1, player_id=1, lineup_type=LineupType.starter, first_name="Старт", last_name="Игрок"),
            _make_lineup_entry(team_id=1, player_id=2, lineup_type=LineupType.substitute, first_name="Запас", last_name="Игрок"),
        ]
        away = []

        msg = format_lineup_telegram_message(game, home, away)
        # Both players should appear
        assert "Старт" in msg or html.escape("Старт") in msg
        assert "Запас" in msg or html.escape("Запас") in msg

    def test_html_escapes_player_names(self):
        from app.services.lineup_embargo import format_lineup_telegram_message

        game = _make_game()
        home = [_make_lineup_entry(
            team_id=1, player_id=1,
            first_name="<script>", last_name="O'Brien&Co",
        )]
        away = []

        msg = format_lineup_telegram_message(game, home, away)
        # Raw HTML must be escaped
        assert "<script>" not in msg
        assert html.escape("<script>") in msg
        assert html.escape("O'Brien&Co") in msg


# ===========================================================================
# TestIsLineupEmbargoed
# ===========================================================================


class TestIsLineupEmbargoed:
    """Test dynamic embargo computation: kickoff - 60 min."""

    def test_embargoed_75min_before(self):
        from app.services.lineup_embargo import is_lineup_embargoed
        from app.utils.timestamps import combine_almaty_local_to_utc

        game = _make_game(game_date=date(2026, 5, 15), game_time=time(18, 0))
        # 75 min before kickoff = 16:45 Almaty
        kickoff = combine_almaty_local_to_utc(date(2026, 5, 15), time(18, 0))
        now = kickoff - timedelta(minutes=75)

        assert is_lineup_embargoed(game, now_utc=now) is True

    def test_not_embargoed_55min_before(self):
        from app.services.lineup_embargo import is_lineup_embargoed
        from app.utils.timestamps import combine_almaty_local_to_utc

        game = _make_game(game_date=date(2026, 5, 15), game_time=time(18, 0))
        kickoff = combine_almaty_local_to_utc(date(2026, 5, 15), time(18, 0))
        now = kickoff - timedelta(minutes=55)

        assert is_lineup_embargoed(game, now_utc=now) is False

    def test_not_embargoed_exactly_60min(self):
        """Edge case: exactly 60 min before kickoff — should NOT be embargoed."""
        from app.services.lineup_embargo import is_lineup_embargoed
        from app.utils.timestamps import combine_almaty_local_to_utc

        game = _make_game(game_date=date(2026, 5, 15), game_time=time(18, 0))
        kickoff = combine_almaty_local_to_utc(date(2026, 5, 15), time(18, 0))
        now = kickoff - timedelta(minutes=60)

        assert is_lineup_embargoed(game, now_utc=now) is False

    def test_not_embargoed_live_game(self):
        from app.services.lineup_embargo import is_lineup_embargoed
        from app.utils.timestamps import combine_almaty_local_to_utc

        game = _make_game(status=GameStatus.live)
        kickoff = combine_almaty_local_to_utc(date(2026, 5, 15), time(18, 0))
        now = kickoff - timedelta(minutes=120)  # way before kickoff

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


# ===========================================================================
# TestShouldSendTelegram
# ===========================================================================


class TestShouldSendTelegram:
    """Test whether Telegram notification should be sent."""

    def test_sends_90min_before(self):
        from app.services.lineup_embargo import should_send_telegram
        from app.utils.timestamps import combine_almaty_local_to_utc

        game = _make_game()
        kickoff = combine_almaty_local_to_utc(date(2026, 5, 15), time(18, 0))
        now = kickoff - timedelta(minutes=85)

        assert should_send_telegram(game, current_hash="abc123", now_utc=now) is True

    def test_does_not_send_too_early(self):
        from app.services.lineup_embargo import should_send_telegram
        from app.utils.timestamps import combine_almaty_local_to_utc

        game = _make_game()
        kickoff = combine_almaty_local_to_utc(date(2026, 5, 15), time(18, 0))
        now = kickoff - timedelta(minutes=120)  # 2 hours before

        assert should_send_telegram(game, current_hash="abc123", now_utc=now) is False

    def test_does_not_resend_same_hash(self):
        from app.services.lineup_embargo import should_send_telegram
        from app.utils.timestamps import combine_almaty_local_to_utc

        game = _make_game()
        game.lineup_telegram_sent_at = datetime(2026, 5, 15, 10, 0)
        game.lineup_telegram_hash = "abc123"
        kickoff = combine_almaty_local_to_utc(date(2026, 5, 15), time(18, 0))
        now = kickoff - timedelta(minutes=85)

        assert should_send_telegram(game, current_hash="abc123", now_utc=now) is False

    def test_resends_on_hash_change(self):
        from app.services.lineup_embargo import should_send_telegram
        from app.utils.timestamps import combine_almaty_local_to_utc

        game = _make_game()
        game.lineup_telegram_sent_at = datetime(2026, 5, 15, 10, 0)
        game.lineup_telegram_hash = "old_hash"
        kickoff = combine_almaty_local_to_utc(date(2026, 5, 15), time(18, 0))
        now = kickoff - timedelta(minutes=85)

        assert should_send_telegram(game, current_hash="new_hash", now_utc=now) is True

    def test_skips_no_time(self):
        from app.services.lineup_embargo import should_send_telegram

        game = _make_game(game_time=None)
        assert should_send_telegram(game, current_hash="abc") is False

    def test_skips_non_created(self):
        from app.services.lineup_embargo import should_send_telegram
        from app.utils.timestamps import combine_almaty_local_to_utc

        game = _make_game(status=GameStatus.live)
        kickoff = combine_almaty_local_to_utc(date(2026, 5, 15), time(18, 0))
        now = kickoff - timedelta(minutes=85)

        assert should_send_telegram(game, current_hash="abc", now_utc=now) is False


# ===========================================================================
# TestProcessLineupEmbargo — integration tests with DB
# ===========================================================================


class TestProcessLineupEmbargo:
    """Integration tests for the embargo processing pipeline."""

    @pytest.fixture
    async def _seed_game_with_lineup(self, test_session, sample_season, sample_teams):
        """Create a game with lineup entries, returning (game, lineups)."""
        from app.models.player import Player

        game = Game(
            id=500,
            sota_id=uuid4(),
            date=date(2026, 5, 15),
            time=time(18, 0),
            tour=1,
            season_id=sample_season.id,
            home_team_id=sample_teams[0].id,
            away_team_id=sample_teams[1].id,
            status=GameStatus.created,
            home_formation="4-4-2",
            away_formation="4-3-3",
        )
        test_session.add(game)

        players = []
        for i in range(1, 5):
            p = Player(
                id=1000 + i,
                sota_id=uuid4(),
                first_name=f"Имя{i}",
                last_name=f"Фамилия{i}",
                birthday=date(1995, 1, 1),
            )
            players.append(p)
        test_session.add_all(players)

        lineups = []
        for i, p in enumerate(players):
            lt = LineupType.starter if i < 3 else LineupType.substitute
            entry = GameLineup(
                game_id=500,
                team_id=sample_teams[0].id,
                player_id=p.id,
                lineup_type=lt,
                shirt_number=i + 1,
                is_captain=(i == 0),
                amplua="M" if i > 0 else "Gk",
                field_position="C",
            )
            lineups.append(entry)
        test_session.add_all(lineups)
        await test_session.commit()

        # Refresh to load relationships
        await test_session.refresh(game)
        return game, lineups

    @pytest.mark.asyncio
    async def test_sends_and_commits_per_game(
        self, test_session, _seed_game_with_lineup
    ):
        from app.services.lineup_embargo import process_lineup_embargo
        from app.utils.timestamps import combine_almaty_local_to_utc

        game, _ = _seed_game_with_lineup
        kickoff = combine_almaty_local_to_utc(game.date, game.time)
        now = kickoff - timedelta(minutes=85)

        with patch("app.services.lineup_embargo.send_telegram_message", new_callable=AsyncMock) as mock_tg:
            result = await process_lineup_embargo(test_session, now_utc=now)

        # Should have sent a message
        mock_tg.assert_called_once()
        msg = mock_tg.call_args[0][0]
        assert "Фамилия1" in msg or "Фамилия" in msg

        # Game should have updated telegram fields
        await test_session.refresh(game)
        assert game.lineup_telegram_sent_at is not None
        assert game.lineup_telegram_hash is not None

    @pytest.mark.asyncio
    async def test_does_not_resend_same_lineup(
        self, test_session, _seed_game_with_lineup
    ):
        from app.services.lineup_embargo import process_lineup_embargo
        from app.utils.timestamps import combine_almaty_local_to_utc

        game, _ = _seed_game_with_lineup
        kickoff = combine_almaty_local_to_utc(game.date, game.time)
        now = kickoff - timedelta(minutes=85)

        # First run — sends
        with patch("app.services.lineup_embargo.send_telegram_message", new_callable=AsyncMock) as mock_tg:
            await process_lineup_embargo(test_session, now_utc=now)
        assert mock_tg.call_count == 1

        # Second run — same lineup, should not resend
        with patch("app.services.lineup_embargo.send_telegram_message", new_callable=AsyncMock) as mock_tg2:
            await process_lineup_embargo(test_session, now_utc=now)
        assert mock_tg2.call_count == 0

    @pytest.mark.asyncio
    async def test_resend_has_updated_prefix(
        self, test_session, _seed_game_with_lineup
    ):
        from app.services.lineup_embargo import process_lineup_embargo
        from app.utils.timestamps import combine_almaty_local_to_utc

        game, lineups = _seed_game_with_lineup
        kickoff = combine_almaty_local_to_utc(game.date, game.time)
        now = kickoff - timedelta(minutes=85)

        # First send
        with patch("app.services.lineup_embargo.send_telegram_message", new_callable=AsyncMock):
            await process_lineup_embargo(test_session, now_utc=now)

        # Change lineup — add a player
        from app.models.player import Player
        new_player = Player(
            id=2000,
            sota_id=uuid4(),
            first_name="Новый",
            last_name="Игрок",
            birthday=date(1998, 6, 1),
        )
        test_session.add(new_player)
        new_entry = GameLineup(
            game_id=500,
            team_id=lineups[0].team_id,
            player_id=2000,
            lineup_type=LineupType.starter,
            shirt_number=99,
            is_captain=False,
            amplua="F",
            field_position="C",
        )
        test_session.add(new_entry)
        await test_session.commit()

        # Expire all to force fresh load including new lineup entries
        test_session.expire_all()

        # Second run — should resend with "ОБНОВЛЕНО"
        with patch("app.services.lineup_embargo.send_telegram_message", new_callable=AsyncMock) as mock_tg:
            await process_lineup_embargo(test_session, now_utc=now)

        assert mock_tg.call_count == 1
        msg = mock_tg.call_args[0][0]
        assert "ОБНОВЛЕН" in msg.upper() or "UPDATED" in msg.upper()

    @pytest.mark.asyncio
    async def test_skips_game_too_early(
        self, test_session, _seed_game_with_lineup
    ):
        from app.services.lineup_embargo import process_lineup_embargo
        from app.utils.timestamps import combine_almaty_local_to_utc

        game, _ = _seed_game_with_lineup
        kickoff = combine_almaty_local_to_utc(game.date, game.time)
        now = kickoff - timedelta(hours=3)  # 3 hours before — too early

        with patch("app.services.lineup_embargo.send_telegram_message", new_callable=AsyncMock) as mock_tg:
            await process_lineup_embargo(test_session, now_utc=now)

        mock_tg.assert_not_called()
