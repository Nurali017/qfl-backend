"""Shared game status computation."""

from datetime import date, timedelta

from app.models.game import Game, GameStatus


def compute_game_status(game: Game) -> str:
    """Return a display-friendly game status string.

    Rules:
    - live → "live"
    - postponed / cancelled → as-is
    - today's finished / technical_defeat → "upcoming" (stay in Келесі until day ends)
    - past finished / technical_defeat → as-is
    - created → always "upcoming"
    """
    if game.status == GameStatus.live:
        return "live"
    if game.status in (GameStatus.postponed, GameStatus.cancelled):
        return game.status.value
    if game.status in (GameStatus.finished, GameStatus.technical_defeat):
        if game.date is not None and game.date >= date.today() - timedelta(days=1):
            return "upcoming"
        return game.status.value
    # created → upcoming
    return "upcoming"
