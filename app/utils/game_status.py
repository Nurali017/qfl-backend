"""Shared game status computation."""

from datetime import date as date_type

from app.models.game import Game


def compute_game_status(game: Game, today: date_type | None = None) -> str:
    """Compute game status based on data.

    Returns:
        "live" - Game is currently in progress
        "finished" - Game has ended
        "upcoming" - Game is scheduled for the future
    """
    if today is None:
        today = date_type.today()

    if game.is_live:
        return "live"
    elif game.home_score is not None and game.away_score is not None:
        return "finished"
    elif game.date and game.date < today:
        return "finished"
    else:
        return "upcoming"
