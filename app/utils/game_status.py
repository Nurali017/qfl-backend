"""Shared game status computation."""

from app.models.game import Game, GameStatus


def compute_game_status(game: Game) -> str:
    """Return a display-friendly game status string.

    Maps the DB enum to API-facing values.  ``created`` is shown as
    ``"upcoming"`` because the frontend relies on that term.
    """
    if game.status == GameStatus.created:
        return "upcoming"
    return game.status.value
