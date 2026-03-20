"""Shared game status computation."""

from app.models.game import Game, GameStatus


def compute_game_status(game: Game, *, for_list: bool = False) -> str:
    """Return a display-friendly game status string.

    Rules:
    - live → "live"
    - postponed / cancelled → as-is
    - finished / technical_defeat → as-is (score must be visible)
    - created → always "upcoming"
    """
    if game.status == GameStatus.live:
        return "live"
    if game.status in (GameStatus.postponed, GameStatus.cancelled):
        return game.status.value
    if game.status in (GameStatus.finished, GameStatus.technical_defeat):
        return game.status.value
    # created → upcoming
    return "upcoming"
