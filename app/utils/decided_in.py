"""Compute how a finished match was decided (regular / extra_time / penalties)."""

from typing import Iterable, Literal

from app.models.game import Game
from app.models.game_event import GameEvent

DecidedIn = Literal["regular", "extra_time", "penalties"]


def compute_decided_in_lite(game: Game) -> DecidedIn | None:
    """Cheap version using only the Game row.

    Detects penalty shootouts (which always have penalty_score set). Cannot
    distinguish 'extra_time' vs 'regular' without loading events, so returns
    ``None`` for that case — callers that only have the Game row should treat
    ``None`` as "unknown / regular".
    """
    if game.home_score is None or game.away_score is None:
        return None
    if game.home_penalty_score is not None and game.away_penalty_score is not None:
        return "penalties"
    return None


def compute_decided_in(game: Game, events: Iterable[GameEvent] | None) -> DecidedIn | None:
    """Full version — requires the game's events to detect extra time.

    Rules:
    - ``penalties`` — penalty shootout happened (penalty_score set on the game).
    - ``extra_time`` — regulation ended in a non-draw but there are events in
      half >= 3 with real match minutes (> 20). This means ET was played and
      the game was decided there, not in regulation.
    - ``regular`` — finished game with a full-time winner, no ET events.
    - ``None`` — unknown (not finished, no scores).
    """
    if game.home_score is None or game.away_score is None:
        return None

    if game.home_penalty_score is not None and game.away_penalty_score is not None:
        return "penalties"

    if events is not None:
        # Extra time = events in half 3/4 with real match minutes (> 20).
        # Shootout rounds use round-number minutes (1..5+), so minute > 20 is
        # a reliable ET signal.
        for event in events:
            if (event.half or 0) >= 3 and (event.minute or 0) > 20:
                return "extra_time"

    return "regular"
