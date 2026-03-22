"""Lineup embargo: public API visibility timing.

- 60 min before kickoff: make lineup visible on public API
- Telegram notification is handled by FCMS sync (prematch PDF)
"""

from datetime import datetime, timedelta

from app.models import Game, GameStatus
from app.utils.timestamps import combine_almaty_local_to_utc, utcnow

PUBLIC_WINDOW_MINUTES = 60


def is_lineup_embargoed(game: Game, *, now_utc: datetime | None = None) -> bool:
    """True if lineup should be hidden on public API.

    Embargo applies only to 'created' games with a scheduled time,
    when current time is more than 60 min before kickoff.
    """
    if game.status != GameStatus.created or game.time is None:
        return False
    kickoff = combine_almaty_local_to_utc(game.date, game.time)
    now = now_utc or utcnow()
    return now < kickoff - timedelta(minutes=PUBLIC_WINDOW_MINUTES)
