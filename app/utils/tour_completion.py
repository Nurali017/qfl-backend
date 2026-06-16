"""Shared predicates for "is this tour complete?" gates.

A tour is complete once every game that should have been played by now is
finished. Games rescheduled into the future (status ``created`` with a future
date) must NOT block tour-level generation (team-of-week, stats-page ISR
revalidation) for the games already played — otherwise a single moved fixture
stalls the whole tour. Postponed/cancelled games are likewise non-blocking.

``games.date`` stores the local Asia/Almaty match date, so "future" is judged
against Almaty today (see ``today_almaty``). Today's not-yet-played games still
count toward the total (and thus still block until they finish).

Used by the three tour-completion gates: ``_post_finish_followup`` (immediate
team-of-week dispatch), ``_retry_missing_team_of_week`` (retry backstop) and
``maybe_trigger_tour_revalidation`` (stats-page revalidation).
"""

from datetime import date

from sqlalchemy import ColumnElement

from app.models.game import Game, GameStatus
from app.utils.timestamps import today_almaty

TERMINAL_STATUSES = (GameStatus.finished, GameStatus.technical_defeat)
NON_BLOCKING_STATUSES = (GameStatus.postponed, GameStatus.cancelled)


def tour_playable_predicate(today: date | None = None) -> ColumnElement[bool]:
    """Game counts toward a tour's ``total``.

    Blocks completion unless it is postponed/cancelled OR rescheduled into the
    future. ``today`` is injectable for deterministic tests; defaults to Almaty
    today.
    """
    effective_today = today or today_almaty()
    return Game.status.notin_(NON_BLOCKING_STATUSES) & (Game.date <= effective_today)


def tour_completed_predicate() -> ColumnElement[bool]:
    """Game counts toward a tour's ``completed`` (terminal status with scores)."""
    return (
        Game.status.in_(TERMINAL_STATUSES)
        & Game.home_score.isnot(None)
        & Game.away_score.isnot(None)
    )
