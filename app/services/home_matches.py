"""Home matches widget service.

For tour-based leagues (pl, 1l, el) the widget anchors on the current ISO
week (Mon..Sun, Asia/Almaty) — finding the week that contains the next
non-terminal fixture, regardless of tour number. All games of that week
go to the upcoming tab, all terminal games of the previous and anchor
weeks go to the finished tab. This naturally handles rescheduled
fixtures whose tour number does not match the current matchday.

For 2L group/final leagues the older tour-based logic still applies via
``_get_widget_group`` / ``_get_widget_final``.

"Completed window" (48h) keeps default_tab="finished" until 48 hours
after the most recent finished match; past 48h default flips to
"upcoming". Past that and with no anchor week available, ``_fallback``
returns the nearest available games.
"""

import logging
from datetime import date, datetime, time as time_type, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models import Game, GameBroadcaster, GameStatus, Season
from app.schemas.game import HomeMatchesWidgetResponse
from app.services.season_filters import get_final_stage_ids, get_group_team_ids
from app.services.season_visibility import is_season_visible_clause
from app.utils.game_grouping import group_games_by_date
from app.utils.has_stats import enrich_games_has_stats
from app.utils.timestamps import to_almaty

logger = logging.getLogger(__name__)

ALMATY_TZ = ZoneInfo("Asia/Almaty")
TERMINAL_STATUSES = {GameStatus.finished, GameStatus.technical_defeat}
NON_BLOCKING_STATUSES = {GameStatus.postponed, GameStatus.cancelled}
COMPLETED_WINDOW = timedelta(hours=48)


OUTLIER_FUTURE_THRESHOLD = timedelta(days=14)


def _is_missed_schedule(game: Game, today: date) -> bool:
    """Created game whose match-day has already passed.

    Such a game is almost certainly a rescheduled fixture that was never
    explicitly marked ``postponed``.  The widget treats it as non-blocking
    so the rest of the tour can close.
    """
    if game.status != GameStatus.created:
        return False
    if game.date is None:
        return False
    return game.date < today


def _tour_terminal_last_date(games: list[Game]) -> date | None:
    """Latest scheduled date among already-played games in the collection."""
    dates = [
        g.date for g in games
        if g.status in TERMINAL_STATUSES and g.date is not None
    ]
    return max(dates) if dates else None


def _is_outlier_future(game: Game, tour_anchor: date | None) -> bool:
    """Created game whose scheduled date is far beyond the tour's main cluster.

    Covers the "admin pushed the fixture to a placeholder date two months
    out" pattern: the tour has a tight match-day window but one fixture
    sits well outside it.  Such a game is effectively rescheduled and must
    not keep the tour appearing in progress.
    """
    if tour_anchor is None:
        return False
    if game.status != GameStatus.created:
        return False
    if game.date is None:
        return False
    return (game.date - tour_anchor) > OUTLIER_FUTURE_THRESHOLD


def _is_playable(
    game: Game,
    today: date | None = None,
    tour_anchor: date | None = None,
) -> bool:
    """Game is part of the regular schedule.

    Excluded:
    - postponed / cancelled
    - stale-created past its match-day (``today`` required)
    - created fixtures scheduled far outside the tour cluster
      (``tour_anchor`` required)
    """
    if game.status in NON_BLOCKING_STATUSES:
        return False
    if today is not None and _is_missed_schedule(game, today):
        return False
    if tour_anchor is not None and _is_outlier_future(game, tour_anchor):
        return False
    return True

_EAGER_OPTIONS = (
    selectinload(Game.home_team),
    selectinload(Game.away_team),
    selectinload(Game.season),
    selectinload(Game.stadium_rel),
    selectinload(Game.stage),
    selectinload(Game.broadcasters).selectinload(GameBroadcaster.broadcaster),
)


async def get_home_widget(
    db: AsyncSession,
    frontend_code: str,
    lang: str,
    *,
    group: str | None = None,
) -> HomeMatchesWidgetResponse:
    """Return home widget data for a tour-based league."""
    season = await _resolve_season(db, frontend_code)
    if not season:
        return _empty(frontend_code)

    season_id = season.id

    # Group-based leagues (2L, Women's League): dispatch to group-specific or final logic
    if group:
        if group == "final":
            return await _get_widget_final(db, season_id, frontend_code, lang)
        return await _get_widget_group(db, season_id, frontend_code, lang, group)

    return await _get_widget_week(db, season_id, frontend_code, lang)


async def _get_widget_week(
    db: AsyncSession,
    season_id: int,
    frontend_code: str,
    lang: str,
) -> HomeMatchesWidgetResponse:
    """Week-based widget flow for tour-based leagues (pl, 1l, el).

    Selection rules:
    - anchor_week = ISO week (Mon..Sun, Asia/Almaty) of the next non-terminal
      game scheduled today or later. Falls back to the week of the latest
      game in the season.
    - upcoming groups = playable non-terminal games inside anchor_week.
    - finished groups = terminal games inside the previous week + anchor week.
    - default_tab = "upcoming" if anchor week has playable games, else
      "finished" while inside the 48h completion window after the last
      finished match. Otherwise → fallback.
    """
    now = _now_almaty()
    today = now.date()

    anchor_date = await _find_anchor_date(db, season_id, today)
    if anchor_date is None:
        return await _fallback(db, season_id, frontend_code, lang)

    anchor_start, anchor_end = _iso_week_bounds(anchor_date)
    prev_start = anchor_start - timedelta(days=7)
    prev_end = anchor_start - timedelta(days=1)

    week_games = await _load_date_range(db, season_id, prev_start, anchor_end)
    anchor_games = [g for g in week_games if anchor_start <= g.date <= anchor_end]
    prev_games = [g for g in week_games if prev_start <= g.date <= prev_end]

    anchor_terminal = [g for g in anchor_games if g.status in TERMINAL_STATUSES]
    anchor_upcoming = [
        g for g in anchor_games
        if g.status not in TERMINAL_STATUSES and _is_playable(g, today, None)
    ]
    prev_terminal = [g for g in prev_games if g.status in TERMINAL_STATUSES]

    finished_combined = sorted(
        prev_terminal + anchor_terminal,
        key=lambda g: (g.date, g.time or time_type(0, 0)),
    )
    finished_groups = _group_widget_games(finished_combined, lang)
    upcoming_groups = _group_widget_games(anchor_upcoming, lang)

    selected_round = _mode_tour(anchor_games) or _mode_tour(prev_games)
    has_upcoming = bool(anchor_upcoming)
    completed_expires = _completion_expires_for_terminal(
        prev_terminal + anchor_terminal, now,
    )

    if completed_expires is not None:
        window_state = "completed_window"
        default_tab = "finished"
    elif has_upcoming:
        window_state = "active_round"
        default_tab = "upcoming"
    else:
        return await _fallback(db, season_id, frontend_code, lang)

    main_groups = upcoming_groups if default_tab == "upcoming" else finished_groups
    if not main_groups:
        main_groups = finished_groups or upcoming_groups

    return HomeMatchesWidgetResponse(
        frontend_code=frontend_code,
        season_id=season_id,
        selected_round=selected_round,
        window_state=window_state,
        default_tab=default_tab,
        show_tabs=bool(finished_groups) and bool(upcoming_groups),
        groups=main_groups,
        finished_groups=finished_groups,
        upcoming_groups=upcoming_groups,
        completed_window_expires_at=completed_expires,
    )


# ── helpers ──────────────────────────────────────────────────────────


def _now_almaty() -> datetime:
    return datetime.now(ALMATY_TZ)


def _today_almaty() -> date:
    return _now_almaty().date()


def _iso_week_bounds(d: date) -> tuple[date, date]:
    """ISO week (Monday..Sunday) containing *d*."""
    monday = d - timedelta(days=d.weekday())
    sunday = monday + timedelta(days=6)
    return monday, sunday


def _mode_tour(games: list[Game]) -> int | None:
    """Most common tour number among games (None if none have tours)."""
    counts: dict[int, int] = {}
    for g in games:
        if g.tour is not None:
            counts[g.tour] = counts.get(g.tour, 0) + 1
    if not counts:
        return None
    return max(counts.items(), key=lambda kv: (kv[1], -kv[0]))[0]


def _completion_expires_for_terminal(
    terminal_games: list[Game], now: datetime,
) -> datetime | None:
    """Return the 48h-window expiry anchored on the last finished match."""
    if not terminal_games:
        return None
    finished_times = [_game_finished_at(g) for g in terminal_games]
    valid = [t for t in finished_times if t is not None]
    if not valid:
        return None
    expires_at = max(valid) + COMPLETED_WINDOW
    return expires_at if expires_at > now else None


async def _load_date_range(
    db: AsyncSession,
    season_id: int,
    date_from: date,
    date_to: date,
) -> list[Game]:
    """Load all season games inside [date_from, date_to] regardless of tour."""
    result = await db.execute(
        select(Game)
        .where(
            Game.season_id == season_id,
            Game.date >= date_from,
            Game.date <= date_to,
        )
        .options(*_EAGER_OPTIONS)
        .order_by(Game.date.asc(), Game.time.asc())
    )
    games = list(result.scalars().all())
    if games:
        await enrich_games_has_stats(db, games)
    return games


async def _find_anchor_date(
    db: AsyncSession, season_id: int, today: date,
) -> date | None:
    """Date used to derive the anchor ISO week.

    Prefers the nearest non-terminal game scheduled today or later. If the
    season has no upcoming games at all, falls back to the latest scheduled
    date in the season (so the widget keeps showing the last weekend).
    """
    upcoming_query = select(func.min(Game.date)).where(
        Game.season_id == season_id,
        Game.date >= today,
        Game.status.in_([GameStatus.created, GameStatus.live]),
    )
    upcoming = (await db.execute(upcoming_query)).scalar()
    if upcoming is not None:
        return upcoming
    latest_query = select(func.max(Game.date)).where(Game.season_id == season_id)
    return (await db.execute(latest_query)).scalar()


def _finished_games(games: list[Game]) -> list[Game]:
    return [game for game in games if game.status in TERMINAL_STATUSES]


def _unfinished_games(games: list[Game], today: date | None = None) -> list[Game]:
    """Playable games awaiting a result.

    Postponed/cancelled, stale-created, and outlier-future games are
    excluded — they do not represent fixtures still going to happen on
    schedule.
    """
    tour_anchor = _tour_terminal_last_date(games)
    return [
        game for game in games
        if game.status not in TERMINAL_STATUSES
        and _is_playable(game, today, tour_anchor)
    ]


def _tour_is_fully_terminal(games: list[Game], today: date | None = None) -> bool:
    """Tour is done when every playable game is terminal and at least one is.

    Postponed/cancelled, stale-created, and outlier-future games are
    skipped — a rescheduled fixture (whether marked explicitly, missed on
    the calendar, or moved far beyond the tour cluster) must not keep the
    tour appearing "in progress" forever.
    """
    tour_anchor = _tour_terminal_last_date(games)
    playable = [g for g in games if _is_playable(g, today, tour_anchor)]
    if not playable:
        return False
    return all(g.status in TERMINAL_STATUSES for g in playable)


def _group_widget_games(games: list[Game], lang: str):
    if not games:
        return []
    return group_games_by_date(games, lang, status_mode="home_widget")


async def _resolve_season(db: AsyncSession, frontend_code: str) -> Season | None:
    result = await db.execute(
        select(Season).where(
            Season.frontend_code == frontend_code,
            is_season_visible_clause(),
        )
    )
    seasons = list(result.scalars().all())
    if not seasons:
        return None
    for s in seasons:
        if s.is_current:
            return s
    today = _today_almaty()
    active = [
        s for s in seasons
        if (s.date_start is None or s.date_start <= today)
        and (s.date_end is None or s.date_end >= today)
    ]
    return max(active or seasons, key=lambda s: (s.date_start or date.min, s.id))


def _game_finished_at(game: Game) -> datetime | None:
    """Effective finished-at in Asia/Almaty."""
    if game.finished_at is not None:
        return to_almaty(game.finished_at)
    # Fallback: scheduled date+time (Almaty local)
    t = game.time if game.time is not None else time_type(23, 59, 59)
    return datetime.combine(game.date, t, tzinfo=ALMATY_TZ)


def _recent_completion_expires(
    games: list[Game], now: datetime, today: date | None = None,
) -> datetime | None:
    """Expiry of the 48h "completed" window anchored on the last terminal
    match of the tour.  Postponed / cancelled / stale-created games are
    ignored when picking the anchor — otherwise a fixture rescheduled to
    September would keep the window open for months."""
    if not _tour_is_fully_terminal(games, today):
        return None
    terminal_games = [g for g in games if g.status in TERMINAL_STATUSES]
    finished_times = [_game_finished_at(game) for game in terminal_games]
    valid = [finished_at for finished_at in finished_times if finished_at is not None]
    if not valid:
        return None
    last_finished_at = max(valid)
    expires_at = last_finished_at + COMPLETED_WINDOW
    return expires_at if expires_at > now else None


async def _fallback(
    db: AsyncSession, season_id: int, frontend_code: str, lang: str,
) -> HomeMatchesWidgetResponse:
    today = _today_almaty()
    # Try upcoming first
    result = await db.execute(
        select(Game)
        .where(Game.season_id == season_id, Game.date >= today)
        .options(*_EAGER_OPTIONS)
        .order_by(Game.date.asc(), Game.time.asc())
        .limit(10)
    )
    games = list(result.scalars().all())
    if not games:
        # No upcoming — latest finished
        result = await db.execute(
            select(Game)
            .where(Game.season_id == season_id)
            .options(*_EAGER_OPTIONS)
            .order_by(Game.date.desc(), Game.time.desc())
            .limit(10)
        )
        games = list(result.scalars().all())
    if games:
        await enrich_games_has_stats(db, games)
    groups = _group_widget_games(games, lang)
    return HomeMatchesWidgetResponse(
        frontend_code=frontend_code,
        season_id=season_id,
        selected_round=games[0].tour if games else None,
        window_state="fallback",
        default_tab="upcoming",
        show_tabs=False,
        groups=groups,
        finished_groups=[],
        upcoming_groups=groups,
        completed_window_expires_at=None,
    )


def _empty(frontend_code: str, season_id: int = 0) -> HomeMatchesWidgetResponse:
    return HomeMatchesWidgetResponse(
        frontend_code=frontend_code,
        season_id=season_id,
        selected_round=None,
        window_state="fallback",
        default_tab="upcoming",
        show_tabs=False,
        groups=[],
        finished_groups=[],
        upcoming_groups=[],
        completed_window_expires_at=None,
    )


# ── 2L group-scoped widget ─────────────────────────────────────────


async def _played_round_hint_for_group(
    db: AsyncSession, season_id: int, team_ids: list[int]
) -> int | None:
    """Max tour where both scores are set, scoped to group teams."""
    result = await db.execute(
        select(func.max(Game.tour)).where(
            Game.season_id == season_id,
            Game.tour.isnot(None),
            Game.home_score.isnot(None),
            Game.away_score.isnot(None),
            Game.home_team_id.in_(team_ids),
            Game.away_team_id.in_(team_ids),
        )
    )
    val = result.scalar()
    return int(val) if val is not None else None


async def _load_tours_for_group(
    db: AsyncSession, season_id: int, tours: list[int], team_ids: list[int]
) -> list[Game]:
    """Load tour games scoped to group teams."""
    result = await db.execute(
        select(Game)
        .where(
            Game.season_id == season_id,
            Game.tour.in_(tours),
            Game.home_team_id.in_(team_ids),
            Game.away_team_id.in_(team_ids),
        )
        .options(*_EAGER_OPTIONS)
        .order_by(Game.date.asc(), Game.time.asc())
    )
    games = list(result.scalars().all())
    if games:
        await enrich_games_has_stats(db, games)
    return games


async def _rule0_first_upcoming_group(
    db: AsyncSession,
    season_id: int,
    frontend_code: str,
    lang: str,
    team_ids: list[int],
) -> HomeMatchesWidgetResponse:
    """Rule 0 for group-scoped widget: first upcoming tour."""
    result = await db.execute(
        select(func.min(Game.tour)).where(
            Game.season_id == season_id,
            Game.tour.isnot(None),
            Game.status.in_([GameStatus.created, GameStatus.live]),
            Game.home_team_id.in_(team_ids),
            Game.away_team_id.in_(team_ids),
        )
    )
    first_tour = result.scalar()
    if first_tour is None:
        return _empty(frontend_code, season_id)
    first_tour = int(first_tour)
    games = await _load_tours_for_group(db, season_id, [first_tour], team_ids)
    upcoming_groups = _group_widget_games(games, lang)
    return HomeMatchesWidgetResponse(
        frontend_code=frontend_code,
        season_id=season_id,
        selected_round=first_tour,
        window_state="active_round",
        default_tab="upcoming",
        show_tabs=False,
        groups=upcoming_groups,
        finished_groups=[],
        upcoming_groups=upcoming_groups,
        completed_window_expires_at=None,
    )


async def _get_widget_group(
    db: AsyncSession,
    season_id: int,
    frontend_code: str,
    lang: str,
    group: str,
) -> HomeMatchesWidgetResponse:
    """Home widget for a 2L group (A or B) — same 4-rule logic as PL, scoped to group teams."""
    team_ids = await get_group_team_ids(db, season_id, group)
    if not team_ids:
        return _empty(frontend_code, season_id)

    hint = await _played_round_hint_for_group(db, season_id, team_ids)
    now = _now_almaty()
    today = now.date()

    # Rule 0: no played round → first upcoming tour
    if hint is None:
        return await _rule0_first_upcoming_group(db, season_id, frontend_code, lang, team_ids)

    # Load window [hint-1, hint, hint+1]
    window = [t for t in (hint - 1, hint, hint + 1) if t > 0]
    all_games = await _load_tours_for_group(db, season_id, window, team_ids)
    by_tour: dict[int, list[Game]] = {}
    for g in all_games:
        if g.tour is not None:
            by_tour.setdefault(g.tour, []).append(g)

    hint_games = by_tour.get(hint, [])
    previous_tour_games = by_tour.get(hint - 1, [])
    next_tour = hint + 1
    next_games = by_tour.get(next_tour, [])

    # Rule 1: current tour is still ongoing (has a playable, not-yet-finished game)
    hint_tour_anchor = _tour_terminal_last_date(hint_games)
    if hint_games and any(
        g.status not in TERMINAL_STATUSES and _is_playable(g, today, hint_tour_anchor)
        for g in hint_games
    ):
        finished_groups = _group_widget_games(
            previous_tour_games if _tour_is_fully_terminal(previous_tour_games, today) else [],
            lang,
        )
        upcoming_groups = _group_widget_games(hint_games, lang)
        return HomeMatchesWidgetResponse(
            frontend_code=frontend_code,
            season_id=season_id,
            selected_round=hint,
            window_state="active_round",
            default_tab="upcoming",
            show_tabs=bool(finished_groups) and bool(upcoming_groups),
            groups=upcoming_groups or finished_groups,
            finished_groups=finished_groups,
            upcoming_groups=upcoming_groups,
            completed_window_expires_at=None,
        )

    # Rule 2: current tour completed, next tour exists
    completed_window_expires_at = _recent_completion_expires(hint_games, now, today)
    if next_games:
        finished_groups = _group_widget_games(
            hint_games if _tour_is_fully_terminal(hint_games, today) else [],
            lang,
        )
        upcoming_groups = _group_widget_games(next_games, lang)
        in_completed_window = completed_window_expires_at is not None
        return HomeMatchesWidgetResponse(
            frontend_code=frontend_code,
            season_id=season_id,
            selected_round=hint if in_completed_window else next_tour,
            window_state="completed_window" if in_completed_window else "active_round",
            default_tab="finished" if in_completed_window else "upcoming",
            show_tabs=bool(finished_groups) and bool(upcoming_groups),
            groups=finished_groups if in_completed_window else (upcoming_groups or finished_groups),
            finished_groups=finished_groups,
            upcoming_groups=upcoming_groups,
            completed_window_expires_at=completed_window_expires_at,
        )

    # Rule 3: no next tour yet, keep finished tour for 24h
    if completed_window_expires_at is not None:
        finished_groups = _group_widget_games(hint_games, lang)
        return HomeMatchesWidgetResponse(
            frontend_code=frontend_code,
            season_id=season_id,
            selected_round=hint,
            window_state="completed_window",
            default_tab="finished",
            show_tabs=False,
            groups=finished_groups,
            finished_groups=finished_groups,
            upcoming_groups=[],
            completed_window_expires_at=completed_window_expires_at,
        )

    # Rule 4: fallback — latest games for this group
    return _empty(frontend_code, season_id)


# ── 2L final-stage widget ──────────────────────────────────────────


async def _get_widget_final(
    db: AsyncSession,
    season_id: int,
    frontend_code: str,
    lang: str,
) -> HomeMatchesWidgetResponse:
    """Home widget for 2L final stage — status-based (no tours)."""
    stage_ids = await get_final_stage_ids(db, season_id)
    if not stage_ids:
        return _empty(frontend_code, season_id)

    result = await db.execute(
        select(Game)
        .where(Game.season_id == season_id, Game.stage_id.in_(stage_ids))
        .options(*_EAGER_OPTIONS)
        .order_by(Game.date.asc(), Game.time.asc())
    )
    games = list(result.scalars().all())
    if not games:
        return _empty(frontend_code, season_id)
    await enrich_games_has_stats(db, games)

    now = _now_almaty()
    finished = _finished_games(games)
    unfinished = _unfinished_games(games)

    finished_groups = _group_widget_games(finished, lang)
    upcoming_groups = _group_widget_games(unfinished, lang)

    has_finished = bool(finished)
    has_upcoming = bool(unfinished)
    show_tabs = has_finished and has_upcoming

    # 24h window: if all games finished, show "finished" for 24h
    if has_finished and not has_upcoming:
        completed_window_expires_at = _recent_completion_expires(games, now)
        return HomeMatchesWidgetResponse(
            frontend_code=frontend_code,
            season_id=season_id,
            selected_round=None,
            window_state="completed_window" if completed_window_expires_at else "fallback",
            default_tab="finished",
            show_tabs=False,
            groups=finished_groups,
            finished_groups=finished_groups,
            upcoming_groups=[],
            completed_window_expires_at=completed_window_expires_at,
        )

    # Has live or upcoming games
    if has_upcoming and not has_finished:
        return HomeMatchesWidgetResponse(
            frontend_code=frontend_code,
            season_id=season_id,
            selected_round=None,
            window_state="active_round",
            default_tab="upcoming",
            show_tabs=False,
            groups=upcoming_groups,
            finished_groups=[],
            upcoming_groups=upcoming_groups,
            completed_window_expires_at=None,
        )

    # Mixed: check 24h window for recently finished
    completed_window_expires_at = _recent_completion_expires(finished, now)
    in_window = completed_window_expires_at is not None
    return HomeMatchesWidgetResponse(
        frontend_code=frontend_code,
        season_id=season_id,
        selected_round=None,
        window_state="completed_window" if in_window else "active_round",
        default_tab="finished" if in_window else "upcoming",
        show_tabs=show_tabs,
        groups=finished_groups if in_window else upcoming_groups,
        finished_groups=finished_groups,
        upcoming_groups=upcoming_groups,
        completed_window_expires_at=completed_window_expires_at,
    )
