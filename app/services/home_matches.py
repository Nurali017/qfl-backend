"""Home matches widget service for tour-based leagues (pl, 1l, el).

Determines which tour to display on the home page.
Single source of truth for round selection, tab state, and 24h window logic.

Selection rules:
0. played_round_hint is null  → first upcoming tour
1. hint has non-terminal games → upcoming=current tour, finished=previous tour
2. hint fully terminal + next tour exists → upcoming=next tour, finished=hint
3. hint fully terminal in 24h window → default finished for 24h
4. fallback                   → nearest available games
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
WINDOW_24H = timedelta(hours=24)

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

    # 2L: dispatch to group-specific or final logic
    if frontend_code == "2l" and group:
        if group == "final":
            return await _get_widget_final(db, season_id, frontend_code, lang)
        return await _get_widget_group(db, season_id, frontend_code, lang, group)

    hint = await _played_round_hint(db, season_id)
    now = _now_almaty()

    # Rule 0: no played round → first upcoming tour
    if hint is None:
        return await _rule0_first_upcoming(db, season_id, frontend_code, lang)

    # Load window [hint-1, hint, hint+1]
    window = [t for t in (hint - 1, hint, hint + 1) if t > 0]
    all_games = await _load_tours(db, season_id, window)
    by_tour: dict[int, list[Game]] = {}
    for g in all_games:
        if g.tour is not None:
            by_tour.setdefault(g.tour, []).append(g)

    hint_games = by_tour.get(hint, [])
    previous_tour_games = by_tour.get(hint - 1, [])
    next_tour = hint + 1
    next_games = by_tour.get(next_tour, [])

    # Rule 1: current tour is still ongoing → upcoming=current, finished=previous tour
    if hint_games and any(g.status not in TERMINAL_STATUSES for g in hint_games):
        finished_groups = await _enrich_and_group(
            db, season_id,
            previous_tour_games if _tour_is_fully_terminal(previous_tour_games) else [],
            lang,
        )
        upcoming_groups = await _enrich_and_group(db, season_id, hint_games, lang)
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

    # Rule 2: current tour already completed → upcoming=next tour, finished=current completed tour
    completed_window_expires_at = _recent_completion_expires(hint_games, now)
    if next_games:
        finished_groups = await _enrich_and_group(
            db, season_id,
            hint_games if _tour_is_fully_terminal(hint_games) else [],
            lang,
        )
        upcoming_groups = await _enrich_and_group(db, season_id, next_games, lang)
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

    # Rule 3: no next tour yet, but keep finished tour open for 24h
    if completed_window_expires_at is not None:
        finished_groups = await _enrich_and_group(db, season_id, hint_games, lang)
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

    # Rule 4: fallback
    return await _fallback(db, season_id, frontend_code, lang)


# ── helpers ──────────────────────────────────────────────────────────


def _now_almaty() -> datetime:
    return datetime.now(ALMATY_TZ)


def _today_almaty() -> date:
    return _now_almaty().date()


def _finished_games(games: list[Game]) -> list[Game]:
    return [game for game in games if game.status in TERMINAL_STATUSES]


def _unfinished_games(games: list[Game]) -> list[Game]:
    return [game for game in games if game.status not in TERMINAL_STATUSES]


def _tour_is_fully_terminal(games: list[Game]) -> bool:
    return bool(games) and all(game.status in TERMINAL_STATUSES for game in games)


def _group_widget_games(games: list[Game], lang: str):
    if not games:
        return []
    return group_games_by_date(games, lang, status_mode="home_widget")


def _filter_outlier_games(games: list[Game], max_gap_days: int = 7) -> list[Game]:
    """Keep only games whose date is within *max_gap_days* of the median date."""
    dated = [(g, g.date) for g in games if g.date is not None]
    if len(dated) <= 1:
        return [g for g, _ in dated]
    sorted_dates = sorted(d for _, d in dated)
    median = sorted_dates[len(sorted_dates) // 2]
    return [g for g, d in dated if abs((d - median).days) <= max_gap_days]


async def _load_same_date_games(
    db: AsyncSession,
    season_id: int,
    game_dates: set[date],
    exclude_ids: set[int],
) -> list[Game]:
    """Load games from any tour on *game_dates*, excluding already-known ids."""
    if not game_dates:
        return []
    query = (
        select(Game)
        .where(
            Game.season_id == season_id,
            Game.date.in_(game_dates),
        )
        .options(*_EAGER_OPTIONS)
        .order_by(Game.date.asc(), Game.time.asc())
    )
    if exclude_ids:
        query = query.where(~Game.id.in_(exclude_ids))
    result = await db.execute(query)
    games = list(result.scalars().all())
    if games:
        await enrich_games_has_stats(db, games)
    return games


async def _enrich_and_group(
    db: AsyncSession,
    season_id: int,
    tour_games: list[Game],
    lang: str,
):
    """Filter outlier dates, add same-date games from other tours, group by date."""
    if not tour_games:
        return []
    filtered = _filter_outlier_games(tour_games)
    if not filtered:
        return group_games_by_date(tour_games, lang, status_mode="home_widget")
    game_dates = {g.date for g in filtered if g.date}
    known_ids = {g.id for g in filtered}
    extra = await _load_same_date_games(db, season_id, game_dates, known_ids)
    all_games = filtered + extra
    return group_games_by_date(all_games, lang, status_mode="home_widget")


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


async def _played_round_hint(db: AsyncSession, season_id: int) -> int | None:
    """Max tour where both scores are set (same as front-map current_round)."""
    result = await db.execute(
        select(func.max(Game.tour)).where(
            Game.season_id == season_id,
            Game.tour.isnot(None),
            Game.home_score.isnot(None),
            Game.away_score.isnot(None),
        )
    )
    val = result.scalar()
    return int(val) if val is not None else None


async def _load_tours(
    db: AsyncSession, season_id: int, tours: list[int]
) -> list[Game]:
    result = await db.execute(
        select(Game)
        .where(Game.season_id == season_id, Game.tour.in_(tours))
        .options(*_EAGER_OPTIONS)
        .order_by(Game.date.asc(), Game.time.asc())
    )
    games = list(result.scalars().all())
    await enrich_games_has_stats(db, games)
    return games


def _game_finished_at(game: Game) -> datetime | None:
    """Effective finished-at in Asia/Almaty."""
    if game.finished_at is not None:
        return to_almaty(game.finished_at)
    # Fallback: scheduled date+time (Almaty local)
    t = game.time if game.time is not None else time_type(23, 59, 59)
    return datetime.combine(game.date, t, tzinfo=ALMATY_TZ)


def _recent_completion_expires(games: list[Game], now: datetime) -> datetime | None:
    if not _tour_is_fully_terminal(games):
        return None
    finished_times = [_game_finished_at(game) for game in games]
    valid = [finished_at for finished_at in finished_times if finished_at is not None]
    if not valid:
        return None
    last_finished_at = max(valid)
    expires_at = last_finished_at + WINDOW_24H
    return expires_at if expires_at > now else None


async def _rule0_first_upcoming(
    db: AsyncSession, season_id: int, frontend_code: str, lang: str,
) -> HomeMatchesWidgetResponse:
    result = await db.execute(
        select(func.min(Game.tour)).where(
            Game.season_id == season_id,
            Game.tour.isnot(None),
            Game.status.in_([GameStatus.created, GameStatus.live]),
        )
    )
    first_tour = result.scalar()
    if first_tour is None:
        return _empty(frontend_code, season_id)
    first_tour = int(first_tour)
    games = await _load_tours(db, season_id, [first_tour])
    upcoming_groups = await _enrich_and_group(db, season_id, games, lang)
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

    # Rule 1: current tour is still ongoing
    if hint_games and any(g.status not in TERMINAL_STATUSES for g in hint_games):
        finished_groups = _group_widget_games(
            previous_tour_games if _tour_is_fully_terminal(previous_tour_games) else [],
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
    completed_window_expires_at = _recent_completion_expires(hint_games, now)
    if next_games:
        finished_groups = _group_widget_games(
            hint_games if _tour_is_fully_terminal(hint_games) else [],
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
