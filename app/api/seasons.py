from collections import defaultdict
from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, desc, nulls_last, case, or_
from sqlalchemy.orm import selectinload

from app.api.deps import get_db
from app.models import (
    Season,
    Game,
    ScoreTable,
    Team,
    Player,
    PlayerSeasonStats,
    TeamSeasonStats,
    Country,
    GameEvent,
    GameEventType,
    Stage,
    SeasonParticipant,
)
from app.services.season_participants import resolve_season_participants
from app.services.cup_rounds import build_playoff_bracket_from_rounds, build_schedule_rounds
from app.utils.localization import get_localized_field
from app.utils.numbers import to_finite_float
from app.utils.positions import infer_position_code
from app.schemas.season import (
    GoalPeriodItem,
    GoalsByPeriodMeta,
    SeasonGoalsByPeriodResponse,
    SeasonListResponse,
    SeasonResponse,
    SeasonStatisticsResponse,
    SeasonSyncUpdate,
)
from app.schemas.game import GameResponse, GameListResponse
from app.schemas.stage import StageResponse, StageListResponse
from app.schemas.playoff_bracket import PlayoffBracketResponse
from app.schemas.season_participant import (
    SeasonParticipantResponse,
    SeasonParticipantListResponse,
    SeasonGroupsResponse,
)
from app.schemas.stats import (
    ScoreTableResponse,
    ScoreTableEntryResponse,
    ScoreTableFilters,
    NextGameInfo,
    ResultsGridResponse,
    TeamResultsGridEntry,
)
from app.schemas.team import TeamInGame, TeamStatsTableEntry, TeamStatsTableResponse
from app.schemas.player import PlayerStatsTableEntry, PlayerStatsTableResponse
from app.schemas.country import CountryInPlayer

router = APIRouter(prefix="/seasons", tags=["seasons"])


def _build_season_response(s: Season) -> SeasonResponse:
    """Build a SeasonResponse from a Season ORM object (with championship loaded)."""
    return SeasonResponse(
        id=s.id,
        name=s.name,
        championship_id=s.championship_id,
        date_start=s.date_start,
        date_end=s.date_end,
        sync_enabled=s.sync_enabled,
        championship_name=s.championship.name if s.championship else None,
        frontend_code=s.frontend_code,
        tournament_type=s.tournament_type,
        tournament_format=s.tournament_format,
        has_table=s.has_table,
        has_bracket=s.has_bracket,
        sponsor_name=s.sponsor_name,
        sponsor_name_kz=s.sponsor_name_kz,
        logo=s.logo,
        current_round=s.current_round,
        total_rounds=s.total_rounds,
        sort_order=s.sort_order,
        colors=s.colors,
        final_stage_ids=s.final_stage_ids,
    )


GOAL_PERIOD_LABELS = ("0-15", "16-30", "31-45+", "46-60", "61-75", "76-90+")


def _get_goal_period_index(half: int | None, minute: int | None) -> int:
    """
    Map a goal event to one of 6 minute buckets.

    Buckets:
    - 0-15, 16-30, 31-45+ (first half with stoppage time)
    - 46-60, 61-75, 76-90+ (second half with stoppage time)
    """
    safe_minute = max(int(minute or 0), 0)

    if half == 1:
        if safe_minute <= 15:
            return 0
        if safe_minute <= 30:
            return 1
        return 2

    if half == 2:
        if safe_minute <= 60:
            return 3
        if safe_minute <= 75:
            return 4
        return 5

    # Fallback to absolute minute buckets if half is unavailable/invalid.
    if safe_minute <= 15:
        return 0
    if safe_minute <= 30:
        return 1
    if safe_minute <= 45:
        return 2
    if safe_minute <= 60:
        return 3
    if safe_minute <= 75:
        return 4
    return 5


@router.get("", response_model=SeasonListResponse)
async def get_seasons(db: AsyncSession = Depends(get_db)):
    """Get all seasons."""
    result = await db.execute(
        select(Season)
        .options(selectinload(Season.championship))
        .order_by(Season.date_start.desc())
    )
    seasons = result.scalars().all()

    items = []
    for s in seasons:
        items.append(_build_season_response(s))

    return SeasonListResponse(items=items, total=len(items))


@router.patch("/{season_id}/sync", response_model=SeasonResponse)
async def update_season_sync(
    season_id: int,
    body: SeasonSyncUpdate,
    db: AsyncSession = Depends(get_db),
):
    """Enable or disable SOTA sync for a season. When disabled, local data is source of truth."""
    result = await db.execute(
        select(Season).where(Season.id == season_id).options(selectinload(Season.championship))
    )
    season = result.scalar_one_or_none()
    if not season:
        raise HTTPException(status_code=404, detail="Season not found")

    season.sync_enabled = body.sync_enabled
    await db.commit()
    await db.refresh(season)

    return _build_season_response(season)


@router.get("/{season_id}", response_model=SeasonResponse)
async def get_season(season_id: int, db: AsyncSession = Depends(get_db)):
    """Get season by ID."""
    result = await db.execute(
        select(Season).where(Season.id == season_id).options(selectinload(Season.championship))
    )
    season = result.scalar_one_or_none()

    if not season:
        raise HTTPException(status_code=404, detail="Season not found")

    return _build_season_response(season)


async def get_next_games_for_teams(
    db: AsyncSession, season_id: int, team_ids: list[int]
) -> dict[int, NextGameInfo]:
    """Get next upcoming game for each team."""
    if not team_ids:
        return {}

    today = date.today()
    query = (
        select(Game)
        .where(
            Game.season_id == season_id,
            Game.home_score.is_(None),
            Game.date >= today,
            or_(
                Game.home_team_id.in_(team_ids),
                Game.away_team_id.in_(team_ids),
            ),
        )
        .options(selectinload(Game.home_team), selectinload(Game.away_team))
        .order_by(Game.date, Game.time)
    )

    result = await db.execute(query)
    games = result.scalars().all()

    next_games: dict[int, NextGameInfo] = {}
    for game in games:
        if game.home_team_id in team_ids and game.home_team_id not in next_games:
            next_games[game.home_team_id] = NextGameInfo(
                game_id=game.id,
                date=game.date,
                opponent_id=game.away_team_id,
                opponent_name=game.away_team.name if game.away_team else None,
                opponent_logo=game.away_team.logo_url if game.away_team else None,
                is_home=True,
            )
        if game.away_team_id in team_ids and game.away_team_id not in next_games:
            next_games[game.away_team_id] = NextGameInfo(
                game_id=game.id,
                date=game.date,
                opponent_id=game.home_team_id,
                opponent_name=game.home_team.name if game.home_team else None,
                opponent_logo=game.home_team.logo_url if game.home_team else None,
                is_home=False,
            )

    return next_games


async def get_group_team_ids(
    db: AsyncSession, season_id: int, group: str
) -> list[int]:
    """Return team_ids belonging to a specific group within a season."""
    result = await db.execute(
        select(SeasonParticipant.team_id).where(
            SeasonParticipant.season_id == season_id,
            SeasonParticipant.group_name == group,
        )
    )
    return [row[0] for row in result.all()]


def _normalize_stage_ids(raw: object) -> list[int]:
    """Normalize JSON payload to list[int] stage IDs."""
    if not isinstance(raw, list):
        return []

    stage_ids: list[int] = []
    for value in raw:
        try:
            stage_ids.append(int(value))
        except (TypeError, ValueError):
            continue
    return stage_ids


async def get_final_stage_ids(db: AsyncSession, season_id: int) -> list[int]:
    """Return configured final stage IDs for a season."""
    result = await db.execute(
        select(Season.final_stage_ids).where(Season.id == season_id)
    )
    row = result.first()
    if row is None:
        return []
    return _normalize_stage_ids(row[0])


async def calculate_dynamic_table(
    db: AsyncSession,
    season_id: int,
    tour_from: int | None,
    tour_to: int | None,
    home_away: str | None,
    lang: str = "ru",
    group_team_ids: list[int] | None = None,
    final_stage_ids: list[int] | None = None,
) -> list[dict]:
    """Calculate league table dynamically from games with filters."""
    query = (
        select(Game)
        .where(
            Game.season_id == season_id,
            Game.home_score.isnot(None),
            Game.away_score.isnot(None),
        )
        .options(selectinload(Game.home_team), selectinload(Game.away_team))
        .order_by(Game.tour, Game.date, Game.time)
    )

    if tour_from is not None:
        query = query.where(Game.tour >= tour_from)
    if tour_to is not None:
        query = query.where(Game.tour <= tour_to)
    if group_team_ids is not None:
        query = query.where(
            Game.home_team_id.in_(group_team_ids),
            Game.away_team_id.in_(group_team_ids),
        )
    if final_stage_ids is not None:
        query = query.where(Game.stage_id.in_(final_stage_ids))

    result = await db.execute(query)
    games = result.scalars().all()

    team_stats: dict[int, dict] = defaultdict(lambda: {
        "team_id": 0,
        "team_name": None,
        "team_logo": None,
        "games_played": 0,
        "wins": 0,
        "draws": 0,
        "losses": 0,
        "goals_scored": 0,
        "goals_conceded": 0,
        "points": 0,
        "form_list": [],
    })

    for game in games:
        home_id = game.home_team_id
        away_id = game.away_team_id
        home_score = game.home_score
        away_score = game.away_score

        if home_away != "away":
            stats = team_stats[home_id]
            stats["team_id"] = home_id
            stats["team_name"] = get_localized_field(game.home_team, "name", lang) if game.home_team else None
            stats["team_logo"] = game.home_team.logo_url if game.home_team else None
            stats["games_played"] += 1
            stats["goals_scored"] += home_score
            stats["goals_conceded"] += away_score

            if home_score > away_score:
                stats["wins"] += 1
                stats["points"] += 3
                stats["form_list"].append("W")
            elif home_score < away_score:
                stats["losses"] += 1
                stats["form_list"].append("L")
            else:
                stats["draws"] += 1
                stats["points"] += 1
                stats["form_list"].append("D")

        if home_away != "home":
            stats = team_stats[away_id]
            stats["team_id"] = away_id
            stats["team_name"] = get_localized_field(game.away_team, "name", lang) if game.away_team else None
            stats["team_logo"] = game.away_team.logo_url if game.away_team else None
            stats["games_played"] += 1
            stats["goals_scored"] += away_score
            stats["goals_conceded"] += home_score

            if away_score > home_score:
                stats["wins"] += 1
                stats["points"] += 3
                stats["form_list"].append("W")
            elif away_score < home_score:
                stats["losses"] += 1
                stats["form_list"].append("L")
            else:
                stats["draws"] += 1
                stats["points"] += 1
                stats["form_list"].append("D")

    table_list = []
    for team_id, stats in team_stats.items():
        if stats["games_played"] > 0:
            stats["goal_difference"] = stats["goals_scored"] - stats["goals_conceded"]
            stats["form"] = "".join(stats["form_list"][-5:])
            del stats["form_list"]
            table_list.append(stats)

    table_list.sort(key=lambda x: (-x["points"], -x["goal_difference"], -x["goals_scored"]))

    for i, entry in enumerate(table_list, 1):
        entry["position"] = i

    return table_list


@router.get("/{season_id}/table")
async def get_season_table(
    season_id: int,
    group: str | None = Query(default=None, description="Filter by group name (e.g. 'A', 'B')"),
    final: bool = Query(default=False, description="Show only final stage matches"),
    tour_from: int | None = Query(default=None, description="From matchweek (inclusive)"),
    tour_to: int | None = Query(default=None, description="To matchweek (inclusive)"),
    home_away: str | None = Query(default=None, pattern="^(home|away)$", description="Filter home/away games"),
    lang: str = Query(default="ru", pattern="^(kz|ru|en)$"),
    db: AsyncSession = Depends(get_db),
):
    """
    Get league table for a season.

    Filters:
    - group: Filter by group name (from SeasonParticipant.group_name)
    - tour_from: Starting matchweek (inclusive)
    - tour_to: Ending matchweek (inclusive)
    - home_away: "home" for home games only, "away" for away games only
    """
    if group and final:
        raise HTTPException(status_code=400, detail="group and final filters are mutually exclusive")

    # Resolve group team_ids if group filter is specified
    group_team_ids: list[int] | None = None
    if group:
        group_team_ids = await get_group_team_ids(db, season_id, group)
        if not group_team_ids:
            filters = ScoreTableFilters(tour_from=tour_from, tour_to=tour_to, home_away=home_away)
            return ScoreTableResponse(season_id=season_id, filters=filters, table=[])

    final_stage_ids: list[int] | None = None
    if final:
        final_stage_ids = await get_final_stage_ids(db, season_id)
        if not final_stage_ids:
            filters = ScoreTableFilters(tour_from=tour_from, tour_to=tour_to, home_away=home_away)
            return ScoreTableResponse(season_id=season_id, filters=filters, table=[])

    has_filters = (
        tour_from is not None
        or tour_to is not None
        or home_away is not None
        or group_team_ids is not None
        or final_stage_ids is not None
    )
    filters = ScoreTableFilters(tour_from=tour_from, tour_to=tour_to, home_away=home_away)

    if has_filters:
        table_data = await calculate_dynamic_table(
            db, season_id, tour_from, tour_to, home_away, lang,
            group_team_ids=group_team_ids,
            final_stage_ids=final_stage_ids,
        )
    else:
        query = (
            select(ScoreTable)
            .where(ScoreTable.season_id == season_id)
            .options(selectinload(ScoreTable.team))
        )
        if group_team_ids is not None:
            query = query.where(ScoreTable.team_id.in_(group_team_ids))
        query = query.order_by(ScoreTable.position)

        result = await db.execute(query)
        entries = result.scalars().all()

        table_data = []
        for i, e in enumerate(entries, 1):
            table_data.append({
                "position": i if group_team_ids else e.position,
                "team_id": e.team_id,
                "team_name": get_localized_field(e.team, "name", lang) if e.team else None,
                "team_logo": e.team.logo_url if e.team else None,
                "games_played": e.games_played,
                "wins": e.wins,
                "draws": e.draws,
                "losses": e.losses,
                "goals_scored": e.goals_scored,
                "goals_conceded": e.goals_conceded,
                "goal_difference": e.goal_difference,
                "points": e.points,
                "form": e.form,
            })

    team_ids = [entry["team_id"] for entry in table_data]
    next_games = await get_next_games_for_teams(db, season_id, team_ids)

    table = []
    for entry in table_data:
        table.append(
            ScoreTableEntryResponse(
                position=entry["position"],
                team_id=entry["team_id"],
                team_name=entry["team_name"],
                team_logo=entry.get("team_logo") or entry.get("logo_url"),
                games_played=entry["games_played"],
                wins=entry["wins"],
                draws=entry["draws"],
                losses=entry["losses"],
                goals_scored=entry["goals_scored"],
                goals_conceded=entry["goals_conceded"],
                goal_difference=entry["goal_difference"],
                points=entry["points"],
                form=entry["form"],
                next_game=next_games.get(entry["team_id"]),
            )
        )

    return ScoreTableResponse(season_id=season_id, filters=filters, table=table)


@router.get("/{season_id}/results-grid", response_model=ResultsGridResponse)
async def get_results_grid(
    season_id: int,
    group: str | None = Query(default=None, description="Filter by group name (e.g. 'A', 'B')"),
    final: bool = Query(default=False, description="Show only final stage matches"),
    lang: str = Query(default="ru", pattern="^(kz|ru|en)$"),
    db: AsyncSession = Depends(get_db),
):
    """
    Get results grid - W/D/L for each team in each tour.

    Returns a matrix where each team has an array of results for each matchweek.
    """
    if group and final:
        raise HTTPException(status_code=400, detail="group and final filters are mutually exclusive")

    # Resolve group team_ids if group filter is specified
    group_team_ids: list[int] | None = None
    if group:
        group_team_ids = await get_group_team_ids(db, season_id, group)
        if not group_team_ids:
            return ResultsGridResponse(season_id=season_id, total_tours=0, teams=[])

    final_stage_ids: list[int] | None = None
    if final:
        final_stage_ids = await get_final_stage_ids(db, season_id)
        if not final_stage_ids:
            return ResultsGridResponse(season_id=season_id, total_tours=0, teams=[])

    # Get teams from score_table (sorted by position) for stable ordering
    score_query = (
        select(ScoreTable)
        .where(ScoreTable.season_id == season_id)
        .options(selectinload(ScoreTable.team))
    )
    if group_team_ids is not None:
        score_query = score_query.where(ScoreTable.team_id.in_(group_team_ids))
    score_query = score_query.order_by(ScoreTable.position)

    score_result = await db.execute(score_query)
    score_entries = score_result.scalars().all()

    # Get all played games for the selected phase
    games_query = (
        select(Game)
        .where(
            Game.season_id == season_id,
            Game.home_score.isnot(None),
            Game.away_score.isnot(None),
            Game.tour.isnot(None),
        )
    )
    if group_team_ids is not None:
        games_query = games_query.where(
            Game.home_team_id.in_(group_team_ids),
            Game.away_team_id.in_(group_team_ids),
        )
    if final_stage_ids is not None:
        games_query = games_query.where(Game.stage_id.in_(final_stage_ids))
    games_query = games_query.order_by(Game.tour)

    games_result = await db.execute(games_query)
    games = games_result.scalars().all()

    if not score_entries and not games:
        return ResultsGridResponse(season_id=season_id, total_tours=0, teams=[])

    # Find max tour
    max_tour = max((g.tour for g in games), default=0)

    score_by_team_id: dict[int, ScoreTable] = {
        entry.team_id: entry for entry in score_entries
    }
    played_team_ids: set[int] = {
        team_id
        for game in games
        for team_id in (game.home_team_id, game.away_team_id)
        if team_id is not None
    }

    if final_stage_ids is not None:
        ordered_team_ids = [entry.team_id for entry in score_entries if entry.team_id in played_team_ids]
        remaining_ids = sorted(played_team_ids - set(ordered_team_ids))
        ordered_team_ids.extend(remaining_ids)
    elif score_entries:
        ordered_team_ids = [entry.team_id for entry in score_entries]
    else:
        ordered_team_ids = sorted(played_team_ids)

    if not ordered_team_ids:
        return ResultsGridResponse(season_id=season_id, total_tours=max_tour, teams=[])

    # Build results dict: team_id -> [result for each tour]
    team_results: dict[int, list[str | None]] = {
        team_id: [None] * max_tour
        for team_id in ordered_team_ids
    }

    # Fill in results from games
    for game in games:
        tour_idx = game.tour - 1
        home_id = game.home_team_id
        away_id = game.away_team_id

        if game.home_score > game.away_score:
            home_result, away_result = "W", "L"
        elif game.home_score < game.away_score:
            home_result, away_result = "L", "W"
        else:
            home_result, away_result = "D", "D"

        if home_id in team_results and tour_idx < len(team_results[home_id]):
            team_results[home_id][tour_idx] = home_result
        if away_id in team_results and tour_idx < len(team_results[away_id]):
            team_results[away_id][tour_idx] = away_result

    missing_team_ids = [
        team_id
        for team_id in ordered_team_ids
        if score_by_team_id.get(team_id) is None or score_by_team_id[team_id].team is None
    ]
    teams_lookup: dict[int, Team] = {}
    if missing_team_ids:
        teams_result = await db.execute(
            select(Team).where(Team.id.in_(missing_team_ids))
        )
        teams_lookup = {team.id: team for team in teams_result.scalars().all()}

    # Build response
    phase_filtered = group_team_ids is not None or final_stage_ids is not None
    teams = []
    for idx, team_id in enumerate(ordered_team_ids, 1):
        score_entry = score_by_team_id.get(team_id)
        team = score_entry.team if score_entry and score_entry.team else teams_lookup.get(team_id)
        position = (
            idx
            if phase_filtered
            else (score_entry.position if score_entry and score_entry.position is not None else idx)
        )
        teams.append(
            TeamResultsGridEntry(
                position=position,
                team_id=team_id,
                team_name=get_localized_field(team, "name", lang) if team else None,
                team_logo=team.logo_url if team else None,
                results=team_results.get(team_id, []),
            )
        )

    return ResultsGridResponse(season_id=season_id, total_tours=max_tour, teams=teams)


@router.get("/{season_id}/games")
async def get_season_games(
    season_id: int,
    tour: int | None = None,
    lang: str = Query(default="ru", pattern="^(kz|ru|en)$"),
    db: AsyncSession = Depends(get_db),
):
    """Get games for a season."""
    query = (
        select(Game)
        .where(Game.season_id == season_id)
        .options(
            selectinload(Game.home_team),
            selectinload(Game.away_team),
            selectinload(Game.season),
        )
        .order_by(Game.date.desc(), Game.time.desc())
    )

    if tour is not None:
        query = query.where(Game.tour == tour)

    result = await db.execute(query)
    games = result.scalars().all()

    items = []
    for g in games:
        home_team = None
        away_team = None
        if g.home_team:
            home_team = {
                "id": g.home_team.id,
                "name": get_localized_field(g.home_team, "name", lang),
                "logo_url": g.home_team.logo_url,
                "score": g.home_score,
            }
        if g.away_team:
            away_team = {
                "id": g.away_team.id,
                "name": get_localized_field(g.away_team, "name", lang),
                "logo_url": g.away_team.logo_url,
                "score": g.away_score,
            }

        items.append({
            "id": g.id,
            "date": g.date.isoformat() if g.date else None,
            "time": g.time.isoformat() if g.time else None,
            "tour": g.tour,
            "season_id": g.season_id,
            "home_score": g.home_score,
            "away_score": g.away_score,
            "has_stats": g.has_stats,
            "stadium": g.stadium,
            "visitors": g.visitors,
            "home_team": home_team,
            "away_team": away_team,
            "season_name": g.season.name if g.season else None,
        })

    return {"items": items, "total": len(items)}


# Available sort fields for player stats
PLAYER_STATS_SORT_FIELDS = [
    "goals", "assists", "xg", "shots", "shots_on_goal",
    "passes", "key_passes", "pass_accuracy",
    "duels", "duels_won", "aerial_duel", "ground_duel",
    "tackle", "interception", "recovery",
    "dribble", "dribble_success",
    "minutes_played", "games_played",
    "yellow_cards", "red_cards",
    "save_shot", "dry_match",
]


@router.get("/{season_id}/player-stats", response_model=PlayerStatsTableResponse)
async def get_player_stats_table(
    season_id: int,
    sort_by: str = Query(default="goals"),
    team_id: int | None = Query(default=None),
    group: str | None = Query(default=None, description="Filter by group name (e.g. 'A', 'B')"),
    position_code: str | None = Query(default=None, pattern="^(GK|DEF|MID|FWD)$"),
    nationality: str | None = Query(default=None, pattern="^(kz|foreign)$"),
    limit: int = Query(default=50, le=100),
    offset: int = Query(default=0),
    lang: str = Query(default="ru", pattern="^(kz|ru|en)$"),
    db: AsyncSession = Depends(get_db),
):
    """
    Get player stats table for a season.

    Sort by: goals, assists, xg, shots, passes, key_passes, duels, tackle,
    interception, dribble, minutes_played, games_played, yellow_cards,
    red_cards, save_shot, dry_match, etc.
    """
    # Validate sort field
    if sort_by not in PLAYER_STATS_SORT_FIELDS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid sort_by field. Available: {', '.join(PLAYER_STATS_SORT_FIELDS)}",
        )

    # Get the sort column
    sort_column = getattr(PlayerSeasonStats, sort_by, None)
    if sort_column is None:
        raise HTTPException(status_code=400, detail=f"Sort field '{sort_by}' not found")

    # Resolve group filter
    if group:
        group_team_ids = await get_group_team_ids(db, season_id, group)
        if not group_team_ids:
            return PlayerStatsTableResponse(items=[], total=0)

    filters = [PlayerSeasonStats.season_id == season_id]
    if team_id is not None:
        filters.append(PlayerSeasonStats.team_id == team_id)
    if group and group_team_ids:
        filters.append(PlayerSeasonStats.team_id.in_(group_team_ids))
    if nationality == "kz":
        filters.append(func.upper(Country.code) == "KZ")
    elif nationality == "foreign":
        filters.append(Country.code.is_not(None))
        filters.append(func.upper(Country.code) != "KZ")

    base_query = (
        select(PlayerSeasonStats, Player, Team, Country)
        .join(Player, PlayerSeasonStats.player_id == Player.id)
        .outerjoin(Team, PlayerSeasonStats.team_id == Team.id)
        .outerjoin(Country, Player.country_id == Country.id)
        .where(*filters)
    )

    def build_entry(
        stats: PlayerSeasonStats,
        player: Player,
        team: Team | None,
        country: Country | None,
    ) -> PlayerStatsTableEntry:
        localized_top_role = get_localized_field(player, "top_role", lang)
        inferred_position_code = infer_position_code(player.player_type, localized_top_role)

        country_data = None
        if country:
            country_data = CountryInPlayer(
                id=country.id,
                code=country.code,
                name=country.name,
                flag_url=country.flag_url,
            )

        return PlayerStatsTableEntry(
            player_id=player.id,
            first_name=get_localized_field(player, "first_name", lang),
            last_name=get_localized_field(player, "last_name", lang),
            photo_url=player.photo_url,
            country=country_data,
            team_id=team.id if team else None,
            team_name=get_localized_field(team, "name", lang) if team else None,
            team_logo=team.logo_url if team else None,
            player_type=player.player_type,
            top_role=localized_top_role,
            position_code=inferred_position_code,
            games_played=stats.games_played,
            minutes_played=stats.minutes_played,
            goals=stats.goals,
            assists=stats.assists,
            goal_and_assist=stats.goal_and_assist,
            xg=to_finite_float(stats.xg),
            shots=stats.shots,
            shots_on_goal=stats.shots_on_goal,
            passes=stats.passes,
            key_passes=stats.key_passes,
            pass_accuracy=to_finite_float(stats.pass_accuracy),
            duels=stats.duels,
            duels_won=stats.duels_won,
            aerial_duel=stats.aerial_duel,
            ground_duel=stats.ground_duel,
            tackle=stats.tackle,
            interception=stats.interception,
            recovery=stats.recovery,
            dribble=stats.dribble,
            dribble_success=stats.dribble_success,
            yellow_cards=stats.yellow_cards,
            red_cards=stats.red_cards,
            save_shot=stats.save_shot,
            dry_match=stats.dry_match,
        )

    if position_code is None:
        count_query = select(func.count()).select_from(base_query.subquery())
        total_result = await db.execute(count_query)
        total = total_result.scalar() or 0

        query = (
            base_query.order_by(nulls_last(desc(sort_column))).offset(offset).limit(limit)
        )
        result = await db.execute(query)
        rows = result.all()
        items = [build_entry(stats, player, team, country) for stats, player, team, country in rows]

        return PlayerStatsTableResponse(
            season_id=season_id,
            sort_by=sort_by,
            items=items,
            total=total,
        )

    # Position code filter (computed from players.player_type/top_role); apply in Python.
    result = await db.execute(base_query)
    rows = result.all()

    items: list[PlayerStatsTableEntry] = []
    for stats, player, team, country in rows:
        entry = build_entry(stats, player, team, country)
        if entry.position_code != position_code:
            continue
        items.append(entry)

    def to_finite_number(value: object) -> float | None:
        return to_finite_float(value)

    def sort_key(item: PlayerStatsTableEntry) -> tuple:
        primary_val = to_finite_number(getattr(item, sort_by, None))
        is_none = 1 if primary_val is None else 0
        primary_sort = 0.0 if primary_val is None else -primary_val
        return (
            is_none,
            primary_sort,
            item.last_name or "",
            item.first_name or "",
            str(item.player_id),
        )

    items_sorted = sorted(items, key=sort_key)
    total = len(items_sorted)
    paginated_items = items_sorted[offset : offset + limit]

    return PlayerStatsTableResponse(
        season_id=season_id,
        sort_by=sort_by,
        items=paginated_items,
        total=total,
    )


@router.get("/{season_id}/statistics", response_model=SeasonStatisticsResponse)
async def get_season_statistics(season_id: int, db: AsyncSession = Depends(get_db)):
    """
    Get aggregated tournament statistics for a season.

    Returns match results, attendance, goals, penalties, fouls, and cards.
    """
    # Verify season exists
    season_result = await db.execute(
        select(Season).where(Season.id == season_id)
    )
    season = season_result.scalar_one_or_none()
    if not season:
        raise HTTPException(status_code=404, detail="Season not found")

    # Query 1: Match stats from Game table
    game_stats_query = select(
        func.count().label("matches_played"),
        func.coalesce(func.sum(Game.home_score), 0).label("home_goals"),
        func.coalesce(func.sum(Game.away_score), 0).label("away_goals"),
        func.coalesce(func.sum(Game.visitors), 0).label("total_attendance"),
        # Count wins (matches where one team won)
        func.sum(case((Game.home_score != Game.away_score, 1), else_=0)).label("wins"),
        # Count draws
        func.sum(case((Game.home_score == Game.away_score, 1), else_=0)).label("draws"),
    ).where(
        Game.season_id == season_id,
        Game.home_score.isnot(None),
        Game.away_score.isnot(None)
    )

    # Query 2: Team stats from TeamSeasonStats
    team_stats_query = select(
        func.coalesce(func.sum(TeamSeasonStats.yellow_cards), 0).label("yellow_cards"),
        func.coalesce(func.sum(TeamSeasonStats.second_yellow_cards), 0).label("second_yellow_cards"),
        func.coalesce(func.sum(TeamSeasonStats.red_cards), 0).label("red_cards"),
        func.coalesce(func.sum(TeamSeasonStats.fouls), 0).label("total_fouls"),
        func.coalesce(func.sum(TeamSeasonStats.penalty), 0).label("penalties"),
    ).where(TeamSeasonStats.season_id == season_id)

    game_result = await db.execute(game_stats_query)
    team_result = await db.execute(team_stats_query)

    game_row = game_result.one()
    team_row = team_result.one()

    matches_played = game_row.matches_played or 0
    total_goals = (game_row.home_goals or 0) + (game_row.away_goals or 0)
    goals_per_match = round(total_goals / matches_played, 2) if matches_played > 0 else 0.0

    total_fouls = team_row.total_fouls or 0
    fouls_per_match = round(total_fouls / matches_played, 0) if matches_played > 0 else 0.0

    # Calculate penalties scored from player stats
    penalties = team_row.penalties or 0
    penalty_goals_query = select(
        func.coalesce(func.sum(PlayerSeasonStats.penalty_success), 0)
    ).where(PlayerSeasonStats.season_id == season_id)
    penalty_result = await db.execute(penalty_goals_query)
    penalties_scored = penalty_result.scalar() or 0

    return SeasonStatisticsResponse(
        season_id=season_id,
        season_name=season.name,
        matches_played=matches_played,
        wins=int(game_row.wins or 0),
        draws=int(game_row.draws or 0),
        total_attendance=int(game_row.total_attendance or 0),
        average_attendance=round((game_row.total_attendance or 0) / matches_played, 0) if matches_played > 0 else 0.0,
        total_goals=total_goals,
        goals_per_match=goals_per_match,
        penalties=penalties,
        penalties_scored=penalties_scored,
        fouls_per_match=fouls_per_match,
        yellow_cards=team_row.yellow_cards or 0,
        second_yellow_cards=team_row.second_yellow_cards or 0,
        red_cards=team_row.red_cards or 0,
    )


@router.get("/{season_id}/goals-by-period", response_model=SeasonGoalsByPeriodResponse)
async def get_goals_by_period(season_id: int, db: AsyncSession = Depends(get_db)):
    """Get goals grouped by minute periods for a season."""
    season_result = await db.execute(
        select(Season).where(Season.id == season_id)
    )
    season = season_result.scalar_one_or_none()
    if not season:
        raise HTTPException(status_code=404, detail="Season not found")

    matches_played_result = await db.execute(
        select(func.count()).select_from(Game).where(
            Game.season_id == season_id,
            Game.home_score.isnot(None),
            Game.away_score.isnot(None),
        )
    )
    matches_played = matches_played_result.scalar() or 0

    events_result = await db.execute(
        select(
            GameEvent.game_id,
            GameEvent.half,
            GameEvent.minute,
            GameEvent.team_id,
            Game.home_team_id,
            Game.away_team_id,
        )
        .join(Game, GameEvent.game_id == Game.id)
        .where(
            Game.season_id == season_id,
            Game.home_score.isnot(None),
            Game.away_score.isnot(None),
            GameEvent.event_type == GameEventType.goal,
        )
    )
    event_rows = events_result.all()

    matches_with_goal_events = len({row.game_id for row in event_rows})

    periods = [GoalPeriodItem(period=label) for label in GOAL_PERIOD_LABELS]

    for row in event_rows:
        period_idx = _get_goal_period_index(row.half, row.minute)
        period_item = periods[period_idx]
        period_item.goals += 1

        if row.team_id is not None:
            if row.team_id == row.home_team_id:
                period_item.home += 1
            elif row.team_id == row.away_team_id:
                period_item.away += 1

    coverage_pct = round((matches_with_goal_events / matches_played) * 100, 1) if matches_played > 0 else 0.0

    return SeasonGoalsByPeriodResponse(
        season_id=season_id,
        period_size_minutes=15,
        periods=periods,
        meta=GoalsByPeriodMeta(
            matches_played=matches_played,
            matches_with_goal_events=matches_with_goal_events,
            coverage_pct=coverage_pct,
        ),
    )


# Available sort fields for team stats
TEAM_STATS_SORT_FIELDS = [
    "points", "goals_scored", "goals_conceded", "goal_difference",
    "wins", "draws", "losses", "games_played",
    "shots", "shots_on_goal", "possession_avg",
    "passes", "pass_accuracy_avg", "key_pass",
    "tackle", "interception", "recovery",
    "dribble", "fouls", "yellow_cards", "red_cards",
    "xg", "corners", "offsides",
]


@router.get("/{season_id}/team-stats", response_model=TeamStatsTableResponse)
async def get_team_stats_table(
    season_id: int,
    sort_by: str = Query(default="points"),
    group: str | None = Query(default=None, description="Filter by group name (e.g. 'A', 'B')"),
    limit: int = Query(default=50, le=100),
    offset: int = Query(default=0),
    lang: str = Query(default="ru", pattern="^(kz|ru|en)$"),
    db: AsyncSession = Depends(get_db),
):
    """
    Get statistics for all teams in a season.

    Sort by: points, goals_scored, goals_conceded, wins, draws, losses,
    shots, passes, possession_avg, tackles, fouls, yellow_cards, etc.
    """
    # Validate sort field
    if sort_by not in TEAM_STATS_SORT_FIELDS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid sort_by field. Available: {', '.join(TEAM_STATS_SORT_FIELDS)}",
        )

    if getattr(TeamSeasonStats, sort_by, None) is None:
        raise HTTPException(status_code=400, detail=f"Sort field '{sort_by}' not found")

    # Resolve group filter
    group_team_ids: list[int] | None = None
    if group:
        group_team_ids = await get_group_team_ids(db, season_id, group)
        if not group_team_ids:
            return TeamStatsTableResponse(season_id=season_id, sort_by=sort_by, items=[], total=0)

    participants = await resolve_season_participants(db, season_id, lang)
    if group_team_ids is not None:
        allowed = set(group_team_ids)
        participants = [p for p in participants if p.team_id in allowed]

    def to_finite_number(value: object) -> float | None:
        if isinstance(value, (int, float)) and value == value:  # NaN check
            return float(value)
        return None

    def sort_items(items: list[TeamStatsTableEntry]) -> list[TeamStatsTableEntry]:
        primary = sort_by if sort_by in TeamStatsTableEntry.model_fields else "points"

        def key(item: TeamStatsTableEntry) -> tuple:
            primary_val = to_finite_number(getattr(item, primary, None))
            points_val = to_finite_number(getattr(item, "points", None))
            gd_val = to_finite_number(getattr(item, "goal_difference", None))
            gs_val = to_finite_number(getattr(item, "goals_scored", None))

            # None values last; numbers sorted descending
            def sortable(v: float | None) -> tuple[int, float]:
                return (1, 0.0) if v is None else (0, -v)

            return (
                sortable(primary_val),
                sortable(points_val),
                sortable(gd_val),
                sortable(gs_val),
            )

        return sorted(items, key=key)

    def append_missing_participants(items: list[TeamStatsTableEntry]) -> None:
        existing_team_ids = {item.team_id for item in items}
        for participant in participants:
            if participant.team_id in existing_team_ids:
                continue
            team = participant.team
            items.append(
                TeamStatsTableEntry(
                    team_id=team.id,
                    team_name=get_localized_field(team, "name", lang),
                    team_logo=team.logo_url,
                )
            )

    main_filters = [TeamSeasonStats.season_id == season_id]
    if group_team_ids is not None:
        main_filters.append(TeamSeasonStats.team_id.in_(group_team_ids))

    main_result = await db.execute(
        select(TeamSeasonStats, Team)
        .join(Team, TeamSeasonStats.team_id == Team.id)
        .where(*main_filters)
    )
    rows = main_result.all()

    # Fallback: if v2 TeamSeasonStats is empty, build a basic table
    if not rows:
        score_table_query = (
            select(ScoreTable, Team)
            .join(Team, ScoreTable.team_id == Team.id)
            .where(ScoreTable.season_id == season_id)
        )
        if group_team_ids is not None:
            score_table_query = score_table_query.where(ScoreTable.team_id.in_(group_team_ids))
        score_table_result = await db.execute(score_table_query)
        score_table_rows = score_table_result.all()

        fallback_items: list[TeamStatsTableEntry] = []
        if score_table_rows:
            for st, team in score_table_rows:
                games = st.games_played or 0
                goals_scored = st.goals_scored or 0
                goals_conceded = st.goals_conceded or 0

                goals_per_match = round(goals_scored / games, 2) if games > 0 else None
                goals_conceded_per_match = (
                    round(goals_conceded / games, 2) if games > 0 else None
                )

                goal_difference = (
                    st.goal_difference
                    if st.goal_difference is not None
                    else (goals_scored - goals_conceded if games > 0 else None)
                )

                fallback_items.append(
                    TeamStatsTableEntry(
                        team_id=team.id,
                        team_name=get_localized_field(team, "name", lang),
                        team_logo=team.logo_url,
                        games_played=st.games_played,
                        wins=st.wins,
                        draws=st.draws,
                        losses=st.losses,
                        goals_scored=st.goals_scored,
                        goals_conceded=st.goals_conceded,
                        goal_difference=goal_difference,
                        points=st.points,
                        goals_per_match=goals_per_match,
                        goals_conceded_per_match=goals_conceded_per_match,
                    )
                )
        else:
            # Cup-style seasons may have no score_table; fallback to finished games aggregation
            games_fallback_query = select(
                Game.home_team_id, Game.away_team_id, Game.home_score, Game.away_score
            ).where(
                Game.season_id == season_id,
                Game.home_score.isnot(None),
                Game.away_score.isnot(None),
            )
            if group_team_ids is not None:
                games_fallback_query = games_fallback_query.where(
                    Game.home_team_id.in_(group_team_ids),
                    Game.away_team_id.in_(group_team_ids),
                )
            games_result = await db.execute(games_fallback_query)
            game_rows = games_result.all()

            team_stats: dict[int, dict] = {}
            for home_id, away_id, home_score, away_score in game_rows:
                for team_id in (home_id, away_id):
                    team_stats.setdefault(
                        team_id,
                        {
                            "team_id": team_id,
                            "games_played": 0,
                            "wins": 0,
                            "draws": 0,
                            "losses": 0,
                            "goals_scored": 0,
                            "goals_conceded": 0,
                            "points": 0,
                        },
                    )

                team_stats[home_id]["games_played"] += 1
                team_stats[away_id]["games_played"] += 1
                team_stats[home_id]["goals_scored"] += home_score
                team_stats[home_id]["goals_conceded"] += away_score
                team_stats[away_id]["goals_scored"] += away_score
                team_stats[away_id]["goals_conceded"] += home_score

                if home_score > away_score:
                    team_stats[home_id]["wins"] += 1
                    team_stats[home_id]["points"] += 3
                    team_stats[away_id]["losses"] += 1
                elif home_score < away_score:
                    team_stats[away_id]["wins"] += 1
                    team_stats[away_id]["points"] += 3
                    team_stats[home_id]["losses"] += 1
                else:
                    team_stats[home_id]["draws"] += 1
                    team_stats[away_id]["draws"] += 1
                    team_stats[home_id]["points"] += 1
                    team_stats[away_id]["points"] += 1

            if team_stats:
                teams_result = await db.execute(
                    select(Team).where(Team.id.in_(list(team_stats.keys())))
                )
                teams_by_id = {t.id: t for t in teams_result.scalars().all()}

                for team_id, stats in team_stats.items():
                    games = stats["games_played"] or 0
                    goals_scored = stats["goals_scored"]
                    goals_conceded = stats["goals_conceded"]
                    goals_per_match = round(goals_scored / games, 2) if games > 0 else None
                    goals_conceded_per_match = (
                        round(goals_conceded / games, 2) if games > 0 else None
                    )
                    team = teams_by_id.get(team_id)
                    fallback_items.append(
                        TeamStatsTableEntry(
                            team_id=team_id,
                            team_name=get_localized_field(team, "name", lang) if team else str(team_id),
                            team_logo=team.logo_url if team else None,
                            games_played=stats["games_played"],
                            wins=stats["wins"],
                            draws=stats["draws"],
                            losses=stats["losses"],
                            goals_scored=goals_scored,
                            goals_conceded=goals_conceded,
                            goal_difference=goals_scored - goals_conceded,
                            points=stats["points"],
                            goals_per_match=goals_per_match,
                            goals_conceded_per_match=goals_conceded_per_match,
                        )
                    )

        append_missing_participants(fallback_items)
        sorted_items = sort_items(fallback_items)
        paged_items = sorted_items[offset : offset + limit]
        return TeamStatsTableResponse(
            season_id=season_id,
            sort_by=sort_by,
            items=paged_items,
            total=len(sorted_items),
        )

    items = []
    for stats, team in rows:
        # Calculate derived metrics
        games = stats.games_played or 1
        goals_per_match = round((stats.goals_scored or 0) / games, 2) if games > 0 else None
        goals_conceded_per_match = round((stats.goals_conceded or 0) / games, 2) if games > 0 else None
        shots_per_match = round((stats.shots or 0) / games, 2) if games > 0 else None
        fouls_per_match = round((stats.fouls or 0) / games, 2) if games > 0 else None

        # Shot accuracy
        shot_accuracy = None
        if stats.shots and stats.shots > 0:
            shot_accuracy = round((stats.shots_on_goal or 0) / stats.shots * 100, 1)

        items.append(
            TeamStatsTableEntry(
                team_id=team.id,
                team_name=get_localized_field(team, "name", lang),
                team_logo=team.logo_url,
                games_played=stats.games_played,
                wins=stats.wins,
                draws=stats.draws,
                losses=stats.losses,
                goals_scored=stats.goals_scored,
                goals_conceded=stats.goals_conceded,
                goal_difference=stats.goals_difference,
                points=stats.points,
                goals_per_match=goals_per_match,
                goals_conceded_per_match=goals_conceded_per_match,
                shots=stats.shots,
                shots_on_goal=stats.shots_on_goal,
                shot_accuracy=shot_accuracy,
                shots_per_match=shots_per_match,
                passes=stats.passes,
                pass_accuracy=to_finite_float(stats.pass_accuracy_avg),
                key_passes=stats.key_pass,
                crosses=stats.pass_cross,
                possession=to_finite_float(stats.possession_avg),
                dribbles=stats.dribble,
                dribble_success=to_finite_float(stats.dribble_ratio),
                tackles=stats.tackle,
                interceptions=stats.interception,
                recoveries=stats.recovery,
                fouls=stats.fouls,
                fouls_per_match=fouls_per_match,
                yellow_cards=stats.yellow_cards,
                second_yellow_cards=stats.second_yellow_cards,
                red_cards=stats.red_cards,
                corners=stats.corners,
                offsides=stats.offsides,
                xg=to_finite_float(stats.xg),
                xg_per_match=to_finite_float(stats.xg_per_match),
            )
        )

    append_missing_participants(items)
    sorted_items = sort_items(items)
    paged_items = sorted_items[offset : offset + limit]

    return TeamStatsTableResponse(
        season_id=season_id,
        sort_by=sort_by,
        items=paged_items,
        total=len(sorted_items),
    )


# ──────────────────────────────────────────
#  Season sub-resources: Stages, Bracket, Teams/Groups
# ──────────────────────────────────────────


@router.get("/{season_id}/stages", response_model=StageListResponse)
async def get_season_stages(
    season_id: int,
    lang: str = Query(default="ru", pattern="^(kz|ru|en)$"),
    db: AsyncSession = Depends(get_db),
):
    """Get stages/tours for a season."""
    result = await db.execute(
        select(Stage)
        .where(Stage.season_id == season_id)
        .order_by(Stage.sort_order, Stage.stage_number, Stage.id)
    )
    stages = result.scalars().all()

    items = [
        StageResponse(
            id=s.id,
            season_id=s.season_id,
            name=get_localized_field(s, "name", lang),
            stage_number=s.stage_number,
            sort_order=s.sort_order,
        )
        for s in stages
    ]

    return StageListResponse(items=items, total=len(items))


@router.get("/{season_id}/stages/{stage_id}/games")
async def get_stage_games(
    season_id: int,
    stage_id: int,
    lang: str = Query(default="ru", pattern="^(kz|ru|en)$"),
    db: AsyncSession = Depends(get_db),
):
    """Get games for a specific stage/tour."""
    result = await db.execute(
        select(Game)
        .where(Game.season_id == season_id, Game.stage_id == stage_id)
        .options(
            selectinload(Game.home_team),
            selectinload(Game.away_team),
            selectinload(Game.season),
        )
        .order_by(Game.date, Game.time)
    )
    games = result.scalars().all()

    items = []
    for g in games:
        home_team = None
        away_team = None
        if g.home_team:
            home_team = {
                "id": g.home_team.id,
                "name": get_localized_field(g.home_team, "name", lang),
                "logo_url": g.home_team.logo_url,
                "score": g.home_score,
            }
        if g.away_team:
            away_team = {
                "id": g.away_team.id,
                "name": get_localized_field(g.away_team, "name", lang),
                "logo_url": g.away_team.logo_url,
                "score": g.away_score,
            }

        items.append({
            "id": g.id,
            "date": g.date.isoformat() if g.date else None,
            "time": g.time.isoformat() if g.time else None,
            "tour": g.tour,
            "season_id": g.season_id,
            "stage_id": g.stage_id,
            "home_score": g.home_score,
            "away_score": g.away_score,
            "home_penalty_score": g.home_penalty_score,
            "away_penalty_score": g.away_penalty_score,
            "has_stats": g.has_stats,
            "stadium": g.stadium,
            "visitors": g.visitors,
            "home_team": home_team,
            "away_team": away_team,
            "season_name": g.season.name if g.season else None,
        })

    return {"items": items, "total": len(items)}


@router.get("/{season_id}/bracket", response_model=PlayoffBracketResponse)
async def get_season_bracket(
    season_id: int,
    lang: str = Query(default="ru", pattern="^(kz|ru|en)$"),
    db: AsyncSession = Depends(get_db),
):
    """Get playoff bracket for a season, derived from games and stages."""
    stage_result = await db.execute(
        select(Stage)
        .where(Stage.season_id == season_id)
        .order_by(Stage.sort_order, Stage.id)
    )
    stages = list(stage_result.scalars().all())

    games_result = await db.execute(
        select(Game)
        .where(Game.season_id == season_id)
        .options(
            selectinload(Game.home_team),
            selectinload(Game.away_team),
            selectinload(Game.stage),
        )
        .order_by(Game.date, Game.time)
    )
    games = list(games_result.scalars().all())

    rounds = build_schedule_rounds(
        games=games,
        stages=stages,
        lang=lang,
        today=date.today(),
        include_games=True,
    )
    bracket = build_playoff_bracket_from_rounds(season_id, rounds)
    return bracket or PlayoffBracketResponse(season_id=season_id, rounds=[])


@router.get("/{season_id}/teams", response_model=SeasonParticipantListResponse)
async def get_season_teams(
    season_id: int,
    lang: str = Query(default="ru", pattern="^(kz|ru|en)$"),
    db: AsyncSession = Depends(get_db),
):
    """Get all teams participating in a season."""
    participants = await resolve_season_participants(db, season_id, lang)
    items = [
        SeasonParticipantResponse(
            id=p.entry_id if p.entry_id is not None else -p.team_id,
            team_id=p.team_id,
            team_name=get_localized_field(p.team, "name", lang),
            team_logo=p.team.logo_url,
            season_id=season_id,
            group_name=p.group_name,
            is_disqualified=p.is_disqualified,
            fine_points=p.fine_points,
            sort_order=p.sort_order,
        )
        for p in participants
    ]

    return SeasonParticipantListResponse(items=items, total=len(items))


@router.get("/{season_id}/groups", response_model=SeasonGroupsResponse)
async def get_season_groups(
    season_id: int,
    lang: str = Query(default="ru", pattern="^(kz|ru|en)$"),
    db: AsyncSession = Depends(get_db),
):
    """Get teams grouped by group_name for a season."""
    result = await db.execute(
        select(SeasonParticipant)
        .where(SeasonParticipant.season_id == season_id)
        .options(selectinload(SeasonParticipant.team))
        .order_by(SeasonParticipant.group_name, SeasonParticipant.sort_order, SeasonParticipant.id)
    )
    entries = result.scalars().all()

    groups: dict[str, list[SeasonParticipantResponse]] = {}
    for tt in entries:
        group_key = tt.group_name or "default"
        item = SeasonParticipantResponse(
            id=tt.id,
            team_id=tt.team_id,
            team_name=get_localized_field(tt.team, "name", lang) if tt.team else None,
            team_logo=tt.team.logo_url if tt.team else None,
            season_id=tt.season_id,
            group_name=tt.group_name,
            is_disqualified=tt.is_disqualified,
            fine_points=tt.fine_points,
            sort_order=tt.sort_order,
        )
        groups.setdefault(group_key, []).append(item)

    return SeasonGroupsResponse(season_id=season_id, groups=groups)
