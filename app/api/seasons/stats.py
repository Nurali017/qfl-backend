"""Season statistics endpoints: player stats, team stats, statistics summary, goals by period."""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, desc, nulls_last, case
from sqlalchemy.orm import selectinload

from app.api.deps import get_db
from app.models import (
    Season, Game, ScoreTable, Team, Player,
    PlayerSeasonStats, TeamSeasonStats, Country,
    GameEvent, GameEventType,
)
from app.services.season_filters import get_group_team_ids
from app.services.season_participants import resolve_season_participants
from app.services.season_visibility import ensure_visible_season_or_404, is_season_visible_clause
from app.utils.localization import get_localized_field
from app.utils.numbers import to_finite_float
from app.utils.positions import infer_position_code
from app.schemas.season import (
    GoalPeriodItem, GoalsByPeriodMeta,
    SeasonGoalsByPeriodResponse, SeasonStatisticsResponse,
)
from app.schemas.team import TeamStatsTableEntry, TeamStatsTableResponse
from app.schemas.player import PlayerStatsTableEntry, PlayerStatsTableResponse
from app.schemas.country import CountryInPlayer

# Import helpers from router module
from app.api.seasons.router import GOAL_PERIOD_LABELS, _get_goal_period_index
from fastapi_cache.decorator import cache

router = APIRouter(prefix="/seasons", tags=["seasons"])

_ensure_visible_season = ensure_visible_season_or_404


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
@cache(expire=7200)
async def get_player_stats_table(
    season_id: int,
    sort_by: str = Query(default="goals"),
    team_id: int | None = Query(default=None),
    group: str | None = Query(default=None, description="Filter by group name (e.g. 'A', 'B')"),
    position_code: str | None = Query(default=None, pattern="^(GK|DEF|MID|FWD)$"),
    nationality: str | None = Query(default=None, pattern="^(kz|foreign)$"),
    limit: int = Query(default=50, le=100),
    offset: int = Query(default=0),
    lang: str = Query(default="kz", pattern="^(kz|ru|en)$"),
    db: AsyncSession = Depends(get_db),
):
    """
    Get player stats table for a season.

    Sort by: goals, assists, xg, shots, passes, key_passes, duels, tackle,
    interception, dribble, minutes_played, games_played, yellow_cards,
    red_cards, save_shot, dry_match, etc.
    """
    await _ensure_visible_season(db, season_id)

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
@cache(expire=7200)
async def get_team_stats_table(
    season_id: int,
    sort_by: str = Query(default="points"),
    group: str | None = Query(default=None, description="Filter by group name (e.g. 'A', 'B')"),
    limit: int = Query(default=50, le=100),
    offset: int = Query(default=0),
    lang: str = Query(default="kz", pattern="^(kz|ru|en)$"),
    db: AsyncSession = Depends(get_db),
):
    """
    Get statistics for all teams in a season.

    Sort by: points, goals_scored, goals_conceded, wins, draws, losses,
    shots, passes, possession_avg, tackles, fouls, yellow_cards, etc.
    """
    await _ensure_visible_season(db, season_id)

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
                for t_id in (home_id, away_id):
                    team_stats.setdefault(
                        t_id,
                        {
                            "team_id": t_id,
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

                for t_id, stats in team_stats.items():
                    games = stats["games_played"] or 0
                    goals_scored = stats["goals_scored"]
                    goals_conceded = stats["goals_conceded"]
                    goals_per_match = round(goals_scored / games, 2) if games > 0 else None
                    goals_conceded_per_match = (
                        round(goals_conceded / games, 2) if games > 0 else None
                    )
                    team = teams_by_id.get(t_id)
                    fallback_items.append(
                        TeamStatsTableEntry(
                            team_id=t_id,
                            team_name=get_localized_field(team, "name", lang) if team else str(t_id),
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


@router.get("/{season_id}/statistics", response_model=SeasonStatisticsResponse)
@cache(expire=7200)
async def get_season_statistics(season_id: int, db: AsyncSession = Depends(get_db)):
    """
    Get aggregated tournament statistics for a season.

    Returns match results, attendance, goals, penalties, fouls, and cards.
    """
    await _ensure_visible_season(db, season_id)

    # Verify season exists
    season_result = await db.execute(
        select(Season).where(
            Season.id == season_id,
            is_season_visible_clause(),
        )
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
    await _ensure_visible_season(db, season_id)

    season_result = await db.execute(
        select(Season).where(
            Season.id == season_id,
            is_season_visible_clause(),
        )
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
