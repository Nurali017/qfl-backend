"""Season statistics endpoints: player stats, team stats, statistics summary, goals by period."""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, desc, nulls_last, case, distinct, extract, text, cast
from sqlalchemy.types import Numeric
from sqlalchemy.orm import selectinload

from app.api.deps import get_db
from app.models import (
    Season, Game, ScoreTable, Team, Player, PlayerTeam,
    PlayerSeasonStats, TeamSeasonStats, Country,
    GameEvent, GameEventType, GameTeamStats, GamePlayerStats,
    PlayerTourStats,
)
from app.services.season_filters import get_group_team_ids
from app.services.season_participants import resolve_season_participants
from app.services.season_scope import compute_season_stats_scope
from app.services.season_visibility import ensure_visible_season_or_404, is_season_visible_clause
from app.utils.localization import get_localized_field
from app.utils.numbers import to_finite_float
from app.utils.team_logo_fallback import resolve_team_logo_url
from app.schemas.season import (
    GoalPeriodItem, GoalsByPeriodMeta,
    SeasonGoalsByPeriodResponse, SeasonStatisticsResponse,
)
from app.schemas.team import TeamStatsTableEntry, TeamStatsTableResponse
from app.schemas.player import PlayerStatsTableEntry, PlayerStatsTableResponse
from app.schemas.country import CountryInPlayer

# Import helpers from router module
from app.api.seasons.router import GOAL_PERIOD_LABELS, _get_goal_period_index

router = APIRouter(prefix="/seasons", tags=["seasons"])

AMPLUA_TO_POSITION = {1: "GK", 2: "DEF", 3: "MID", 4: "FWD"}
POSITION_TO_AMPLUA = {v: k for k, v in AMPLUA_TO_POSITION.items()}

_ensure_visible_season = ensure_visible_season_or_404


# Available sort fields for player stats
PLAYER_STATS_SORT_FIELDS = [
    "goal", "goal_pass", "goal_and_assist", "xg", "shot", "shots_on_goal",
    "passes", "key_pass", "pass_ratio",
    "duel", "duel_success", "aerial_duel", "ground_duel",
    "tackle", "interception", "recovery",
    "dribble", "dribble_success",
    "time_on_field_total", "games_played",
    "yellow_cards", "second_yellow_cards", "red_cards",
    "save_shot", "dry_match",
    "owngoal", "penalty_success", "goal_out_box", "xg_per_90",
    "shots_blocked_opponent",
    "pass_acc", "pass_forward", "pass_progressive", "pass_cross", "pass_to_box", "pass_to_3rd",
    "dribble_per_90", "corner", "offside",
    "tackle_per_90", "aerial_duel_success", "ground_duel_success",
    "foul", "foul_taken",
    "goals_conceded", "goals_conceded_penalty", "save_shot_ratio", "save_shot_penalty", "exit", "exit_success",
]


@router.get("/{season_id}/player-stats", response_model=PlayerStatsTableResponse)
async def get_player_stats_table(
    season_id: int,
    sort_by: str = Query(default="goal"),
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

    Sort by: goal, goal_pass, xg, shot, passes, key_pass, duel, tackle,
    interception, dribble, time_on_field_total, games_played, yellow_cards,
    red_cards, save_shot, dry_match, etc.
    """
    await _ensure_visible_season(db, season_id)

    # Validate sort field
    if sort_by not in PLAYER_STATS_SORT_FIELDS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid sort_by field. Available: {', '.join(PLAYER_STATS_SORT_FIELDS)}",
        )

    # Get the sort column — red_cards sorts by the computed sum (direct + second yellow)
    # For goal/goal_pass/dry_match, sort by SOTA rank (ASC) to match SOTA's order
    RANK_SORT_FIELDS = {"goal": "goal_rank", "goal_pass": "goal_pass_rank", "dry_match": "dry_match_rank"}
    if sort_by in RANK_SORT_FIELDS:
        sort_column = getattr(PlayerSeasonStats, RANK_SORT_FIELDS[sort_by])
        use_rank_sort = True
    elif sort_by == "red_cards":
        sort_column = (
            func.coalesce(PlayerSeasonStats.red_cards, 0)
            + func.coalesce(PlayerSeasonStats.second_yellow_cards, 0)
        )
        use_rank_sort = False
    else:
        sort_column = getattr(PlayerSeasonStats, sort_by, None)
        if sort_column is None:
            raise HTTPException(status_code=400, detail=f"Sort field '{sort_by}' not found")
        use_rank_sort = False

    # Resolve group filter
    if group:
        group_team_ids = await get_group_team_ids(db, season_id, group)
        if not group_team_ids:
            return PlayerStatsTableResponse(items=[], total=0)

    filters = [PlayerSeasonStats.season_id == season_id]
    # For rank-based sorts (goal, goal_pass, dry_match), only show SOTA-ranked players
    if use_rank_sort:
        filters.append(sort_column.is_not(None))
    if team_id is not None:
        filters.append(PlayerSeasonStats.team_id == team_id)
    if group and group_team_ids:
        filters.append(PlayerSeasonStats.team_id.in_(group_team_ids))
    if nationality == "kz":
        filters.append(func.upper(Country.code) == "KZ")
    elif nationality == "foreign":
        filters.append(Country.code.is_not(None))
        filters.append(func.upper(Country.code) != "KZ")

    contract_photo_subq = (
        select(PlayerTeam.photo_url)
        .where(
            PlayerTeam.player_id == PlayerSeasonStats.player_id,
            PlayerTeam.team_id == PlayerSeasonStats.team_id,
            PlayerTeam.season_id == PlayerSeasonStats.season_id,
        )
        .limit(1)
        .correlate(PlayerSeasonStats)
        .scalar_subquery()
    )

    contract_photo_avatar_subq = (
        select(PlayerTeam.photo_url_avatar)
        .where(
            PlayerTeam.player_id == PlayerSeasonStats.player_id,
            PlayerTeam.team_id == PlayerSeasonStats.team_id,
            PlayerTeam.season_id == PlayerSeasonStats.season_id,
        )
        .limit(1)
        .correlate(PlayerSeasonStats)
        .scalar_subquery()
    )

    contract_photo_leaderboard_subq = (
        select(PlayerTeam.photo_url_leaderboard)
        .where(
            PlayerTeam.player_id == PlayerSeasonStats.player_id,
            PlayerTeam.team_id == PlayerSeasonStats.team_id,
            PlayerTeam.season_id == PlayerSeasonStats.season_id,
        )
        .limit(1)
        .correlate(PlayerSeasonStats)
        .scalar_subquery()
    )

    contract_amplua_subq = (
        select(PlayerTeam.amplua)
        .where(
            PlayerTeam.player_id == PlayerSeasonStats.player_id,
            PlayerTeam.team_id == PlayerSeasonStats.team_id,
            PlayerTeam.season_id == PlayerSeasonStats.season_id,
        )
        .limit(1)
        .correlate(PlayerSeasonStats)
        .scalar_subquery()
    )

    if position_code:
        filters.append(contract_amplua_subq == POSITION_TO_AMPLUA[position_code])

    base_query = (
        select(
            PlayerSeasonStats, Player, Team, Country,
            contract_photo_subq.label("contract_photo"),
            contract_photo_avatar_subq.label("contract_photo_avatar"),
            contract_photo_leaderboard_subq.label("contract_photo_leaderboard"),
            contract_amplua_subq.label("contract_amplua"),
        )
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
        contract_photo: str | None = None,
        contract_photo_avatar: str | None = None,
        contract_photo_leaderboard: str | None = None,
        contract_amplua: int | None = None,
    ) -> PlayerStatsTableEntry:
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
            photo_url=contract_photo,
            photo_url_avatar=contract_photo_avatar,
            photo_url_leaderboard=contract_photo_leaderboard,
            country=country_data,
            team_id=team.id if team else None,
            team_name=get_localized_field(team, "name", lang) if team else None,
            team_logo=resolve_team_logo_url(team),
            player_type=player.player_type,
            position_code=AMPLUA_TO_POSITION.get(contract_amplua),
            games_played=stats.games_played,
            time_on_field_total=stats.time_on_field_total,
            goal=stats.goal,
            goal_pass=stats.goal_pass,
            goal_and_assist=stats.goal_and_assist,
            xg=to_finite_float(stats.xg),
            shot=stats.shot,
            shots_on_goal=stats.shots_on_goal,
            passes=stats.passes,
            key_pass=stats.key_pass,
            pass_ratio=to_finite_float(stats.pass_ratio),
            duel=stats.duel,
            duel_success=stats.duel_success,
            aerial_duel=stats.aerial_duel,
            ground_duel=stats.ground_duel,
            tackle=stats.tackle,
            interception=stats.interception,
            recovery=stats.recovery,
            dribble=stats.dribble,
            dribble_success=stats.dribble_success,
            yellow_cards=stats.yellow_cards,
            second_yellow_cards=stats.second_yellow_cards,
            red_cards=(stats.red_cards or 0) + (stats.second_yellow_cards or 0),
            save_shot=stats.save_shot,
            dry_match=stats.dry_match,
            owngoal=stats.owngoal,
            penalty_success=stats.penalty_success,
            goal_out_box=stats.goal_out_box,
            xg_per_90=to_finite_float(stats.xg_per_90),
            shots_blocked_opponent=stats.shots_blocked_opponent,
            pass_acc=stats.pass_acc,
            pass_forward=stats.pass_forward,
            pass_progressive=stats.pass_progressive,
            pass_cross=stats.pass_cross,
            pass_cross_acc=stats.pass_cross_acc,
            pass_to_box=stats.pass_to_box,
            pass_to_3rd=stats.pass_to_3rd,
            dribble_per_90=to_finite_float(stats.dribble_per_90),
            corner=stats.corner,
            offside=stats.offside,
            tackle_per_90=to_finite_float(stats.tackle_per_90),
            aerial_duel_success=stats.aerial_duel_success,
            ground_duel_success=stats.ground_duel_success,
            foul=stats.foul,
            foul_taken=stats.foul_taken,
            goals_conceded=stats.goals_conceded,
            goals_conceded_penalty=stats.goals_conceded_penalty,
            save_shot_ratio=to_finite_float(stats.save_shot_ratio),
            save_shot_penalty=stats.save_shot_penalty,
            exit=stats.exit,
            exit_success=stats.exit_success,
        )

    count_query = select(func.count()).select_from(base_query.subquery())
    total_result = await db.execute(count_query)
    total = total_result.scalar() or 0

    if use_rank_sort:
        query = base_query.order_by(nulls_last(sort_column.asc())).offset(offset).limit(limit)
    else:
        query = base_query.order_by(nulls_last(desc(sort_column))).offset(offset).limit(limit)
    result = await db.execute(query)
    rows = result.all()
    items = [
        build_entry(
            stats,
            player,
            team,
            country,
            contract_photo,
            contract_photo_avatar,
            contract_photo_leaderboard,
            contract_amplua,
        )
        for stats, player, team, country, contract_photo, contract_photo_avatar, contract_photo_leaderboard, contract_amplua in rows
    ]

    return PlayerStatsTableResponse(
        season_id=season_id,
        sort_by=sort_by,
        items=items,
        total=total,
    )


# Map response field names (sent by frontend) to DB column names.
# Fields that match the DB column name map to themselves.
TEAM_STATS_SORT_ALIAS: dict[str, str] = {
    "goal_difference": "goals_difference",
}

# Computed fields that exist on TeamStatsTableEntry but not on the DB model.
# These are sorted in Python via sort_items() and must bypass the DB column check.
TEAM_STATS_RESPONSE_ONLY_FIELDS = {
    "goals_per_match", "goals_conceded_per_match",
    "shot_accuracy", "shot_per_match",
    "foul_per_match",
}

# Available sort fields for team stats (accept both response names and DB names)
TEAM_STATS_SORT_FIELDS = [
    "points", "goal", "goals_conceded", "goal_difference",
    "win", "draw", "match_loss", "games_played",
    "shot", "shots_on_goal", "possession_percent_average",
    "passes", "pass_ratio", "key_pass",
    "tackle", "interception", "recovery",
    "dribble", "dribble_ratio", "pass_cross",
    "foul", "yellow_cards", "second_yellow_cards", "red_cards",
    "xg", "corner", "offside",
    # Computed response-only fields (sorted in Python, not DB)
    "goals_per_match", "goals_conceded_per_match",
    "shot_accuracy", "shot_per_match",
    "foul_per_match",
    # Extra fields
    "shots_off_goal",
    "pass_per_match", "pass_forward", "pass_long", "pass_progressive", "pass_to_box", "pass_to_3rd", "goal_pass",
    "duel", "duel_ratio",
    "aerial_duel_offence", "aerial_duel_defence",
    "ground_duel_offence", "ground_duel_defence",
    "tackle_per_match", "tackle1_1", "interception_per_match", "recovery_per_match",
    "foul_taken", "penalty", "penalty_ratio",
    "opponent_xg", "visitor_total", "average_visitors",
    "freekick_shot",
]


@router.get("/{season_id}/team-stats", response_model=TeamStatsTableResponse)
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

    # Resolve alias: frontend sends response field names, DB may use different names
    response_sort_by = sort_by  # keep original for Python-side sort on response objects
    db_sort_by = TEAM_STATS_SORT_ALIAS.get(sort_by, sort_by)

    if sort_by not in TEAM_STATS_RESPONSE_ONLY_FIELDS:
        if getattr(TeamSeasonStats, db_sort_by, None) is None:
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
        primary = response_sort_by if response_sort_by in TeamStatsTableEntry.model_fields else "points"

        def key(item: TeamStatsTableEntry) -> tuple:
            primary_val = to_finite_number(getattr(item, primary, None))
            points_val = to_finite_number(getattr(item, "points", None))
            gd_val = to_finite_number(getattr(item, "goal_difference", None))
            gs_val = to_finite_number(getattr(item, "goal", None))

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
                    team_logo=resolve_team_logo_url(team),
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
                        team_logo=resolve_team_logo_url(team),
                        games_played=st.games_played,
                        win=st.wins,
                        draw=st.draws,
                        match_loss=st.losses,
                        goal=st.goals_scored,
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
                            team_logo=resolve_team_logo_url(team),
                            games_played=stats["games_played"],
                            win=stats["wins"],
                            draw=stats["draws"],
                            match_loss=stats["losses"],
                            goal=goals_scored,
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
        goals_per_match = round((stats.goal or 0) / games, 2) if games > 0 else None
        goals_conceded_per_match = round((stats.goals_conceded or 0) / games, 2) if games > 0 else None
        shot_per_match_val = round((stats.shot or 0) / games, 2) if games > 0 else None
        foul_per_match = round((stats.foul or 0) / games, 2) if games > 0 else None

        # Shot accuracy
        shot_accuracy = None
        if stats.shot and stats.shot > 0:
            shot_accuracy = round((stats.shots_on_goal or 0) / stats.shot * 100, 1)

        items.append(
            TeamStatsTableEntry(
                team_id=team.id,
                team_name=get_localized_field(team, "name", lang),
                team_logo=resolve_team_logo_url(team),
                games_played=stats.games_played,
                win=stats.win,
                draw=stats.draw,
                match_loss=stats.match_loss,
                goal=stats.goal,
                goals_conceded=stats.goals_conceded,
                goal_difference=stats.goals_difference,
                points=stats.points,
                goals_per_match=goals_per_match,
                goals_conceded_per_match=goals_conceded_per_match,
                shot=stats.shot,
                shots_on_goal=stats.shots_on_goal,
                shot_accuracy=shot_accuracy,
                shot_per_match=shot_per_match_val,
                passes=stats.passes,
                pass_ratio=to_finite_float(stats.pass_ratio),
                key_pass=stats.key_pass,
                pass_cross=stats.pass_cross,
                possession_percent_average=to_finite_float(stats.possession_percent_average),
                dribble=stats.dribble,
                dribble_ratio=to_finite_float(stats.dribble_ratio),
                tackle=stats.tackle,
                interception=stats.interception,
                recovery=stats.recovery,
                foul=stats.foul,
                foul_per_match=foul_per_match,
                yellow_cards=stats.yellow_cards,
                second_yellow_cards=stats.second_yellow_cards,
                red_cards=stats.red_cards,
                corner=stats.corner,
                offside=stats.offside,
                xg=to_finite_float(stats.xg),
                xg_per_match=to_finite_float(stats.xg_per_match),
                shots_off_goal=stats.shots_off_goal,
                pass_per_match=to_finite_float(stats.pass_per_match),
                pass_forward=stats.pass_forward,
                pass_long=stats.pass_long,
                pass_progressive=stats.pass_progressive,
                pass_to_box=stats.pass_to_box,
                pass_to_3rd=stats.pass_to_3rd,
                goal_pass=stats.goal_pass,
                duel=stats.duel,
                duel_ratio=to_finite_float(stats.duel_ratio),
                aerial_duel_offence=stats.aerial_duel_offence,
                aerial_duel_offence_ratio=to_finite_float(stats.aerial_duel_offence_ratio),
                aerial_duel_defence=stats.aerial_duel_defence,
                aerial_duel_defence_ratio=to_finite_float(stats.aerial_duel_defence_ratio),
                ground_duel_offence=stats.ground_duel_offence,
                ground_duel_offence_ratio=to_finite_float(stats.ground_duel_offence_ratio),
                ground_duel_defence=stats.ground_duel_defence,
                ground_duel_defence_ratio=to_finite_float(stats.ground_duel_defence_ratio),
                tackle_per_match=to_finite_float(stats.tackle_per_match),
                tackle1_1=stats.tackle1_1,
                tackle1_1_ratio=to_finite_float(stats.tackle1_1_ratio),
                interception_per_match=to_finite_float(stats.interception_per_match),
                recovery_per_match=to_finite_float(stats.recovery_per_match),
                foul_taken=stats.foul_taken,
                penalty=stats.penalty,
                penalty_ratio=to_finite_float(stats.penalty_ratio),
                opponent_xg=to_finite_float(stats.opponent_xg),
                visitor_total=stats.visitor_total,
                average_visitors=to_finite_float(stats.average_visitors),
                freekick_shot=stats.freekick_shot,
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
async def get_season_statistics(
    season_id: int,
    max_round: int | None = Query(None, description="Filter stats up to this round (tour) number"),
    db: AsyncSession = Depends(get_db),
):
    """
    Get aggregated tournament statistics for a season.

    Returns match results, attendance, goals, penalties, fouls, and cards.
    When max_round is specified, only games with tour <= max_round are included.
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

    # Compute scope: completed round and effective round cap
    max_completed_round, effective_max_round = await compute_season_stats_scope(
        db, season_id, season, max_round
    )

    # Base game filters (reused across queries)
    game_base_filters = [
        Game.season_id == season_id,
        Game.home_score.isnot(None),
        Game.away_score.isnot(None),
        Game.extended_stats_synced_at.isnot(None),
    ]
    if effective_max_round is not None:
        game_base_filters.append(Game.tour <= effective_max_round)

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
    ).where(*game_base_filters)

    # Query 2: Team stats from GameTeamStats (aggregated per-game data)
    team_stats_query = select(
        func.coalesce(func.sum(GameTeamStats.yellow_cards), 0).label("yellow_cards"),
        func.coalesce(func.sum(GameTeamStats.red_cards), 0).label("red_cards"),
        func.coalesce(func.sum(GameTeamStats.fouls), 0).label("total_fouls"),
        func.coalesce(func.sum(GameTeamStats.penalties), 0).label("penalties"),
    ).join(Game, GameTeamStats.game_id == Game.id).where(*game_base_filters)

    game_result = await db.execute(game_stats_query)
    team_result = await db.execute(team_stats_query)

    game_row = game_result.one()
    team_row = team_result.one()

    matches_played = game_row.matches_played or 0
    total_goals = (game_row.home_goals or 0) + (game_row.away_goals or 0)
    goals_per_match = round(total_goals / matches_played, 2) if matches_played > 0 else 0.0

    total_fouls = team_row.total_fouls or 0
    fouls_per_match = round(total_fouls / matches_played, 0) if matches_played > 0 else 0.0

    # Calculate penalties scored from GameEvent
    penalties = team_row.penalties or 0
    penalty_scored_query = select(func.count()).select_from(GameEvent).join(
        Game, GameEvent.game_id == Game.id
    ).where(
        Game.season_id == season_id,
        Game.extended_stats_synced_at.isnot(None),
        GameEvent.event_type == GameEventType.penalty,
    )
    if effective_max_round is not None:
        penalty_scored_query = penalty_scored_query.where(Game.tour <= effective_max_round)
    penalty_result = await db.execute(penalty_scored_query)
    penalties_scored = penalty_result.scalar() or 0

    # Count red cards from game_events (direct reds only)
    red_card_count_query = select(func.count()).select_from(GameEvent).join(
        Game, GameEvent.game_id == Game.id
    ).where(
        Game.season_id == season_id,
        Game.extended_stats_synced_at.isnot(None),
        GameEvent.event_type == GameEventType.red_card,
    )
    if effective_max_round is not None:
        red_card_count_query = red_card_count_query.where(Game.tour <= effective_max_round)
    red_card_result = await db.execute(red_card_count_query)
    total_red_cards = red_card_result.scalar() or 0

    # Count second yellow cards from game_events
    second_yellow_count_query = select(func.count()).select_from(GameEvent).join(
        Game, GameEvent.game_id == Game.id
    ).where(
        Game.season_id == season_id,
        Game.extended_stats_synced_at.isnot(None),
        GameEvent.event_type == GameEventType.second_yellow,
    )
    if effective_max_round is not None:
        second_yellow_count_query = second_yellow_count_query.where(Game.tour <= effective_max_round)
    second_yellow_result = await db.execute(second_yellow_count_query)
    total_second_yellows = second_yellow_result.scalar() or 0

    # Query 3a: avg xG per match — from per-game player stats
    # Two separate queries: total rows and xG sum (avoids JSONB has_key which breaks SQLite)
    xg_total_query = select(
        func.count().label("total_rows"),
    ).select_from(GamePlayerStats).join(
        Game, GamePlayerStats.game_id == Game.id
    ).where(*game_base_filters)
    total_rows = (await db.execute(xg_total_query)).scalar() or 0

    xg_sum_query = select(
        func.count().label("rows_with_xg"),
        func.coalesce(func.sum(cast(GamePlayerStats.extra_stats['xg'].as_string(), Numeric)), 0).label("total_xg"),
    ).select_from(GamePlayerStats).join(
        Game, GamePlayerStats.game_id == Game.id
    ).where(
        *game_base_filters,
        GamePlayerStats.extra_stats['xg'].isnot(None),
    )
    xg_row = (await db.execute(xg_sum_query)).one()

    rows_with_xg = xg_row.rows_with_xg or 0
    total_xg = float(xg_row.total_xg or 0)
    xg_coverage = rows_with_xg / total_rows if total_rows > 0 else 0

    if xg_coverage >= 0.8 and matches_played > 0:
        avg_xg_per_match = round(total_xg / matches_played, 2)
    elif effective_max_round is None and matches_played > 0:
        # Full-season, low coverage — fallback to TeamSeasonStats
        season_agg_query = select(
            func.coalesce(func.sum(TeamSeasonStats.xg), 0).label("total_xg"),
            func.coalesce(func.sum(TeamSeasonStats.games_played), 0).label("total_team_games"),
        ).where(TeamSeasonStats.season_id == season_id)
        season_agg_row = (await db.execute(season_agg_query)).one()
        tt_games = int(season_agg_row.total_team_games or 0)
        tt_xg = float(season_agg_row.total_xg or 0)
        avg_xg_per_match = round(tt_xg / (tt_games / 2), 2) if tt_games > 0 else None
    else:
        # Round-scoped, low coverage — null
        avg_xg_per_match = None

    # Query 3b: Pass accuracy — scoped GameTeamStats only
    # Coverage denominator = matches_played * 2 (two team rows per match)
    pa_query = select(
        func.avg(GameTeamStats.pass_accuracy).label("avg_pa"),
        func.count(GameTeamStats.pass_accuracy).label("with_pa"),
    ).join(Game, GameTeamStats.game_id == Game.id).where(*game_base_filters)
    pa_row = (await db.execute(pa_query)).one()
    pa_expected = matches_played * 2
    pa_with = pa_row.with_pa or 0
    pa_coverage = pa_with / pa_expected if pa_expected > 0 else 0

    if pa_coverage >= 0.8 and pa_row.avg_pa is not None:
        pass_accuracy = round(float(pa_row.avg_pa), 1)
    elif effective_max_round is None and matches_played > 0:
        # Full-season fallback: TeamSeasonStats.pass_ratio
        pa_fb = await db.execute(
            select(func.avg(TeamSeasonStats.pass_ratio))
            .where(TeamSeasonStats.season_id == season_id)
        )
        pa_val = pa_fb.scalar()
        pass_accuracy = round(float(pa_val), 1) if pa_val is not None else None
    else:
        pass_accuracy = None

    # Query 4: Shots on target % from GameTeamStats
    shots_query = select(
        func.sum(GameTeamStats.shots_on_goal).label("total_shots_on_goal"),
        func.sum(GameTeamStats.shots).label("total_shots"),
    ).join(Game, GameTeamStats.game_id == Game.id).where(*game_base_filters)
    shots_result = await db.execute(shots_query)
    shots_row = shots_result.one()
    total_shots = int(shots_row.total_shots or 0)
    total_shots_on_goal = int(shots_row.total_shots_on_goal or 0)
    shots_on_target_pct = round(total_shots_on_goal / total_shots * 100, 1) if total_shots > 0 else 0.0

    # Query 5: Clean sheets — count of 0-0 draws
    clean_sheets_filters = [
        Game.season_id == season_id,
        Game.home_score == 0,
        Game.away_score == 0,
        Game.extended_stats_synced_at.isnot(None),
    ]
    if effective_max_round is not None:
        clean_sheets_filters.append(Game.tour <= effective_max_round)
    clean_sheets_query = select(
        func.count().label("clean_sheets"),
    ).where(*clean_sheets_filters)
    clean_sheets_result = await db.execute(clean_sheets_query)
    clean_sheets = int(clean_sheets_result.scalar() or 0)

    # Query 6: Player demographics — total players, minutes, Kazakh minutes %
    # Always use per-game stats (GamePlayerStats) filtered by cutoff
    minutes_query = select(
        func.count(distinct(GamePlayerStats.player_id)).label("total_players"),
        func.coalesce(func.sum(GamePlayerStats.minutes_played), 0).label("total_minutes"),
        func.coalesce(func.sum(
            case(
                (func.upper(Country.code) == "KZ", GamePlayerStats.minutes_played),
                else_=0,
            )
        ), 0).label("kazakh_minutes"),
    ).select_from(GamePlayerStats).join(
        Game, GamePlayerStats.game_id == Game.id
    ).join(
        Player, GamePlayerStats.player_id == Player.id
    ).outerjoin(
        Country, Player.country_id == Country.id
    ).where(*game_base_filters)
    minutes_result = await db.execute(minutes_query)
    min_row = minutes_result.one()
    total_players = int(min_row.total_players or 0)
    total_minutes = int(min_row.total_minutes or 0)
    kazakh_minutes = int(min_row.kazakh_minutes or 0)
    kazakh_minutes_pct = round(kazakh_minutes / total_minutes * 100, 1) if total_minutes > 0 else 0.0

    # Average age
    # Average age — only players who played in games past cutoff
    age_subq = (
        select(Player.birthday)
        .join(GamePlayerStats, GamePlayerStats.player_id == Player.id)
        .join(Game, GamePlayerStats.game_id == Game.id)
        .where(*game_base_filters)
        .distinct(Player.id)
        .subquery()
    )
    age_query = select(
        func.avg(extract("year", func.age(age_subq.c.birthday))).label("avg_age"),
    )
    age_result = await db.execute(age_query)
    average_age = round(float(age_result.scalar() or 0), 1)

    return SeasonStatisticsResponse(
        season_id=season_id,
        season_name=season.name,
        max_completed_round=max_completed_round,
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
        second_yellow_cards=total_second_yellows,
        red_cards=total_red_cards + total_second_yellows,
        avg_xg_per_match=avg_xg_per_match,
        pass_accuracy=pass_accuracy,
        shots_on_target_pct=shots_on_target_pct,
        clean_sheets=clean_sheets,
        total_players=total_players,
        total_minutes=total_minutes,
        kazakh_minutes_pct=kazakh_minutes_pct,
        average_age=average_age,
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

    _, effective_max_round = await compute_season_stats_scope(db, season_id, season)

    game_filters = [
        Game.season_id == season_id,
        Game.home_score.isnot(None),
        Game.away_score.isnot(None),
        Game.extended_stats_synced_at.isnot(None),
    ]
    if effective_max_round is not None:
        game_filters.append(Game.tour <= effective_max_round)

    matches_played_result = await db.execute(
        select(func.count()).select_from(Game).where(*game_filters)
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
            *game_filters,
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
