"""Season table endpoints: standings table, results grid, league performance."""

from collections import defaultdict

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.api.deps import get_db
from app.models import Game, ScoreTable, Team
from app.services.season_filters import get_group_team_ids, get_final_stage_ids
from app.services.standings import (
    calculate_dynamic_table,
    read_score_table,
    get_next_games_for_teams,
)
from app.services.season_visibility import ensure_visible_season_or_404
from app.utils.localization import get_localized_field
from app.schemas.stats import (
    ScoreTableResponse,
    ScoreTableEntryResponse,
    ScoreTableFilters,
    ResultsGridResponse,
    TeamResultsGridEntry,
)

router = APIRouter(prefix="/seasons", tags=["seasons"])

_ensure_visible_season = ensure_visible_season_or_404


@router.get("/{season_id}/table")
async def get_season_table(
    season_id: int,
    group: str | None = Query(default=None, description="Filter by group name (e.g. 'A', 'B')"),
    final: bool = Query(default=False, description="Show only final stage matches"),
    tour_from: int | None = Query(default=None, description="From matchweek (inclusive)"),
    tour_to: int | None = Query(default=None, description="To matchweek (inclusive)"),
    home_away: str | None = Query(default=None, pattern="^(home|away)$", description="Filter home/away games"),
    lang: str = Query(default="kz", pattern="^(kz|ru|en)$"),
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
    await _ensure_visible_season(db, season_id)

    if group and final:
        raise HTTPException(status_code=400, detail="group and final filters are mutually exclusive")

    # Resolve group team_ids if group filter is specified
    group_team_ids: list[int] | None = None
    if group:
        group_team_ids = await get_group_team_ids(db, season_id, group)
        if not group_team_ids:
            filters = ScoreTableFilters(tour_from=tour_from, tour_to=tour_to, home_away=home_away)
            return ScoreTableResponse(season_id=season_id, filters=filters, table=[])

    final_stage_ids_list: list[int] | None = None
    if final:
        final_stage_ids_list = await get_final_stage_ids(db, season_id)
        if not final_stage_ids_list:
            filters = ScoreTableFilters(tour_from=tour_from, tour_to=tour_to, home_away=home_away)
            return ScoreTableResponse(season_id=season_id, filters=filters, table=[])

    has_filters = (
        tour_from is not None
        or tour_to is not None
        or home_away is not None
        or group_team_ids is not None
        or final_stage_ids_list is not None
    )
    filters = ScoreTableFilters(tour_from=tour_from, tour_to=tour_to, home_away=home_away)

    if has_filters:
        table_data = await calculate_dynamic_table(
            db, season_id, tour_from, tour_to, home_away, lang,
            group_team_ids=group_team_ids,
            final_stage_ids=final_stage_ids_list,
        )
        # Fallback: if dynamic calculation returned nothing (e.g. no games played yet),
        # fall back to score_table
        if not table_data:
            table_data = await read_score_table(db, season_id, group_team_ids, lang)
    else:
        table_data = await read_score_table(db, season_id, group_team_ids, lang)

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
    lang: str = Query(default="kz", pattern="^(kz|ru|en)$"),
    db: AsyncSession = Depends(get_db),
):
    """
    Get results grid - W/D/L for each team in each tour.

    Returns a matrix where each team has an array of results for each matchweek.
    """
    await _ensure_visible_season(db, season_id)

    if group and final:
        raise HTTPException(status_code=400, detail="group and final filters are mutually exclusive")

    # Resolve group team_ids if group filter is specified
    group_team_ids: list[int] | None = None
    if group:
        group_team_ids = await get_group_team_ids(db, season_id, group)
        if not group_team_ids:
            return ResultsGridResponse(season_id=season_id, total_tours=0, teams=[])

    final_stage_ids_list: list[int] | None = None
    if final:
        final_stage_ids_list = await get_final_stage_ids(db, season_id)
        if not final_stage_ids_list:
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
    if final_stage_ids_list is not None:
        games_query = games_query.where(Game.stage_id.in_(final_stage_ids_list))
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

    if final_stage_ids_list is not None:
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
    phase_filtered = group_team_ids is not None or final_stage_ids_list is not None
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


@router.get("/{season_id}/league-performance")
async def get_league_performance(
    season_id: int,
    team_ids: str | None = Query(default=None, description="Comma-separated team IDs to filter"),
    lang: str = Query(default="kz", pattern="^(kz|ru|en)$"),
    db: AsyncSession = Depends(get_db),
):
    """
    Get league position for each team at each matchweek.
    Returns positions over time for the league performance chart.
    """
    await _ensure_visible_season(db, season_id)

    filter_team_ids: set[int] | None = None
    if team_ids:
        filter_team_ids = {
            int(x.strip()) for x in team_ids.split(",") if x.strip().isdigit()
        }

    query = (
        select(Game)
        .where(
            Game.season_id == season_id,
            Game.home_score.isnot(None),
            Game.away_score.isnot(None),
            Game.tour.isnot(None),
        )
        .options(selectinload(Game.home_team), selectinload(Game.away_team))
        .order_by(Game.tour, Game.date, Game.time)
    )

    result = await db.execute(query)
    games = result.scalars().all()

    if not games:
        return {"season_id": season_id, "max_tour": 0, "teams": []}

    max_tour = max(g.tour for g in games)

    games_by_tour: dict[int, list] = defaultdict(list)
    for g in games:
        games_by_tour[g.tour].append(g)

    # Collect all team IDs and info
    team_info: dict[int, dict] = {}
    for game in games:
        if game.home_team_id not in team_info and game.home_team:
            team_info[game.home_team_id] = {
                "name": get_localized_field(game.home_team, "name", lang),
                "logo": game.home_team.logo_url,
            }
        if game.away_team_id not in team_info and game.away_team:
            team_info[game.away_team_id] = {
                "name": get_localized_field(game.away_team, "name", lang),
                "logo": game.away_team.logo_url,
            }

    # Initialize cumulative stats for all teams
    cumulative: dict[int, dict] = {
        tid: {"points": 0, "gd": 0, "gs": 0}
        for tid in team_info
    }
    positions_by_team: dict[int, list[int]] = {tid: [] for tid in team_info}

    for tour in range(1, max_tour + 1):
        for game in games_by_tour.get(tour, []):
            h_id, a_id = game.home_team_id, game.away_team_id
            h_score, a_score = game.home_score, game.away_score

            cumulative[h_id]["gs"] += h_score
            cumulative[h_id]["gd"] += h_score - a_score
            cumulative[a_id]["gs"] += a_score
            cumulative[a_id]["gd"] += a_score - h_score

            if h_score > a_score:
                cumulative[h_id]["points"] += 3
            elif h_score < a_score:
                cumulative[a_id]["points"] += 3
            else:
                cumulative[h_id]["points"] += 1
                cumulative[a_id]["points"] += 1

        standings = sorted(
            cumulative.items(),
            key=lambda x: (-x[1]["points"], -x[1]["gd"], -x[1]["gs"]),
        )
        for pos, (tid, _) in enumerate(standings, 1):
            positions_by_team[tid].append(pos)

    teams_result = []
    for tid, positions in positions_by_team.items():
        if filter_team_ids and tid not in filter_team_ids:
            continue
        info = team_info.get(tid, {"name": None, "logo": None})
        teams_result.append({
            "team_id": tid,
            "team_name": info["name"],
            "team_logo": info["logo"],
            "positions": positions,
        })

    teams_result.sort(
        key=lambda t: t["positions"][-1] if t["positions"] else 999
    )

    return {"season_id": season_id, "max_tour": max_tour, "teams": teams_result}
