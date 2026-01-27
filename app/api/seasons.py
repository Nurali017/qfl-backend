from collections import defaultdict
from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, desc, nulls_last, case, or_
from sqlalchemy.orm import selectinload

from app.api.deps import get_db
from app.models import Season, Game, ScoreTable, Team, Player, PlayerSeasonStats, TeamSeasonStats, Country
from app.utils.localization import get_localized_field
from app.schemas.season import SeasonResponse, SeasonListResponse, SeasonStatisticsResponse
from app.schemas.game import GameResponse, GameListResponse
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


@router.get("", response_model=SeasonListResponse)
async def get_seasons(db: AsyncSession = Depends(get_db)):
    """Get all seasons."""
    result = await db.execute(
        select(Season)
        .options(selectinload(Season.tournament))
        .order_by(Season.date_start.desc())
    )
    seasons = result.scalars().all()

    items = []
    for s in seasons:
        tournament_name = None
        if s.tournament:
            tournament_name = s.tournament.name
        items.append(
            SeasonResponse(
                id=s.id,
                name=s.name,
                tournament_id=s.tournament_id,
                date_start=s.date_start,
                date_end=s.date_end,
                tournament_name=tournament_name,
            )
        )

    return SeasonListResponse(items=items, total=len(items))


@router.get("/{season_id}", response_model=SeasonResponse)
async def get_season(season_id: int, db: AsyncSession = Depends(get_db)):
    """Get season by ID."""
    result = await db.execute(
        select(Season).where(Season.id == season_id).options(selectinload(Season.tournament))
    )
    season = result.scalar_one_or_none()

    if not season:
        raise HTTPException(status_code=404, detail="Season not found")

    return SeasonResponse(
        id=season.id,
        name=season.name,
        tournament_id=season.tournament_id,
        date_start=season.date_start,
        date_end=season.date_end,
        tournament_name=season.tournament.name if season.tournament else None,
    )


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


async def calculate_dynamic_table(
    db: AsyncSession,
    season_id: int,
    tour_from: int | None,
    tour_to: int | None,
    home_away: str | None,
    lang: str = "ru",
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
    tour_from: int | None = Query(default=None, description="From matchweek (inclusive)"),
    tour_to: int | None = Query(default=None, description="To matchweek (inclusive)"),
    home_away: str | None = Query(default=None, pattern="^(home|away)$", description="Filter home/away games"),
    lang: str = Query(default="ru", pattern="^(kz|ru|en)$"),
    db: AsyncSession = Depends(get_db),
):
    """
    Get league table for a season.

    Filters:
    - tour_from: Starting matchweek (inclusive)
    - tour_to: Ending matchweek (inclusive)
    - home_away: "home" for home games only, "away" for away games only
    """
    has_filters = tour_from is not None or tour_to is not None or home_away is not None
    filters = ScoreTableFilters(tour_from=tour_from, tour_to=tour_to, home_away=home_away)

    if has_filters:
        table_data = await calculate_dynamic_table(db, season_id, tour_from, tour_to, home_away, lang)
    else:
        result = await db.execute(
            select(ScoreTable)
            .where(ScoreTable.season_id == season_id)
            .options(selectinload(ScoreTable.team))
            .order_by(ScoreTable.position)
        )
        entries = result.scalars().all()

        table_data = []
        for e in entries:
            table_data.append({
                "position": e.position,
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
    lang: str = Query(default="ru", pattern="^(kz|ru|en)$"),
    db: AsyncSession = Depends(get_db),
):
    """
    Get results grid - W/D/L for each team in each tour.

    Returns a matrix where each team has an array of results for each matchweek.
    """
    # Get teams from score_table (sorted by position)
    score_result = await db.execute(
        select(ScoreTable)
        .where(ScoreTable.season_id == season_id)
        .options(selectinload(ScoreTable.team))
        .order_by(ScoreTable.position)
    )
    score_entries = score_result.scalars().all()

    if not score_entries:
        return ResultsGridResponse(season_id=season_id, total_tours=0, teams=[])

    # Get all played games for the season
    games_result = await db.execute(
        select(Game)
        .where(
            Game.season_id == season_id,
            Game.home_score.isnot(None),
            Game.away_score.isnot(None),
            Game.tour.isnot(None),
        )
        .order_by(Game.tour)
    )
    games = games_result.scalars().all()

    # Find max tour
    max_tour = max((g.tour for g in games), default=0)

    # Build results dict: team_id -> [result for each tour]
    team_results: dict[int, list[str | None]] = {}
    for entry in score_entries:
        team_results[entry.team_id] = [None] * max_tour

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

    # Build response
    teams = []
    for entry in score_entries:
        teams.append(
            TeamResultsGridEntry(
                position=entry.position,
                team_id=entry.team_id,
                team_name=get_localized_field(entry.team, "name", lang) if entry.team else None,
                team_logo=entry.team.logo_url if entry.team else None,
                results=team_results.get(entry.team_id, []),
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

    # Count total
    count_query = select(func.count()).select_from(PlayerSeasonStats).where(
        PlayerSeasonStats.season_id == season_id
    )
    total_result = await db.execute(count_query)
    total = total_result.scalar() or 0

    # Main query with JOIN
    query = (
        select(PlayerSeasonStats, Player, Team, Country)
        .join(Player, PlayerSeasonStats.player_id == Player.id)
        .outerjoin(Team, PlayerSeasonStats.team_id == Team.id)
        .outerjoin(Country, Player.country_id == Country.id)
        .where(PlayerSeasonStats.season_id == season_id)
        .order_by(nulls_last(desc(sort_column)))
        .offset(offset)
        .limit(limit)
    )

    result = await db.execute(query)
    rows = result.all()

    items = []
    for stats, player, team, country in rows:
        # Build country data
        country_data = None
        if country:
            country_data = CountryInPlayer(
                id=country.id,
                code=country.code,
                name=country.name,
                flag_url=country.flag_url,
            )

        items.append(
            PlayerStatsTableEntry(
                player_id=player.id,
                first_name=get_localized_field(player, "first_name", lang),
                last_name=get_localized_field(player, "last_name", lang),
                photo_url=player.photo_url,
                country=country_data,
                team_id=team.id if team else None,
                team_name=get_localized_field(team, "name", lang) if team else None,
                team_logo=team.logo_url if team else None,
                games_played=stats.games_played,
                minutes_played=stats.minutes_played,
                goals=stats.goals,
                assists=stats.assists,
                goal_and_assist=stats.goal_and_assist,
                xg=float(stats.xg) if stats.xg else None,
                shots=stats.shots,
                shots_on_goal=stats.shots_on_goal,
                passes=stats.passes,
                key_passes=stats.key_passes,
                pass_accuracy=float(stats.pass_accuracy) if stats.pass_accuracy else None,
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
        )

    return PlayerStatsTableResponse(
        season_id=season_id,
        sort_by=sort_by,
        items=items,
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

    # Get the sort column
    sort_column = getattr(TeamSeasonStats, sort_by, None)
    if sort_column is None:
        raise HTTPException(status_code=400, detail=f"Sort field '{sort_by}' not found")

    # Count total
    count_query = select(func.count()).select_from(TeamSeasonStats).where(
        TeamSeasonStats.season_id == season_id
    )
    total_result = await db.execute(count_query)
    total = total_result.scalar() or 0

    # Main query with JOIN
    query = (
        select(TeamSeasonStats, Team)
        .join(Team, TeamSeasonStats.team_id == Team.id)
        .where(TeamSeasonStats.season_id == season_id)
        .order_by(nulls_last(desc(sort_column)))
        .offset(offset)
        .limit(limit)
    )

    result = await db.execute(query)
    rows = result.all()

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
                pass_accuracy=float(stats.pass_accuracy_avg) if stats.pass_accuracy_avg else None,
                key_passes=stats.key_pass,
                crosses=stats.pass_cross,
                possession=float(stats.possession_avg) if stats.possession_avg else None,
                dribbles=stats.dribble,
                dribble_success=float(stats.dribble_ratio) if stats.dribble_ratio else None,
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
                xg=float(stats.xg) if stats.xg else None,
                xg_per_match=float(stats.xg_per_match) if stats.xg_per_match else None,
            )
        )

    return TeamStatsTableResponse(
        season_id=season_id,
        sort_by=sort_by,
        items=items,
        total=total,
    )
