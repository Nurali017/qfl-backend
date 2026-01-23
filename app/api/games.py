from uuid import UUID
from datetime import date as date_type, datetime
from collections import defaultdict
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, or_
from sqlalchemy.orm import selectinload

from app.api.deps import get_db
from app.models import (
    Game,
    GameTeamStats,
    GamePlayerStats,
    GameEvent,
    GameEventType,
    Team,
    Player,
    GameLineup,
    GameReferee,
    Coach,
    TeamCoach,
    Stadium,
    Referee,
)
from app.schemas.game import (
    GameResponse,
    GameListResponse,
    GameDetailResponse,
    MatchCenterGame,
    MatchCenterDateGroup,
    MatchCenterResponse,
    StadiumInfo,
    TeamInMatchCenter,
)
from app.schemas.stats import (
    GameStatsResponse,
    GameTeamStatsResponse,
    GamePlayerStatsResponse,
)
from app.schemas.team import TeamInGame
from app.utils.date_helpers import format_match_date, get_localized_field
from app.config import get_settings

settings = get_settings()

router = APIRouter(prefix="/games", tags=["games"])


async def get_player_names_fallback(
    db: AsyncSession,
    game_id: UUID,
    player_ids: list[UUID]
) -> dict[UUID, tuple[str | None, str | None]]:
    """
    Get player names with fallback to GameEvent.player_name.

    If Player.first_name or Player.last_name is NULL,
    attempts to get name from GameEvent.player_name.

    Returns: dict {player_id: (first_name, last_name)}
    """
    # Get names from GameEvent for all players in one query
    result = await db.execute(
        select(GameEvent.player_id, GameEvent.player_name)
        .where(
            GameEvent.game_id == game_id,
            GameEvent.player_id.in_(player_ids),
            GameEvent.player_name.isnot(None)
        )
        .distinct(GameEvent.player_id)
    )

    # Create lookup dict with name parsing
    event_names = {}
    for row in result:
        if row.player_name:
            # Parse "First Last" format
            parts = row.player_name.strip().split(maxsplit=1)
            first = parts[0] if len(parts) > 0 else None
            last = parts[1] if len(parts) > 1 else None
            event_names[row.player_id] = (first, last)

    return event_names


def compute_game_status(game: Game, today: date_type | None = None) -> str:
    """
    Compute game status based on data.

    Returns:
        "live" - Game is currently in progress
        "finished" - Game has ended
        "upcoming" - Game is scheduled for the future
    """
    if today is None:
        today = date_type.today()

    if game.is_live:
        return "live"
    elif game.home_score is not None and game.away_score is not None:
        return "finished"
    elif game.date and game.date < today:
        return "finished"  # Past game, treat as finished even without score
    else:
        return "upcoming"


def group_games_by_date(
    games: list[Game],
    language: str = "ru",
    today: date_type | None = None
) -> list[dict]:
    """
    Group games by date with formatted labels.

    Args:
        games: List of Game objects to group
        language: Language for date formatting (kz, ru, en)
        today: Current date for status computation (defaults to today)

    Returns:
        List of dicts with date, date_label, and games
    """
    if today is None:
        today = date_type.today()

    grouped = defaultdict(list)

    for game in games:
        if game.date:
            # Build game dict with status
            game_dict = {
                "id": game.id,
                "date": game.date,
                "time": game.time,
                "tour": game.tour,
                "season_id": game.season_id,
                "home_score": game.home_score,
                "away_score": game.away_score,
                "is_live": game.is_live,
                "has_stats": game.has_stats,
                "has_lineup": game.has_lineup,
                "visitors": game.visitors,
                "status": compute_game_status(game, today),
                "ticket_url": getattr(game, "ticket_url", None),
                "video_url": game.video_url,
                # Teams and stadium will be added by caller
                "game_obj": game,  # Keep reference for relationship access
            }
            grouped[game.date].append(game_dict)

    result = []
    for game_date in sorted(grouped.keys()):
        date_label = format_match_date(game_date, language)
        result.append({
            "date": game_date,
            "date_label": date_label,
            "games": grouped[game_date]
        })

    return result


def detect_formation(positions: list[str | None]) -> str | None:
    """
    Detect team formation from player positions.
    Returns formation string like "4-3-3", "4-4-2", etc.
    """
    if not positions or len(positions) < 10:
        return None

    defenders = 0
    midfielders = 0
    forwards = 0

    for pos in positions[:11]:  # Only first 11 players
        if not pos:
            continue
        pos_code = pos.split()[0] if pos else ""

        if pos_code in ("GK",):
            continue  # Goalkeeper doesn't count
        elif pos_code in ("CD", "LD", "RD", "LB", "RB", "CB"):
            defenders += 1
        elif pos_code in ("DM", "CM", "AM", "LM", "RM", "LW", "RW"):
            midfielders += 1
        elif pos_code in ("CF", "ST", "FW"):
            forwards += 1

    if defenders + midfielders + forwards < 10:
        return None

    return f"{defenders}-{midfielders}-{forwards}"


@router.get("")
async def get_games(
    season_id: int | None = Query(default=None),
    team_id: int | None = None,
    team_ids: list[int] | None = Query(default=None),
    tour: int | None = None,
    tours: list[int] | None = Query(default=None),
    month: int | None = Query(default=None, ge=1, le=12),
    year: int | None = None,
    date_from: date_type | None = None,
    date_to: date_type | None = None,
    status: str | None = Query(default=None, pattern="^(upcoming|finished|live|all)$"),
    hide_past: bool = False,
    group_by_date: bool = False,
    language: str = Query(default="ru", pattern="^(kz|ru|en)$"),
    limit: int = Query(default=50, le=100),
    offset: int = Query(default=0),
    db: AsyncSession = Depends(get_db),
):
    """
    Get games with comprehensive filtering and optional date grouping.

    Filters:
    - season_id: Filter by season (defaults to current season)
    - team_id: Filter by single team (home or away)
    - team_ids: Filter by multiple teams (home or away) - use ?team_ids=1&team_ids=5
    - tour: Filter by single tour/round number
    - tours: Filter by multiple tours - use ?tours=18&tours=19&tours=20
    - month: Filter by month (1-12, requires year)
    - year: Year for month filter
    - date_from: Start of date range
    - date_to: End of date range
    - status: Filter by match status (upcoming, finished, live, all)
    - hide_past: Hide matches before today
    - group_by_date: Group results by date with formatted labels
    - language: Language for localized fields (kz, ru, en)
    """
    if season_id is None:
        season_id = settings.current_season_id

    today = date_type.today()

    # Build base query
    query = select(Game).where(Game.season_id == season_id)

    # Team filtering
    if team_id:
        query = query.where(
            or_(Game.home_team_id == team_id, Game.away_team_id == team_id)
        )
    elif team_ids:
        query = query.where(
            or_(
                Game.home_team_id.in_(team_ids),
                Game.away_team_id.in_(team_ids)
            )
        )

    # Tour filtering
    if tour is not None:
        query = query.where(Game.tour == tour)
    elif tours:
        query = query.where(Game.tour.in_(tours))

    # Date range filtering
    if month is not None and year is not None:
        # Calculate first and last day of the month
        from calendar import monthrange
        first_day = date_type(year, month, 1)
        last_day_num = monthrange(year, month)[1]
        last_day = date_type(year, month, last_day_num)
        query = query.where(Game.date >= first_day, Game.date <= last_day)
    else:
        if date_from:
            query = query.where(Game.date >= date_from)
        if date_to:
            query = query.where(Game.date <= date_to)

    # Hide past matches
    if hide_past:
        query = query.where(Game.date >= today)

    # Status filtering
    if status and status != "all":
        if status == "upcoming":
            query = query.where(
                or_(
                    Game.date > today,
                    (Game.date == today) & (Game.home_score.is_(None))
                )
            )
        elif status == "finished":
            query = query.where(
                or_(
                    Game.home_score.is_not(None),
                    Game.date < today
                )
            )
        elif status == "live":
            query = query.where(Game.is_live == True)

    # Count total
    count_query = select(func.count()).select_from(query.subquery())
    total_result = await db.execute(count_query)
    total = total_result.scalar() or 0

    # Get paginated results with eager loading
    query = (
        query.options(
            selectinload(Game.home_team),
            selectinload(Game.away_team),
            selectinload(Game.season),
            selectinload(Game.stadium_rel),
        )
        .order_by(Game.date.asc(), Game.time.asc())
        .offset(offset)
        .limit(limit)
    )

    result = await db.execute(query)
    games = result.scalars().all()

    # Build response with localized fields
    def build_team_dict(team: Team | None, score: int | None) -> dict | None:
        if not team:
            return None

        return {
            "id": team.id,
            "name": get_localized_field(team, "name", language),
            "name_kz": team.name_kz,
            "name_en": team.name_en,
            "logo_url": team.logo_url,
            "primary_color": team.primary_color,
            "secondary_color": team.secondary_color,
            "accent_color": team.accent_color,
        }

    def build_stadium_dict(stadium: Stadium | None) -> dict | None:
        if not stadium:
            return None

        return {
            "id": stadium.id,
            "name": get_localized_field(stadium, "name", language),
            "city": get_localized_field(stadium, "city", language),
            "capacity": stadium.capacity,
        }

    # Return grouped format if requested
    if group_by_date:
        grouped = group_games_by_date(games, language, today)

        # Add team and stadium info to each game
        for group in grouped:
            for game_dict in group["games"]:
                game_obj = game_dict.pop("game_obj")
                game_dict["home_team"] = build_team_dict(game_obj.home_team, game_obj.home_score)
                game_dict["away_team"] = build_team_dict(game_obj.away_team, game_obj.away_score)
                game_dict["stadium"] = build_stadium_dict(game_obj.stadium_rel)

        return {"groups": grouped, "total": total}

    # Standard list format
    items = []
    for g in games:
        game_status = compute_game_status(g, today)

        items.append({
            "id": str(g.id),
            "date": g.date.isoformat() if g.date else None,
            "time": g.time.isoformat() if g.time else None,
            "tour": g.tour,
            "season_id": g.season_id,
            "home_score": g.home_score,
            "away_score": g.away_score,
            "has_stats": g.has_stats,
            "has_lineup": g.has_lineup,
            "is_live": g.is_live,
            "stadium": g.stadium,  # Legacy field
            "visitors": g.visitors,
            "status": game_status,
            "ticket_url": getattr(g, "ticket_url", None),
            "video_url": g.video_url,
            "home_team": build_team_dict(g.home_team, g.home_score),
            "away_team": build_team_dict(g.away_team, g.away_score),
            "stadium_info": build_stadium_dict(g.stadium_rel),
            "season_name": get_localized_field(g.season, "name", language) if g.season else None,
        })

    return {"items": items, "total": total}


@router.get("/{game_id}")
async def get_game(
    game_id: UUID,
    language: str = Query(default="ru", description="Language: kz or ru"),
    db: AsyncSession = Depends(get_db),
):
    """Get game by ID."""
    result = await db.execute(
        select(Game)
        .where(Game.id == game_id)
        .options(
            selectinload(Game.home_team),
            selectinload(Game.away_team),
            selectinload(Game.season),
        )
    )
    game = result.scalar_one_or_none()

    if not game:
        raise HTTPException(status_code=404, detail="Game not found")

    home_team = None
    away_team = None
    if game.home_team:
        home_team = {
            "id": game.home_team.id,
            "name": game.home_team.name,
            "logo_url": game.home_team.logo_url,
            "score": game.home_score,
            "primary_color": game.home_team.primary_color,
            "secondary_color": game.home_team.secondary_color,
            "accent_color": game.home_team.accent_color,
        }
    if game.away_team:
        away_team = {
            "id": game.away_team.id,
            "name": game.away_team.name,
            "logo_url": game.away_team.logo_url,
            "score": game.away_score,
            "primary_color": game.away_team.primary_color,
            "secondary_color": game.away_team.secondary_color,
            "accent_color": game.away_team.accent_color,
        }

    return {
        "id": str(game.id),
        "date": game.date.isoformat() if game.date else None,
        "time": game.time.isoformat() if game.time else None,
        "tour": game.tour,
        "season_id": game.season_id,
        "home_score": game.home_score,
        "away_score": game.away_score,
        "has_stats": game.has_stats,
        "has_lineup": game.has_lineup,
        "stadium": game.stadium,
        "visitors": game.visitors,
        "video_url": game.video_url,
        "home_team": home_team,
        "away_team": away_team,
        "season_name": game.season.name if game.season else None,
    }


@router.get("/{game_id}/stats")
async def get_game_stats(game_id: UUID, db: AsyncSession = Depends(get_db)):
    """Get statistics for a game."""
    # Get team stats
    team_stats_result = await db.execute(
        select(GameTeamStats)
        .where(GameTeamStats.game_id == game_id)
        .options(selectinload(GameTeamStats.team))
    )
    team_stats = team_stats_result.scalars().all()

    team_stats_response = []
    for ts in team_stats:
        team_stats_response.append({
            "team_id": ts.team_id,
            "team_name": ts.team.name if ts.team else None,
            "logo_url": ts.team.logo_url if ts.team else None,
            "primary_color": ts.team.primary_color if ts.team else None,
            "secondary_color": ts.team.secondary_color if ts.team else None,
            "accent_color": ts.team.accent_color if ts.team else None,
            "possession": float(ts.possession) if ts.possession else None,
            "possession_percent": ts.possession_percent,
            "shots": ts.shots,
            "shots_on_goal": ts.shots_on_goal,
            "passes": ts.passes,
            "pass_accuracy": float(ts.pass_accuracy) if ts.pass_accuracy else None,
            "fouls": ts.fouls,
            "yellow_cards": ts.yellow_cards,
            "red_cards": ts.red_cards,
            "corners": ts.corners,
            "offsides": ts.offsides,
            "extra_stats": ts.extra_stats,
        })

    # Get goals and assists from game_events (single source of truth)
    goals_result = await db.execute(
        select(GameEvent.player_id, func.count(GameEvent.id).label("count"))
        .where(GameEvent.game_id == game_id, GameEvent.event_type == GameEventType.goal)
        .group_by(GameEvent.player_id)
    )
    player_goals = {row.player_id: row.count for row in goals_result}

    assists_result = await db.execute(
        select(GameEvent.player_id, func.count(GameEvent.id).label("count"))
        .where(GameEvent.game_id == game_id, GameEvent.event_type == GameEventType.assist)
        .group_by(GameEvent.player_id)
    )
    player_assists = {row.player_id: row.count for row in assists_result}

    # Get player stats
    player_stats_result = await db.execute(
        select(GamePlayerStats)
        .where(GamePlayerStats.game_id == game_id)
        .options(
            selectinload(GamePlayerStats.player),
            selectinload(GamePlayerStats.team),
        )
        .order_by(GamePlayerStats.team_id, GamePlayerStats.started.desc())
    )
    player_stats = player_stats_result.scalars().all()

    # Get fallback names from GameEvent
    player_ids = [ps.player_id for ps in player_stats]
    fallback_names = await get_player_names_fallback(db, game_id, player_ids)

    player_stats_response = []
    for ps in player_stats:
        # Determine names with fallback
        if ps.player and ps.player.first_name and ps.player.last_name:
            first_name = ps.player.first_name
            last_name = ps.player.last_name
        elif ps.player_id in fallback_names:
            first_name, last_name = fallback_names[ps.player_id]
        else:
            first_name = ps.player.first_name if ps.player else None
            last_name = ps.player.last_name if ps.player else None

        player_stats_response.append({
            "player_id": ps.player_id,
            "first_name": first_name,
            "last_name": last_name,
            "team_id": ps.team_id,
            "team_name": ps.team.name if ps.team else None,
            "team_primary_color": ps.team.primary_color if ps.team else None,
            "team_secondary_color": ps.team.secondary_color if ps.team else None,
            "team_accent_color": ps.team.accent_color if ps.team else None,
            "position": ps.position,
            "minutes_played": ps.minutes_played,
            "started": ps.started,
            "goals": player_goals.get(ps.player_id, 0),
            "assists": player_assists.get(ps.player_id, 0),
            "shots": ps.shots,
            "passes": ps.passes,
            "pass_accuracy": float(ps.pass_accuracy) if ps.pass_accuracy else None,
            "yellow_cards": ps.yellow_cards,
            "red_cards": ps.red_cards,
            "extra_stats": ps.extra_stats,
        })

    return {
        "game_id": str(game_id),
        "team_stats": team_stats_response,
        "player_stats": player_stats_response,
    }


@router.get("/{game_id}/lineup")
async def get_game_lineup(
    game_id: UUID,
    language: str = Query(default="ru", description="Language: kz, ru, or en"),
    db: AsyncSession = Depends(get_db),
):
    """
    Get pre-game lineup data for a game.
    Includes referees, coaches for both teams, and player lineups.
    """
    # Get game to know home/away team IDs
    game_result = await db.execute(
        select(Game)
        .where(Game.id == game_id)
        .options(
            selectinload(Game.home_team),
            selectinload(Game.away_team),
        )
    )
    game = game_result.scalar_one_or_none()

    if not game:
        raise HTTPException(status_code=404, detail="Game not found")

    # Get referees for this game
    referees_result = await db.execute(
        select(GameReferee)
        .where(GameReferee.game_id == game_id)
        .options(selectinload(GameReferee.referee))
    )
    game_referees = referees_result.scalars().all()

    referees_response = []
    for gr in game_referees:
        ref = gr.referee
        if ref:
            # Select name based on language
            if language == "kz":
                first_name = ref.first_name_kz or ref.first_name
                last_name = ref.last_name_kz or ref.last_name
            elif language == "en":
                first_name = ref.first_name_en or ref.first_name
                last_name = ref.last_name_en or ref.last_name
            else:
                first_name = ref.first_name
                last_name = ref.last_name

            referees_response.append({
                "id": ref.id,
                "first_name": first_name,
                "last_name": last_name,
                "role": gr.role.value,
                "photo_url": ref.photo_url,
                "country": ref.country,
            })

    # Get coaches for home and away teams
    async def get_team_coaches(team_id: int) -> list:
        coaches_result = await db.execute(
            select(TeamCoach)
            .where(TeamCoach.team_id == team_id, TeamCoach.season_id == game.season_id)
            .options(selectinload(TeamCoach.coach))
        )
        team_coaches = coaches_result.scalars().all()

        coaches_list = []
        for tc in team_coaches:
            coach = tc.coach
            if coach:
                if language == "kz":
                    first_name = coach.first_name_kz or coach.first_name
                    last_name = coach.last_name_kz or coach.last_name
                elif language == "en":
                    first_name = coach.first_name_en or coach.first_name
                    last_name = coach.last_name_en or coach.last_name
                else:
                    first_name = coach.first_name
                    last_name = coach.last_name

                coaches_list.append({
                    "id": coach.id,
                    "first_name": first_name,
                    "last_name": last_name,
                    "role": tc.role.value,
                    "photo_url": coach.photo_url,
                    "country": coach.country,
                })
        return coaches_list

    home_coaches = await get_team_coaches(game.home_team_id) if game.home_team_id else []
    away_coaches = await get_team_coaches(game.away_team_id) if game.away_team_id else []

    # Get lineups for home and away teams
    async def get_team_lineup(team_id: int, formation: str | None) -> dict:
        lineup_result = await db.execute(
            select(GameLineup)
            .where(GameLineup.game_id == game_id, GameLineup.team_id == team_id)
            .options(selectinload(GameLineup.player))
            .order_by(GameLineup.lineup_type, GameLineup.shirt_number)
        )
        lineup_entries = lineup_result.scalars().all()

        starters = []
        substitutes = []

        for entry in lineup_entries:
            player = entry.player
            position = player.top_role if player else None
            player_data = {
                "player_id": str(entry.player_id),
                "first_name": player.first_name if player else None,
                "last_name": player.last_name if player else None,
                "shirt_number": entry.shirt_number,
                "is_captain": entry.is_captain,
                "position": position,
                "photo_url": player.photo_url if player else None,
            }

            if entry.lineup_type.value == "starter":
                starters.append(player_data)
            else:
                substitutes.append(player_data)

        return {"formation": formation, "starters": starters, "substitutes": substitutes}

    # Use formations from game (synced from SOTA)
    home_lineup = await get_team_lineup(game.home_team_id, game.home_formation) if game.home_team_id else {"formation": None, "starters": [], "substitutes": []}
    away_lineup = await get_team_lineup(game.away_team_id, game.away_formation) if game.away_team_id else {"formation": None, "starters": [], "substitutes": []}

    return {
        "game_id": str(game_id),
        "has_lineup": game.has_lineup,
        "referees": referees_response,
        "coaches": {
            "home_team": home_coaches,
            "away_team": away_coaches,
        },
        "lineups": {
            "home_team": home_lineup,
            "away_team": away_lineup,
        },
    }
