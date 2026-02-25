"""Game list and detail endpoints."""

import logging
from datetime import date as date_type

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, or_
from sqlalchemy.orm import selectinload

from app.api.deps import get_db
from app.models import (
    Game, Team, Stadium, GameReferee, Season,
)
from app.utils.date_helpers import get_localized_field
from app.utils.team_logo_fallback import resolve_team_logo_url
from app.utils.game_status import compute_game_status
from app.utils.game_grouping import group_games_by_date
from app.config import get_settings
from app.services.season_visibility import ensure_visible_season_or_404
from app.services.season_filters import get_group_team_ids, get_final_stage_ids
from fastapi_cache.decorator import cache

settings = get_settings()
logger = logging.getLogger(__name__)

router = APIRouter(prefix="/games", tags=["games"])


@router.get("")
@cache(expire=1800)
async def get_games(
    season_id: int | None = Query(default=None),
    group: str | None = Query(default=None, description="Filter by group name (e.g. 'A', 'B')"),
    final: bool = Query(default=False, description="Show only final stage matches"),
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
    lang: str = Query(default="kz", pattern="^(kz|ru|en)$"),
    limit: int = Query(default=50, le=100),
    offset: int = Query(default=0),
    db: AsyncSession = Depends(get_db),
):
    """
    Get games with comprehensive filtering and optional date grouping.

    Filters:
    - season_id: Filter by season (defaults to current season)
    - group: Filter by group name using season_participants
    - final: Show only games from season.final_stage_ids
    - team_id: Filter by single team (home or away)
    - team_ids: Filter by multiple teams (home or away) - use ?team_ids=1&team_ids=5
    - tour: Filter by single tour/round number
    - tours: Filter by multiple tours - use ?tours=18&tours=19&tours=20
    - month: Filter by month (1-12). Works independently or with year
    - year: Year for month filter (used when month is provided)
    - date_from: Start of date range
    - date_to: End of date range
    - status: Filter by match status (upcoming, finished, live, all)
    - hide_past: Hide matches before today
    - group_by_date: Group results by date with formatted labels
    - lang: Language for localized fields (kz, ru, en)

    `group` and `final=true` cannot be used together.
    """
    if season_id is None:
        season_id = settings.current_season_id
    await ensure_visible_season_or_404(db, season_id)

    if group and final:
        raise HTTPException(status_code=400, detail="group and final filters are mutually exclusive")

    today = date_type.today()
    group_team_ids: list[int] | None = None
    if group:
        group_team_ids = await get_group_team_ids(db, season_id, group)
        if not group_team_ids:
            return {"groups": [], "total": 0} if group_by_date else {"items": [], "total": 0}

    final_stage_ids: list[int] | None = None
    if final:
        final_stage_ids = await get_final_stage_ids(db, season_id)
        if not final_stage_ids:
            return {"groups": [], "total": 0} if group_by_date else {"items": [], "total": 0}

    # Build base query
    query = select(Game).where(Game.season_id == season_id)
    if group_team_ids is not None:
        query = query.where(
            Game.home_team_id.in_(group_team_ids),
            Game.away_team_id.in_(group_team_ids),
        )
    if final_stage_ids is not None:
        query = query.where(Game.stage_id.in_(final_stage_ids))

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
        # Calculate first and last day of the month for a specific year
        from calendar import monthrange
        first_day = date_type(year, month, 1)
        last_day_num = monthrange(year, month)[1]
        last_day = date_type(year, month, last_day_num)
        query = query.where(Game.date >= first_day, Game.date <= last_day)
    elif month is not None:
        # Month-only filtering across years (still scoped by season_id)
        query = query.where(func.extract("month", Game.date) == month)
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
            selectinload(Game.stage),
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
            "name": get_localized_field(team, "name", lang),
            "name_kz": team.name_kz,
            "name_en": team.name_en,
            "logo_url": resolve_team_logo_url(team),
            "primary_color": team.primary_color,
            "secondary_color": team.secondary_color,
            "accent_color": team.accent_color,
        }

    def build_stadium_dict(stadium: Stadium | None) -> dict | None:
        if not stadium:
            return None

        return {
            "id": stadium.id,
            "name": get_localized_field(stadium, "name", lang),
            "city": get_localized_field(stadium, "city", lang),
            "capacity": stadium.capacity,
            "address": get_localized_field(stadium, "address", lang),
            "photo_url": stadium.photo_url,
        }

    # Return grouped format if requested
    if group_by_date:
        grouped = group_games_by_date(games, lang, today)

        # Add team and stadium info to each game
        for group_item in grouped:
            for game_dict in group_item["games"]:
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
            "id": g.id,
            "date": g.date.isoformat() if g.date else None,
            "time": g.time.isoformat() if g.time else None,
            "tour": g.tour,
            "season_id": g.season_id,
            "stage_id": g.stage_id,
            "stage_name": get_localized_field(g.stage, "name", lang) if g.stage else None,
            "home_score": g.home_score,
            "away_score": g.away_score,
            "home_penalty_score": g.home_penalty_score,
            "away_penalty_score": g.away_penalty_score,
            "has_stats": g.has_stats,
            "has_lineup": g.has_lineup,
            "is_live": g.is_live,
            "is_technical": g.is_technical,
            "is_schedule_tentative": g.is_schedule_tentative,
            "stadium": g.stadium,  # Legacy field
            "visitors": g.visitors,
            "status": game_status,
            "has_score": g.home_score is not None and g.away_score is not None,
            "ticket_url": getattr(g, "ticket_url", None),
            "video_url": g.video_url,
            "protocol_url": g.protocol_url,
            "home_team": build_team_dict(g.home_team, g.home_score),
            "away_team": build_team_dict(g.away_team, g.away_score),
            "stadium_info": build_stadium_dict(g.stadium_rel),
            "season_name": get_localized_field(g.season, "name", lang) if g.season else None,
        })

    return {"items": items, "total": total}


@router.get("/{game_id}")
@cache(expire=1800)
async def get_game(
    game_id: int,
    lang: str = Query(default="kz", pattern="^(kz|ru|en)$"),
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
            selectinload(Game.stadium_rel),
            selectinload(Game.stage),
            selectinload(Game.referees).selectinload(GameReferee.referee),
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
            "name": get_localized_field(game.home_team, "name", lang),
            "logo_url": resolve_team_logo_url(game.home_team),
            "score": game.home_score,
            "primary_color": game.home_team.primary_color,
            "secondary_color": game.home_team.secondary_color,
            "accent_color": game.home_team.accent_color,
        }
    if game.away_team:
        away_team = {
            "id": game.away_team.id,
            "name": get_localized_field(game.away_team, "name", lang),
            "logo_url": resolve_team_logo_url(game.away_team),
            "score": game.away_score,
            "primary_color": game.away_team.primary_color,
            "secondary_color": game.away_team.secondary_color,
            "accent_color": game.away_team.accent_color,
        }

    # Build stadium object
    stadium_dict = None
    if game.stadium_rel:
        stadium_dict = {
            "id": game.stadium_rel.id,
            "name": get_localized_field(game.stadium_rel, "name", lang),
            "city": get_localized_field(game.stadium_rel, "city", lang),
            "capacity": game.stadium_rel.capacity,
            "address": get_localized_field(game.stadium_rel, "address", lang),
            "photo_url": game.stadium_rel.photo_url,
        }

    # Get main referee name
    referee_name = None
    if game.referees:
        main_referee = next((gr for gr in game.referees if gr.role.value == "main"), None)
        if main_referee and main_referee.referee:
            ref = main_referee.referee
            if lang == "kz":
                first_name = ref.first_name_kz or ref.first_name
                last_name = ref.last_name_kz or ref.last_name
            elif lang == "en":
                first_name = ref.first_name_en or ref.first_name
                last_name = ref.last_name_en or ref.last_name
            else:
                first_name = ref.first_name
                last_name = ref.last_name
            referee_name = f"{first_name} {last_name}".strip()

    # Compute game status
    today = date_type.today()
    game_status = compute_game_status(game, today)

    return {
        "id": game.id,
        "date": game.date.isoformat() if game.date else None,
        "time": game.time.isoformat() if game.time else None,
        "tour": game.tour,
        "season_id": game.season_id,
        "stage_id": game.stage_id,
        "stage_name": get_localized_field(game.stage, "name", lang) if game.stage else None,
        "home_score": game.home_score,
        "away_score": game.away_score,
        "home_penalty_score": game.home_penalty_score,
        "away_penalty_score": game.away_penalty_score,
        "has_stats": game.has_stats,
        "has_lineup": game.has_lineup,
        "is_live": game.is_live,
        "is_technical": game.is_technical,
        "is_schedule_tentative": game.is_schedule_tentative,
        "stadium": stadium_dict,
        "referee": referee_name,
        "visitors": game.visitors,
        "ticket_url": game.ticket_url,
        "video_url": game.video_url,
        "protocol_url": game.protocol_url,
        "status": game_status,
        "has_score": game.home_score is not None and game.away_score is not None,
        "home_team": home_team,
        "away_team": away_team,
        "season_name": game.season.name if game.season else None,
    }
