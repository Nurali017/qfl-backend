from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import desc, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.api.deps import get_db
from app.models import (
    Game,
    Language,
    News,
    Player,
    PlayerTeam,
    Season,
    Team,
)
from app.models.news import NewsTeam
from app.schemas.news import NewsListItem
from app.models.coach import Coach, TeamCoach
from app.schemas.game import TeamGameItem
from app.schemas.team import (
    TeamDetailResponse,
    TeamListResponse,
    TeamSeasonEntry,
    TeamSeasonsResponse,
    TeamStadiumInfo,
    TeamWithScore,
)
from app.models import Championship
from app.services.season_participants import resolve_season_participants
from app.services.season_visibility import ensure_visible_season_or_404, is_season_visible_clause, resolve_visible_season_id
from app.services.team_overview import _extract_year
from app.utils.localization import get_localized_name, get_localized_city, get_localized_field
from app.utils.error_messages import get_error_message
from app.utils.positions import infer_position_code
from app.utils.team_logo_fallback import resolve_team_logo_url

router = APIRouter(prefix="/teams", tags=["teams"])


@router.get("", response_model=TeamListResponse)
async def get_teams(
    season_id: int | None = None,
    lang: str = Query(default="kz", description="Language: kz, ru, or en"),
    db: AsyncSession = Depends(get_db),
):
    """Get all teams, optionally filtered by season."""
    if season_id is not None:
        await ensure_visible_season_or_404(db, season_id)
        participants = await resolve_season_participants(db, season_id, lang)
        if not participants:
            raise HTTPException(
                status_code=409,
                detail=get_error_message("season_teams_not_configured", lang),
            )
        items = []
        for participant in participants:
            team = participant.team
            items.append({
                "id": team.id,
                "name": get_localized_name(team, lang),
                "logo_url": resolve_team_logo_url(team),
                "primary_color": team.primary_color,
                "secondary_color": team.secondary_color,
                "accent_color": team.accent_color,
            })
        return {"items": items, "total": len(items)}

    result = await db.execute(select(Team).order_by(Team.name))
    teams = result.scalars().all()
    items = [
        {
            "id": t.id,
            "name": get_localized_name(t, lang),
            "logo_url": resolve_team_logo_url(t),
            "primary_color": t.primary_color,
            "secondary_color": t.secondary_color,
            "accent_color": t.accent_color,
        }
        for t in teams
    ]
    return {"items": items, "total": len(items)}


@router.get("/{team_id}", response_model=TeamDetailResponse)
async def get_team(
    team_id: int,
    lang: str = Query(default="kz", description="Language: kz, ru, or en"),
    db: AsyncSession = Depends(get_db),
):
    """Get team by ID."""
    result = await db.execute(
        select(Team)
        .where(Team.id == team_id)
        .options(
            selectinload(Team.stadium),
            selectinload(Team.club),
        )
    )
    team = result.scalar_one_or_none()

    if not team:
        raise HTTPException(status_code=404, detail=get_error_message("team_not_found", lang))

    stadium_data = None
    if team.stadium:
        stadium_data = {
            "name": get_localized_name(team.stadium, lang),
            "city": get_localized_city(team.stadium, lang) if hasattr(team.stadium, 'city') else None,
        }

    return {
        "id": team.id,
        "name": get_localized_name(team, lang),
        "city": get_localized_city(team, lang),
        "logo_url": resolve_team_logo_url(team),
        "primary_color": team.primary_color,
        "secondary_color": team.secondary_color,
        "accent_color": team.accent_color,
        "website": team.website,
        "stadium": stadium_data,
        "club_id": team.club_id,
        "club_name": get_localized_field(team.club, "name", lang) if team.club else None,
    }


@router.get("/{team_id}/seasons", response_model=TeamSeasonsResponse)
async def get_team_seasons(
    team_id: int,
    lang: str = Query(default="kz", pattern="^(kz|ru|en)$"),
    db: AsyncSession = Depends(get_db),
):
    """Get all seasons a team has participated in, based on games data."""
    # Subquery to get distinct season IDs for this team
    season_ids_subq = (
        select(Game.season_id)
        .where(or_(Game.home_team_id == team_id, Game.away_team_id == team_id))
        .distinct()
        .subquery()
    )
    from app.models import Championship
    result = await db.execute(
        select(Season.id, Season, Championship)
        .join(Championship, Season.championship_id == Championship.id)
        .where(
            Season.id.in_(select(season_ids_subq.c.season_id)),
            is_season_visible_clause(),
        )
        .order_by(Season.date_start.desc().nullslast(), Season.id.desc())
    )
    rows = result.all()

    items: list[TeamSeasonEntry] = []
    for _, season, championship in rows:
        season_name = get_localized_name(season, lang)
        season_year = season.date_start.year if season.date_start else _extract_year(season_name)
        items.append(
            TeamSeasonEntry(
                season_id=season.id,
                season_name=season_name,
                championship_name=get_localized_name(championship, lang),
                frontend_code=season.frontend_code,
                season_year=season_year,
            )
        )

    return TeamSeasonsResponse(items=items, total=len(items))


@router.get("/{team_id}/players")
async def get_team_players(
    team_id: int,
    season_id: int = Query(default=None),
    lang: str = Query(default="kz", description="Language: kz, ru, or en"),
    db: AsyncSession = Depends(get_db),
):
    """Get players for a team in a specific season."""
    season_id = await resolve_visible_season_id(db, season_id)

    result = await db.execute(
        select(PlayerTeam)
        .where(PlayerTeam.team_id == team_id, PlayerTeam.season_id == season_id)
        .options(
            selectinload(PlayerTeam.player).selectinload(Player.country)
        )
    )
    player_teams = result.scalars().all()

    items = []
    for pt in player_teams:
        p = pt.player
        country_data = None
        if p.country:
            country_data = {
                "id": p.country.id,
                "code": p.country.code,
                "name": get_localized_name(p.country, lang),
                "flag_url": p.country.flag_url,
            }
        position = infer_position_code(pt.position_ru or pt.position_kz, pt.position_en) \
            or infer_position_code(p.player_type, p.top_role)
        items.append({
            "id": p.id,
            "first_name": get_localized_field(p, "first_name", lang),
            "last_name": get_localized_field(p, "last_name", lang),
            "birthday": p.birthday,
            "player_type": p.player_type,
            "position": position,
            "country": country_data,
            "photo_url": pt.photo_url or p.photo_url,
            "age": p.age,
            "top_role": get_localized_field(p, "top_role", lang),
            "team_id": pt.team_id,
            "number": pt.number,
        })

    return {"items": items, "total": len(items)}


@router.get("/{team_id}/games")
async def get_team_games(
    team_id: int,
    season_id: int = Query(default=None),
    lang: str = Query(default="kz", description="Language: kz, ru, or en"),
    db: AsyncSession = Depends(get_db),
):
    """Get games for a team."""
    season_id = await resolve_visible_season_id(db, season_id)

    query = (
        select(Game)
        .where(
            Game.season_id == season_id,
            or_(Game.home_team_id == team_id, Game.away_team_id == team_id),
        )
        .options(
            selectinload(Game.home_team),
            selectinload(Game.away_team),
            selectinload(Game.season),
            selectinload(Game.stadium_rel),
        )
        .order_by(Game.date.desc())
    )

    result = await db.execute(query)
    games = result.scalars().all()

    items = []
    for g in games:
        home_team = None
        away_team = None
        if g.home_team:
            home_team = TeamWithScore(
                id=g.home_team.id,
                name=get_localized_name(g.home_team, lang),
                logo_url=resolve_team_logo_url(g.home_team),
                score=g.home_score,
            )
        if g.away_team:
            away_team = TeamWithScore(
                id=g.away_team.id,
                name=get_localized_name(g.away_team, lang),
                logo_url=resolve_team_logo_url(g.away_team),
                score=g.away_score,
            )

        # Build stadium from relationship
        stadium_data = None
        if g.stadium_rel:
            stadium_data = TeamStadiumInfo(
                name=get_localized_name(g.stadium_rel, lang),
                city=get_localized_city(g.stadium_rel, lang) if hasattr(g.stadium_rel, 'city') else None,
            )

        items.append(TeamGameItem(
            id=g.id,
            date=g.date,
            time=g.time,
            tour=g.tour,
            season_id=g.season_id,
            home_score=g.home_score,
            away_score=g.away_score,
            has_stats=g.has_stats,
            stadium=stadium_data,
            visitors=g.visitors,
            home_team=home_team,
            away_team=away_team,
            season_name=get_localized_name(g.season, lang) if g.season else None,
        ))

    return {"items": items, "total": len(items)}


@router.get("/{team_id}/coaches")
async def get_team_coaches(
    team_id: int,
    season_id: int = Query(default=None),
    lang: str = Query(default="kz", description="Language: kz, ru, or en"),
    db: AsyncSession = Depends(get_db),
):
    """Get coaching staff for a team in a specific season."""
    season_id = await resolve_visible_season_id(db, season_id)

    result = await db.execute(
        select(TeamCoach)
        .where(
            TeamCoach.team_id == team_id,
            TeamCoach.season_id == season_id,
            TeamCoach.is_active == True,
        )
        .options(
            selectinload(TeamCoach.coach).selectinload(Coach.country),
        )
    )
    team_coaches = result.scalars().all()

    items = []
    for tc in team_coaches:
        c = tc.coach
        country_data = None
        if c.country:
            country_data = {
                "id": c.country.id,
                "code": c.country.code,
                "name": get_localized_name(c.country, lang),
                "flag_url": c.country.flag_url,
            }
        items.append({
            "id": c.id,
            "first_name": get_localized_field(c, "first_name", lang),
            "last_name": get_localized_field(c, "last_name", lang),
            "photo_url": c.photo_url,
            "role": tc.role.value,
            "country": country_data,
        })

    # Sort: head_coach first, then by role
    role_order = {"head_coach": 0, "assistant": 1, "goalkeeper_coach": 2, "fitness_coach": 3, "other": 4}
    items.sort(key=lambda x: role_order.get(x["role"], 99))

    return {"items": items, "total": len(items)}


@router.get("/{team_id}/news", response_model=list[NewsListItem])
async def get_team_news(
    team_id: int,
    lang: str = Query("kz", pattern="^(kz|ru)$"),
    limit: int = Query(10, ge=1, le=20),
    db: AsyncSession = Depends(get_db),
):
    """Get news articles linked to a team."""
    lang_enum = Language.KZ if lang == "kz" else Language.RU
    result = await db.execute(
        select(News)
        .join(NewsTeam, News.translation_group_id == NewsTeam.translation_group_id)
        .where(NewsTeam.team_id == team_id, News.language == lang_enum)
        .order_by(desc(News.publish_date), desc(News.id))
        .limit(limit)
    )
    return result.scalars().all()
