import json

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import Response
from sqlalchemy import desc, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.api.deps import get_db
from app.utils.cache import cache_get, cache_set
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
from app.utils.game_status import compute_game_status
from app.utils.localization import get_localized_name, get_localized_city, get_localized_field
from app.utils.error_messages import get_error_message
from app.utils.has_stats import enrich_games_has_stats
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
        .where(
            PlayerTeam.team_id == team_id,
            PlayerTeam.season_id == season_id,
            PlayerTeam.role == 1,
            PlayerTeam.is_hidden == False,
        )
        .options(
            selectinload(PlayerTeam.player).selectinload(Player.country)
        )
    )
    player_teams = result.scalars().all()

    _AMPLUA_TO_POSITION = {1: "GK", 2: "DEF", 3: "MID", 4: "FWD"}
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
        position = (
            infer_position_code(pt.position_ru or pt.position_kz, pt.position_en)
            or infer_position_code(p.player_type, p.top_role)
            or _AMPLUA_TO_POSITION.get(pt.amplua)
        )
        items.append({
            "id": p.id,
            "first_name": get_localized_field(p, "first_name", lang),
            "last_name": get_localized_field(p, "last_name", lang),
            "birthday": p.birthday,
            "player_type": p.player_type,
            "position": position,
            "country": country_data,
            "photo_url": pt.photo_url,
            "photo_url_avatar": pt.photo_url_avatar,
            "photo_url_leaderboard": pt.photo_url_leaderboard,
            "photo_url_player_page": pt.photo_url_player_page,
            "age": p.age,
            "top_role": get_localized_field(p, "top_role", lang),
            "team_id": pt.team_id,
            "number": pt.number,
            "is_active": pt.is_active,
            "joined_at": pt.joined_at.isoformat() if pt.joined_at else None,
            "left_at": pt.left_at.isoformat() if pt.left_at else None,
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

    cache_key = f"team_games:{team_id}:{season_id}:{lang}"
    cached = cache_get(cache_key)
    if cached is not None:
        return Response(content=cached, media_type="application/json")

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
    games = list(result.scalars().all())
    await enrich_games_has_stats(db, games)

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
            has_lineup=g.has_lineup,
            is_live=g.is_live,
            is_technical=g.is_technical,
            show_timeline=g.show_timeline,
            status=compute_game_status(g, for_list=True),
            minute=g.live_minute if g.is_live else None,
            half=g.live_half if g.is_live else None,
            live_phase=g.live_phase if g.is_live else None,
            stadium=stadium_data,
            visitors=g.visitors,
            home_team=home_team,
            away_team=away_team,
            season_name=get_localized_name(g.season, lang) if g.season else None,
        ))

    result_dict = {"items": [item.model_dump(mode="json") for item in items], "total": len(items)}
    json_bytes = json.dumps(result_dict, default=str, ensure_ascii=False).encode()
    cache_set(cache_key, json_bytes, 30)
    return Response(content=json_bytes, media_type="application/json")


@router.get("/{team_id}/coaches")
async def get_team_coaches(
    team_id: int,
    season_id: int = Query(default=None),
    lang: str = Query(default="kz", description="Language: kz, ru, or en"),
    db: AsyncSession = Depends(get_db),
):
    """Get coaching staff for a team in a specific season."""
    season_id = await resolve_visible_season_id(db, season_id)

    _ROLE_FALLBACK_KZ = {2: "Бапкер", 3: "Персонал", 4: "Әкімші"}
    _ROLE_FALLBACK_RU = {2: "Тренер", 3: "Персонал", 4: "Администратор"}

    result = await db.execute(
        select(PlayerTeam)
        .where(
            PlayerTeam.team_id == team_id,
            PlayerTeam.season_id == season_id,
            PlayerTeam.role != 1,
            PlayerTeam.is_hidden == False,
            PlayerTeam.is_active == True,
        )
        .options(
            selectinload(PlayerTeam.player).selectinload(Player.country),
        )
    )
    contracts = result.scalars().all()

    items = []
    for ct in contracts:
        p = ct.player
        country_data = None
        if p.country:
            country_data = {
                "id": p.country.id,
                "code": p.country.code,
                "name": get_localized_name(p.country, lang),
                "flag_url": p.country.flag_url,
            }

        if lang == "kz":
            role_text = ct.position_kz or ct.position_ru or _ROLE_FALLBACK_KZ.get(ct.role, "")
        else:
            role_text = ct.position_ru or ct.position_kz or _ROLE_FALLBACK_RU.get(ct.role, "")

        items.append({
            "id": p.id,
            "first_name": get_localized_field(p, "first_name", lang),
            "last_name": get_localized_field(p, "last_name", lang),
            "photo_url": ct.photo_url,
            "role": role_text,
            "country": country_data,
        })

    # Sort by role integer: 2=Coach, 3=Staff, 4=Admin; head coach first within group
    def _coach_priority(ct):
        role = ct.role or 99
        pos = (ct.position_kz or ct.position_ru or "").strip()
        is_head = 0 if pos in ("Бас бапкер", "Главный тренер") else 1
        return (role, is_head)

    sort_key = {c.player.id: _coach_priority(c) for c in contracts}
    items.sort(key=lambda x: sort_key.get(x["id"], (99, 1)))

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
