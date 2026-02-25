from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.api.deps import get_db
from app.models import Club, Team
from app.utils.localization import get_localized_field
from app.schemas.club import (
    ClubResponse,
    ClubListResponse,
    ClubWithTeamsResponse,
    TeamBrief,
)
from fastapi_cache.decorator import cache

router = APIRouter(prefix="/clubs", tags=["clubs"])


@router.get("", response_model=ClubListResponse)
@cache(expire=43200)
async def get_clubs(
    city_id: int | None = Query(default=None),
    lang: str = Query(default="kz", pattern="^(kz|ru|en)$"),
    db: AsyncSession = Depends(get_db),
):
    """Get all clubs with optional city filter."""
    query = (
        select(Club)
        .where(Club.is_active == True)
        .options(selectinload(Club.city_rel))
        .order_by(Club.name)
    )

    if city_id is not None:
        query = query.where(Club.city_id == city_id)

    result = await db.execute(query)
    clubs = result.scalars().all()

    items = [
        ClubResponse(
            id=c.id,
            name=get_localized_field(c, "name", lang),
            short_name=get_localized_field(c, "short_name", lang),
            logo_url=c.logo_url,
            city_name=get_localized_field(c.city_rel, "name", lang) if c.city_rel else None,
            is_active=c.is_active,
        )
        for c in clubs
    ]

    return ClubListResponse(items=items, total=len(items))


@router.get("/{club_id}", response_model=ClubWithTeamsResponse)
async def get_club(
    club_id: int,
    lang: str = Query(default="kz", pattern="^(kz|ru|en)$"),
    db: AsyncSession = Depends(get_db),
):
    """Get a club with its teams."""
    result = await db.execute(
        select(Club)
        .where(Club.id == club_id)
        .options(
            selectinload(Club.city_rel),
            selectinload(Club.teams),
        )
    )
    club = result.scalar_one_or_none()

    if not club:
        raise HTTPException(status_code=404, detail="Club not found")

    teams = [
        TeamBrief(
            id=t.id,
            name=get_localized_field(t, "name", lang),
            logo_url=t.logo_url,
        )
        for t in club.teams
    ]

    return ClubWithTeamsResponse(
        id=club.id,
        name=get_localized_field(club, "name", lang),
        short_name=get_localized_field(club, "short_name", lang),
        logo_url=club.logo_url,
        city_name=get_localized_field(club.city_rel, "name", lang) if club.city_rel else None,
        is_active=club.is_active,
        teams=teams,
    )
