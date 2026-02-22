from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.api.deps import get_db
from app.models import City
from app.utils.localization import get_localized_field
from app.schemas.city import CityResponse, CityListResponse

router = APIRouter(prefix="/cities", tags=["cities"])


@router.get("", response_model=CityListResponse)
async def get_cities(
    lang: str = Query(default="ru", pattern="^(kz|ru|en)$"),
    db: AsyncSession = Depends(get_db),
):
    """Get all cities."""
    result = await db.execute(
        select(City)
        .options(selectinload(City.country))
        .order_by(City.name)
    )
    cities = result.scalars().all()

    items = [
        CityResponse(
            id=c.id,
            name=get_localized_field(c, "name", lang),
            country_id=c.country_id,
            country_name=get_localized_field(c.country, "name", lang) if c.country else None,
        )
        for c in cities
    ]

    return CityListResponse(items=items, total=len(items))
