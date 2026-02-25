from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.api.deps import get_db
from app.models import Partner
from app.services.season_visibility import ensure_visible_season_or_404
from app.schemas.partner import PartnerResponse, PartnerListResponse
from fastapi_cache.decorator import cache

router = APIRouter(prefix="/partners", tags=["partners"])


@router.get("", response_model=PartnerListResponse)
@cache(expire=43200)
async def get_partners(
    championship_id: int | None = Query(default=None),
    season_id: int | None = Query(default=None),
    db: AsyncSession = Depends(get_db),
):
    """Get partners/sponsors with optional filters."""
    if season_id is not None:
        await ensure_visible_season_or_404(db, season_id)

    query = select(Partner).where(Partner.is_active == True)

    if championship_id is not None:
        query = query.where(Partner.championship_id == championship_id)
    if season_id is not None:
        query = query.where(Partner.season_id == season_id)

    query = query.order_by(Partner.sort_order, Partner.id)

    result = await db.execute(query)
    partners = result.scalars().all()

    items = [
        PartnerResponse(
            id=p.id,
            name=p.name,
            logo_url=p.logo_url,
            website=p.website,
            sort_order=p.sort_order,
            is_active=p.is_active,
        )
        for p in partners
    ]

    return PartnerListResponse(items=items, total=len(items))
