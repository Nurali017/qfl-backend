from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func

from app.api.deps import get_db
from app.api.admin.deps import require_roles
from app.models import Season
from app.services.season_visibility import is_season_visible_clause
from app.schemas.admin.seasons import (
    AdminSeasonUpdateRequest,
    AdminSeasonResponse,
    AdminSeasonsListResponse,
)

router = APIRouter(
    prefix="/seasons",
    tags=["admin-seasons"],
    dependencies=[Depends(require_roles("superadmin", "editor"))],
)


@router.get("", response_model=AdminSeasonsListResponse)
async def list_seasons(
    limit: int = Query(default=50, le=200),
    offset: int = Query(default=0),
    db: AsyncSession = Depends(get_db),
):
    count_result = await db.execute(
        select(func.count()).select_from(Season).where(is_season_visible_clause())
    )
    total = count_result.scalar() or 0

    result = await db.execute(
        select(Season)
        .where(is_season_visible_clause())
        .order_by(Season.date_start.desc(), Season.id.desc())
        .offset(offset)
        .limit(limit)
    )
    items = [AdminSeasonResponse.model_validate(s) for s in result.scalars().all()]
    return AdminSeasonsListResponse(items=items, total=total)


@router.get("/{id}", response_model=AdminSeasonResponse)
async def get_season(id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(Season).where(Season.id == id, is_season_visible_clause())
    )
    obj = result.scalar_one_or_none()
    if not obj:
        raise HTTPException(status_code=404, detail="Season not found")
    return AdminSeasonResponse.model_validate(obj)


@router.patch("/{id}", response_model=AdminSeasonResponse)
async def update_season(
    id: int,
    body: AdminSeasonUpdateRequest,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(Season).where(Season.id == id, is_season_visible_clause())
    )
    obj = result.scalar_one_or_none()
    if not obj:
        raise HTTPException(status_code=404, detail="Season not found")

    for key, value in body.model_dump(exclude_unset=True).items():
        setattr(obj, key, value)

    await db.commit()
    await db.refresh(obj)
    return AdminSeasonResponse.model_validate(obj)
