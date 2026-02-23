from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func

from app.api.deps import get_db
from app.api.admin.deps import require_roles
from app.models import Partner
from app.services.season_visibility import (
    ensure_visible_season_or_404,
    season_unscoped_or_visible_clause,
)
from app.schemas.admin.partners import (
    AdminPartnerCreateRequest,
    AdminPartnerUpdateRequest,
    AdminPartnerResponse,
    AdminPartnersListResponse,
)

router = APIRouter(
    prefix="/partners",
    tags=["admin-partners"],
    dependencies=[Depends(require_roles("superadmin", "editor"))],
)


@router.get("", response_model=AdminPartnersListResponse)
async def list_partners(
    championship_id: int | None = Query(default=None),
    season_id: int | None = Query(default=None),
    limit: int = Query(default=50, le=100),
    offset: int = Query(default=0),
    db: AsyncSession = Depends(get_db),
):
    if season_id is not None:
        await ensure_visible_season_or_404(db, season_id)

    base = select(Partner).where(
        season_unscoped_or_visible_clause(Partner.season_id)
    )
    if championship_id is not None:
        base = base.where(Partner.championship_id == championship_id)
    if season_id is not None:
        base = base.where(Partner.season_id == season_id)

    count_result = await db.execute(select(func.count()).select_from(base.subquery()))
    total = count_result.scalar() or 0

    result = await db.execute(
        base.order_by(Partner.sort_order, Partner.id).offset(offset).limit(limit)
    )
    items = [AdminPartnerResponse.model_validate(p) for p in result.scalars().all()]
    return AdminPartnersListResponse(items=items, total=total)


@router.post("", response_model=AdminPartnerResponse, status_code=status.HTTP_201_CREATED)
async def create_partner(
    body: AdminPartnerCreateRequest,
    db: AsyncSession = Depends(get_db),
):
    if body.season_id is not None:
        await ensure_visible_season_or_404(db, body.season_id)

    obj = Partner(**body.model_dump())
    db.add(obj)
    await db.commit()
    await db.refresh(obj)
    return AdminPartnerResponse.model_validate(obj)


@router.patch("/{id}", response_model=AdminPartnerResponse)
async def update_partner(
    id: int,
    body: AdminPartnerUpdateRequest,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(Partner)
        .where(
            Partner.id == id,
            season_unscoped_or_visible_clause(Partner.season_id),
        )
    )
    obj = result.scalar_one_or_none()
    if not obj:
        raise HTTPException(status_code=404, detail="Partner not found")

    if body.season_id is not None:
        await ensure_visible_season_or_404(db, body.season_id)

    for key, value in body.model_dump(exclude_unset=True).items():
        setattr(obj, key, value)

    await db.commit()
    await db.refresh(obj)
    return AdminPartnerResponse.model_validate(obj)


@router.delete("/{id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_partner(id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(Partner)
        .where(
            Partner.id == id,
            season_unscoped_or_visible_clause(Partner.season_id),
        )
    )
    obj = result.scalar_one_or_none()
    if not obj:
        raise HTTPException(status_code=404, detail="Partner not found")

    await db.delete(obj)
    await db.commit()
