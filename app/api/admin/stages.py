from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func

from app.api.deps import get_db
from app.api.admin.deps import require_roles
from app.caching import invalidate_pattern
from app.models import Season, Stage
from app.services.season_visibility import ensure_visible_season_or_404, is_season_visible_clause
from app.schemas.admin.stages import (
    AdminStageCreateRequest,
    AdminStageUpdateRequest,
    AdminStageResponse,
    AdminStagesListResponse,
)

router = APIRouter(
    prefix="/stages",
    tags=["admin-stages"],
    dependencies=[Depends(require_roles("superadmin", "editor"))],
)


@router.get("", response_model=AdminStagesListResponse)
async def list_stages(
    season_id: int = Query(...),
    limit: int = Query(default=100, le=200),
    offset: int = Query(default=0),
    db: AsyncSession = Depends(get_db),
):
    await ensure_visible_season_or_404(db, season_id)

    base = select(Stage).where(Stage.season_id == season_id)
    count_result = await db.execute(select(func.count()).select_from(base.subquery()))
    total = count_result.scalar() or 0

    result = await db.execute(
        base.order_by(Stage.sort_order, Stage.stage_number, Stage.id)
        .offset(offset)
        .limit(limit)
    )
    items = [AdminStageResponse.model_validate(s) for s in result.scalars().all()]
    return AdminStagesListResponse(items=items, total=total)


@router.post("", response_model=AdminStageResponse, status_code=status.HTTP_201_CREATED)
async def create_stage(
    body: AdminStageCreateRequest,
    db: AsyncSession = Depends(get_db),
):
    await ensure_visible_season_or_404(db, body.season_id)

    obj = Stage(**body.model_dump())
    db.add(obj)
    await db.commit()
    await db.refresh(obj)
    await invalidate_pattern("*app.api.seasons*")
    return AdminStageResponse.model_validate(obj)


@router.patch("/{id}", response_model=AdminStageResponse)
async def update_stage(
    id: int,
    body: AdminStageUpdateRequest,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(Stage)
        .join(Season, Season.id == Stage.season_id)
        .where(
            Stage.id == id,
            is_season_visible_clause(),
        )
    )
    obj = result.scalar_one_or_none()
    if not obj:
        raise HTTPException(status_code=404, detail="Stage not found")

    for key, value in body.model_dump(exclude_unset=True).items():
        setattr(obj, key, value)

    await db.commit()
    await db.refresh(obj)
    await invalidate_pattern("*app.api.seasons*")
    return AdminStageResponse.model_validate(obj)


@router.delete("/{id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_stage(id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(Stage)
        .join(Season, Season.id == Stage.season_id)
        .where(
            Stage.id == id,
            is_season_visible_clause(),
        )
    )
    obj = result.scalar_one_or_none()
    if not obj:
        raise HTTPException(status_code=404, detail="Stage not found")

    await db.delete(obj)
    await db.commit()
    await invalidate_pattern("*app.api.seasons*")
