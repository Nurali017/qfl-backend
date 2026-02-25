from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func

from app.api.deps import get_db
from app.api.admin.deps import require_roles
from app.caching import invalidate_pattern
from app.models import Championship
from app.schemas.admin.championships import (
    AdminChampionshipCreateRequest,
    AdminChampionshipUpdateRequest,
    AdminChampionshipResponse,
    AdminChampionshipsListResponse,
)

router = APIRouter(
    prefix="/championships",
    tags=["admin-championships"],
    dependencies=[Depends(require_roles("superadmin", "editor"))],
)


@router.get("", response_model=AdminChampionshipsListResponse)
async def list_championships(
    limit: int = Query(default=50, le=100),
    offset: int = Query(default=0),
    db: AsyncSession = Depends(get_db),
):
    count_result = await db.execute(select(func.count()).select_from(Championship))
    total = count_result.scalar() or 0

    result = await db.execute(
        select(Championship)
        .order_by(Championship.sort_order, Championship.id)
        .offset(offset)
        .limit(limit)
    )
    items = [AdminChampionshipResponse.model_validate(c) for c in result.scalars().all()]
    return AdminChampionshipsListResponse(items=items, total=total)


@router.get("/{id}", response_model=AdminChampionshipResponse)
async def get_championship(id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Championship).where(Championship.id == id))
    obj = result.scalar_one_or_none()
    if not obj:
        raise HTTPException(status_code=404, detail="Championship not found")
    return AdminChampionshipResponse.model_validate(obj)


@router.post("", response_model=AdminChampionshipResponse, status_code=status.HTTP_201_CREATED)
async def create_championship(
    body: AdminChampionshipCreateRequest,
    db: AsyncSession = Depends(get_db),
):
    obj = Championship(**body.model_dump())
    db.add(obj)
    await db.commit()
    await db.refresh(obj)
    await invalidate_pattern("*app.api.championships*")
    return AdminChampionshipResponse.model_validate(obj)


@router.patch("/{id}", response_model=AdminChampionshipResponse)
async def update_championship(
    id: int,
    body: AdminChampionshipUpdateRequest,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(Championship).where(Championship.id == id))
    obj = result.scalar_one_or_none()
    if not obj:
        raise HTTPException(status_code=404, detail="Championship not found")

    for key, value in body.model_dump(exclude_unset=True).items():
        setattr(obj, key, value)

    await db.commit()
    await db.refresh(obj)
    await invalidate_pattern("*app.api.championships*")
    return AdminChampionshipResponse.model_validate(obj)


@router.delete("/{id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_championship(id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Championship).where(Championship.id == id))
    obj = result.scalar_one_or_none()
    if not obj:
        raise HTTPException(status_code=404, detail="Championship not found")

    await db.delete(obj)
    await db.commit()
    await invalidate_pattern("*app.api.championships*")
