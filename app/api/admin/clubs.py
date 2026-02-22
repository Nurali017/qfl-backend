from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func

from app.api.deps import get_db
from app.api.admin.deps import require_roles
from app.models import Club
from app.schemas.admin.clubs import (
    AdminClubCreateRequest,
    AdminClubUpdateRequest,
    AdminClubResponse,
    AdminClubsListResponse,
)

router = APIRouter(
    prefix="/clubs",
    tags=["admin-clubs"],
    dependencies=[Depends(require_roles("superadmin", "editor"))],
)


@router.get("", response_model=AdminClubsListResponse)
async def list_clubs(
    limit: int = Query(default=50, le=100),
    offset: int = Query(default=0),
    db: AsyncSession = Depends(get_db),
):
    count_result = await db.execute(select(func.count()).select_from(Club))
    total = count_result.scalar() or 0

    result = await db.execute(
        select(Club).order_by(Club.name).offset(offset).limit(limit)
    )
    items = [AdminClubResponse.model_validate(c) for c in result.scalars().all()]
    return AdminClubsListResponse(items=items, total=total)


@router.get("/{id}", response_model=AdminClubResponse)
async def get_club(id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Club).where(Club.id == id))
    obj = result.scalar_one_or_none()
    if not obj:
        raise HTTPException(status_code=404, detail="Club not found")
    return AdminClubResponse.model_validate(obj)


@router.post("", response_model=AdminClubResponse, status_code=status.HTTP_201_CREATED)
async def create_club(
    body: AdminClubCreateRequest,
    db: AsyncSession = Depends(get_db),
):
    obj = Club(**body.model_dump())
    db.add(obj)
    await db.commit()
    await db.refresh(obj)
    return AdminClubResponse.model_validate(obj)


@router.patch("/{id}", response_model=AdminClubResponse)
async def update_club(
    id: int,
    body: AdminClubUpdateRequest,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(Club).where(Club.id == id))
    obj = result.scalar_one_or_none()
    if not obj:
        raise HTTPException(status_code=404, detail="Club not found")

    for key, value in body.model_dump(exclude_unset=True).items():
        setattr(obj, key, value)

    await db.commit()
    await db.refresh(obj)
    return AdminClubResponse.model_validate(obj)


@router.delete("/{id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_club(id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Club).where(Club.id == id))
    obj = result.scalar_one_or_none()
    if not obj:
        raise HTTPException(status_code=404, detail="Club not found")

    await db.delete(obj)
    await db.commit()
