from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func

from app.api.deps import get_db
from app.api.admin.deps import require_roles
from app.models import SeasonParticipant
from app.schemas.admin.season_participants import (
    AdminSeasonParticipantCreateRequest,
    AdminSeasonParticipantUpdateRequest,
    AdminSeasonParticipantResponse,
    AdminSeasonParticipantsListResponse,
)

router = APIRouter(
    prefix="/season-participants",
    tags=["admin-season-participants"],
    dependencies=[Depends(require_roles("superadmin", "editor"))],
)


@router.get("", response_model=AdminSeasonParticipantsListResponse)
async def list_season_participants(
    season_id: int = Query(...),
    limit: int = Query(default=100, le=200),
    offset: int = Query(default=0),
    db: AsyncSession = Depends(get_db),
):
    base = select(SeasonParticipant).where(SeasonParticipant.season_id == season_id)
    count_result = await db.execute(select(func.count()).select_from(base.subquery()))
    total = count_result.scalar() or 0

    result = await db.execute(
        base.order_by(SeasonParticipant.sort_order, SeasonParticipant.id)
        .offset(offset)
        .limit(limit)
    )
    items = [AdminSeasonParticipantResponse.model_validate(t) for t in result.scalars().all()]
    return AdminSeasonParticipantsListResponse(items=items, total=total)


@router.post("", response_model=AdminSeasonParticipantResponse, status_code=status.HTTP_201_CREATED)
async def create_season_participant(
    body: AdminSeasonParticipantCreateRequest,
    db: AsyncSession = Depends(get_db),
):
    obj = SeasonParticipant(**body.model_dump())
    db.add(obj)
    await db.commit()
    await db.refresh(obj)
    return AdminSeasonParticipantResponse.model_validate(obj)


@router.patch("/{id}", response_model=AdminSeasonParticipantResponse)
async def update_season_participant(
    id: int,
    body: AdminSeasonParticipantUpdateRequest,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(SeasonParticipant).where(SeasonParticipant.id == id))
    obj = result.scalar_one_or_none()
    if not obj:
        raise HTTPException(status_code=404, detail="Season participant entry not found")

    for key, value in body.model_dump(exclude_unset=True).items():
        setattr(obj, key, value)

    await db.commit()
    await db.refresh(obj)
    return AdminSeasonParticipantResponse.model_validate(obj)


@router.delete("/{id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_season_participant(id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(SeasonParticipant).where(SeasonParticipant.id == id))
    obj = result.scalar_one_or_none()
    if not obj:
        raise HTTPException(status_code=404, detail="Season participant entry not found")

    await db.delete(obj)
    await db.commit()
