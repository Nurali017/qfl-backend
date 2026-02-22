from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func

from app.api.deps import get_db
from app.api.admin.deps import require_roles
from app.models import PlayoffBracket
from app.schemas.admin.playoff_brackets import (
    AdminPlayoffBracketCreateRequest,
    AdminPlayoffBracketUpdateRequest,
    AdminPlayoffBracketResponse,
    AdminPlayoffBracketsListResponse,
)

router = APIRouter(
    prefix="/playoff-brackets",
    tags=["admin-playoff-brackets"],
    dependencies=[Depends(require_roles("superadmin", "editor"))],
)


@router.get("", response_model=AdminPlayoffBracketsListResponse)
async def list_playoff_brackets(
    season_id: int = Query(...),
    limit: int = Query(default=100, le=200),
    offset: int = Query(default=0),
    db: AsyncSession = Depends(get_db),
):
    base = select(PlayoffBracket).where(PlayoffBracket.season_id == season_id)
    count_result = await db.execute(select(func.count()).select_from(base.subquery()))
    total = count_result.scalar() or 0

    result = await db.execute(
        base.order_by(PlayoffBracket.sort_order, PlayoffBracket.id)
        .offset(offset)
        .limit(limit)
    )
    items = [AdminPlayoffBracketResponse.model_validate(b) for b in result.scalars().all()]
    return AdminPlayoffBracketsListResponse(items=items, total=total)


@router.post("", response_model=AdminPlayoffBracketResponse, status_code=status.HTTP_201_CREATED)
async def create_playoff_bracket(
    body: AdminPlayoffBracketCreateRequest,
    db: AsyncSession = Depends(get_db),
):
    obj = PlayoffBracket(**body.model_dump())
    db.add(obj)
    await db.commit()
    await db.refresh(obj)
    return AdminPlayoffBracketResponse.model_validate(obj)


@router.patch("/{id}", response_model=AdminPlayoffBracketResponse)
async def update_playoff_bracket(
    id: int,
    body: AdminPlayoffBracketUpdateRequest,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(PlayoffBracket).where(PlayoffBracket.id == id))
    obj = result.scalar_one_or_none()
    if not obj:
        raise HTTPException(status_code=404, detail="Playoff bracket entry not found")

    for key, value in body.model_dump(exclude_unset=True).items():
        setattr(obj, key, value)

    await db.commit()
    await db.refresh(obj)
    return AdminPlayoffBracketResponse.model_validate(obj)


@router.delete("/{id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_playoff_bracket(id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(PlayoffBracket).where(PlayoffBracket.id == id))
    obj = result.scalar_one_or_none()
    if not obj:
        raise HTTPException(status_code=404, detail="Playoff bracket entry not found")

    await db.delete(obj)
    await db.commit()
