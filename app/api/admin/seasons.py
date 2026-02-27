from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, update
from sqlalchemy.exc import IntegrityError

from app.api.deps import get_db
from app.api.admin.deps import require_roles
from app.caching import invalidate_pattern
from app.models import Championship, Season
from app.services.season_visibility import is_season_visible_clause
from app.schemas.admin.seasons import (
    AdminSeasonCreateRequest,
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


@router.post("", response_model=AdminSeasonResponse, status_code=status.HTTP_201_CREATED)
async def create_season(
    body: AdminSeasonCreateRequest,
    db: AsyncSession = Depends(get_db),
):
    if body.frontend_code != "cup":
        raise HTTPException(status_code=400, detail="frontend_code must be 'cup'")

    existing_id = await db.execute(select(Season.id).where(Season.id == body.id))
    if existing_id.scalar_one_or_none() is not None:
        raise HTTPException(status_code=409, detail=f"Season id {body.id} already exists")

    championship = await db.execute(
        select(Championship.id).where(Championship.id == body.championship_id)
    )
    if championship.scalar_one_or_none() is None:
        raise HTTPException(status_code=400, detail="championship_id does not exist")

    obj = Season(**body.model_dump())
    db.add(obj)
    try:
        await db.commit()
    except IntegrityError as exc:
        await db.rollback()
        raise HTTPException(status_code=400, detail=f"Failed to create season: {exc.orig}") from exc

    await db.refresh(obj)
    await invalidate_pattern("*app.api.seasons*")
    await invalidate_pattern("*app.api.cup*")
    await invalidate_pattern("*app.api.games*")
    await invalidate_pattern("*app.api.championships*")
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

    update_data = body.model_dump(exclude_unset=True)

    # Auto-reset: when setting is_current=True, reset all other seasons first
    if update_data.get("is_current") is True:
        await db.execute(
            update(Season).where(Season.id != id, Season.is_current == True).values(is_current=False)
        )

    for key, value in update_data.items():
        setattr(obj, key, value)

    await db.commit()
    await db.refresh(obj)
    await invalidate_pattern("*app.api.seasons*")
    await invalidate_pattern("*app.api.cup*")
    await invalidate_pattern("*app.api.games*")
    await invalidate_pattern("*app.api.championships*")
    return AdminSeasonResponse.model_validate(obj)
