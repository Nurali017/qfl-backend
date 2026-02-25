from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func

from app.api.deps import get_db
from app.api.admin.deps import require_roles
from app.caching import invalidate_pattern
from app.models import Season
from app.models.team_of_week import TeamOfWeek
from app.services.season_visibility import ensure_visible_season_or_404, is_season_visible_clause
from app.schemas.admin.team_of_week import (
    AdminTeamOfWeekCreateRequest,
    AdminTeamOfWeekUpdateRequest,
    AdminTeamOfWeekResponse,
    AdminTeamOfWeekListResponse,
)

router = APIRouter(
    prefix="/team-of-week",
    tags=["admin-team-of-week"],
    dependencies=[Depends(require_roles("superadmin", "editor"))],
)


@router.get("", response_model=AdminTeamOfWeekListResponse)
async def list_team_of_week(
    season_id: int | None = Query(default=None),
    tour_key: str | None = Query(default=None),
    locale: str | None = Query(default=None),
    limit: int = Query(default=50, le=200),
    offset: int = Query(default=0),
    db: AsyncSession = Depends(get_db),
):
    if season_id is not None:
        await ensure_visible_season_or_404(db, season_id)

    base = (
        select(TeamOfWeek)
        .join(Season, Season.id == TeamOfWeek.season_id)
        .where(is_season_visible_clause())
    )
    if season_id is not None:
        base = base.where(TeamOfWeek.season_id == season_id)
    if tour_key is not None:
        base = base.where(TeamOfWeek.tour_key == tour_key)
    if locale is not None:
        base = base.where(TeamOfWeek.locale == locale)

    count_result = await db.execute(select(func.count()).select_from(base.subquery()))
    total = count_result.scalar() or 0

    result = await db.execute(
        base.order_by(TeamOfWeek.season_id, TeamOfWeek.tour_key).offset(offset).limit(limit)
    )
    items = [AdminTeamOfWeekResponse.model_validate(r) for r in result.scalars().all()]
    return AdminTeamOfWeekListResponse(items=items, total=total)


@router.get("/{id}", response_model=AdminTeamOfWeekResponse)
async def get_team_of_week(id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(TeamOfWeek)
        .join(Season, Season.id == TeamOfWeek.season_id)
        .where(
            TeamOfWeek.id == id,
            is_season_visible_clause(),
        )
    )
    obj = result.scalar_one_or_none()
    if not obj:
        raise HTTPException(status_code=404, detail="TeamOfWeek not found")
    return AdminTeamOfWeekResponse.model_validate(obj)


@router.post("", response_model=AdminTeamOfWeekResponse, status_code=status.HTTP_201_CREATED)
async def create_team_of_week(
    body: AdminTeamOfWeekCreateRequest,
    db: AsyncSession = Depends(get_db),
):
    await ensure_visible_season_or_404(db, body.season_id)

    obj = TeamOfWeek(**body.model_dump())
    db.add(obj)
    await db.commit()
    await db.refresh(obj)
    await invalidate_pattern("*app.api.teams*")
    return AdminTeamOfWeekResponse.model_validate(obj)


@router.patch("/{id}", response_model=AdminTeamOfWeekResponse)
async def update_team_of_week(
    id: int,
    body: AdminTeamOfWeekUpdateRequest,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(TeamOfWeek)
        .join(Season, Season.id == TeamOfWeek.season_id)
        .where(
            TeamOfWeek.id == id,
            is_season_visible_clause(),
        )
    )
    obj = result.scalar_one_or_none()
    if not obj:
        raise HTTPException(status_code=404, detail="TeamOfWeek not found")

    if body.season_id is not None:
        await ensure_visible_season_or_404(db, body.season_id)

    for key, value in body.model_dump(exclude_unset=True).items():
        setattr(obj, key, value)

    await db.commit()
    await db.refresh(obj)
    await invalidate_pattern("*app.api.teams*")
    return AdminTeamOfWeekResponse.model_validate(obj)


@router.delete("/{id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_team_of_week(id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(TeamOfWeek)
        .join(Season, Season.id == TeamOfWeek.season_id)
        .where(
            TeamOfWeek.id == id,
            is_season_visible_clause(),
        )
    )
    obj = result.scalar_one_or_none()
    if not obj:
        raise HTTPException(status_code=404, detail="TeamOfWeek not found")

    await db.delete(obj)
    await db.commit()
    await invalidate_pattern("*app.api.teams*")
