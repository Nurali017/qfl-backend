from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import delete, select, func

from app.api.deps import get_db
from app.api.admin.deps import require_roles
from app.caching import invalidate_pattern
from app.models import Season, SeasonParticipant, Team
from app.services.season_visibility import ensure_visible_season_or_404, is_season_visible_clause
from app.schemas.admin.season_participants import (
    AdminSeasonParticipantsBulkSetRequest,
    AdminSeasonParticipantsBulkSetResponse,
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
    await ensure_visible_season_or_404(db, season_id)

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
    await ensure_visible_season_or_404(db, body.season_id)

    obj = SeasonParticipant(**body.model_dump())
    db.add(obj)
    await db.commit()
    await db.refresh(obj)
    await invalidate_pattern("*app.api.seasons*")
    return AdminSeasonParticipantResponse.model_validate(obj)


@router.post("/bulk-set", response_model=AdminSeasonParticipantsBulkSetResponse)
async def bulk_set_season_participants(
    body: AdminSeasonParticipantsBulkSetRequest,
    db: AsyncSession = Depends(get_db),
):
    await ensure_visible_season_or_404(db, body.season_id)

    if not body.replace:
        raise HTTPException(status_code=400, detail="bulk-set only supports replace=true")

    if len(body.team_ids) != len(set(body.team_ids)):
        raise HTTPException(status_code=400, detail="team_ids must not contain duplicates")

    if body.team_ids:
        existing_team_ids_result = await db.execute(
            select(Team.id).where(Team.id.in_(body.team_ids))
        )
        existing_team_ids = set(existing_team_ids_result.scalars().all())
        missing = [team_id for team_id in body.team_ids if team_id not in existing_team_ids]
        if missing:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown team_ids: {missing}",
            )

    await db.execute(
        delete(SeasonParticipant).where(SeasonParticipant.season_id == body.season_id)
    )
    await db.flush()

    created_rows: list[SeasonParticipant] = []
    for idx, team_id in enumerate(body.team_ids, start=1):
        row = SeasonParticipant(
            season_id=body.season_id,
            team_id=team_id,
            sort_order=idx,
            is_disqualified=False,
            fine_points=0,
        )
        db.add(row)
        created_rows.append(row)

    await db.commit()

    for row in created_rows:
        await db.refresh(row)

    await invalidate_pattern("*app.api.seasons*")
    await invalidate_pattern("*app.api.cup*")
    await invalidate_pattern("*app.api.games*")
    return AdminSeasonParticipantsBulkSetResponse(
        season_id=body.season_id,
        total=len(created_rows),
        item_ids=[row.id for row in created_rows],
    )


@router.patch("/{id}", response_model=AdminSeasonParticipantResponse)
async def update_season_participant(
    id: int,
    body: AdminSeasonParticipantUpdateRequest,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(SeasonParticipant)
        .join(Season, Season.id == SeasonParticipant.season_id)
        .where(
            SeasonParticipant.id == id,
            is_season_visible_clause(),
        )
    )
    obj = result.scalar_one_or_none()
    if not obj:
        raise HTTPException(status_code=404, detail="Season participant entry not found")

    for key, value in body.model_dump(exclude_unset=True).items():
        setattr(obj, key, value)

    await db.commit()
    await db.refresh(obj)
    await invalidate_pattern("*app.api.seasons*")
    return AdminSeasonParticipantResponse.model_validate(obj)


@router.delete("/{id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_season_participant(id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(SeasonParticipant)
        .join(Season, Season.id == SeasonParticipant.season_id)
        .where(
            SeasonParticipant.id == id,
            is_season_visible_clause(),
        )
    )
    obj = result.scalar_one_or_none()
    if not obj:
        raise HTTPException(status_code=404, detail="Season participant entry not found")

    await db.delete(obj)
    await db.commit()
    await invalidate_pattern("*app.api.seasons*")
