from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func

from app.api.deps import get_db
from app.api.admin.deps import require_roles
from app.models import TeamTournament
from app.schemas.admin.team_tournaments import (
    AdminTeamTournamentCreateRequest,
    AdminTeamTournamentUpdateRequest,
    AdminTeamTournamentResponse,
    AdminTeamTournamentsListResponse,
)

router = APIRouter(
    prefix="/team-tournaments",
    tags=["admin-team-tournaments"],
    dependencies=[Depends(require_roles("superadmin", "editor"))],
)


@router.get("", response_model=AdminTeamTournamentsListResponse)
async def list_team_tournaments(
    season_id: int = Query(...),
    limit: int = Query(default=100, le=200),
    offset: int = Query(default=0),
    db: AsyncSession = Depends(get_db),
):
    base = select(TeamTournament).where(TeamTournament.season_id == season_id)
    count_result = await db.execute(select(func.count()).select_from(base.subquery()))
    total = count_result.scalar() or 0

    result = await db.execute(
        base.order_by(TeamTournament.sort_order, TeamTournament.id)
        .offset(offset)
        .limit(limit)
    )
    items = [AdminTeamTournamentResponse.model_validate(t) for t in result.scalars().all()]
    return AdminTeamTournamentsListResponse(items=items, total=total)


@router.post("", response_model=AdminTeamTournamentResponse, status_code=status.HTTP_201_CREATED)
async def create_team_tournament(
    body: AdminTeamTournamentCreateRequest,
    db: AsyncSession = Depends(get_db),
):
    obj = TeamTournament(**body.model_dump())
    db.add(obj)
    await db.commit()
    await db.refresh(obj)
    return AdminTeamTournamentResponse.model_validate(obj)


@router.patch("/{id}", response_model=AdminTeamTournamentResponse)
async def update_team_tournament(
    id: int,
    body: AdminTeamTournamentUpdateRequest,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(TeamTournament).where(TeamTournament.id == id))
    obj = result.scalar_one_or_none()
    if not obj:
        raise HTTPException(status_code=404, detail="Team tournament entry not found")

    for key, value in body.model_dump(exclude_unset=True).items():
        setattr(obj, key, value)

    await db.commit()
    await db.refresh(obj)
    return AdminTeamTournamentResponse.model_validate(obj)


@router.delete("/{id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_team_tournament(id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(TeamTournament).where(TeamTournament.id == id))
    obj = result.scalar_one_or_none()
    if not obj:
        raise HTTPException(status_code=404, detail="Team tournament entry not found")

    await db.delete(obj)
    await db.commit()
