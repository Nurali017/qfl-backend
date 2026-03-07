"""Admin teams endpoints — list, detail, update."""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.api.admin.deps import require_roles
from app.api.deps import get_db
from app.caching import invalidate_pattern
from app.models import AdminUser, Player, PlayerTeam, Team
from app.schemas.admin.teams import (
    AdminTeamDetailResponse,
    AdminTeamListItem,
    AdminTeamsListResponse,
    AdminTeamUpdateRequest,
)
from app.utils.localization import get_localized_name

router = APIRouter(prefix="/teams", tags=["admin-teams"])


def _team_detail_dict(team: Team) -> dict:
    return {
        "id": team.id,
        "name": team.name,
        "name_kz": team.name_kz,
        "name_en": team.name_en,
        "website": team.website,
        "city": team.city,
        "city_kz": team.city_kz,
        "city_en": team.city_en,
        "stadium_id": team.stadium_id,
        "stadium_name": team.stadium.name if team.stadium else None,
        "primary_color": team.primary_color,
        "secondary_color": team.secondary_color,
        "accent_color": team.accent_color,
        "logo_url": team.logo_url,
    }


@router.get("", response_model=AdminTeamsListResponse)
async def list_teams(
    lang: str = Query("ru", pattern="^(kz|ru)$"),
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "editor", "analyst")),
):
    """List all teams with stadium info."""
    result = await db.execute(
        select(Team).options(selectinload(Team.stadium)).order_by(Team.name)
    )
    teams = result.scalars().all()
    items = [
        AdminTeamListItem(
            id=t.id,
            name=get_localized_name(t, lang),
            stadium_id=t.stadium_id,
            stadium_name=t.stadium.name if t.stadium else None,
        )
        for t in teams
    ]
    return AdminTeamsListResponse(items=items, total=len(items))


@router.get("/{team_id}", response_model=AdminTeamDetailResponse)
async def get_team(
    team_id: int,
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "editor")),
):
    """Get full team details."""
    result = await db.execute(
        select(Team).options(selectinload(Team.stadium)).where(Team.id == team_id)
    )
    team = result.scalar_one_or_none()
    if not team:
        raise HTTPException(status_code=404, detail="Team not found")
    return _team_detail_dict(team)


@router.patch("/{team_id}", response_model=AdminTeamDetailResponse)
async def update_team(
    team_id: int,
    body: AdminTeamUpdateRequest,
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "editor")),
):
    """Update team fields."""
    result = await db.execute(select(Team).where(Team.id == team_id))
    team = result.scalar_one_or_none()
    if not team:
        raise HTTPException(status_code=404, detail="Team not found")

    for key, value in body.model_dump(exclude_unset=True).items():
        setattr(team, key, value)

    await db.commit()
    await db.refresh(team)
    await invalidate_pattern("*app.api.teams*")

    # Reload with stadium
    result = await db.execute(
        select(Team).options(selectinload(Team.stadium)).where(Team.id == team_id)
    )
    team = result.scalar_one()
    return _team_detail_dict(team)


@router.get("/{team_id}/players")
async def list_team_players(
    team_id: int,
    season_id: int = Query(...),
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "editor", "analyst")),
):
    """List players for a team in a given season (for lineup selector)."""
    result = await db.execute(
        select(Player, PlayerTeam.number)
        .join(PlayerTeam, PlayerTeam.player_id == Player.id)
        .where(PlayerTeam.team_id == team_id, PlayerTeam.season_id == season_id)
        .order_by(Player.last_name.asc(), Player.first_name.asc())
    )
    rows = result.all()
    items = [
        {
            "id": player.id,
            "first_name": player.first_name,
            "last_name": player.last_name,
            "number": number,
        }
        for player, number in rows
    ]
    return {"items": items, "total": len(items)}
