"""Admin teams read-only endpoints (used by admin frontend for dropdowns etc.)."""

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.admin.deps import require_roles
from app.api.deps import get_db
from app.models import AdminUser, Player, PlayerTeam, Team
from app.utils.localization import get_localized_name

router = APIRouter(prefix="/teams", tags=["admin-teams"])


@router.get("")
async def list_teams(
    lang: str = Query("ru", pattern="^(kz|ru)$"),
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "editor", "analyst")),
):
    """List all teams (for admin dropdowns/selects)."""
    result = await db.execute(select(Team).order_by(Team.name))
    teams = result.scalars().all()
    items = [
        {"id": t.id, "name": get_localized_name(t, lang)}
        for t in teams
    ]
    return {"items": items, "total": len(items)}


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
