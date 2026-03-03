from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.admin.deps import require_roles
from app.api.deps import get_db
from app.models import AdminUser, Stadium

router = APIRouter(prefix="/stadiums", tags=["admin-stadiums"])


@router.get("")
async def list_stadiums(
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "editor")),
):
    """List all stadiums (for dropdowns)."""
    result = await db.execute(select(Stadium).order_by(Stadium.name))
    stadiums = result.scalars().all()
    items = [{"id": s.id, "name": s.name} for s in stadiums]
    return {"items": items, "total": len(items)}
