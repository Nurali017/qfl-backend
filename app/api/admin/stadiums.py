from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.admin.deps import require_roles
from app.api.deps import get_db
from app.models import AdminUser, Stadium, FieldType

router = APIRouter(prefix="/stadiums", tags=["admin-stadiums"])


class StadiumPatch(BaseModel):
    field_type: str | None = None


@router.get("")
async def list_stadiums(
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "editor")),
):
    """List all stadiums (for dropdowns)."""
    result = await db.execute(select(Stadium).order_by(Stadium.name))
    stadiums = result.scalars().all()
    items = [
        {
            "id": s.id,
            "name": s.name,
            "field_type": s.field_type.value if s.field_type else None,
        }
        for s in stadiums
    ]
    return {"items": items, "total": len(items)}


@router.patch("/{stadium_id}")
async def update_stadium(
    stadium_id: int,
    body: StadiumPatch,
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "editor")),
):
    """Update stadium fields (currently field_type)."""
    result = await db.execute(select(Stadium).where(Stadium.id == stadium_id))
    stadium = result.scalar_one_or_none()
    if not stadium:
        raise HTTPException(status_code=404, detail="Stadium not found")

    if body.field_type is not None:
        try:
            stadium.field_type = FieldType(body.field_type)
        except ValueError:
            raise HTTPException(
                status_code=422,
                detail=f"Invalid field_type. Must be one of: {[e.value for e in FieldType]}",
            )
    else:
        stadium.field_type = None

    await db.commit()
    await db.refresh(stadium)
    return {
        "id": stadium.id,
        "name": stadium.name,
        "field_type": stadium.field_type.value if stadium.field_type else None,
    }
