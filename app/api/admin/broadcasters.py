from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db
from app.api.admin.deps import require_roles
from app.models.broadcaster import Broadcaster
from app.schemas.admin.broadcasters import (
    AdminBroadcasterResponse,
    AdminBroadcasterCreateRequest,
    AdminBroadcasterUpdateRequest,
    AdminBroadcastersListResponse,
)
from app.utils.timestamps import utcnow

router = APIRouter(
    prefix="/broadcasters",
    tags=["admin-broadcasters"],
    dependencies=[Depends(require_roles("superadmin", "editor"))],
)


def _to_response(b: Broadcaster) -> AdminBroadcasterResponse:
    return AdminBroadcasterResponse(
        id=b.id,
        name=b.name,
        logo_url=b.logo_url,
        type=b.type,
        website=b.website,
        sort_order=b.sort_order,
        is_active=b.is_active,
        created_at=b.created_at,
        updated_at=b.updated_at,
    )


@router.get("", response_model=AdminBroadcastersListResponse)
async def list_broadcasters(db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(Broadcaster).order_by(Broadcaster.sort_order, Broadcaster.name)
    )
    items = result.scalars().all()
    return AdminBroadcastersListResponse(
        items=[_to_response(b) for b in items],
        total=len(items),
    )


@router.post("", response_model=AdminBroadcasterResponse, status_code=201)
async def create_broadcaster(
    body: AdminBroadcasterCreateRequest,
    db: AsyncSession = Depends(get_db),
):
    now = utcnow()
    broadcaster = Broadcaster(
        name=body.name,
        logo_url=body.logo_url,
        type=body.type,
        website=body.website,
        sort_order=body.sort_order,
        is_active=body.is_active,
        created_at=now,
        updated_at=now,
    )
    db.add(broadcaster)
    await db.commit()
    await db.refresh(broadcaster)
    return _to_response(broadcaster)


@router.patch("/{broadcaster_id}", response_model=AdminBroadcasterResponse)
async def update_broadcaster(
    broadcaster_id: int,
    body: AdminBroadcasterUpdateRequest,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(Broadcaster).where(Broadcaster.id == broadcaster_id))
    broadcaster = result.scalar_one_or_none()
    if not broadcaster:
        raise HTTPException(status_code=404, detail="Broadcaster not found")

    update_data = body.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(broadcaster, field, value)
    broadcaster.updated_at = utcnow()

    await db.commit()
    await db.refresh(broadcaster)
    return _to_response(broadcaster)


@router.delete("/{broadcaster_id}")
async def delete_broadcaster(
    broadcaster_id: int,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(Broadcaster).where(Broadcaster.id == broadcaster_id))
    broadcaster = result.scalar_one_or_none()
    if not broadcaster:
        raise HTTPException(status_code=404, detail="Broadcaster not found")
    await db.delete(broadcaster)
    await db.commit()
    return {"ok": True}
