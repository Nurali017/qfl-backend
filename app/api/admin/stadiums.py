from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.admin.deps import require_roles
from app.api.deps import get_db
from app.models import Club, FieldType, Game, SeasonParticipant, Stadium, Team
from app.schemas.admin.stadiums import (
    AdminStadiumCreateRequest,
    AdminStadiumResponse,
    AdminStadiumUpdateRequest,
    AdminStadiumsListResponse,
)

router = APIRouter(prefix="/stadiums", tags=["admin-stadiums"])

# GET is consumed by dropdowns on operator/editor screens (games, poster, teams),
# so reads stay broad; mutations are restricted to superadmin.
_read_roles = require_roles("superadmin", "editor", "operator")
_write_roles = require_roles("superadmin")


def _coerce_field_type(value: str | None) -> FieldType | None:
    """Validate an incoming field_type string against the enum (None passes through)."""
    if value is None:
        return None
    try:
        return FieldType(value)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Invalid field_type. Must be one of: {[e.value for e in FieldType]}",
        )


def _to_response(stadium: Stadium, counts: dict[str, int] | None = None) -> AdminStadiumResponse:
    counts = counts or {}
    return AdminStadiumResponse(
        id=stadium.id,
        legacy_id=stadium.legacy_id,
        name=stadium.name,
        name_kz=stadium.name_kz,
        name_ru=stadium.name_ru,
        name_en=stadium.name_en,
        city=stadium.city,
        city_kz=stadium.city_kz,
        city_ru=stadium.city_ru,
        city_en=stadium.city_en,
        city_id=stadium.city_id,
        capacity=stadium.capacity,
        field_type=stadium.field_type.value if stadium.field_type else None,
        address=stadium.address,
        address_kz=stadium.address_kz,
        address_en=stadium.address_en,
        photo_url=stadium.photo_url,
        created_at=stadium.created_at,
        updated_at=stadium.updated_at,
        games_count=counts.get("games_count", 0),
        teams_count=counts.get("teams_count", 0),
        clubs_count=counts.get("clubs_count", 0),
        participants_count=counts.get("participants_count", 0),
    )


async def _reference_counts(db: AsyncSession, stadium_id: int) -> dict[str, int]:
    """Count rows referencing this stadium across all 4 nullable FKs."""
    async def _count(model) -> int:
        result = await db.execute(
            select(func.count()).select_from(model).where(model.stadium_id == stadium_id)
        )
        return int(result.scalar() or 0)

    return {
        "games_count": await _count(Game),
        "teams_count": await _count(Team),
        "clubs_count": await _count(Club),
        "participants_count": await _count(SeasonParticipant),
    }


async def _get_stadium_or_404(db: AsyncSession, stadium_id: int) -> Stadium:
    result = await db.execute(select(Stadium).where(Stadium.id == stadium_id))
    stadium = result.scalar_one_or_none()
    if not stadium:
        raise HTTPException(status_code=404, detail="Stadium not found")
    return stadium


@router.get("", response_model=AdminStadiumsListResponse, dependencies=[Depends(_read_roles)])
async def list_stadiums(
    limit: int | None = Query(default=None, ge=1),
    offset: int = Query(default=0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    """List all stadiums.

    Returns the full list by default (no truncating limit) so dropdowns in
    games/poster/teams keep working. `limit`/`offset` are optional.
    """
    count_result = await db.execute(select(func.count()).select_from(Stadium))
    total = int(count_result.scalar() or 0)

    query = select(Stadium).order_by(Stadium.name).offset(offset)
    if limit is not None:
        query = query.limit(limit)

    result = await db.execute(query)
    items = [_to_response(s) for s in result.scalars().all()]
    return AdminStadiumsListResponse(items=items, total=total)


@router.get("/{stadium_id}", response_model=AdminStadiumResponse, dependencies=[Depends(_read_roles)])
async def get_stadium(stadium_id: int, db: AsyncSession = Depends(get_db)):
    stadium = await _get_stadium_or_404(db, stadium_id)
    counts = await _reference_counts(db, stadium_id)
    return _to_response(stadium, counts)


@router.post(
    "",
    response_model=AdminStadiumResponse,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(_write_roles)],
)
async def create_stadium(
    body: AdminStadiumCreateRequest,
    db: AsyncSession = Depends(get_db),
):
    data = body.model_dump(exclude_unset=True)
    data["field_type"] = _coerce_field_type(data.get("field_type"))
    stadium = Stadium(**data)
    db.add(stadium)
    await db.commit()
    await db.refresh(stadium)
    return _to_response(stadium)


@router.patch("/{stadium_id}", response_model=AdminStadiumResponse, dependencies=[Depends(_write_roles)])
async def update_stadium(
    stadium_id: int,
    body: AdminStadiumUpdateRequest,
    db: AsyncSession = Depends(get_db),
):
    stadium = await _get_stadium_or_404(db, stadium_id)

    update_data = body.model_dump(exclude_unset=True)
    if "field_type" in update_data:
        update_data["field_type"] = _coerce_field_type(update_data["field_type"])

    for field, value in update_data.items():
        setattr(stadium, field, value)

    await db.commit()
    await db.refresh(stadium)
    return _to_response(stadium)


@router.delete("/{stadium_id}", dependencies=[Depends(_write_roles)])
async def delete_stadium(stadium_id: int, db: AsyncSession = Depends(get_db)):
    stadium = await _get_stadium_or_404(db, stadium_id)

    counts = await _reference_counts(db, stadium_id)
    if any(counts.values()):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                "Stadium is in use: "
                f"games={counts['games_count']}, teams={counts['teams_count']}, "
                f"clubs={counts['clubs_count']}, season participants={counts['participants_count']}"
            ),
        )

    await db.delete(stadium)
    await db.commit()
    return {"ok": True}
