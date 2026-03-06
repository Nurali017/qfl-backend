from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.api.admin.deps import require_roles
from app.api.deps import get_db
from app.models import AdminUser, Country, GameReferee, Referee
from app.schemas.admin.referees import (
    AdminRefereeCreateRequest,
    AdminRefereeResponse,
    AdminRefereesListResponse,
    AdminRefereesMetaResponse,
    AdminRefereeUpdateRequest,
)
from app.schemas.admin.players import AdminMetaCountry

router = APIRouter(prefix="/referees", tags=["admin-referees"])


def _serialize_referee(referee: Referee, game_assignments_count: int = 0) -> AdminRefereeResponse:
    return AdminRefereeResponse(
        id=referee.id,
        first_name=referee.first_name,
        last_name=referee.last_name,
        first_name_kz=referee.first_name_kz,
        first_name_ru=referee.first_name_ru,
        first_name_en=referee.first_name_en,
        last_name_kz=referee.last_name_kz,
        last_name_ru=referee.last_name_ru,
        last_name_en=referee.last_name_en,
        country_id=referee.country_id,
        country_name=referee.country.name if referee.country else None,
        photo_url=referee.photo_url,
        game_assignments_count=game_assignments_count,
    )


@router.get("/meta", response_model=AdminRefereesMetaResponse)
async def get_referees_meta(
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "editor")),
):
    countries_result = await db.execute(select(Country).order_by(Country.name.asc()))
    countries = countries_result.scalars().all()
    return AdminRefereesMetaResponse(
        countries=[
            AdminMetaCountry(id=c.id, code=c.code, name=c.name)
            for c in countries
        ]
    )


@router.get("", response_model=AdminRefereesListResponse)
async def list_referees(
    search: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "editor")),
):
    query = select(Referee).options(selectinload(Referee.country))

    if search:
        normalized = f"%{search.strip()}%"
        query = query.where(
            or_(
                Referee.first_name.ilike(normalized),
                Referee.last_name.ilike(normalized),
                Referee.first_name_kz.ilike(normalized),
                Referee.last_name_kz.ilike(normalized),
                Referee.first_name_ru.ilike(normalized),
                Referee.last_name_ru.ilike(normalized),
                Referee.first_name_en.ilike(normalized),
                Referee.last_name_en.ilike(normalized),
            )
        )

    total = (
        await db.execute(select(func.count()).select_from(query.subquery()))
    ).scalar() or 0

    result = await db.execute(
        query.order_by(Referee.last_name.asc(), Referee.first_name.asc(), Referee.id.asc())
        .offset(offset)
        .limit(limit)
    )
    referees = list(result.scalars().all())

    # Get game assignment counts
    counts_map: dict[int, int] = {}
    if referees:
        referee_ids = [r.id for r in referees]
        counts_result = await db.execute(
            select(GameReferee.referee_id, func.count())
            .where(GameReferee.referee_id.in_(referee_ids))
            .group_by(GameReferee.referee_id)
        )
        counts_map = dict(counts_result.all())

    return AdminRefereesListResponse(
        items=[_serialize_referee(r, counts_map.get(r.id, 0)) for r in referees],
        total=total,
    )


@router.get("/{referee_id}", response_model=AdminRefereeResponse)
async def get_referee(
    referee_id: int,
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "editor")),
):
    result = await db.execute(
        select(Referee)
        .where(Referee.id == referee_id)
        .options(selectinload(Referee.country))
    )
    referee = result.scalar_one_or_none()
    if referee is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Referee not found")

    count = (
        await db.execute(
            select(func.count()).select_from(GameReferee).where(GameReferee.referee_id == referee_id)
        )
    ).scalar() or 0

    return _serialize_referee(referee, count)


@router.post("", response_model=AdminRefereeResponse, status_code=status.HTTP_201_CREATED)
async def create_referee(
    payload: AdminRefereeCreateRequest,
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "editor")),
):
    referee = Referee(
        first_name=payload.first_name,
        last_name=payload.last_name,
        first_name_kz=payload.first_name_kz,
        first_name_ru=payload.first_name_ru,
        first_name_en=payload.first_name_en,
        last_name_kz=payload.last_name_kz,
        last_name_ru=payload.last_name_ru,
        last_name_en=payload.last_name_en,
        country_id=payload.country_id,
        photo_url=payload.photo_url,
    )
    db.add(referee)
    await db.commit()
    await db.refresh(referee, attribute_names=["country"])

    return _serialize_referee(referee, 0)


@router.patch("/{referee_id}", response_model=AdminRefereeResponse)
async def update_referee(
    referee_id: int,
    payload: AdminRefereeUpdateRequest,
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "editor")),
):
    result = await db.execute(select(Referee).where(Referee.id == referee_id))
    referee = result.scalar_one_or_none()
    if referee is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Referee not found")

    update_data = payload.model_dump(exclude_unset=True)
    for field_name, value in update_data.items():
        setattr(referee, field_name, value)

    await db.commit()
    await db.refresh(referee, attribute_names=["country"])

    count = (
        await db.execute(
            select(func.count()).select_from(GameReferee).where(GameReferee.referee_id == referee_id)
        )
    ).scalar() or 0

    return _serialize_referee(referee, count)


@router.delete("/{referee_id}")
async def delete_referee(
    referee_id: int,
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "editor")),
):
    result = await db.execute(select(Referee).where(Referee.id == referee_id))
    referee = result.scalar_one_or_none()
    if referee is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Referee not found")

    game_count = (
        await db.execute(
            select(func.count()).select_from(GameReferee).where(GameReferee.referee_id == referee_id)
        )
    ).scalar() or 0

    if game_count > 0:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Cannot delete referee with {game_count} game assignment(s)",
        )

    await db.delete(referee)
    await db.commit()
    return {"message": "Referee deleted"}
