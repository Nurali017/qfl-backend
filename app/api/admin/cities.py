from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func

from app.api.deps import get_db
from app.api.admin.deps import require_roles
from app.models import City
from app.schemas.admin.cities import (
    AdminCityCreateRequest,
    AdminCityUpdateRequest,
    AdminCityResponse,
    AdminCitiesListResponse,
)

router = APIRouter(
    prefix="/cities",
    tags=["admin-cities"],
    dependencies=[Depends(require_roles("superadmin", "editor"))],
)


@router.get("", response_model=AdminCitiesListResponse)
async def list_cities(
    limit: int = Query(default=100, le=200),
    offset: int = Query(default=0),
    db: AsyncSession = Depends(get_db),
):
    count_result = await db.execute(select(func.count()).select_from(City))
    total = count_result.scalar() or 0

    result = await db.execute(
        select(City).order_by(City.name).offset(offset).limit(limit)
    )
    items = [AdminCityResponse.model_validate(c) for c in result.scalars().all()]
    return AdminCitiesListResponse(items=items, total=total)


@router.post("", response_model=AdminCityResponse, status_code=status.HTTP_201_CREATED)
async def create_city(
    body: AdminCityCreateRequest,
    db: AsyncSession = Depends(get_db),
):
    obj = City(**body.model_dump())
    db.add(obj)
    await db.commit()
    await db.refresh(obj)
    return AdminCityResponse.model_validate(obj)


@router.patch("/{id}", response_model=AdminCityResponse)
async def update_city(
    id: int,
    body: AdminCityUpdateRequest,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(City).where(City.id == id))
    obj = result.scalar_one_or_none()
    if not obj:
        raise HTTPException(status_code=404, detail="City not found")

    for key, value in body.model_dump(exclude_unset=True).items():
        setattr(obj, key, value)

    await db.commit()
    await db.refresh(obj)
    return AdminCityResponse.model_validate(obj)


@router.delete("/{id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_city(id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(City).where(City.id == id))
    obj = result.scalar_one_or_none()
    if not obj:
        raise HTTPException(status_code=404, detail="City not found")

    await db.delete(obj)
    await db.commit()
