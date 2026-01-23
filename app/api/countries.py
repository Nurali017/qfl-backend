"""Country API endpoints."""

from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File
from fastapi.responses import Response
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.api.deps import get_db
from app.models import Country
from app.schemas.country import (
    CountryCreate,
    CountryUpdate,
    CountryResponse,
    CountryListResponse,
)
from app.services.file_storage import FileStorageService
from app.utils.localization import get_localized_name

router = APIRouter(prefix="/countries", tags=["countries"])


@router.get("", response_model=CountryListResponse)
async def list_countries(
    lang: str = Query(default="kz", description="Language: kz, ru, or en"),
    include_inactive: bool = Query(default=False),
    db: AsyncSession = Depends(get_db),
):
    """List all countries."""
    query = select(Country)
    if not include_inactive:
        query = query.where(Country.is_active == True)
    query = query.order_by(Country.name)

    result = await db.execute(query)
    countries = result.scalars().all()

    items = []
    for c in countries:
        items.append(CountryResponse(
            id=c.id,
            code=c.code,
            name=get_localized_name(c, lang),
            name_kz=c.name_kz,
            name_en=c.name_en,
            flag_url=c.flag_url,
            is_active=c.is_active,
        ))

    return CountryListResponse(items=items, total=len(items))


@router.get("/{country_id}", response_model=CountryResponse)
async def get_country(
    country_id: int,
    lang: str = Query(default="kz", description="Language: kz, ru, or en"),
    db: AsyncSession = Depends(get_db),
):
    """Get country by ID."""
    result = await db.execute(select(Country).where(Country.id == country_id))
    country = result.scalar_one_or_none()

    if not country:
        raise HTTPException(status_code=404, detail="Country not found")

    return CountryResponse(
        id=country.id,
        code=country.code,
        name=get_localized_name(country, lang),
        name_kz=country.name_kz,
        name_en=country.name_en,
        flag_url=country.flag_url,
        is_active=country.is_active,
    )


@router.post("", response_model=CountryResponse)
async def create_country(
    country: CountryCreate,
    db: AsyncSession = Depends(get_db),
):
    """Create a new country."""
    existing = await db.execute(
        select(Country).where(Country.code == country.code.upper())
    )
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=400, detail="Country code already exists")

    db_country = Country(
        code=country.code.upper(),
        name=country.name,
        name_kz=country.name_kz,
        name_en=country.name_en,
    )
    db.add(db_country)
    await db.commit()
    await db.refresh(db_country)

    return CountryResponse.model_validate(db_country)


@router.put("/{country_id}", response_model=CountryResponse)
async def update_country(
    country_id: int,
    country: CountryUpdate,
    db: AsyncSession = Depends(get_db),
):
    """Update a country."""
    result = await db.execute(select(Country).where(Country.id == country_id))
    db_country = result.scalar_one_or_none()

    if not db_country:
        raise HTTPException(status_code=404, detail="Country not found")

    update_data = country.model_dump(exclude_unset=True)
    if "code" in update_data and update_data["code"]:
        update_data["code"] = update_data["code"].upper()

    for field, value in update_data.items():
        setattr(db_country, field, value)

    await db.commit()
    await db.refresh(db_country)

    return CountryResponse.model_validate(db_country)


@router.post("/{country_id}/flag", response_model=CountryResponse)
async def upload_country_flag(
    country_id: int,
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
):
    """Upload flag image for a country."""
    result = await db.execute(select(Country).where(Country.id == country_id))
    country = result.scalar_one_or_none()

    if not country:
        raise HTTPException(status_code=404, detail="Country not found")

    content = await file.read()

    upload_result = await FileStorageService.upload_country_flag(
        file_data=content,
        country_code=country.code,
        content_type=file.content_type or "image/webp",
    )

    country.flag_url = upload_result["url"]
    await db.commit()
    await db.refresh(country)

    return CountryResponse.model_validate(country)


@router.get("/{country_id}/flag")
async def get_country_flag(
    country_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Get flag image for a country."""
    result = await db.execute(select(Country).where(Country.id == country_id))
    country = result.scalar_one_or_none()

    if not country:
        raise HTTPException(status_code=404, detail="Country not found")

    flag_result = await FileStorageService.get_country_flag(country.code)

    if not flag_result:
        raise HTTPException(status_code=404, detail="Flag not found")

    content, metadata = flag_result

    return Response(
        content=content,
        media_type=metadata.get("content_type", "image/webp"),
        headers={
            "Content-Disposition": f'inline; filename="{metadata.get("filename", "flag.webp")}"',
            "Cache-Control": "public, max-age=86400",
        },
    )


@router.delete("/{country_id}")
async def delete_country(
    country_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Soft delete a country (sets is_active=False)."""
    result = await db.execute(select(Country).where(Country.id == country_id))
    country = result.scalar_one_or_none()

    if not country:
        raise HTTPException(status_code=404, detail="Country not found")

    country.is_active = False
    await db.commit()

    return {"message": "Country deactivated"}
