from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db
from app.models import Page, Language
from app.schemas.page import PageResponse, PageListResponse
from app.services.file_storage import FileStorageService
from app.utils.file_urls import get_file_data_with_url, resolve_file_url, to_object_name
from app.utils.error_messages import get_error_message

router = APIRouter(prefix="/pages", tags=["pages"])


@router.get("", response_model=list[PageListResponse])
async def get_pages(
    lang: str = Query("ru", pattern="^(kz|ru|en)$"),
    db: AsyncSession = Depends(get_db),
):
    """Get all pages for a language."""
    lang_enum = Language.KZ if lang == "kz" else Language.RU
    result = await db.execute(
        select(Page).where(Page.language == lang_enum).order_by(Page.slug)
    )
    return result.scalars().all()


@router.get("/{slug}", response_model=PageResponse)
async def get_page(
    slug: str,
    lang: str = Query("ru", pattern="^(kz|ru|en)$"),
    db: AsyncSession = Depends(get_db),
):
    """Get page by slug."""
    lang_enum = Language.KZ if lang == "kz" else Language.RU
    result = await db.execute(
        select(Page).where(Page.slug == slug, Page.language == lang_enum)
    )
    page = result.scalar_one_or_none()
    if not page:
        raise HTTPException(status_code=404, detail=get_error_message("page_not_found", lang))
    return page


@router.get("/contacts/{language}", response_model=PageResponse)
async def get_contacts(
    language: str,
    db: AsyncSession = Depends(get_db),
):
    """Get contacts page."""
    lang = Language.KZ if language == "kz" else Language.RU
    # Try both possible slugs
    slugs = ["baylanystar", "kontakty"] if lang == Language.KZ else ["kontakty", "baylanystar"]
    for slug in slugs:
        result = await db.execute(
            select(Page).where(Page.slug == slug, Page.language == lang)
        )
        page = result.scalar_one_or_none()
        if page:
            return page
    raise HTTPException(status_code=404, detail="Contacts page not found")


@router.get("/documents/{language}")
async def get_documents(
    language: str,
    db: AsyncSession = Depends(get_db),
):
    """Get documents page with PDF files from MinIO."""
    lang = Language.KZ if language == "kz" else Language.RU
    slugs = ["kuzhattar", "dokumenty"] if lang == Language.KZ else ["dokumenty", "kuzhattar"]
    for slug in slugs:
        result = await db.execute(
            select(Page).where(Page.slug == slug, Page.language == lang)
        )
        page = result.scalar_one_or_none()
        if page:
            # Get document files from MinIO
            files = await FileStorageService.list_files(category="document", limit=100)
            # Filter by language
            lang_files = [f for f in files if f.get("language") == language.upper()]

            response = PageResponse.model_validate(page).model_dump()
            response["files"] = [get_file_data_with_url(f) for f in lang_files]
            return response
    raise HTTPException(status_code=404, detail="Documents page not found")


@router.get("/leadership/{language}")
async def get_leadership(
    language: str,
    db: AsyncSession = Depends(get_db),
):
    """Get leadership page with photos from MinIO."""
    lang = Language.KZ if language == "kz" else Language.RU
    slugs = ["basshylyk", "rukovodstvo"] if lang == Language.KZ else ["rukovodstvo", "basshylyk"]
    for slug in slugs:
        result = await db.execute(
            select(Page).where(Page.slug == slug, Page.language == lang)
        )
        page = result.scalar_one_or_none()
        if page:
            # Get leadership photos from MinIO
            photos = await FileStorageService.list_files(category="leadership", limit=100)

            response = PageResponse.model_validate(page).model_dump()
            response["photos"] = [get_file_data_with_url(p) for p in photos]

            # Resolve photo URLs inside structured_data.members
            members = (response.get("structured_data") or {}).get("members")
            if members:
                for member in members:
                    photo = member.get("photo")
                    if photo:
                        member["photo"] = resolve_file_url(to_object_name(photo))

            return response
    raise HTTPException(status_code=404, detail="Leadership page not found")
