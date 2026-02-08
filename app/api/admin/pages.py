from datetime import datetime
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.admin.deps import require_roles
from app.api.deps import get_db
from app.models import AdminUser, Language, Page
from app.schemas.admin.pages import (
    AdminPageMaterialListResponse,
    AdminPageMaterialResponse,
    AdminPageMaterialUpdateRequest,
    AdminPageTranslationCreateRequest,
    AdminPageTranslationPayload,
    AdminPageTranslationResponse,
)

router = APIRouter(prefix="/pages", tags=["admin-pages"])


def _lang_from_str(lang: str) -> Language:
    if lang == "ru":
        return Language.RU
    if lang == "kz":
        return Language.KZ
    raise HTTPException(status_code=400, detail="Language must be 'ru' or 'kz'")


def _apply_payload(item: Page, payload: AdminPageTranslationPayload, admin_id: int) -> None:
    item.slug = payload.slug
    item.title = payload.title
    item.content = payload.content
    item.content_text = payload.content_text
    item.url = payload.url
    item.updated_by_admin_id = admin_id


def _to_translation_response(item: Page) -> AdminPageTranslationResponse:
    return AdminPageTranslationResponse(
        id=item.id,
        language=item.language.value,
        slug=item.slug,
        title=item.title,
        content=item.content,
        content_text=item.content_text,
        url=item.url,
        updated_at=item.updated_at,
    )


def _to_material_response(items: list[Page]) -> AdminPageMaterialResponse:
    ru = next((item for item in items if item.language == Language.RU), None)
    kz = next((item for item in items if item.language == Language.KZ), None)
    updated_at = max((item.updated_at for item in items), default=None)
    return AdminPageMaterialResponse(
        group_id=items[0].translation_group_id,
        ru=_to_translation_response(ru) if ru else None,
        kz=_to_translation_response(kz) if kz else None,
        updated_at=updated_at,
    )


@router.get("/materials", response_model=AdminPageMaterialListResponse)
async def list_materials(
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "editor")),
):
    result = await db.execute(select(Page).order_by(desc(Page.updated_at), Page.id.asc()))
    rows = result.scalars().all()

    grouped: dict[UUID, list[Page]] = {}
    for row in rows:
        grouped.setdefault(row.translation_group_id, []).append(row)

    materials = [_to_material_response(items) for items in grouped.values()]
    materials.sort(key=lambda m: m.updated_at or datetime.min, reverse=True)

    total = len(materials)
    start = (page - 1) * per_page
    end = start + per_page
    return AdminPageMaterialListResponse(items=materials[start:end], total=total)


@router.get("/materials/{group_id}", response_model=AdminPageMaterialResponse)
async def get_material(
    group_id: UUID,
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "editor")),
):
    result = await db.execute(select(Page).where(Page.translation_group_id == group_id))
    rows = result.scalars().all()
    if not rows:
        raise HTTPException(status_code=404, detail="Material not found")
    return _to_material_response(rows)


@router.post("/materials", response_model=AdminPageMaterialResponse, status_code=status.HTTP_201_CREATED)
async def create_material(
    payload: AdminPageMaterialUpdateRequest,
    db: AsyncSession = Depends(get_db),
    current_admin: AdminUser = Depends(require_roles("superadmin", "editor")),
):
    if payload.ru is None or payload.kz is None:
        raise HTTPException(status_code=400, detail="Both RU and KZ translations are required")

    group_id = uuid4()
    ru_item = Page(language=Language.RU, translation_group_id=group_id, created_by_admin_id=current_admin.id)
    kz_item = Page(language=Language.KZ, translation_group_id=group_id, created_by_admin_id=current_admin.id)

    _apply_payload(ru_item, payload.ru, current_admin.id)
    _apply_payload(kz_item, payload.kz, current_admin.id)

    db.add_all([ru_item, kz_item])
    await db.commit()
    await db.refresh(ru_item)
    await db.refresh(kz_item)

    return _to_material_response([ru_item, kz_item])


@router.put("/materials/{group_id}", response_model=AdminPageMaterialResponse)
async def update_material(
    group_id: UUID,
    payload: AdminPageMaterialUpdateRequest,
    db: AsyncSession = Depends(get_db),
    current_admin: AdminUser = Depends(require_roles("superadmin", "editor")),
):
    if payload.ru is None and payload.kz is None:
        raise HTTPException(status_code=400, detail="At least one translation payload is required")

    result = await db.execute(select(Page).where(Page.translation_group_id == group_id))
    rows = result.scalars().all()
    if not rows:
        raise HTTPException(status_code=404, detail="Material not found")

    by_lang = {row.language: row for row in rows}

    if payload.ru is not None:
        ru_item = by_lang.get(Language.RU)
        if not ru_item:
            raise HTTPException(status_code=400, detail="RU translation is missing. Use add translation endpoint")
        _apply_payload(ru_item, payload.ru, current_admin.id)

    if payload.kz is not None:
        kz_item = by_lang.get(Language.KZ)
        if not kz_item:
            raise HTTPException(status_code=400, detail="KZ translation is missing. Use add translation endpoint")
        _apply_payload(kz_item, payload.kz, current_admin.id)

    await db.commit()

    refreshed = await db.execute(select(Page).where(Page.translation_group_id == group_id))
    return _to_material_response(refreshed.scalars().all())


@router.post("/materials/{group_id}/translation/{lang}", response_model=AdminPageMaterialResponse)
async def create_missing_translation(
    group_id: UUID,
    lang: str,
    payload: AdminPageTranslationCreateRequest,
    db: AsyncSession = Depends(get_db),
    current_admin: AdminUser = Depends(require_roles("superadmin", "editor")),
):
    lang_enum = _lang_from_str(lang)

    result = await db.execute(select(Page).where(Page.translation_group_id == group_id))
    rows = result.scalars().all()
    if not rows:
        raise HTTPException(status_code=404, detail="Material not found")

    if any(item.language == lang_enum for item in rows):
        raise HTTPException(status_code=409, detail="Translation already exists")

    item = Page(
        language=lang_enum,
        translation_group_id=group_id,
        created_by_admin_id=current_admin.id,
    )
    _apply_payload(item, payload.data, current_admin.id)

    db.add(item)
    await db.commit()

    refreshed = await db.execute(select(Page).where(Page.translation_group_id == group_id))
    return _to_material_response(refreshed.scalars().all())
