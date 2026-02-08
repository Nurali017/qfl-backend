from datetime import datetime
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.admin.deps import require_roles
from app.api.deps import get_db
from app.models import AdminUser, Language, News
from app.models.news import ArticleType
from app.schemas.admin.news import (
    AdminNewsMaterialCreateRequest,
    AdminNewsMaterialListResponse,
    AdminNewsMaterialResponse,
    AdminNewsMaterialUpdateRequest,
    AdminNewsTranslationCreateRequest,
    AdminNewsTranslationPayload,
    AdminNewsTranslationResponse,
)

router = APIRouter(prefix="/news", tags=["admin-news"])


def _lang_from_str(lang: str) -> Language:
    if lang == "ru":
        return Language.RU
    if lang == "kz":
        return Language.KZ
    raise HTTPException(status_code=400, detail="Language must be 'ru' or 'kz'")


def _article_type_from_str(value: str | None) -> ArticleType | None:
    if value is None:
        return None
    normalized = value.strip().upper()
    if not normalized:
        return None
    try:
        return ArticleType(normalized)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="article_type must be NEWS or ANALYTICS") from exc


async def _next_news_id(db: AsyncSession) -> int:
    result = await db.execute(select(News.id).order_by(News.id.desc()).limit(1))
    current = result.scalar_one_or_none()
    return (current or 0) + 1


def _apply_payload(item: News, payload: AdminNewsTranslationPayload, admin_id: int) -> None:
    item.title = payload.title
    item.excerpt = payload.excerpt
    item.content = payload.content
    item.content_text = payload.content_text
    item.image_url = payload.image_url
    item.video_url = payload.video_url
    item.category = payload.category
    item.tournament_id = payload.tournament_id
    item.article_type = _article_type_from_str(payload.article_type)
    item.is_slider = payload.is_slider
    item.slider_order = payload.slider_order
    item.publish_date = payload.publish_date
    item.source_url = payload.source_url
    item.updated_by_admin_id = admin_id


def _to_translation_response(item: News) -> AdminNewsTranslationResponse:
    return AdminNewsTranslationResponse(
        id=item.id,
        language=item.language.value,
        title=item.title,
        excerpt=item.excerpt,
        content=item.content,
        content_text=item.content_text,
        image_url=item.image_url,
        video_url=item.video_url,
        category=item.category,
        tournament_id=item.tournament_id,
        article_type=item.article_type.value if item.article_type else None,
        is_slider=item.is_slider,
        slider_order=item.slider_order,
        publish_date=item.publish_date,
        source_id=item.source_id,
        source_url=item.source_url,
        updated_at=item.updated_at,
    )


def _to_material_response(items: list[News]) -> AdminNewsMaterialResponse:
    ru = next((item for item in items if item.language == Language.RU), None)
    kz = next((item for item in items if item.language == Language.KZ), None)
    updated_at = max((item.updated_at for item in items), default=None)
    return AdminNewsMaterialResponse(
        group_id=items[0].translation_group_id,
        ru=_to_translation_response(ru) if ru else None,
        kz=_to_translation_response(kz) if kz else None,
        updated_at=updated_at,
    )


@router.get("/materials", response_model=AdminNewsMaterialListResponse)
async def list_materials(
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "editor")),
):
    result = await db.execute(select(News).order_by(desc(News.updated_at), desc(News.id)))
    rows = result.scalars().all()

    grouped: dict[UUID, list[News]] = {}
    for row in rows:
        grouped.setdefault(row.translation_group_id, []).append(row)

    materials = [_to_material_response(items) for items in grouped.values()]
    materials.sort(key=lambda m: m.updated_at or datetime.min, reverse=True)

    total = len(materials)
    start = (page - 1) * per_page
    end = start + per_page
    return AdminNewsMaterialListResponse(items=materials[start:end], total=total)


@router.get("/materials/{group_id}", response_model=AdminNewsMaterialResponse)
async def get_material(
    group_id: UUID,
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "editor")),
):
    result = await db.execute(select(News).where(News.translation_group_id == group_id).order_by(News.language.asc()))
    rows = result.scalars().all()
    if not rows:
        raise HTTPException(status_code=404, detail="Material not found")
    return _to_material_response(rows)


@router.post("/materials", response_model=AdminNewsMaterialResponse, status_code=status.HTTP_201_CREATED)
async def create_material(
    payload: AdminNewsMaterialCreateRequest,
    db: AsyncSession = Depends(get_db),
    current_admin: AdminUser = Depends(require_roles("superadmin", "editor")),
):
    group_id = uuid4()
    base_id = await _next_news_id(db)
    ru_item = News(
        id=base_id,
        language=Language.RU,
        translation_group_id=group_id,
        created_by_admin_id=current_admin.id,
    )
    kz_item = News(
        id=base_id + 1,
        language=Language.KZ,
        translation_group_id=group_id,
        created_by_admin_id=current_admin.id,
    )

    _apply_payload(ru_item, payload.ru, current_admin.id)
    _apply_payload(kz_item, payload.kz, current_admin.id)

    db.add_all([ru_item, kz_item])
    await db.commit()
    await db.refresh(ru_item)
    await db.refresh(kz_item)

    return _to_material_response([ru_item, kz_item])


@router.patch("/materials/{group_id}", response_model=AdminNewsMaterialResponse)
async def update_material(
    group_id: UUID,
    payload: AdminNewsMaterialUpdateRequest,
    db: AsyncSession = Depends(get_db),
    current_admin: AdminUser = Depends(require_roles("superadmin", "editor")),
):
    if payload.ru is None and payload.kz is None:
        raise HTTPException(status_code=400, detail="At least one translation payload is required")

    result = await db.execute(select(News).where(News.translation_group_id == group_id))
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

    refreshed = await db.execute(select(News).where(News.translation_group_id == group_id))
    return _to_material_response(refreshed.scalars().all())


@router.post("/materials/{group_id}/translation/{lang}", response_model=AdminNewsMaterialResponse)
async def create_missing_translation(
    group_id: UUID,
    lang: str,
    payload: AdminNewsTranslationCreateRequest,
    db: AsyncSession = Depends(get_db),
    current_admin: AdminUser = Depends(require_roles("superadmin", "editor")),
):
    lang_enum = _lang_from_str(lang)

    result = await db.execute(select(News).where(News.translation_group_id == group_id))
    rows = result.scalars().all()
    if not rows:
        raise HTTPException(status_code=404, detail="Material not found")

    if any(item.language == lang_enum for item in rows):
        raise HTTPException(status_code=409, detail="Translation already exists")

    item = News(
        id=await _next_news_id(db),
        language=lang_enum,
        translation_group_id=group_id,
        created_by_admin_id=current_admin.id,
    )
    _apply_payload(item, payload.data, current_admin.id)

    db.add(item)
    await db.commit()

    refreshed = await db.execute(select(News).where(News.translation_group_id == group_id))
    return _to_material_response(refreshed.scalars().all())


@router.delete("/materials/{group_id}")
async def delete_material(
    group_id: UUID,
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "editor")),
):
    result = await db.execute(select(News).where(News.translation_group_id == group_id))
    rows = result.scalars().all()
    if not rows:
        raise HTTPException(status_code=404, detail="Material not found")

    for row in rows:
        await db.delete(row)

    await db.commit()
    return {"message": "Material deleted"}
