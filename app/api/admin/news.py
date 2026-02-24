from datetime import datetime
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import desc, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.admin.deps import require_roles
from app.api.deps import get_db
from app.models import AdminUser, Language, News
from app.models.news import ArticleType
from app.schemas.admin.news import (
    AdminNewsArticleTypeUpdateRequest,
    AdminNewsClassifyRequest,
    AdminNewsClassifyResponse,
    AdminNewsClassifySummary,
    AdminNewsNeedsReviewItem,
    AdminNewsMaterialCreateRequest,
    AdminNewsMaterialListResponse,
    AdminNewsMaterialResponse,
    AdminNewsMaterialUpdateRequest,
    AdminNewsTranslationCreateRequest,
    AdminNewsTranslationPatchPayload,
    AdminNewsTranslationPayload,
    AdminNewsTranslationResponse,
)
from app.services.news_classifier import NewsClassifierService

router = APIRouter(prefix="/news", tags=["admin-news"])


def _lang_from_str(lang: str) -> Language:
    if lang == "ru":
        return Language.RU
    if lang == "kz":
        return Language.KZ
    raise HTTPException(status_code=400, detail="Language must be 'ru' or 'kz'")


def _article_type_from_str(
    value: str | None,
    *,
    allow_unclassified: bool = False,
) -> ArticleType | str | None:
    if value is None:
        return None
    normalized = value.strip().upper()
    if not normalized:
        return None
    if allow_unclassified and normalized == "UNCLASSIFIED":
        return "UNCLASSIFIED"
    try:
        return ArticleType(normalized)
    except ValueError as exc:
        if allow_unclassified:
            message = "article_type must be NEWS, ANALYTICS, or UNCLASSIFIED"
        else:
            message = "article_type must be NEWS or ANALYTICS"
        raise HTTPException(status_code=400, detail=message) from exc


async def _next_news_id(db: AsyncSession) -> int:
    result = await db.execute(select(News.id).order_by(News.id.desc()).limit(1))
    current = result.scalar_one_or_none()
    return (current or 0) + 1


def _apply_payload(
    item: News,
    payload: AdminNewsTranslationPayload | AdminNewsTranslationPatchPayload,
    admin_id: int,
    *,
    partial: bool = False,
) -> None:
    changed_fields = payload.model_fields_set if partial else None

    def should_update(field_name: str) -> bool:
        return not partial or field_name in (changed_fields or set())

    if should_update("title"):
        if payload.title is None:
            raise HTTPException(status_code=400, detail="title cannot be null")
        item.title = payload.title

    if should_update("is_slider"):
        if payload.is_slider is None:
            raise HTTPException(status_code=400, detail="is_slider cannot be null")
        item.is_slider = payload.is_slider

    if should_update("article_type"):
        parsed_article_type = _article_type_from_str(payload.article_type)
        if parsed_article_type == "UNCLASSIFIED":
            raise HTTPException(status_code=400, detail="article_type must be NEWS or ANALYTICS")
        item.article_type = parsed_article_type

    if should_update("excerpt"):
        item.excerpt = payload.excerpt
    if should_update("content"):
        item.content = payload.content
    if should_update("content_text"):
        item.content_text = payload.content_text
    if should_update("image_url"):
        item.image_url = payload.image_url
    if should_update("video_url"):
        item.video_url = payload.video_url
    if should_update("category"):
        item.category = payload.category
    if should_update("championship_code"):
        item.championship_code = payload.championship_code
    if should_update("slider_order"):
        item.slider_order = payload.slider_order
    if should_update("publish_date"):
        item.publish_date = payload.publish_date
    if should_update("source_url"):
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
        championship_code=item.championship_code,
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
    article_type: str | None = Query(None, description="NEWS, ANALYTICS, or UNCLASSIFIED"),
    search: str | None = Query(None, description="Search in title/excerpt/content"),
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "editor")),
):
    article_type_filter = _article_type_from_str(article_type, allow_unclassified=True)

    query = select(News).order_by(desc(News.updated_at), desc(News.id))
    if isinstance(article_type_filter, ArticleType):
        query = query.where(News.article_type == article_type_filter)
    elif article_type_filter == "UNCLASSIFIED":
        query = query.where(News.article_type.is_(None))

    if search and search.strip():
        search_term = f"%{search.strip()}%"
        query = query.where(
            or_(
                News.title.ilike(search_term),
                News.excerpt.ilike(search_term),
                News.content_text.ilike(search_term),
                News.content.ilike(search_term),
            )
        )

    result = await db.execute(query)
    matched_rows = result.scalars().all()
    if not matched_rows:
        return AdminNewsMaterialListResponse(items=[], total=0)

    group_ids = {row.translation_group_id for row in matched_rows}
    full_rows_result = await db.execute(
        select(News)
        .where(News.translation_group_id.in_(group_ids))
        .order_by(desc(News.updated_at), desc(News.id))
    )
    rows = full_rows_result.scalars().all()

    grouped: dict[UUID, list[News]] = {}
    for row in rows:
        grouped.setdefault(row.translation_group_id, []).append(row)

    materials = [_to_material_response(items) for items in grouped.values()]
    materials.sort(key=lambda m: m.updated_at or datetime.min, reverse=True)

    total = len(materials)
    start = (page - 1) * per_page
    end = start + per_page
    return AdminNewsMaterialListResponse(items=materials[start:end], total=total)


@router.post("/materials/classify", response_model=AdminNewsClassifyResponse)
async def classify_materials(
    payload: AdminNewsClassifyRequest,
    db: AsyncSession = Depends(get_db),
    current_admin: AdminUser = Depends(require_roles("superadmin", "editor")),
):
    if payload.min_confidence < 0 or payload.min_confidence > 1:
        raise HTTPException(status_code=400, detail="min_confidence must be between 0 and 1")

    result = await db.execute(select(News).order_by(desc(News.publish_date), desc(News.id)))
    rows = result.scalars().all()

    grouped: dict[UUID, list[News]] = {}
    for row in rows:
        grouped.setdefault(row.translation_group_id, []).append(row)

    groups = list(grouped.items())
    if payload.only_unclassified:
        groups = [(group_id, items) for group_id, items in groups if any(item.article_type is None for item in items)]

    if payload.championship_code:
        groups = [
            (group_id, items)
            for group_id, items in groups
            if any(item.championship_code == payload.championship_code for item in items)
        ]

    if payload.date_from or payload.date_to:
        filtered_groups: list[tuple[UUID, list[News]]] = []
        for group_id, items in groups:
            has_in_range = False
            for item in items:
                if item.publish_date is None:
                    continue
                if payload.date_from and item.publish_date < payload.date_from:
                    continue
                if payload.date_to and item.publish_date > payload.date_to:
                    continue
                has_in_range = True
                break
            if has_in_range:
                filtered_groups.append((group_id, items))
        groups = filtered_groups

    groups.sort(
        key=lambda pair: max((item.updated_at for item in pair[1]), default=datetime.min),
        reverse=True,
    )
    if payload.limit is not None:
        groups = groups[: payload.limit]

    classifier = NewsClassifierService()
    needs_review: list[AdminNewsNeedsReviewItem] = []
    updated_group_ids: list[UUID] = []
    classified_groups = 0

    for group_id, items in groups:
        decision = await classifier.classify_group(
            items,
            min_confidence=payload.min_confidence,
        )
        if decision.article_type is None:
            needs_review.append(
                AdminNewsNeedsReviewItem(
                    group_id=group_id,
                    representative_news_id=decision.representative_news_id,
                    representative_title=decision.representative_title,
                    confidence=decision.confidence,
                    source=decision.source,
                    reason=decision.reason,
                )
            )
            continue

        classified_groups += 1
        will_change = any(item.article_type != decision.article_type for item in items)
        if will_change:
            updated_group_ids.append(group_id)
            if payload.apply:
                for item in items:
                    item.article_type = decision.article_type
                    item.updated_by_admin_id = current_admin.id

    if payload.apply and updated_group_ids:
        await db.commit()

    summary = AdminNewsClassifySummary(
        dry_run=not payload.apply,
        total_groups=len(groups),
        classified_groups=classified_groups,
        updated_groups=len(updated_group_ids),
        unchanged_groups=max(len(groups) - len(updated_group_ids) - len(needs_review), 0),
        needs_review_count=len(needs_review),
    )

    return AdminNewsClassifyResponse(
        summary=summary,
        needs_review=needs_review,
        updated_group_ids=updated_group_ids,
    )


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
        _apply_payload(ru_item, payload.ru, current_admin.id, partial=True)

    if payload.kz is not None:
        kz_item = by_lang.get(Language.KZ)
        if not kz_item:
            raise HTTPException(status_code=400, detail="KZ translation is missing. Use add translation endpoint")
        _apply_payload(kz_item, payload.kz, current_admin.id, partial=True)

    await db.commit()

    refreshed = await db.execute(select(News).where(News.translation_group_id == group_id))
    return _to_material_response(refreshed.scalars().all())


@router.patch("/materials/{group_id}/article-type", response_model=AdminNewsMaterialResponse)
async def set_material_article_type(
    group_id: UUID,
    payload: AdminNewsArticleTypeUpdateRequest,
    db: AsyncSession = Depends(get_db),
    current_admin: AdminUser = Depends(require_roles("superadmin", "editor")),
):
    normalized = _article_type_from_str(payload.article_type)
    if normalized == "UNCLASSIFIED":
        raise HTTPException(status_code=400, detail="article_type must be NEWS or ANALYTICS")
    result = await db.execute(select(News).where(News.translation_group_id == group_id))
    rows = result.scalars().all()
    if not rows:
        raise HTTPException(status_code=404, detail="Material not found")

    for item in rows:
        item.article_type = normalized
        item.updated_by_admin_id = current_admin.id

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
