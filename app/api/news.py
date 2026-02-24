from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy import select, func, desc, asc, update, delete, or_
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db
from app.models import News, Language
from app.models.news import ArticleType, NewsLike
from app.schemas.news import NewsResponse, NewsListItem, NewsListResponse, NewsReactionsResponse
from app.services.file_storage import FileStorageService
from app.utils.file_urls import get_file_data_with_url
from app.utils.error_messages import get_error_message
from app.utils.news_html_normalizer import normalize_news_html_content, normalize_news_media_url


def get_client_ip(request: Request) -> str:
    return (
        request.headers.get("X-Real-IP")
        or request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
        or (request.client.host if request.client else "unknown")
    )

router = APIRouter(prefix="/news", tags=["news"])


def _resolve_language(lang: str) -> Language:
    if lang == "kz":
        return Language.KZ
    return Language.RU


def _article_type_from_query(value: str | None) -> ArticleType | None:
    if value is None:
        return None
    normalized = value.strip().upper()
    if not normalized:
        return None
    if normalized == "NEWS":
        return ArticleType.NEWS
    if normalized == "ANALYTICS":
        return ArticleType.ANALYTICS
    raise HTTPException(status_code=400, detail="article_type must be news or analytics")


def _news_order_by(sort: str):
    if sort == "date_asc":
        return [asc(News.publish_date), asc(News.id)]
    if sort == "views_desc":
        return [desc(News.views_count), desc(News.publish_date), desc(News.id)]
    if sort == "likes_desc":
        return [desc(News.likes_count), desc(News.publish_date), desc(News.id)]
    return [desc(News.publish_date), desc(News.id)]


@router.get("", response_model=NewsListResponse)
async def get_news_list(
    lang: str = Query("kz", pattern="^(kz|ru|en)$"),
    championship_code: str | None = Query(None, description="Filter by championship code (pl, 1l, cup, 2l, el)"),
    article_type: str | None = Query(None, description="Filter by type: news or analytics"),
    search: str | None = Query(None, description="Search in title/excerpt/content"),
    sort: str = Query(
        "date_desc",
        pattern="^(date_desc|date_asc|views_desc|likes_desc)$",
        description="Sorting mode",
    ),
    date_from: date | None = Query(None, description="Filter from publish date (inclusive)"),
    date_to: date | None = Query(None, description="Filter to publish date (inclusive)"),
    page: int = Query(1, ge=1, description="Page number"),
    per_page: int = Query(20, ge=1, le=100, description="Items per page"),
    db: AsyncSession = Depends(get_db),
):
    """Get paginated news list."""
    lang_enum = _resolve_language(lang)
    article_type_enum = _article_type_from_query(article_type)

    # Base query
    query = select(News).where(News.language == lang_enum)

    # Filter by championship_code
    if championship_code:
        query = query.where(News.championship_code == championship_code)

    # Filter by article_type
    if article_type_enum is not None:
        query = query.where(News.article_type == article_type_enum)

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

    if date_from:
        query = query.where(News.publish_date >= date_from)
    if date_to:
        query = query.where(News.publish_date <= date_to)

    # Count total
    count_query = select(func.count()).select_from(query.subquery())
    total = (await db.execute(count_query)).scalar()

    # Paginate and order
    query = query.order_by(*_news_order_by(sort))
    query = query.offset((page - 1) * per_page).limit(per_page)

    result = await db.execute(query)
    items = result.scalars().all()

    total_count = total or 0
    return NewsListResponse(
        items=[NewsListItem.model_validate(item) for item in items],
        total=total_count,
        page=page,
        per_page=per_page,
        pages=(total_count + per_page - 1) // per_page if total_count > 0 else 0,
    )


@router.get("/article-types", response_model=dict[str, int])
async def get_article_types(
    lang: str = Query("kz", pattern="^(kz|ru|en)$"),
    db: AsyncSession = Depends(get_db),
):
    """Get count of articles by type."""
    lang_enum = _resolve_language(lang)

    result = await db.execute(
        select(News.article_type, func.count(News.id))
        .where(News.language == lang_enum)
        .group_by(News.article_type)
    )

    counts = {row[0].value if row[0] else "unclassified": row[1] for row in result.fetchall()}
    return counts


@router.get("/latest", response_model=list[NewsListItem])
async def get_latest_news(
    lang: str = Query("kz", pattern="^(kz|ru|en)$"),
    championship_code: str | None = Query(None, description="Filter by championship code (pl, 1l, cup, 2l, el)"),
    limit: int = Query(10, ge=1, le=50, description="Number of news items"),
    db: AsyncSession = Depends(get_db),
):
    """Get latest news."""
    lang_enum = _resolve_language(lang)

    query = select(News).where(News.language == lang_enum)

    # Filter by championship_code
    if championship_code:
        query = query.where(News.championship_code == championship_code)

    query = query.order_by(desc(News.publish_date), desc(News.id)).limit(limit)

    result = await db.execute(query)
    return result.scalars().all()


@router.get("/slider", response_model=list[NewsListItem])
async def get_slider_news(
    lang: str = Query("kz", pattern="^(kz|ru|en)$"),
    championship_code: str | None = Query(None, description="Filter by championship code (pl, 1l, cup, 2l, el)"),
    db: AsyncSession = Depends(get_db),
):
    """Get news for slider."""
    lang_enum = _resolve_language(lang)

    query = select(News).where(News.language == lang_enum, News.is_slider == True)

    # Filter by championship_code
    if championship_code:
        query = query.where(News.championship_code == championship_code)

    query = query.order_by(asc(News.slider_order), desc(News.publish_date))

    result = await db.execute(query)
    return result.scalars().all()


@router.get("/{news_id}/navigation")
async def get_news_navigation(
    news_id: int,
    lang: str = Query("kz", pattern="^(kz|ru|en)$"),
    db: AsyncSession = Depends(get_db),
):
    """Get previous and next news articles for navigation."""
    lang_enum = _resolve_language(lang)

    # Get current article
    current = (await db.execute(
        select(News.publish_date, News.id).where(News.id == news_id, News.language == lang_enum)
    )).first()

    if not current:
        raise HTTPException(status_code=404, detail=get_error_message("news_not_found", lang))

    result = {}

    # Previous article (older: publish_date < current OR same date with smaller id)
    prev_query = (
        select(News.id, News.title)
        .where(News.language == lang_enum)
        .where(
            (News.publish_date < current.publish_date)
            | ((News.publish_date == current.publish_date) & (News.id < current.id))
        )
        .order_by(desc(News.publish_date), desc(News.id))
        .limit(1)
    )
    prev_row = (await db.execute(prev_query)).first()
    if prev_row:
        result["previous"] = {"id": prev_row.id, "title": prev_row.title}

    # Next article (newer: publish_date > current OR same date with larger id)
    next_query = (
        select(News.id, News.title)
        .where(News.language == lang_enum)
        .where(
            (News.publish_date > current.publish_date)
            | ((News.publish_date == current.publish_date) & (News.id > current.id))
        )
        .order_by(asc(News.publish_date), asc(News.id))
        .limit(1)
    )
    next_row = (await db.execute(next_query)).first()
    if next_row:
        result["next"] = {"id": next_row.id, "title": next_row.title}

    return result


@router.post("/{news_id}/view")
async def record_news_view(
    news_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Increment view counter for a news article."""
    result = await db.execute(
        update(News)
        .where(News.id == news_id)
        .values(views_count=News.views_count + 1)
    )
    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="News not found")
    await db.commit()
    return {"ok": True}


@router.post("/{news_id}/like")
async def toggle_news_like(
    news_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Toggle like for a news article (IP-based)."""
    # Check news exists (PK is composite (id, language), so use .first())
    news = (await db.execute(
        select(News).where(News.id == news_id).limit(1)
    )).scalar_one_or_none()
    if not news:
        raise HTTPException(status_code=404, detail="News not found")

    client_ip = get_client_ip(request)

    # Check if already liked
    existing = (await db.execute(
        select(NewsLike).where(NewsLike.news_id == news_id, NewsLike.client_ip == client_ip)
    )).scalar_one_or_none()

    if existing:
        # Unlike
        await db.execute(delete(NewsLike).where(NewsLike.id == existing.id))
        await db.execute(
            update(News)
            .where(News.id == news_id)
            .values(likes_count=func.greatest(News.likes_count - 1, 0))
        )
        await db.commit()
        # Re-fetch to get updated count
        news = (await db.execute(
            select(News).where(News.id == news_id).limit(1)
        )).scalar_one()
        return {"likes": news.likes_count, "liked": False}
    else:
        # Like
        db.add(NewsLike(news_id=news_id, client_ip=client_ip))
        await db.execute(
            update(News)
            .where(News.id == news_id)
            .values(likes_count=News.likes_count + 1)
        )
        await db.commit()
        # Re-fetch to get updated count
        news = (await db.execute(
            select(News).where(News.id == news_id).limit(1)
        )).scalar_one()
        return {"likes": news.likes_count, "liked": True}


@router.get("/{news_id}/reactions", response_model=NewsReactionsResponse)
async def get_news_reactions(
    news_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Get view/like counts and whether current IP has liked."""
    news = (await db.execute(
        select(News).where(News.id == news_id).limit(1)
    )).scalar_one_or_none()
    if not news:
        raise HTTPException(status_code=404, detail="News not found")

    client_ip = get_client_ip(request)
    liked = (await db.execute(
        select(NewsLike.id).where(NewsLike.news_id == news_id, NewsLike.client_ip == client_ip)
    )).scalar_one_or_none() is not None

    return NewsReactionsResponse(views=news.views_count, likes=news.likes_count, liked=liked)


@router.get("/{news_id}", response_model=NewsResponse)
async def get_news_item(
    news_id: int,
    lang: str = Query("kz", pattern="^(kz|ru|en)$"),
    db: AsyncSession = Depends(get_db),
):
    """Get single news article by ID with images from MinIO."""
    lang_enum = _resolve_language(lang)
    result = await db.execute(
        select(News).where(News.id == news_id, News.language == lang_enum)
    )
    news = result.scalar_one_or_none()
    if not news:
        raise HTTPException(status_code=404, detail=get_error_message("news_not_found", lang))

    # Get images from MinIO
    images = await FileStorageService.get_files_by_news_id(str(news_id))

    response = NewsResponse.model_validate(news).model_dump()
    response["image_url"] = normalize_news_media_url(
        response.get("image_url"),
        source_url=response.get("source_url"),
    )

    normalized_content = await normalize_news_html_content(
        response.get("content"),
        source_url=response.get("source_url"),
        db=db,
    )
    response["content"] = normalized_content.content

    normalized_images: list[dict] = []
    for img in images:
        image_data = get_file_data_with_url(img)
        image_data["url"] = normalize_news_media_url(
            image_data.get("url"),
            source_url=response.get("source_url"),
        )
        normalized_images.append(image_data)

    response["images"] = normalized_images
    return response
