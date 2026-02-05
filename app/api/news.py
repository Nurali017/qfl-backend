from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, func, desc, asc
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db
from app.models import News, Language
from app.schemas.news import NewsResponse, NewsListItem, NewsListResponse
from app.services.file_storage import FileStorageService
from app.utils.file_urls import get_file_data_with_url
from app.utils.error_messages import get_error_message

router = APIRouter(prefix="/news", tags=["news"])


@router.get("", response_model=NewsListResponse)
async def get_news_list(
    lang: str = Query("ru", pattern="^(kz|ru|en)$"),
    tournament_id: str | None = Query(None, description="Filter by tournament ID (pl, 1l, cup, 2l, el)"),
    article_type: str | None = Query(None, description="Filter by type: news or analytics"),
    page: int = Query(1, ge=1, description="Page number"),
    per_page: int = Query(20, ge=1, le=100, description="Items per page"),
    db: AsyncSession = Depends(get_db),
):
    """Get paginated news list."""
    lang_enum = Language.KZ if lang == "kz" else Language.RU

    # Base query
    query = select(News).where(News.language == lang_enum)

    # Filter by tournament_id
    if tournament_id:
        query = query.where(News.tournament_id == tournament_id)

    # Filter by article_type
    if article_type:
        from app.models.news import ArticleType
        if article_type.upper() == "NEWS":
            query = query.where(News.article_type == ArticleType.NEWS)
        elif article_type.upper() == "ANALYTICS":
            query = query.where(News.article_type == ArticleType.ANALYTICS)

    # Count total
    count_query = select(func.count()).select_from(query.subquery())
    total = (await db.execute(count_query)).scalar()

    # Paginate and order
    query = query.order_by(desc(News.publish_date), desc(News.id))
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
    lang: str = Query("ru", pattern="^(kz|ru|en)$"),
    db: AsyncSession = Depends(get_db),
):
    """Get count of articles by type."""
    lang_enum = Language.KZ if lang == "kz" else Language.RU

    result = await db.execute(
        select(News.article_type, func.count(News.id))
        .where(News.language == lang_enum)
        .group_by(News.article_type)
    )

    counts = {row[0].value if row[0] else "unclassified": row[1] for row in result.fetchall()}
    return counts


@router.get("/latest", response_model=list[NewsListItem])
async def get_latest_news(
    lang: str = Query("ru", pattern="^(kz|ru|en)$"),
    tournament_id: str | None = Query(None, description="Filter by tournament ID (pl, 1l, cup, 2l, el)"),
    limit: int = Query(10, ge=1, le=50, description="Number of news items"),
    db: AsyncSession = Depends(get_db),
):
    """Get latest news."""
    lang_enum = Language.KZ if lang == "kz" else Language.RU

    query = select(News).where(News.language == lang_enum)

    # Filter by tournament_id
    if tournament_id:
        query = query.where(News.tournament_id == tournament_id)

    query = query.order_by(desc(News.publish_date), desc(News.id)).limit(limit)

    result = await db.execute(query)
    return result.scalars().all()


@router.get("/slider", response_model=list[NewsListItem])
async def get_slider_news(
    lang: str = Query("ru", pattern="^(kz|ru|en)$"),
    tournament_id: str | None = Query(None, description="Filter by tournament ID (pl, 1l, cup, 2l, el)"),
    db: AsyncSession = Depends(get_db),
):
    """Get news for slider."""
    lang_enum = Language.KZ if lang == "kz" else Language.RU

    query = select(News).where(News.language == lang_enum, News.is_slider == True)

    # Filter by tournament_id
    if tournament_id:
        query = query.where(News.tournament_id == tournament_id)

    query = query.order_by(asc(News.slider_order), desc(News.publish_date))

    result = await db.execute(query)
    return result.scalars().all()


@router.get("/{news_id}")
async def get_news_item(
    news_id: int,
    lang: str = Query("ru", pattern="^(kz|ru|en)$"),
    db: AsyncSession = Depends(get_db),
):
    """Get single news article by ID with images from MinIO."""
    lang_enum = Language.KZ if lang == "kz" else Language.RU
    result = await db.execute(
        select(News).where(News.id == news_id, News.language == lang_enum)
    )
    news = result.scalar_one_or_none()
    if not news:
        raise HTTPException(status_code=404, detail=get_error_message("news_not_found", lang))

    # Get images from MinIO
    images = await FileStorageService.get_files_by_news_id(str(news_id))

    response = NewsResponse.model_validate(news).model_dump()
    response["images"] = [get_file_data_with_url(img) for img in images]
    return response
