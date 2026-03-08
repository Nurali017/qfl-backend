from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.api.deps import get_db
from app.models.media_video import MediaVideo
from app.schemas.media_video import MediaVideoResponse, MediaVideoListResponse

router = APIRouter(prefix="/media-videos", tags=["media-videos"])


@router.get("", response_model=MediaVideoListResponse)
async def get_media_videos(
    limit: int = Query(default=10, le=50),
    db: AsyncSession = Depends(get_db),
):
    """Get active media videos ordered by sort_order."""
    query = (
        select(MediaVideo)
        .where(MediaVideo.is_active == True)
        .order_by(MediaVideo.sort_order, MediaVideo.id)
        .limit(limit)
    )
    result = await db.execute(query)
    videos = result.scalars().all()

    items = [MediaVideoResponse.model_validate(v) for v in videos]
    return MediaVideoListResponse(items=items, total=len(items))
