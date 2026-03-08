from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db
from app.api.admin.deps import require_roles
from app.models.media_video import MediaVideo
from app.schemas.admin.media_videos import (
    AdminMediaVideoResponse,
    AdminMediaVideoCreateRequest,
    AdminMediaVideoUpdateRequest,
    AdminMediaVideosListResponse,
)
from app.utils.timestamps import utcnow
from app.utils.youtube import extract_youtube_id

router = APIRouter(
    prefix="/media-videos",
    tags=["admin-media-videos"],
    dependencies=[Depends(require_roles("superadmin", "editor"))],
)


def _to_response(v: MediaVideo) -> AdminMediaVideoResponse:
    return AdminMediaVideoResponse(
        id=v.id,
        title=v.title,
        youtube_id=v.youtube_id,
        sort_order=v.sort_order,
        is_active=v.is_active,
        created_at=v.created_at,
        updated_at=v.updated_at,
    )


@router.get("", response_model=AdminMediaVideosListResponse)
async def list_media_videos(db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(MediaVideo).order_by(MediaVideo.sort_order, MediaVideo.id)
    )
    items = result.scalars().all()
    return AdminMediaVideosListResponse(
        items=[_to_response(v) for v in items],
        total=len(items),
    )


@router.post("", response_model=AdminMediaVideoResponse, status_code=201)
async def create_media_video(
    body: AdminMediaVideoCreateRequest,
    db: AsyncSession = Depends(get_db),
):
    youtube_id = extract_youtube_id(body.youtube_url)
    if not youtube_id:
        raise HTTPException(status_code=422, detail="Invalid YouTube URL")

    now = utcnow()
    video = MediaVideo(
        title=body.title,
        youtube_id=youtube_id,
        sort_order=body.sort_order,
        is_active=body.is_active,
        created_at=now,
        updated_at=now,
    )
    db.add(video)
    await db.commit()
    await db.refresh(video)
    return _to_response(video)


@router.patch("/{video_id}", response_model=AdminMediaVideoResponse)
async def update_media_video(
    video_id: int,
    body: AdminMediaVideoUpdateRequest,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(MediaVideo).where(MediaVideo.id == video_id))
    video = result.scalar_one_or_none()
    if not video:
        raise HTTPException(status_code=404, detail="Media video not found")

    update_data = body.model_dump(exclude_unset=True)

    if "youtube_url" in update_data:
        youtube_id = extract_youtube_id(update_data.pop("youtube_url"))
        if not youtube_id:
            raise HTTPException(status_code=422, detail="Invalid YouTube URL")
        video.youtube_id = youtube_id

    for field, value in update_data.items():
        setattr(video, field, value)
    video.updated_at = utcnow()

    await db.commit()
    await db.refresh(video)
    return _to_response(video)


@router.delete("/{video_id}", status_code=204)
async def delete_media_video(
    video_id: int,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(MediaVideo).where(MediaVideo.id == video_id))
    video = result.scalar_one_or_none()
    if not video:
        raise HTTPException(status_code=404, detail="Media video not found")
    await db.delete(video)
    await db.commit()
