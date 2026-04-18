from pydantic import BaseModel


class MediaVideoResponse(BaseModel):
    id: int
    title: str
    youtube_id: str
    sort_order: int = 0
    view_count: int | None = None

    model_config = {"from_attributes": True}


class MediaVideoListResponse(BaseModel):
    items: list[MediaVideoResponse]
    total: int
