import datetime as dt
from typing import Optional
from pydantic import BaseModel


class AdminMediaVideoResponse(BaseModel):
    id: int
    title: str
    youtube_id: str
    sort_order: int = 0
    is_active: bool = True
    created_at: Optional[dt.datetime] = None
    updated_at: Optional[dt.datetime] = None

    model_config = {"from_attributes": True}


class AdminMediaVideoCreateRequest(BaseModel):
    title: str
    youtube_url: str
    sort_order: int = 0
    is_active: bool = True


class AdminMediaVideoUpdateRequest(BaseModel):
    title: Optional[str] = None
    youtube_url: Optional[str] = None
    sort_order: Optional[int] = None
    is_active: Optional[bool] = None


class AdminMediaVideosListResponse(BaseModel):
    items: list[AdminMediaVideoResponse]
    total: int
