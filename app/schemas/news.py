from datetime import datetime, date
from pydantic import BaseModel
from typing import Any


class NewsResponse(BaseModel):
    id: int
    source_id: int | None = None  # Original ID from kffleague.kz
    source_url: str | None = None  # Original URL from kffleague.kz
    language: str
    title: str
    excerpt: str | None = None
    content: str | None = None
    content_text: str | None = None
    image_url: str | None = None
    video_url: str | None = None  # YouTube embed URL
    category: str | None = None
    article_type: str | None = None
    is_slider: bool = False
    slider_order: int | None = None
    publish_date: date | None = None
    structured_data: dict[str, Any] | None = None
    updated_at: datetime

    model_config = {"from_attributes": True}


class NewsListItem(BaseModel):
    id: int
    language: str
    title: str
    excerpt: str | None = None
    image_url: str | None = None
    category: str | None = None
    article_type: str | None = None
    is_slider: bool = False
    slider_order: int | None = None
    publish_date: date | None = None

    model_config = {"from_attributes": True}


class NewsListResponse(BaseModel):
    items: list[NewsListItem]
    total: int
    page: int
    per_page: int
    pages: int
