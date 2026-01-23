from datetime import datetime
from pydantic import BaseModel
from typing import Any


class PageResponse(BaseModel):
    id: int
    slug: str
    language: str
    title: str
    content: str | None = None
    content_text: str | None = None
    url: str | None = None
    structured_data: dict[str, Any] | None = None
    updated_at: datetime

    model_config = {"from_attributes": True}


class PageListResponse(BaseModel):
    slug: str
    language: str
    title: str
    url: str | None = None

    model_config = {"from_attributes": True}
