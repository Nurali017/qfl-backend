from datetime import date, datetime
from uuid import UUID

from pydantic import BaseModel

from app.schemas.admin.common import LanguageCode
from app.utils.file_urls import FileUrl


class AdminNewsTranslationPayload(BaseModel):
    title: str
    excerpt: str | None = None
    content: str | None = None
    content_text: str | None = None
    image_url: str | None = None
    video_url: str | None = None
    category: str | None = None
    tournament_id: str | None = None
    article_type: str | None = None
    is_slider: bool = False
    slider_order: int | None = None
    publish_date: date | None = None
    source_url: str | None = None


class AdminNewsMaterialCreateRequest(BaseModel):
    ru: AdminNewsTranslationPayload
    kz: AdminNewsTranslationPayload


class AdminNewsMaterialUpdateRequest(BaseModel):
    ru: AdminNewsTranslationPayload | None = None
    kz: AdminNewsTranslationPayload | None = None


class AdminNewsTranslationCreateRequest(BaseModel):
    data: AdminNewsTranslationPayload


class AdminNewsTranslationResponse(BaseModel):
    id: int
    language: LanguageCode
    title: str
    excerpt: str | None = None
    content: str | None = None
    content_text: str | None = None
    image_url: FileUrl = None
    video_url: str | None = None
    category: str | None = None
    tournament_id: str | None = None
    article_type: str | None = None
    is_slider: bool = False
    slider_order: int | None = None
    publish_date: date | None = None
    source_id: int | None = None
    source_url: str | None = None
    updated_at: datetime


class AdminNewsMaterialResponse(BaseModel):
    group_id: UUID
    ru: AdminNewsTranslationResponse | None
    kz: AdminNewsTranslationResponse | None
    updated_at: datetime | None = None


class AdminNewsMaterialListResponse(BaseModel):
    items: list[AdminNewsMaterialResponse]
    total: int
