from datetime import datetime
from uuid import UUID

from pydantic import BaseModel

from app.schemas.admin.common import LanguageCode


class AdminPageTranslationPayload(BaseModel):
    slug: str
    title: str
    content: str | None = None
    content_text: str | None = None
    url: str | None = None


class AdminPageMaterialUpdateRequest(BaseModel):
    ru: AdminPageTranslationPayload | None = None
    kz: AdminPageTranslationPayload | None = None


class AdminPageTranslationCreateRequest(BaseModel):
    data: AdminPageTranslationPayload


class AdminPageTranslationResponse(BaseModel):
    id: int
    language: LanguageCode
    slug: str
    title: str
    content: str | None = None
    content_text: str | None = None
    url: str | None = None
    updated_at: datetime


class AdminPageMaterialResponse(BaseModel):
    group_id: UUID
    ru: AdminPageTranslationResponse | None
    kz: AdminPageTranslationResponse | None
    updated_at: datetime | None = None


class AdminPageMaterialListResponse(BaseModel):
    items: list[AdminPageMaterialResponse]
    total: int
