from datetime import date, datetime
from uuid import UUID

from pydantic import BaseModel

from app.schemas.admin.common import LanguageCode


class AdminNewsTranslationPayload(BaseModel):
    title: str
    excerpt: str | None = None
    content: str | None = None
    content_text: str | None = None
    image_url: str | None = None
    video_url: str | None = None
    category: str | None = None
    championship_code: str | None = None
    article_type: str | None = None
    is_slider: bool = False
    slider_order: int | None = None
    publish_date: date | None = None
    source_url: str | None = None


class AdminNewsTranslationPatchPayload(BaseModel):
    title: str | None = None
    excerpt: str | None = None
    content: str | None = None
    content_text: str | None = None
    image_url: str | None = None
    video_url: str | None = None
    category: str | None = None
    championship_code: str | None = None
    article_type: str | None = None
    is_slider: bool | None = None
    slider_order: int | None = None
    publish_date: date | None = None
    source_url: str | None = None


class AdminNewsMaterialCreateRequest(BaseModel):
    ru: AdminNewsTranslationPayload
    kz: AdminNewsTranslationPayload


class AdminNewsMaterialUpdateRequest(BaseModel):
    ru: AdminNewsTranslationPatchPayload | None = None
    kz: AdminNewsTranslationPatchPayload | None = None


class AdminNewsTranslationCreateRequest(BaseModel):
    data: AdminNewsTranslationPayload


class AdminNewsTranslationResponse(BaseModel):
    id: int
    language: LanguageCode
    title: str
    excerpt: str | None = None
    content: str | None = None
    content_text: str | None = None
    image_url: str | None = None
    video_url: str | None = None
    category: str | None = None
    championship_code: str | None = None
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


class AdminNewsArticleTypeUpdateRequest(BaseModel):
    article_type: str | None = None


class AdminNewsClassifyRequest(BaseModel):
    apply: bool = False
    limit: int | None = None
    only_unclassified: bool = True
    championship_code: str | None = None
    date_from: date | None = None
    date_to: date | None = None
    min_confidence: float = 0.70


class AdminNewsNeedsReviewItem(BaseModel):
    group_id: UUID
    representative_news_id: int | None = None
    representative_title: str | None = None
    confidence: float
    source: str
    reason: str | None = None


class AdminNewsClassifySummary(BaseModel):
    dry_run: bool
    total_groups: int
    classified_groups: int
    updated_groups: int
    unchanged_groups: int
    needs_review_count: int


class AdminNewsClassifyResponse(BaseModel):
    summary: AdminNewsClassifySummary
    needs_review: list[AdminNewsNeedsReviewItem]
    updated_group_ids: list[UUID]
