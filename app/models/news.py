from datetime import datetime
import uuid

from sqlalchemy import Boolean, Date, DateTime, Enum as SQLEnum, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.dialects.postgresql import JSONB, UUID
import enum

from app.database import Base
from app.utils.file_urls import FileUrlType
from app.models.page import Language


class ArticleType(str, enum.Enum):
    NEWS = "NEWS"
    ANALYTICS = "ANALYTICS"


class News(Base):
    """News articles"""
    __tablename__ = "news"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    translation_group_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), default=uuid.uuid4, index=True, nullable=False
    )
    source_id: Mapped[int | None] = mapped_column(Integer, index=True)  # Original ID from kffleague.kz
    source_url: Mapped[str | None] = mapped_column(String(500))  # Original URL from kffleague.kz
    language: Mapped[Language] = mapped_column(SQLEnum(Language), nullable=False)
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    excerpt: Mapped[str | None] = mapped_column(Text)
    content: Mapped[str | None] = mapped_column(Text)
    content_text: Mapped[str | None] = mapped_column(Text)
    image_url: Mapped[str | None] = mapped_column(FileUrlType)
    video_url: Mapped[str | None] = mapped_column(String(500))  # YouTube embed URL
    category: Mapped[str | None] = mapped_column(String(100))
    tournament_id: Mapped[str | None] = mapped_column(String(10), index=True)  # pl, 1l, cup, 2l, el
    article_type: Mapped[ArticleType | None] = mapped_column(
        SQLEnum(ArticleType, name='article_type'),
        nullable=True
    )
    is_slider: Mapped[bool] = mapped_column(Boolean, default=False)
    slider_order: Mapped[int | None] = mapped_column(Integer)
    publish_date: Mapped[datetime | None] = mapped_column(Date)
    structured_data: Mapped[dict | None] = mapped_column(JSONB)
    created_by_admin_id: Mapped[int | None] = mapped_column(ForeignKey("admin_users.id"))
    updated_by_admin_id: Mapped[int | None] = mapped_column(ForeignKey("admin_users.id"))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    __table_args__ = (
        UniqueConstraint("translation_group_id", "language", name="uq_news_translation_group_language"),
        {"comment": "News articles in multiple languages"},
    )
