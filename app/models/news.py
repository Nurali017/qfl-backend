from datetime import datetime
from sqlalchemy import Integer, String, Text, DateTime, Date, Boolean, Enum as SQLEnum
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.dialects.postgresql import JSONB
import enum

from app.database import Base
from app.models.page import Language


class ArticleType(str, enum.Enum):
    NEWS = "NEWS"
    ANALYTICS = "ANALYTICS"


class News(Base):
    """News articles"""
    __tablename__ = "news"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source_id: Mapped[int | None] = mapped_column(Integer, index=True)  # Original ID from kffleague.kz
    source_url: Mapped[str | None] = mapped_column(String(500))  # Original URL from kffleague.kz
    language: Mapped[Language] = mapped_column(SQLEnum(Language), nullable=False)
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    excerpt: Mapped[str | None] = mapped_column(Text)
    content: Mapped[str | None] = mapped_column(Text)
    content_text: Mapped[str | None] = mapped_column(Text)
    image_url: Mapped[str | None] = mapped_column(String(500))
    video_url: Mapped[str | None] = mapped_column(String(500))  # YouTube embed URL
    category: Mapped[str | None] = mapped_column(String(100))
    article_type: Mapped[ArticleType | None] = mapped_column(
        SQLEnum(ArticleType, name='article_type'),
        nullable=True
    )
    is_slider: Mapped[bool] = mapped_column(Boolean, default=False)
    slider_order: Mapped[int | None] = mapped_column(Integer)
    publish_date: Mapped[datetime | None] = mapped_column(Date)
    structured_data: Mapped[dict | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    __table_args__ = (
        {"comment": "News articles in multiple languages"},
    )
