from datetime import datetime
import uuid

from sqlalchemy import DateTime, Enum as SQLEnum, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.dialects.postgresql import JSONB, UUID
import enum

from app.database import Base


class Language(str, enum.Enum):
    KZ = "kz"
    RU = "ru"


class Page(Base):
    """Static pages (contacts, documents, leadership, etc.)"""
    __tablename__ = "pages"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    translation_group_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), default=uuid.uuid4, index=True, nullable=False
    )
    slug: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    language: Mapped[Language] = mapped_column(SQLEnum(Language), nullable=False)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    content: Mapped[str | None] = mapped_column(Text)
    content_text: Mapped[str | None] = mapped_column(Text)
    url: Mapped[str | None] = mapped_column(String(500))
    structured_data: Mapped[dict | None] = mapped_column(JSONB)
    created_by_admin_id: Mapped[int | None] = mapped_column(ForeignKey("admin_users.id"))
    updated_by_admin_id: Mapped[int | None] = mapped_column(ForeignKey("admin_users.id"))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    __table_args__ = (
        UniqueConstraint("slug", "language", name="ix_pages_slug_language"),
        UniqueConstraint("translation_group_id", "language", name="uq_pages_translation_group_language"),
        {"comment": "Static pages content in multiple languages"},
    )
