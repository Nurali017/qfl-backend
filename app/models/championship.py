from datetime import datetime
from sqlalchemy import Integer, String, Text, DateTime, Boolean
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class Championship(Base):
    """Championship/League — top-level grouping for tournaments.

    Examples: Премьер-Лига, Первая Лига, Кубок РК, Вторая Лига, etc.
    Maps to legacy MySQL `championships` table.
    """
    __tablename__ = "championships"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    legacy_id: Mapped[int | None] = mapped_column(Integer, unique=True, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)  # Russian (default)
    name_kz: Mapped[str | None] = mapped_column(String(255))
    name_en: Mapped[str | None] = mapped_column(String(255))
    short_name: Mapped[str | None] = mapped_column(String(50))  # "ПЛ", "1Л", "Кубок"
    short_name_kz: Mapped[str | None] = mapped_column(String(50))
    short_name_en: Mapped[str | None] = mapped_column(String(50))
    slug: Mapped[str | None] = mapped_column(String(100), unique=True)  # "premier-league"
    sota_ids: Mapped[str | None] = mapped_column(Text)  # "7" or "74;75;139"
    sort_order: Mapped[int] = mapped_column(Integer, default=0)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    # Relationships
    tournaments: Mapped[list["Tournament"]] = relationship(
        "Tournament", back_populates="championship"
    )
    partners: Mapped[list["Partner"]] = relationship(
        "Partner", back_populates="championship"
    )
