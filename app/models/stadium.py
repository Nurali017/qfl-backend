from datetime import datetime
from sqlalchemy import Integer, String, DateTime, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base
from app.utils.file_urls import FileUrlType
from app.utils.timestamps import utcnow


class Stadium(Base):
    """Stadium/Venue for matches."""
    __tablename__ = "stadiums"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    legacy_id: Mapped[int | None] = mapped_column(Integer, unique=True, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    name_kz: Mapped[str | None] = mapped_column(String(255))
    name_ru: Mapped[str | None] = mapped_column(String(255))
    name_en: Mapped[str | None] = mapped_column(String(255))
    city: Mapped[str | None] = mapped_column(String(100))
    city_kz: Mapped[str | None] = mapped_column(String(100))
    city_ru: Mapped[str | None] = mapped_column(String(100))
    city_en: Mapped[str | None] = mapped_column(String(100))
    city_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("cities.id"), index=True)
    capacity: Mapped[int | None] = mapped_column(Integer)
    address: Mapped[str | None] = mapped_column(String(500))
    address_kz: Mapped[str | None] = mapped_column(String(500))
    address_en: Mapped[str | None] = mapped_column(String(500))
    photo_url: Mapped[str | None] = mapped_column(FileUrlType)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=utcnow, onupdate=utcnow
    )

    # Relationships
    games: Mapped[list["Game"]] = relationship("Game", back_populates="stadium_rel")
    city_rel: Mapped["City"] = relationship("City", back_populates="stadiums")
