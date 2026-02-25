from datetime import datetime
from sqlalchemy import Integer, String, DateTime, Boolean
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base
from app.utils.file_urls import FileUrlType
from app.utils.timestamps import utcnow


class Country(Base):
    """Country reference data with multilingual names and flags."""
    __tablename__ = "countries"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    legacy_id: Mapped[int | None] = mapped_column(Integer, unique=True, index=True)
    code: Mapped[str] = mapped_column(String(2), unique=True, nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)  # Russian (default)
    name_kz: Mapped[str | None] = mapped_column(String(100))
    name_en: Mapped[str | None] = mapped_column(String(100))
    flag_url: Mapped[str | None] = mapped_column(FileUrlType)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=utcnow, onupdate=utcnow
    )

    # Relationships
    players: Mapped[list["Player"]] = relationship("Player", back_populates="country")
    coaches: Mapped[list["Coach"]] = relationship("Coach", back_populates="country")
    referees: Mapped[list["Referee"]] = relationship("Referee", back_populates="country")
