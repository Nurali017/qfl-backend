from datetime import datetime
from sqlalchemy import Integer, String, DateTime, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base
from app.utils.file_urls import FileUrlType


class Referee(Base):
    """Match referee/official."""
    __tablename__ = "referees"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    first_name: Mapped[str] = mapped_column(String(100), nullable=False)
    last_name: Mapped[str] = mapped_column(String(100), nullable=False)
    first_name_kz: Mapped[str | None] = mapped_column(String(100))
    first_name_ru: Mapped[str | None] = mapped_column(String(100))
    first_name_en: Mapped[str | None] = mapped_column(String(100))
    last_name_kz: Mapped[str | None] = mapped_column(String(100))
    last_name_ru: Mapped[str | None] = mapped_column(String(100))
    last_name_en: Mapped[str | None] = mapped_column(String(100))
    country_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("countries.id"), index=True)
    photo_url: Mapped[str | None] = mapped_column(FileUrlType)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    # Relationships
    country: Mapped["Country"] = relationship("Country", back_populates="referees")
    game_assignments: Mapped[list["GameReferee"]] = relationship(
        "GameReferee", back_populates="referee"
    )
