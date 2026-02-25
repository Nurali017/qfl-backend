from datetime import datetime
from sqlalchemy import Integer, String, DateTime, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base
from app.utils.timestamps import utcnow


class Stage(Base):
    """Stage/Tour/Round within a season.

    Represents tour definitions (e.g., "Тур 1", "Тур 2", "Четвертьфинал").
    Maps to legacy MySQL `stages` table.
    """
    __tablename__ = "stages"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    legacy_id: Mapped[int | None] = mapped_column(Integer, unique=True, index=True)
    season_id: Mapped[int] = mapped_column(Integer, ForeignKey("seasons.id"), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)  # Russian (default)
    name_kz: Mapped[str | None] = mapped_column(String(255))
    name_en: Mapped[str | None] = mapped_column(String(255))
    stage_number: Mapped[int | None] = mapped_column(Integer)  # Tour number
    sort_order: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=utcnow, onupdate=utcnow
    )

    # Relationships
    season: Mapped["Season"] = relationship("Season", back_populates="stages")
    games: Mapped[list["Game"]] = relationship("Game", back_populates="stage")
