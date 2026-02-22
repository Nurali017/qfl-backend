from datetime import datetime
from sqlalchemy import Integer, String, DateTime, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class Tournament(Base):
    __tablename__ = "tournaments"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)  # Russian (default)
    name_kz: Mapped[str | None] = mapped_column(String(255))
    name_en: Mapped[str | None] = mapped_column(String(255))
    country_code: Mapped[str | None] = mapped_column(String(10))
    country_name: Mapped[str | None] = mapped_column(String(100))  # Russian (default)
    country_name_kz: Mapped[str | None] = mapped_column(String(100))
    country_name_en: Mapped[str | None] = mapped_column(String(100))
    championship_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("championships.id"), index=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    # Relationships
    seasons: Mapped[list["Season"]] = relationship("Season", back_populates="tournament")
    championship: Mapped["Championship"] = relationship(
        "Championship", back_populates="tournaments"
    )
