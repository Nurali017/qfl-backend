from datetime import datetime
from sqlalchemy import Integer, String, DateTime, Boolean
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class Country(Base):
    """Country reference data with multilingual names and flags."""
    __tablename__ = "countries"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    code: Mapped[str] = mapped_column(String(2), unique=True, nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)  # Russian (default)
    name_kz: Mapped[str | None] = mapped_column(String(100))
    name_en: Mapped[str | None] = mapped_column(String(100))
    flag_url: Mapped[str | None] = mapped_column(String(500))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    # Relationships
    players: Mapped[list["Player"]] = relationship("Player", back_populates="country")
    coaches: Mapped[list["Coach"]] = relationship("Coach", back_populates="country")
    referees: Mapped[list["Referee"]] = relationship("Referee", back_populates="country")
