from datetime import datetime
from sqlalchemy import Integer, String, DateTime, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class City(Base):
    """City reference data.

    Maps to legacy MySQL `cities` table.
    Normalizes city info that was previously stored as text in teams/stadiums.
    """
    __tablename__ = "cities"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    legacy_id: Mapped[int | None] = mapped_column(Integer, unique=True, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)  # Russian (default)
    name_kz: Mapped[str | None] = mapped_column(String(255))
    name_en: Mapped[str | None] = mapped_column(String(255))
    country_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("countries.id"))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    # Relationships
    country: Mapped["Country"] = relationship("Country")
    stadiums: Mapped[list["Stadium"]] = relationship(
        "Stadium", back_populates="city_rel"
    )
    clubs: Mapped[list["Club"]] = relationship(
        "Club", back_populates="city_rel"
    )
