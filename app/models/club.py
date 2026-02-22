from datetime import datetime
from sqlalchemy import Integer, String, DateTime, ForeignKey, Boolean
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base
from app.utils.file_urls import FileUrlType


class Club(Base):
    """Club — parent organization that can have multiple teams.

    Example: "Астана" club has PL team, reserves, women's team, etc.
    Maps to legacy MySQL `clubs` table.
    """
    __tablename__ = "clubs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    legacy_id: Mapped[int | None] = mapped_column(Integer, unique=True, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)  # Russian (default)
    name_kz: Mapped[str | None] = mapped_column(String(255))
    name_en: Mapped[str | None] = mapped_column(String(255))
    short_name: Mapped[str | None] = mapped_column(String(50))
    logo_url: Mapped[str | None] = mapped_column(FileUrlType)
    city_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("cities.id"))
    stadium_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("stadiums.id"))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    # Relationships
    teams: Mapped[list["Team"]] = relationship("Team", back_populates="club")
    city_rel: Mapped["City"] = relationship("City", back_populates="clubs")
    stadium: Mapped["Stadium"] = relationship("Stadium")
