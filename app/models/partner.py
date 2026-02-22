from datetime import datetime
from sqlalchemy import Integer, String, DateTime, Boolean, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base
from app.utils.file_urls import FileUrlType


class Partner(Base):
    """Partner/Sponsor associated with a championship and/or season.

    Maps to legacy MySQL `partners` table.
    """
    __tablename__ = "partners"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    legacy_id: Mapped[int | None] = mapped_column(Integer, unique=True, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    logo_url: Mapped[str | None] = mapped_column(FileUrlType)
    website: Mapped[str | None] = mapped_column(String(500))
    championship_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("championships.id"))
    season_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("seasons.id"))
    sort_order: Mapped[int] = mapped_column(Integer, default=0)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    show_in_news: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    # Relationships
    championship: Mapped["Championship"] = relationship(
        "Championship", back_populates="partners"
    )
    season: Mapped["Season"] = relationship("Season")
