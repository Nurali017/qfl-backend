from datetime import datetime
from sqlalchemy import Integer, String, DateTime, ForeignKey, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base
from app.utils.timestamps import utcnow


class TeamOfWeek(Base):
    __tablename__ = "team_of_week"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    season_id: Mapped[int] = mapped_column(Integer, ForeignKey("seasons.id"), nullable=False)
    tour_key: Mapped[str] = mapped_column(String(50), nullable=False)
    locale: Mapped[str] = mapped_column(String(5), nullable=False, default="ru")
    scheme: Mapped[str | None] = mapped_column(String(20), nullable=True)
    payload: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=utcnow, onupdate=utcnow
    )

    season: Mapped["Season"] = relationship("Season")

    __table_args__ = (
        UniqueConstraint("season_id", "tour_key", "locale", name="uq_team_of_week_season_tour_locale"),
    )
