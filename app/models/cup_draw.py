from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, JSON, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base
from app.utils.timestamps import utcnow


class CupDraw(Base):
    __tablename__ = "cup_draws"
    __table_args__ = (
        UniqueConstraint("season_id", "round_key", name="uq_cup_draw_season_round"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    season_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("seasons.id"), index=True, nullable=False
    )
    round_key: Mapped[str] = mapped_column(String(20), nullable=False)
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, server_default="draft", default="draft"
    )
    pairs: Mapped[list[dict] | None] = mapped_column(JSON, default=list)

    created_by: Mapped[int | None] = mapped_column(Integer, ForeignKey("admin_users.id"))
    published_by: Mapped[int | None] = mapped_column(Integer, ForeignKey("admin_users.id"))
    published_at: Mapped[datetime | None] = mapped_column(DateTime)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, onupdate=utcnow)

    season: Mapped["Season"] = relationship("Season")
    creator: Mapped["AdminUser"] = relationship("AdminUser", foreign_keys=[created_by])
    publisher: Mapped["AdminUser"] = relationship("AdminUser", foreign_keys=[published_by])
