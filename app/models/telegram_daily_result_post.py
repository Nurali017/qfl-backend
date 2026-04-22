from datetime import date, datetime

from sqlalchemy import Date, DateTime, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base
from app.utils.timestamps import utcnow


class TelegramDailyResultPost(Base):
    """Tracks posted Telegram daily-results digests per season/date/locale."""

    __tablename__ = "telegram_daily_result_posts"
    __table_args__ = (
        UniqueConstraint(
            "season_id",
            "for_date",
            "locale",
            name="uq_telegram_daily_result_posts_scope",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    season_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("seasons.id"),
        nullable=False,
        index=True,
    )
    for_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    locale: Mapped[str] = mapped_column(String(8), nullable=False, default="kz")
    message_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    tour: Mapped[int | None] = mapped_column(Integer, nullable=True)
    game_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False
    )

    season: Mapped["Season"] = relationship("Season")
