"""Persisted marker: tour aggregate sync completed successfully."""

from datetime import datetime

from sqlalchemy import Integer, DateTime, ForeignKey, Index, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class TourSyncStatus(Base):
    """Persisted marker: tour aggregate sync completed successfully."""

    __tablename__ = "tour_sync_status"
    __table_args__ = (
        UniqueConstraint("season_id", "tour", name="uq_tour_sync_status"),
        Index("ix_tour_sync_status_season", "season_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    season_id: Mapped[int] = mapped_column(Integer, ForeignKey("seasons.id"))
    tour: Mapped[int] = mapped_column(Integer, nullable=False)
    synced_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
