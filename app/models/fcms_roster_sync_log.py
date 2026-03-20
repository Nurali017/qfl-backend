from datetime import datetime

from sqlalchemy import DateTime, Integer, String, Text, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base
from app.utils.timestamps import utcnow


class FcmsRosterSyncLog(Base):
    __tablename__ = "fcms_roster_sync_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    competition_name: Mapped[str] = mapped_column(String(200))
    competition_id: Mapped[int] = mapped_column(Integer)
    season_id: Mapped[int] = mapped_column(Integer, ForeignKey("seasons.id"))
    status: Mapped[str] = mapped_column(String(30), default="running")
    teams_synced: Mapped[int] = mapped_column(Integer, default=0)
    total_auto_updates: Mapped[int] = mapped_column(Integer, default=0)
    total_new_players: Mapped[int] = mapped_column(Integer, default=0)
    total_auto_deactivated: Mapped[int] = mapped_column(Integer, default=0)
    total_deregistered: Mapped[int] = mapped_column(Integer, default=0)
    results: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    resolved_items: Mapped[dict] = mapped_column(JSONB, default=dict, server_default="{}")
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    triggered_by: Mapped[str] = mapped_column(String(100), default="celery_beat")
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
