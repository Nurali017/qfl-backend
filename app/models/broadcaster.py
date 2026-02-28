from datetime import datetime
from sqlalchemy import Integer, String, Boolean, DateTime
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base
from app.utils.file_urls import FileUrlType
from app.utils.timestamps import utcnow


class Broadcaster(Base):
    """TV or YouTube channel for match broadcasting."""
    __tablename__ = "broadcasters"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    logo_url: Mapped[str | None] = mapped_column(FileUrlType)
    type: Mapped[str | None] = mapped_column(String(20))  # "tv" | "youtube"
    website: Mapped[str | None] = mapped_column(String(500))
    sort_order: Mapped[int] = mapped_column(Integer, default=0, nullable=False, server_default="0")
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False, server_default="true")
    created_at: Mapped[datetime | None] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime | None] = mapped_column(DateTime, default=utcnow, onupdate=utcnow)

    # Relationships
    game_assignments: Mapped[list["GameBroadcaster"]] = relationship(
        "GameBroadcaster", back_populates="broadcaster"
    )
