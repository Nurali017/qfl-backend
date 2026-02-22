from datetime import datetime
from sqlalchemy import Integer, String, DateTime, Boolean, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base
from app.models.sql_types import GAME_ID_SQL_TYPE


class PlayoffBracket(Base):
    """Playoff bracket entry for cup tournaments.

    Represents a single bracket position (e.g., Quarter-final, left side, match 1).
    Maps to legacy MySQL `playoff_brackets` table.
    """
    __tablename__ = "playoff_brackets"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    legacy_id: Mapped[int | None] = mapped_column(Integer, unique=True, index=True)
    season_id: Mapped[int] = mapped_column(Integer, ForeignKey("seasons.id"), nullable=False)
    round_name: Mapped[str] = mapped_column(String(50), nullable=False)  # "1_8", "1_4", "1_2", "final"
    side: Mapped[str] = mapped_column(String(10), default="left")  # "left", "right", "center"
    sort_order: Mapped[int] = mapped_column(Integer, default=1)
    game_id: Mapped[int | None] = mapped_column(GAME_ID_SQL_TYPE, ForeignKey("games.id"))
    is_visible: Mapped[bool] = mapped_column(Boolean, default=True)
    is_third_place: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    # Relationships
    season: Mapped["Season"] = relationship("Season")
    game: Mapped["Game"] = relationship("Game")
