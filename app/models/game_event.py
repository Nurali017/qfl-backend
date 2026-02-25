import enum
from datetime import datetime
from sqlalchemy import Integer, String, DateTime, ForeignKey, Enum, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base
from app.models.sql_types import PLAYER_ID_SQL_TYPE, GAME_ID_SQL_TYPE
from app.utils.timestamps import utcnow


class GameEventType(str, enum.Enum):
    """Types of match events."""
    goal = "goal"
    assist = "assist"
    yellow_card = "yellow_card"
    red_card = "red_card"
    substitution = "substitution"


class GameEvent(Base):
    """
    Match event (goal, card, substitution) from live SOTA data.

    Stores individual events from the /em/<game_id>-list.json endpoint.
    """
    __tablename__ = "game_events"
    __table_args__ = (
        Index("ix_game_events_game_id", "game_id"),
        Index("ix_game_events_game_minute", "game_id", "half", "minute"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    game_id: Mapped[int] = mapped_column(
        GAME_ID_SQL_TYPE, ForeignKey("games.id"), nullable=False
    )

    # Event timing
    half: Mapped[int] = mapped_column(Integer, nullable=False)  # 1 or 2
    minute: Mapped[int] = mapped_column(Integer, nullable=False)

    # Event type
    event_type: Mapped[GameEventType] = mapped_column(
        Enum(GameEventType), nullable=False
    )

    # Team reference
    team_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("teams.id"))
    team_name: Mapped[str | None] = mapped_column(String(255))

    # Primary player (scorer, carded player, player coming off)
    player_id: Mapped[int | None] = mapped_column(PLAYER_ID_SQL_TYPE, ForeignKey("players.id"))
    player_number: Mapped[int | None] = mapped_column(Integer)
    player_name: Mapped[str | None] = mapped_column(String(255))

    # Secondary player (player coming on for substitutions)
    player2_id: Mapped[int | None] = mapped_column(PLAYER_ID_SQL_TYPE, ForeignKey("players.id"))
    player2_number: Mapped[int | None] = mapped_column(Integer)
    player2_name: Mapped[str | None] = mapped_column(String(255))
    player2_team_name: Mapped[str | None] = mapped_column(String(255))

    # Assist info (only for goal events)
    assist_player_id: Mapped[int | None] = mapped_column(PLAYER_ID_SQL_TYPE, ForeignKey("players.id"))
    assist_player_name: Mapped[str | None] = mapped_column(String(255))

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=utcnow, nullable=False
    )

    # Relationships
    game: Mapped["Game"] = relationship("Game", back_populates="events")
    team: Mapped["Team"] = relationship("Team", foreign_keys=[team_id])
    player: Mapped["Player"] = relationship("Player", foreign_keys=[player_id])
    player2: Mapped["Player"] = relationship("Player", foreign_keys=[player2_id])
    assist_player: Mapped["Player"] = relationship("Player", foreign_keys=[assist_player_id])
