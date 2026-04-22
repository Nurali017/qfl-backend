import enum
from datetime import datetime
from sqlalchemy import Boolean, Integer, String, DateTime, ForeignKey, Enum, Index, text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base
from app.models.sql_types import PLAYER_ID_SQL_TYPE, GAME_ID_SQL_TYPE
from app.utils.file_urls import FileUrlType
from app.utils.timestamps import utcnow


class GameEventType(str, enum.Enum):
    """Types of match events."""
    goal = "goal"
    own_goal = "own_goal"
    penalty = "penalty"
    missed_penalty = "missed_penalty"
    assist = "assist"
    yellow_card = "yellow_card"
    second_yellow = "second_yellow"
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
        Index("ix_game_events_game_type", "game_id", "event_type"),
        Index("ix_game_events_game_source", "game_id", "source"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    game_id: Mapped[int] = mapped_column(
        GAME_ID_SQL_TYPE, ForeignKey("games.id"), nullable=False
    )

    # Event timing
    # 1=H1, 2=H2, 3=ET1, 4=ET2, 5=Shootout. Super Cup (which skips ET) uses
    # half=3 for the shootout. Disambiguation: shootout halves only contain
    # penalty/missed_penalty events with round-number "minutes" (<=20).
    half: Mapped[int] = mapped_column(Integer, nullable=False)
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

    # Source: "sota" (from sync) or "manual" (created via admin panel)
    source: Mapped[str] = mapped_column(
        String(16), nullable=False, default="sota", server_default=text("'sota'")
    )

    # Assist info (only for goal events)
    assist_player_id: Mapped[int | None] = mapped_column(PLAYER_ID_SQL_TYPE, ForeignKey("players.id"))
    assist_player_name: Mapped[str | None] = mapped_column(String(255))
    assist_manual_override: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default=text("false")
    )

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, nullable=False
    )

    # Public Telegram post tracking (goals/red cards)
    telegram_sent_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    # Telegram message id of the goal text post — used as reply_to target
    # when the video clip is attached asynchronously.
    telegram_message_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    telegram_video_sent_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Goal highlight clip (MinIO object name, resolved to full URL on read).
    # Populated by goal_video_sync_service from Google Drive during live matches.
    video_url: Mapped[str | None] = mapped_column(FileUrlType(), nullable=True)

    # Relationships
    game: Mapped["Game"] = relationship("Game", back_populates="events")
    team: Mapped["Team"] = relationship("Team", foreign_keys=[team_id])
    player: Mapped["Player"] = relationship("Player", foreign_keys=[player_id])
    player2: Mapped["Player"] = relationship("Player", foreign_keys=[player2_id])
    assist_player: Mapped["Player"] = relationship("Player", foreign_keys=[assist_player_id])
