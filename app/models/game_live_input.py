"""Live-stream ingest endpoint per game (Cloudflare Stream / other providers)."""
from __future__ import annotations

import enum
from datetime import datetime

from sqlalchemy import DateTime, Enum, ForeignKey, Index, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base
from app.models.sql_types import GAME_ID_SQL_TYPE
from app.utils.timestamps import utcnow


class GameLiveInputStatus(str, enum.Enum):
    """Lifecycle of a live-stream ingest endpoint.

    pending  — endpoint created in provider, broadcaster not yet connected
    live     — provider confirmed active ingest, stream is playing
    ended    — manually stopped OR provider disconnected after kickoff
    failed   — provider returned an error during creation / polling
    """

    pending = "pending"
    live = "live"
    ended = "ended"
    failed = "failed"


class GameLiveInput(Base):
    __tablename__ = "game_live_inputs"
    __table_args__ = (
        Index("ix_game_live_inputs_status", "status"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    game_id: Mapped[int] = mapped_column(
        GAME_ID_SQL_TYPE,
        ForeignKey("games.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
        index=True,
    )

    # Which provider issued this endpoint — allows future migration to Yandex/etc.
    provider: Mapped[str] = mapped_column(String(32), nullable=False, default="cloudflare")
    provider_input_uid: Mapped[str] = mapped_column(String(100), nullable=False, unique=True, index=True)

    # Ingest endpoints — what we give to the broadcaster
    srt_url: Mapped[str] = mapped_column(Text, nullable=False)
    srt_passphrase: Mapped[str | None] = mapped_column(Text, nullable=True)
    srt_stream_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    rtmp_url: Mapped[str] = mapped_column(Text, nullable=False)
    rtmp_stream_key: Mapped[str] = mapped_column(Text, nullable=False)

    # Playback endpoints — what our player consumes
    playback_hls_url: Mapped[str] = mapped_column(Text, nullable=False)
    playback_dash_url: Mapped[str | None] = mapped_column(Text, nullable=True)

    status: Mapped[GameLiveInputStatus] = mapped_column(
        Enum(GameLiveInputStatus, name="game_live_input_status"),
        nullable=False,
        default=GameLiveInputStatus.pending,
        server_default="pending",
    )

    # Timestamps — broadcaster connected / disconnected
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_status_check_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # Failure diagnostics
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False
    )

    game: Mapped["Game"] = relationship("Game", foreign_keys=[game_id])
