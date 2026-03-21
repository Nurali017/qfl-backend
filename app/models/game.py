import enum
from datetime import datetime, date, time
from uuid import UUID
from sqlalchemy import Integer, String, Date, Time, Boolean, DateTime, ForeignKey, Index, Enum, Text
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base
from app.models.sql_types import GAME_ID_SQL_TYPE
from app.utils.file_urls import FileUrlType
from app.utils.timestamps import utcnow


class GameStatus(str, enum.Enum):
    """Match status."""
    created = "created"
    live = "live"
    finished = "finished"
    postponed = "postponed"
    cancelled = "cancelled"
    technical_defeat = "technical_defeat"


class Game(Base):
    __tablename__ = "games"
    __table_args__ = (
        Index("ix_games_season_date_time", "season_id", "date", "time"),
    )

    id: Mapped[int] = mapped_column(GAME_ID_SQL_TYPE, primary_key=True, autoincrement=True)
    sota_id: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True), unique=True, index=True)
    legacy_id: Mapped[int | None] = mapped_column(Integer, unique=True, index=True)
    vsporte_id: Mapped[str | None] = mapped_column(String(100), index=True)
    date: Mapped[date] = mapped_column(Date, nullable=False)
    time: Mapped[time | None] = mapped_column(Time)
    tour: Mapped[int | None] = mapped_column(Integer)
    stage_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("stages.id"), index=True)
    season_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("seasons.id"), index=True)
    home_team_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("teams.id"), index=True)
    away_team_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("teams.id"), index=True)
    home_score: Mapped[int | None] = mapped_column(Integer)
    away_score: Mapped[int | None] = mapped_column(Integer)
    home_penalty_score: Mapped[int | None] = mapped_column(Integer)
    away_penalty_score: Mapped[int | None] = mapped_column(Integer)
    has_stats: Mapped[bool] = mapped_column(Boolean, default=False)
    has_lineup: Mapped[bool] = mapped_column(Boolean, default=False)
    is_schedule_tentative: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=False, server_default="false"
    )

    # Game status enum — single source of truth for match lifecycle
    status: Mapped[GameStatus] = mapped_column(
        Enum(GameStatus), nullable=False, default=GameStatus.created, server_default="created"
    )

    # Featured match flag (В-2)
    is_featured: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=False, server_default="false"
    )

    # Sync disable flag — prevents SOTA sync from overwriting manual edits
    sync_disabled: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=False, server_default="false"
    )

    # Show/hide timeline and live minutes on the public website
    show_timeline: Mapped[bool] = mapped_column(
        Boolean, default=True, nullable=False, server_default="true"
    )

    # Live match tracking
    home_formation: Mapped[str | None] = mapped_column(String(20))  # e.g., "4-2-3-1"
    away_formation: Mapped[str | None] = mapped_column(String(20))  # e.g., "4-3-3"
    home_kit_color: Mapped[str | None] = mapped_column(String(10))
    away_kit_color: Mapped[str | None] = mapped_column(String(10))
    lineup_live_synced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    lineup_source: Mapped[str | None] = mapped_column(String(32))  # team_squad, sota_api, vsporte_api, matches_players
    lineup_render_mode: Mapped[str | None] = mapped_column(String(16))  # field, list, hidden
    lineup_backfilled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    # Lineup embargo: Telegram notification tracking
    lineup_telegram_sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    lineup_telegram_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)

    # Stadium FK
    stadium_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("stadiums.id"), index=True)

    visitors: Mapped[int | None] = mapped_column(Integer)
    ticket_url: Mapped[str | None] = mapped_column(String(500))  # URL for ticket purchase
    ticket_url_fetched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    is_free_entry: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=False, server_default="false"
    )
    video_url: Mapped[str | None] = mapped_column(String(500))  # URL for video replay
    youtube_live_url: Mapped[str | None] = mapped_column(String(500))  # YouTube live stream URL
    protocol_url: Mapped[str | None] = mapped_column(FileUrlType)  # Match protocol PDF

    # Live minute/half from SOTA time endpoint
    live_minute: Mapped[int | None] = mapped_column(Integer, nullable=True)
    live_half: Mapped[int | None] = mapped_column(Integer, nullable=True)
    live_phase: Mapped[str | None] = mapped_column(String(20), nullable=True)

    # Half timestamps for auto-ticking live minute
    half1_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    half2_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # FCMS integration
    fcms_match_id: Mapped[int | None] = mapped_column(Integer, unique=True, index=True)
    fcms_protocol_synced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    # When match was marked as finished (for post-match protocol sync window)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, default=None)

    # When extended stats (xG, season stats) were synced post-match
    extended_stats_synced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, default=None)

    # AI-generated match preview
    preview_ru: Mapped[str | None] = mapped_column(Text, nullable=True)
    preview_kz: Mapped[str | None] = mapped_column(Text, nullable=True)
    preview_generated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # Broadcast fields (В-1)
    where_broadcast: Mapped[str | None] = mapped_column(String(500))  # Where to watch
    video_review_url: Mapped[str | None] = mapped_column(String(500))  # Video review URL

    # Weather data (fetched from OpenWeatherMap)
    weather_temp: Mapped[int | None] = mapped_column(Integer)
    weather_condition: Mapped[str | None] = mapped_column(String(50))
    weather_fetched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )

    # Derived boolean helpers (backward-compatible with removed columns)
    @hybrid_property
    def is_live(self) -> bool:
        return self.status == GameStatus.live

    @is_live.inplace.expression
    @classmethod
    def _is_live_expression(cls):
        return cls.status == GameStatus.live

    @hybrid_property
    def is_technical(self) -> bool:
        return self.status == GameStatus.technical_defeat

    @is_technical.inplace.expression
    @classmethod
    def _is_technical_expression(cls):
        return cls.status == GameStatus.technical_defeat

    # Relationships
    season: Mapped["Season"] = relationship("Season", back_populates="games")
    home_team: Mapped["Team"] = relationship(
        "Team", back_populates="home_games", foreign_keys=[home_team_id]
    )
    away_team: Mapped["Team"] = relationship(
        "Team", back_populates="away_games", foreign_keys=[away_team_id]
    )
    team_stats: Mapped[list["GameTeamStats"]] = relationship(
        "GameTeamStats", back_populates="game"
    )
    player_stats: Mapped[list["GamePlayerStats"]] = relationship(
        "GamePlayerStats", back_populates="game"
    )

    # New relationships
    stadium_rel: Mapped["Stadium"] = relationship("Stadium", back_populates="games")
    referees: Mapped[list["GameReferee"]] = relationship(
        "GameReferee", back_populates="game", cascade="all, delete-orphan"
    )
    lineups: Mapped[list["GameLineup"]] = relationship(
        "GameLineup", back_populates="game", cascade="all, delete-orphan"
    )
    events: Mapped[list["GameEvent"]] = relationship(
        "GameEvent", back_populates="game", cascade="all, delete-orphan"
    )
    stage: Mapped["Stage"] = relationship("Stage", back_populates="games")
    broadcasters: Mapped[list["GameBroadcaster"]] = relationship(
        "GameBroadcaster", back_populates="game", cascade="all, delete-orphan"
    )
