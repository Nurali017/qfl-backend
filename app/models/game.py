from datetime import datetime, date, time
from uuid import UUID
from sqlalchemy import Integer, String, Date, Time, Boolean, DateTime, ForeignKey
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base
from app.utils.file_urls import FileUrlType


class Game(Base):
    __tablename__ = "games"

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True)
    date: Mapped[date] = mapped_column(Date, nullable=False)
    time: Mapped[time | None] = mapped_column(Time)
    tour: Mapped[int | None] = mapped_column(Integer)
    season_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("seasons.id"))
    home_team_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("teams.id"))
    away_team_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("teams.id"))
    home_score: Mapped[int | None] = mapped_column(Integer)
    away_score: Mapped[int | None] = mapped_column(Integer)
    has_stats: Mapped[bool] = mapped_column(Boolean, default=False)
    has_lineup: Mapped[bool] = mapped_column(Boolean, default=False)

    # Live match tracking
    is_live: Mapped[bool] = mapped_column(Boolean, default=False)
    home_formation: Mapped[str | None] = mapped_column(String(20))  # e.g., "4-2-3-1"
    away_formation: Mapped[str | None] = mapped_column(String(20))  # e.g., "4-3-3"

    # Stadium - keep string for backward compatibility, add FK
    stadium: Mapped[str | None] = mapped_column(String(255))  # Legacy field
    stadium_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("stadiums.id"))

    visitors: Mapped[int | None] = mapped_column(Integer)
    ticket_url: Mapped[str | None] = mapped_column(String(500))  # URL for ticket purchase
    video_url: Mapped[str | None] = mapped_column(String(500))  # URL for video replay
    protocol_url: Mapped[str | None] = mapped_column(FileUrlType)  # Match protocol PDF
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

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
