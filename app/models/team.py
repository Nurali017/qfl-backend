from datetime import datetime
from sqlalchemy import Integer, String, Text, DateTime, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base
from app.utils.file_urls import FileUrlType


class Team(Base):
    __tablename__ = "teams"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    legacy_id: Mapped[int | None] = mapped_column(Integer, unique=True, index=True)
    club_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("clubs.id"), index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)  # Russian (default)
    name_kz: Mapped[str | None] = mapped_column(String(255))
    name_en: Mapped[str | None] = mapped_column(String(255))
    logo_url: Mapped[str | None] = mapped_column(FileUrlType)
    logo_updated_at: Mapped[datetime | None] = mapped_column(DateTime)
    city: Mapped[str | None] = mapped_column(String(100))  # Russian (default)
    city_kz: Mapped[str | None] = mapped_column(String(100))
    city_en: Mapped[str | None] = mapped_column(String(100))
    website: Mapped[str | None] = mapped_column(String(500))  # Official team website
    stadium_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("stadiums.id"))

    # Color palette extracted from logo
    primary_color: Mapped[str | None] = mapped_column(String(7))  # Hex: #FF5733
    secondary_color: Mapped[str | None] = mapped_column(String(7))
    accent_color: Mapped[str | None] = mapped_column(String(7))
    colors_updated_at: Mapped[datetime | None] = mapped_column(DateTime)

    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    # Relationships
    player_teams: Mapped[list["PlayerTeam"]] = relationship(
        "PlayerTeam", back_populates="team"
    )
    home_games: Mapped[list["Game"]] = relationship(
        "Game", back_populates="home_team", foreign_keys="Game.home_team_id"
    )
    away_games: Mapped[list["Game"]] = relationship(
        "Game", back_populates="away_team", foreign_keys="Game.away_team_id"
    )
    game_stats: Mapped[list["GameTeamStats"]] = relationship(
        "GameTeamStats", back_populates="team"
    )
    player_game_stats: Mapped[list["GamePlayerStats"]] = relationship(
        "GamePlayerStats", back_populates="team"
    )
    score_table_entries: Mapped[list["ScoreTable"]] = relationship(
        "ScoreTable", back_populates="team"
    )
    season_stats: Mapped[list["TeamSeasonStats"]] = relationship(
        "TeamSeasonStats", back_populates="team"
    )
    player_season_stats: Mapped[list["PlayerSeasonStats"]] = relationship(
        "PlayerSeasonStats", back_populates="team"
    )
    coaches: Mapped[list["TeamCoach"]] = relationship(
        "TeamCoach", back_populates="team"
    )
    stadium: Mapped["Stadium"] = relationship("Stadium", backref="teams")
    club: Mapped["Club"] = relationship("Club", back_populates="teams")
