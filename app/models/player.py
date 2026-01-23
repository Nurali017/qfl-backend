from datetime import datetime, date
from uuid import UUID
from sqlalchemy import String, Date, Integer, Text, DateTime, ForeignKey
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class Player(Base):
    __tablename__ = "players"

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True)
    first_name: Mapped[str | None] = mapped_column(String(100))  # Russian (default)
    first_name_kz: Mapped[str | None] = mapped_column(String(100))
    first_name_en: Mapped[str | None] = mapped_column(String(100))
    last_name: Mapped[str | None] = mapped_column(String(100))  # Russian (default)
    last_name_kz: Mapped[str | None] = mapped_column(String(100))
    last_name_en: Mapped[str | None] = mapped_column(String(100))
    birthday: Mapped[date | None] = mapped_column(Date)
    player_type: Mapped[str | None] = mapped_column(String(50))
    country_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("countries.id"), index=True)
    photo_url: Mapped[str | None] = mapped_column(Text)
    age: Mapped[int | None] = mapped_column(Integer)
    top_role: Mapped[str | None] = mapped_column(String(100))  # Russian (default)
    top_role_kz: Mapped[str | None] = mapped_column(String(100))
    top_role_en: Mapped[str | None] = mapped_column(String(100))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    # Relationships
    country: Mapped["Country"] = relationship("Country", back_populates="players")
    player_teams: Mapped[list["PlayerTeam"]] = relationship(
        "PlayerTeam", back_populates="player"
    )
    game_stats: Mapped[list["GamePlayerStats"]] = relationship(
        "GamePlayerStats", back_populates="player"
    )
    season_stats: Mapped[list["PlayerSeasonStats"]] = relationship(
        "PlayerSeasonStats", back_populates="player"
    )
    game_lineups: Mapped[list["GameLineup"]] = relationship(
        "GameLineup", back_populates="player"
    )
