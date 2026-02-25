from datetime import datetime, date
from uuid import UUID
from sqlalchemy import String, Date, Integer, DateTime, ForeignKey
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base
from app.models.sql_types import PLAYER_ID_SQL_TYPE
from app.utils.file_urls import FileUrlType
from app.utils.timestamps import utcnow


class Player(Base):
    __tablename__ = "players"

    id: Mapped[int] = mapped_column(PLAYER_ID_SQL_TYPE, primary_key=True, autoincrement=True)
    legacy_id: Mapped[int | None] = mapped_column(Integer, unique=True, index=True)
    sota_id: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True), unique=True, index=True)
    first_name: Mapped[str | None] = mapped_column(String(100))  # Russian (default)
    first_name_kz: Mapped[str | None] = mapped_column(String(100))
    first_name_en: Mapped[str | None] = mapped_column(String(100))
    last_name: Mapped[str | None] = mapped_column(String(100))  # Russian (default)
    last_name_kz: Mapped[str | None] = mapped_column(String(100))
    last_name_en: Mapped[str | None] = mapped_column(String(100))
    birthday: Mapped[date | None] = mapped_column(Date)
    player_type: Mapped[str | None] = mapped_column(String(50))
    country_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("countries.id"), index=True)
    photo_url: Mapped[str | None] = mapped_column(FileUrlType)
    age: Mapped[int | None] = mapped_column(Integer)
    height: Mapped[int | None] = mapped_column(Integer)  # cm
    weight: Mapped[int | None] = mapped_column(Integer)  # kg
    gender: Mapped[str | None] = mapped_column(String(10))  # "M", "F"
    top_role: Mapped[str | None] = mapped_column(String(100))  # Russian (default)
    top_role_kz: Mapped[str | None] = mapped_column(String(100))
    top_role_en: Mapped[str | None] = mapped_column(String(100))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=utcnow, onupdate=utcnow
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
