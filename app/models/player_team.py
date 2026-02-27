from sqlalchemy import Boolean, Integer, String, ForeignKey, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base
from app.models.sql_types import PLAYER_ID_SQL_TYPE
from app.utils.file_urls import FileUrlType


class PlayerTeam(Base):
    __tablename__ = "player_teams"
    __table_args__ = (
        UniqueConstraint("player_id", "team_id", "season_id", name="uq_player_team_season"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    player_id: Mapped[int] = mapped_column(PLAYER_ID_SQL_TYPE, ForeignKey("players.id"))
    team_id: Mapped[int] = mapped_column(Integer, ForeignKey("teams.id"), index=True)
    season_id: Mapped[int] = mapped_column(Integer, ForeignKey("seasons.id"), index=True)
    role: Mapped[int | None] = mapped_column(Integer, server_default="1")
    amplua: Mapped[int | None] = mapped_column(Integer)
    position_ru: Mapped[str | None] = mapped_column(String(200))
    position_kz: Mapped[str | None] = mapped_column(String(200))
    position_en: Mapped[str | None] = mapped_column(String(200))
    number: Mapped[int | None] = mapped_column(Integer)

    # Contract visibility (К-2)
    is_active: Mapped[bool] = mapped_column(
        Boolean, default=True, nullable=False, server_default="true"
    )
    is_hidden: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=False, server_default="false"
    )

    # Contract photo (В-7)
    photo_url: Mapped[str | None] = mapped_column(FileUrlType)

    # Relationships
    player: Mapped["Player"] = relationship("Player", back_populates="player_teams")
    team: Mapped["Team"] = relationship("Team", back_populates="player_teams")
    season: Mapped["Season"] = relationship("Season", back_populates="player_teams")
