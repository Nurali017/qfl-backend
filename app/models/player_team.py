from sqlalchemy import Integer, ForeignKey, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base
from app.models.sql_types import PLAYER_ID_SQL_TYPE


class PlayerTeam(Base):
    __tablename__ = "player_teams"
    __table_args__ = (
        UniqueConstraint("player_id", "team_id", "season_id", name="uq_player_team_season"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    player_id: Mapped[int] = mapped_column(PLAYER_ID_SQL_TYPE, ForeignKey("players.id"))
    team_id: Mapped[int] = mapped_column(Integer, ForeignKey("teams.id"))
    season_id: Mapped[int] = mapped_column(Integer, ForeignKey("seasons.id"))
    number: Mapped[int | None] = mapped_column(Integer)

    # Relationships
    player: Mapped["Player"] = relationship("Player", back_populates="player_teams")
    team: Mapped["Team"] = relationship("Team", back_populates="player_teams")
    season: Mapped["Season"] = relationship("Season", back_populates="player_teams")
