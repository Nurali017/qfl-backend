from uuid import UUID
from sqlalchemy import Integer, ForeignKey, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class PlayerTeam(Base):
    __tablename__ = "player_teams"
    __table_args__ = (
        UniqueConstraint("player_id", "team_id", "season_id", name="uq_player_team_season"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    player_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("players.id")
    )
    team_id: Mapped[int] = mapped_column(Integer, ForeignKey("teams.id"))
    season_id: Mapped[int] = mapped_column(Integer, ForeignKey("seasons.id"))
    number: Mapped[int | None] = mapped_column(Integer)

    # Relationships
    player: Mapped["Player"] = relationship("Player", back_populates="player_teams")
    team: Mapped["Team"] = relationship("Team", back_populates="player_teams")
    season: Mapped["Season"] = relationship("Season", back_populates="player_teams")
