from uuid import UUID
import enum
from sqlalchemy import Integer, Boolean, ForeignKey, Enum as SQLEnum, UniqueConstraint, Index
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class LineupType(str, enum.Enum):
    """Type of lineup entry."""
    starter = "starter"  # Starting 11
    substitute = "substitute"  # On the bench


class GameLineup(Base):
    """
    Player lineup for a game.
    Stores which players started and which were on the bench.
    """
    __tablename__ = "game_lineups"
    __table_args__ = (
        UniqueConstraint("game_id", "player_id", name="uq_game_lineup_player"),
        Index("ix_game_lineup_game_team", "game_id", "team_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    game_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("games.id"), nullable=False, index=True
    )
    team_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("teams.id"), nullable=False, index=True
    )
    player_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("players.id"), nullable=False, index=True
    )

    lineup_type: Mapped[LineupType] = mapped_column(
        SQLEnum(LineupType, name='lineuptype', create_type=False),
        nullable=False, default=LineupType.starter
    )
    shirt_number: Mapped[int | None] = mapped_column(Integer)
    is_captain: Mapped[bool] = mapped_column(Boolean, default=False)

    # Relationships
    game: Mapped["Game"] = relationship("Game", back_populates="lineups")
    team: Mapped["Team"] = relationship("Team")
    player: Mapped["Player"] = relationship("Player", back_populates="game_lineups")
