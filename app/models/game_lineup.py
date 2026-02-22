import enum
from sqlalchemy import Integer, Boolean, String, ForeignKey, Enum as SQLEnum, UniqueConstraint, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base
from app.models.sql_types import PLAYER_ID_SQL_TYPE, GAME_ID_SQL_TYPE


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
    game_id: Mapped[int] = mapped_column(
        GAME_ID_SQL_TYPE, ForeignKey("games.id"), nullable=False, index=True
    )
    team_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("teams.id"), nullable=False, index=True
    )
    player_id: Mapped[int] = mapped_column(
        PLAYER_ID_SQL_TYPE, ForeignKey("players.id"), nullable=False, index=True
    )

    lineup_type: Mapped[LineupType] = mapped_column(
        SQLEnum(LineupType, name='lineuptype', create_type=False),
        nullable=False, default=LineupType.starter
    )
    shirt_number: Mapped[int | None] = mapped_column(Integer)
    is_captain: Mapped[bool] = mapped_column(Boolean, default=False)

    # Match-specific position from SOTA live endpoint
    amplua: Mapped[str | None] = mapped_column(String(10))  # Gk, D, DM, M, AM, F
    field_position: Mapped[str | None] = mapped_column(String(5))  # C, L, R, LC, RC

    # Relationships
    game: Mapped["Game"] = relationship("Game", back_populates="lineups")
    team: Mapped["Team"] = relationship("Team")
    player: Mapped["Player"] = relationship("Player", back_populates="game_lineups")
