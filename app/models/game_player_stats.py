from sqlalchemy import Integer, String, Boolean, Numeric, ForeignKey, UniqueConstraint, Index
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base
from app.models.sql_types import PLAYER_ID_SQL_TYPE, GAME_ID_SQL_TYPE


class GamePlayerStats(Base):
    """
    Player statistics for a single game from SOTA API.

    All metrics from SOTA are stored as proper columns.
    extra_stats is kept only for potential future new fields.
    """

    __tablename__ = "game_player_stats"
    __table_args__ = (
        UniqueConstraint("game_id", "player_id", name="uq_game_player_stats"),
        Index("ix_game_player_stats_team_id", "team_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    game_id: Mapped[int] = mapped_column(GAME_ID_SQL_TYPE, ForeignKey("games.id"), index=True)
    player_id: Mapped[int] = mapped_column(PLAYER_ID_SQL_TYPE, ForeignKey("players.id"), index=True)
    team_id: Mapped[int] = mapped_column(Integer, ForeignKey("teams.id"))

    # Basic info
    minutes_played: Mapped[int | None] = mapped_column(Integer)
    started: Mapped[bool | None] = mapped_column(Boolean)
    position: Mapped[str | None] = mapped_column(String(20))

    # NOTE: goals and assists are calculated from game_events table
    # to avoid data duplication (single source of truth)

    # Shots
    shots: Mapped[int] = mapped_column(Integer, default=0)  # shot from API
    shots_on_goal: Mapped[int] = mapped_column(Integer, default=0)
    shots_off_goal: Mapped[int] = mapped_column(Integer, default=0)

    # Passes
    passes: Mapped[int] = mapped_column(Integer, default=0)  # pass from API
    pass_accuracy: Mapped[float | None] = mapped_column(Numeric(5, 2))

    # Duels
    duel: Mapped[int] = mapped_column(Integer, default=0)

    # Defense
    tackle: Mapped[int] = mapped_column(Integer, default=0)

    # Other
    corner: Mapped[int] = mapped_column(Integer, default=0)
    offside: Mapped[int] = mapped_column(Integer, default=0)
    foul: Mapped[int] = mapped_column(Integer, default=0)

    # Discipline
    yellow_cards: Mapped[int] = mapped_column(Integer, default=0)
    red_cards: Mapped[int] = mapped_column(Integer, default=0)

    # Extra stats for future unknown fields from API
    extra_stats: Mapped[dict | None] = mapped_column(JSONB)

    # Relationships
    game: Mapped["Game"] = relationship("Game", back_populates="player_stats")
    player: Mapped["Player"] = relationship("Player", back_populates="game_stats")
    team: Mapped["Team"] = relationship("Team", back_populates="player_game_stats")
