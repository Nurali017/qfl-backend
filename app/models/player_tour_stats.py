from datetime import datetime

from sqlalchemy import Integer, Numeric, DateTime, ForeignKey, UniqueConstraint, Index
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base
from app.models.sql_types import PLAYER_ID_SQL_TYPE
from app.utils.timestamps import utcnow


class PlayerTourStats(Base):
    """
    Cumulative player statistics per tour from SOTA API v2.

    Each row = one player's stats accumulated through a given tour.
    Used as the authoritative source for max_round trend queries
    instead of aggregating per-game GamePlayerStats.
    """

    __tablename__ = "player_tour_stats"
    __table_args__ = (
        UniqueConstraint("player_id", "season_id", "tour", name="uq_player_tour_stats"),
        Index("ix_player_tour_stats_season_tour", "season_id", "tour"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    player_id: Mapped[int] = mapped_column(PLAYER_ID_SQL_TYPE, ForeignKey("players.id"), index=True)
    season_id: Mapped[int] = mapped_column(Integer, ForeignKey("seasons.id"), index=True)
    team_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("teams.id"), index=True)
    tour: Mapped[int] = mapped_column(Integer, nullable=False)

    # Basic stats
    games_played: Mapped[int | None] = mapped_column(Integer)
    time_on_field_total: Mapped[int | None] = mapped_column(Integer)

    # Goals & Assists
    goal: Mapped[int | None] = mapped_column(Integer)
    goal_pass: Mapped[int | None] = mapped_column(Integer)

    # Shots
    shot: Mapped[int | None] = mapped_column(Integer)

    # Passes
    passes: Mapped[int | None] = mapped_column(Integer)
    pass_ratio: Mapped[float | None] = mapped_column(Numeric(5, 2))

    # xG
    xg: Mapped[float | None] = mapped_column(Numeric(6, 2))

    # Duels
    duel: Mapped[int | None] = mapped_column(Integer)

    # Defense
    tackle: Mapped[int | None] = mapped_column(Integer)

    # Discipline
    yellow_cards: Mapped[int | None] = mapped_column(Integer)
    red_cards: Mapped[int | None] = mapped_column(Integer)

    # Extra stats for the remaining ~35 v2 keys
    extra_stats: Mapped[dict | None] = mapped_column(JSONB)

    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=utcnow, onupdate=utcnow
    )

    # Relationships
    player: Mapped["Player"] = relationship("Player", back_populates="tour_stats")
    season: Mapped["Season"] = relationship("Season", back_populates="player_tour_stats")
    team: Mapped["Team"] = relationship("Team", back_populates="player_tour_stats")
