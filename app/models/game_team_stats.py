from sqlalchemy import Integer, Numeric, ForeignKey, UniqueConstraint, Index
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base
from app.models.sql_types import GAME_ID_SQL_TYPE


class GameTeamStats(Base):
    """
    Team statistics for a single game from SOTA API.

    All metrics from SOTA are stored as proper columns.
    extra_stats is kept only for potential future new fields.
    """

    __tablename__ = "game_team_stats"
    __table_args__ = (
        UniqueConstraint("game_id", "team_id", name="uq_game_team_stats"),
        Index("ix_game_team_stats_game_id", "game_id"),
        Index("ix_game_team_stats_team_id", "team_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    game_id: Mapped[int] = mapped_column(GAME_ID_SQL_TYPE, ForeignKey("games.id"))
    team_id: Mapped[int] = mapped_column(Integer, ForeignKey("teams.id"))

    # Possession
    possession: Mapped[float | None] = mapped_column(Numeric(5, 2))
    possession_percent: Mapped[int | None] = mapped_column(Integer)

    # Shots
    shots: Mapped[int | None] = mapped_column(Integer)  # shot from API
    shots_on_goal: Mapped[int | None] = mapped_column(Integer)
    shots_off_goal: Mapped[int | None] = mapped_column(Integer)

    # Passes
    passes: Mapped[int | None] = mapped_column(Integer)  # pass from API
    pass_accuracy: Mapped[float | None] = mapped_column(Numeric(5, 2))

    # Discipline
    fouls: Mapped[int | None] = mapped_column(Integer)  # foul from API
    yellow_cards: Mapped[int | None] = mapped_column(Integer)
    red_cards: Mapped[int | None] = mapped_column(Integer)

    # Set pieces
    corners: Mapped[int | None] = mapped_column(Integer)  # corner from API
    offsides: Mapped[int | None] = mapped_column(Integer)  # offside from API

    # Extended stats (from SOTA live endpoint)
    shots_on_bar: Mapped[int | None] = mapped_column(Integer)
    shots_blocked: Mapped[int | None] = mapped_column(Integer)
    penalties: Mapped[int | None] = mapped_column(Integer)
    saves: Mapped[int | None] = mapped_column(Integer)

    # New metrics from SOTA v1 /games/{id}/teams/ endpoint
    minutes: Mapped[int | None] = mapped_column(Integer)
    xg: Mapped[float | None] = mapped_column(Numeric(6, 2))
    freekicks: Mapped[int | None] = mapped_column(Integer)
    freekick_shots: Mapped[int | None] = mapped_column(Integer)
    freekick_passes: Mapped[int | None] = mapped_column(Integer)
    throw_ins: Mapped[int | None] = mapped_column(Integer)
    goal_kicks: Mapped[int | None] = mapped_column(Integer)
    assists: Mapped[int | None] = mapped_column(Integer)
    passes_forward: Mapped[int | None] = mapped_column(Integer)
    passes_progressive: Mapped[int | None] = mapped_column(Integer)
    key_passes: Mapped[int | None] = mapped_column(Integer)
    passes_to_final_third: Mapped[int | None] = mapped_column(Integer)
    passes_to_box: Mapped[int | None] = mapped_column(Integer)
    crosses: Mapped[int | None] = mapped_column(Integer)

    # Ratio metrics (0-100%)
    shot_accuracy: Mapped[float | None] = mapped_column(Numeric(5, 2))
    corner_accuracy: Mapped[float | None] = mapped_column(Numeric(5, 2))
    freekick_shot_accuracy: Mapped[float | None] = mapped_column(Numeric(5, 2))
    freekick_pass_accuracy: Mapped[float | None] = mapped_column(Numeric(5, 2))
    throw_in_accuracy: Mapped[float | None] = mapped_column(Numeric(5, 2))
    goal_kick_accuracy: Mapped[float | None] = mapped_column(Numeric(5, 2))
    penalty_accuracy: Mapped[float | None] = mapped_column(Numeric(5, 2))

    # Extra stats for future unknown fields from API
    extra_stats: Mapped[dict | None] = mapped_column(JSONB)

    # Relationships
    game: Mapped["Game"] = relationship("Game", back_populates="team_stats")
    team: Mapped["Team"] = relationship("Team", back_populates="game_stats")
