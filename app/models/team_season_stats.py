from datetime import datetime
from sqlalchemy import Integer, Numeric, ForeignKey, UniqueConstraint, DateTime, Index
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base
from app.utils.timestamps import utcnow


class TeamSeasonStats(Base):
    """
    Aggregated team statistics for a season from SOTA API v2.

    All metrics from SOTA are stored as proper columns.
    extra_stats is kept only for potential future new fields.
    """

    __tablename__ = "team_season_stats"
    __table_args__ = (
        UniqueConstraint("team_id", "season_id", name="uq_team_season_stats"),
        Index("ix_team_season_stats_points", "points"),
        Index("ix_team_season_stats_goal", "goal"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    team_id: Mapped[int] = mapped_column(Integer, ForeignKey("teams.id"), index=True)
    season_id: Mapped[int] = mapped_column(Integer, ForeignKey("seasons.id"), index=True)

    # Basic stats
    games_played: Mapped[int | None] = mapped_column(Integer)
    games_total: Mapped[int | None] = mapped_column(Integer)
    win: Mapped[int | None] = mapped_column(Integer)
    draw: Mapped[int | None] = mapped_column(Integer)
    match_loss: Mapped[int | None] = mapped_column(Integer)
    goal: Mapped[int | None] = mapped_column(Integer)
    goals_conceded: Mapped[int | None] = mapped_column(Integer)
    goals_difference: Mapped[int | None] = mapped_column(Integer)
    points: Mapped[int | None] = mapped_column(Integer)

    # Expected goals
    xg: Mapped[float | None] = mapped_column(Numeric(10, 2))
    xg_per_match: Mapped[float | None] = mapped_column(Numeric(10, 2))
    opponent_xg: Mapped[float | None] = mapped_column(Numeric(10, 2))

    # Shots
    shot: Mapped[int | None] = mapped_column(Integer)
    shots_on_goal: Mapped[int | None] = mapped_column(Integer)
    shots_on_goal_per_match: Mapped[float | None] = mapped_column(Numeric(10, 2))
    shots_off_goal: Mapped[int | None] = mapped_column(Integer)
    shot_per_90: Mapped[float | None] = mapped_column(Numeric(10, 2))
    shot_per_match: Mapped[float | None] = mapped_column(Numeric(10, 2))
    shot_to_goal: Mapped[float | None] = mapped_column(Numeric(10, 2))  # shots per goal
    goal_to_shot_ratio: Mapped[float | None] = mapped_column(Numeric(10, 2))
    shots_against_penalty: Mapped[int | None] = mapped_column(Integer)

    # Possession
    possession_percent_average: Mapped[float | None] = mapped_column(Numeric(10, 2))

    # Passes
    passes: Mapped[int | None] = mapped_column(Integer)
    pass_per_match: Mapped[float | None] = mapped_column(Numeric(10, 2))
    pass_ratio: Mapped[float | None] = mapped_column(Numeric(10, 2))
    pass_forward: Mapped[int | None] = mapped_column(Integer)
    pass_forward_per_match: Mapped[float | None] = mapped_column(Numeric(10, 2))
    pass_forward_ratio: Mapped[float | None] = mapped_column(Numeric(10, 2))
    pass_long: Mapped[int | None] = mapped_column(Integer)
    pass_long_per_match: Mapped[float | None] = mapped_column(Numeric(10, 2))
    pass_long_ratio: Mapped[float | None] = mapped_column(Numeric(10, 2))
    pass_progressive: Mapped[int | None] = mapped_column(Integer)
    pass_progressive_per_match: Mapped[float | None] = mapped_column(Numeric(10, 2))
    pass_cross: Mapped[int | None] = mapped_column(Integer)
    pass_cross_per_match: Mapped[float | None] = mapped_column(Numeric(10, 2))
    pass_cross_ratio: Mapped[float | None] = mapped_column(Numeric(10, 2))
    pass_to_box: Mapped[int | None] = mapped_column(Integer)
    pass_to_box_per_match: Mapped[float | None] = mapped_column(Numeric(10, 2))
    pass_to_box_ratio: Mapped[float | None] = mapped_column(Numeric(10, 2))
    pass_to_3rd: Mapped[int | None] = mapped_column(Integer)
    pass_to_3rd_acc: Mapped[int | None] = mapped_column(Integer)
    pass_to_3rd_per_match: Mapped[float | None] = mapped_column(Numeric(10, 2))
    pass_to_3rd_ratio: Mapped[float | None] = mapped_column(Numeric(10, 2))
    key_pass: Mapped[int | None] = mapped_column(Integer)
    key_pass_per_match: Mapped[float | None] = mapped_column(Numeric(10, 2))
    key_pass_ratio: Mapped[float | None] = mapped_column(Numeric(10, 2))
    goal_pass: Mapped[int | None] = mapped_column(Integer)  # assists

    # Free kicks
    freekick_pass: Mapped[int | None] = mapped_column(Integer)
    freekick_pass_per_match: Mapped[float | None] = mapped_column(Numeric(10, 2))
    freekick_shot: Mapped[int | None] = mapped_column(Integer)
    freekick_shot_per_match: Mapped[float | None] = mapped_column(Numeric(10, 2))

    # Duels
    duel: Mapped[int | None] = mapped_column(Integer)
    duel_per_match: Mapped[float | None] = mapped_column(Numeric(10, 2))
    duel_ratio: Mapped[float | None] = mapped_column(Numeric(10, 2))
    aerial_duel_offence: Mapped[int | None] = mapped_column(Integer)
    aerial_duel_offence_per_match: Mapped[float | None] = mapped_column(Numeric(10, 2))
    aerial_duel_offence_ratio: Mapped[float | None] = mapped_column(Numeric(10, 2))
    aerial_duel_defence: Mapped[int | None] = mapped_column(Integer)
    aerial_duel_defence_per_match: Mapped[float | None] = mapped_column(Numeric(10, 2))
    aerial_duel_defence_ratio: Mapped[float | None] = mapped_column(Numeric(10, 2))
    ground_duel_offence: Mapped[int | None] = mapped_column(Integer)
    ground_duel_offence_per_match: Mapped[float | None] = mapped_column(Numeric(10, 2))
    ground_duel_offence_ratio: Mapped[float | None] = mapped_column(Numeric(10, 2))
    ground_duel_defence: Mapped[int | None] = mapped_column(Integer)
    ground_duel_defence_per_match: Mapped[float | None] = mapped_column(Numeric(10, 2))
    ground_duel_defence_ratio: Mapped[float | None] = mapped_column(Numeric(10, 2))

    # Defense
    tackle: Mapped[int | None] = mapped_column(Integer)
    tackle_per_match: Mapped[float | None] = mapped_column(Numeric(10, 2))
    tackle1_1: Mapped[int | None] = mapped_column(Integer)  # tackle1-1 from API
    tackle1_1_per_match: Mapped[int | None] = mapped_column(Integer)  # total count, not per-match
    tackle1_1_ratio: Mapped[float | None] = mapped_column(Numeric(10, 2))
    interception: Mapped[int | None] = mapped_column(Integer)
    interception_per_match: Mapped[float | None] = mapped_column(Numeric(10, 2))
    recovery: Mapped[int | None] = mapped_column(Integer)
    recovery_per_match: Mapped[float | None] = mapped_column(Numeric(10, 2))

    # Dribbles
    dribble: Mapped[int | None] = mapped_column(Integer)
    dribble_per_match: Mapped[float | None] = mapped_column(Numeric(10, 2))
    dribble_ratio: Mapped[float | None] = mapped_column(Numeric(10, 2))

    # Penalties
    penalty: Mapped[int | None] = mapped_column(Integer)
    penalty_ratio: Mapped[float | None] = mapped_column(Numeric(10, 2))
    save_penalty_ratio: Mapped[float | None] = mapped_column(Numeric(10, 2))

    # Discipline
    foul: Mapped[int | None] = mapped_column(Integer)
    foul_taken: Mapped[int | None] = mapped_column(Integer)
    yellow_cards: Mapped[int | None] = mapped_column(Integer)
    second_yellow_cards: Mapped[int | None] = mapped_column(Integer)
    red_cards: Mapped[int | None] = mapped_column(Integer)

    # Set pieces
    corner: Mapped[int | None] = mapped_column(Integer)
    corner_per_match: Mapped[float | None] = mapped_column(Numeric(10, 2))
    offside: Mapped[int | None] = mapped_column(Integer)
    out: Mapped[int | None] = mapped_column(Integer)

    # Visitors
    visitor_total: Mapped[int | None] = mapped_column(Integer)
    average_visitors: Mapped[float | None] = mapped_column(Numeric(10, 2))

    # Extra stats for future unknown fields from API
    extra_stats: Mapped[dict | None] = mapped_column(JSONB)

    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=utcnow, onupdate=utcnow
    )

    # Relationships
    team: Mapped["Team"] = relationship("Team", back_populates="season_stats")
    season: Mapped["Season"] = relationship("Season", back_populates="team_stats")
