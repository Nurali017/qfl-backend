from datetime import datetime
from uuid import UUID

from sqlalchemy import Integer, Numeric, DateTime, ForeignKey, UniqueConstraint, Index
from sqlalchemy.dialects.postgresql import UUID as PGUUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class PlayerSeasonStats(Base):
    """
    Aggregated player statistics for a season from SOTA API v2.

    All metrics from SOTA are stored as proper columns.
    extra_stats is kept only for potential future new fields.
    """

    __tablename__ = "player_season_stats"
    __table_args__ = (
        UniqueConstraint("player_id", "season_id", name="uq_player_season_stats"),
        Index("ix_player_season_stats_goals", "goals"),
        Index("ix_player_season_stats_assists", "assists"),
        Index("ix_player_season_stats_minutes", "minutes_played"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    player_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("players.id"), index=True
    )
    season_id: Mapped[int] = mapped_column(Integer, ForeignKey("seasons.id"), index=True)
    team_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("teams.id"), index=True)

    # Basic stats
    games_played: Mapped[int | None] = mapped_column(Integer)
    games_starting: Mapped[int | None] = mapped_column(Integer)
    games_as_subst: Mapped[int | None] = mapped_column(Integer)  # games as substitute
    games_be_subst: Mapped[int | None] = mapped_column(Integer)  # games being substituted
    games_unused: Mapped[int | None] = mapped_column(Integer)  # games on bench unused
    minutes_played: Mapped[int | None] = mapped_column(Integer)

    # Goals & Assists
    goals: Mapped[int | None] = mapped_column(Integer)
    assists: Mapped[int | None] = mapped_column(Integer)
    goal_and_assist: Mapped[int | None] = mapped_column(Integer)  # goals + assists
    goal_out_box: Mapped[int | None] = mapped_column(Integer)  # goals from outside box
    owngoal: Mapped[int | None] = mapped_column(Integer)
    penalty_success: Mapped[int | None] = mapped_column(Integer)
    xg: Mapped[float | None] = mapped_column(Numeric(6, 2))
    xg_per_90: Mapped[float | None] = mapped_column(Numeric(4, 2))

    # Shots
    shots: Mapped[int | None] = mapped_column(Integer)
    shots_on_goal: Mapped[int | None] = mapped_column(Integer)
    shots_blocked_opponent: Mapped[int | None] = mapped_column(Integer)

    # Passes
    passes: Mapped[int | None] = mapped_column(Integer)
    pass_accuracy: Mapped[float | None] = mapped_column(Numeric(5, 2))  # pass_ratio from API
    pass_acc: Mapped[int | None] = mapped_column(Integer)  # accurate passes count
    key_passes: Mapped[int | None] = mapped_column(Integer)
    pass_forward: Mapped[int | None] = mapped_column(Integer)
    pass_forward_ratio: Mapped[float | None] = mapped_column(Numeric(5, 2))
    pass_progressive: Mapped[int | None] = mapped_column(Integer)
    pass_cross: Mapped[int | None] = mapped_column(Integer)
    pass_cross_acc: Mapped[int | None] = mapped_column(Integer)
    pass_cross_ratio: Mapped[float | None] = mapped_column(Numeric(5, 2))
    pass_cross_per_90: Mapped[float | None] = mapped_column(Numeric(4, 2))
    pass_to_box: Mapped[int | None] = mapped_column(Integer)
    pass_to_box_ratio: Mapped[float | None] = mapped_column(Numeric(5, 2))
    pass_to_3rd: Mapped[int | None] = mapped_column(Integer)  # passes to final third
    pass_to_3rd_ratio: Mapped[float | None] = mapped_column(Numeric(5, 2))

    # Duels
    duels: Mapped[int | None] = mapped_column(Integer)
    duels_won: Mapped[int | None] = mapped_column(Integer)  # duel_success from API
    aerial_duel: Mapped[int | None] = mapped_column(Integer)
    aerial_duel_success: Mapped[int | None] = mapped_column(Integer)
    ground_duel: Mapped[int | None] = mapped_column(Integer)
    ground_duel_success: Mapped[int | None] = mapped_column(Integer)

    # Defense
    tackle: Mapped[int | None] = mapped_column(Integer)
    tackle_per_90: Mapped[float | None] = mapped_column(Numeric(4, 2))
    interception: Mapped[int | None] = mapped_column(Integer)
    recovery: Mapped[int | None] = mapped_column(Integer)

    # Dribbles
    dribble: Mapped[int | None] = mapped_column(Integer)
    dribble_success: Mapped[int | None] = mapped_column(Integer)
    dribble_per_90: Mapped[float | None] = mapped_column(Numeric(4, 2))

    # Other
    corner: Mapped[int | None] = mapped_column(Integer)
    offside: Mapped[int | None] = mapped_column(Integer)
    foul: Mapped[int | None] = mapped_column(Integer)  # fouls committed
    foul_taken: Mapped[int | None] = mapped_column(Integer)  # fouls received

    # Discipline
    yellow_cards: Mapped[int | None] = mapped_column(Integer)
    second_yellow_cards: Mapped[int | None] = mapped_column(Integer)
    red_cards: Mapped[int | None] = mapped_column(Integer)

    # Goalkeeper stats
    goals_conceded: Mapped[int | None] = mapped_column(Integer)
    goals_conceded_penalty: Mapped[int | None] = mapped_column(Integer)
    goals_conceeded_per_90: Mapped[float | None] = mapped_column(Numeric(4, 2))
    save_shot: Mapped[int | None] = mapped_column(Integer)
    save_shot_ratio: Mapped[float | None] = mapped_column(Numeric(5, 2))
    saved_shot_per_90: Mapped[float | None] = mapped_column(Numeric(4, 2))
    save_shot_penalty: Mapped[int | None] = mapped_column(Integer)
    save_shot_penalty_success: Mapped[int | None] = mapped_column(Integer)
    dry_match: Mapped[int | None] = mapped_column(Integer)  # clean sheets
    exit: Mapped[int | None] = mapped_column(Integer)  # keeper exits
    exit_success: Mapped[int | None] = mapped_column(Integer)

    # Extra stats for future unknown fields from API
    extra_stats: Mapped[dict | None] = mapped_column(JSONB)

    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    # Relationships
    player: Mapped["Player"] = relationship("Player", back_populates="season_stats")
    season: Mapped["Season"] = relationship("Season", back_populates="player_stats")
    team: Mapped["Team"] = relationship("Team", back_populates="player_season_stats")
