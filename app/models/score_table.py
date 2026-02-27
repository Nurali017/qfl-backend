from datetime import datetime
from sqlalchemy import Integer, String, Text, DateTime, ForeignKey, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base
from app.utils.timestamps import utcnow


class ScoreTable(Base):
    __tablename__ = "score_table"
    __table_args__ = (
        UniqueConstraint("season_id", "team_id", name="uq_score_table_season_team"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    season_id: Mapped[int] = mapped_column(Integer, ForeignKey("seasons.id"))
    team_id: Mapped[int] = mapped_column(Integer, ForeignKey("teams.id"))
    position: Mapped[int | None] = mapped_column(Integer)
    games_played: Mapped[int | None] = mapped_column(Integer)
    wins: Mapped[int | None] = mapped_column(Integer)
    draws: Mapped[int | None] = mapped_column(Integer)
    losses: Mapped[int | None] = mapped_column(Integer)
    goals_scored: Mapped[int | None] = mapped_column(Integer)
    goals_conceded: Mapped[int | None] = mapped_column(Integer)
    goal_difference: Mapped[int | None] = mapped_column(Integer)
    points: Mapped[int | None] = mapped_column(Integer)
    form: Mapped[str | None] = mapped_column(String(20))
    note: Mapped[str | None] = mapped_column(Text)  # Reason/note for point deductions etc. (В-9)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=utcnow, onupdate=utcnow
    )

    # Relationships
    season: Mapped["Season"] = relationship("Season", back_populates="score_table_entries")
    team: Mapped["Team"] = relationship("Team", back_populates="score_table_entries")
