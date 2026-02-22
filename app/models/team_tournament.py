from datetime import datetime
from sqlalchemy import Integer, String, DateTime, Boolean, ForeignKey, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class TeamTournament(Base):
    """Team-to-tournament/season assignment.

    Tracks which teams participate in which season, with group assignments
    and disciplinary data (disqualifications, fine points).
    Maps to legacy MySQL `commands_by_tournaments` table.
    """
    __tablename__ = "team_tournaments"
    __table_args__ = (
        UniqueConstraint("team_id", "season_id", name="uq_team_tournament_season"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    legacy_id: Mapped[int | None] = mapped_column(Integer, unique=True, index=True)
    team_id: Mapped[int] = mapped_column(Integer, ForeignKey("teams.id"), nullable=False)
    season_id: Mapped[int] = mapped_column(Integer, ForeignKey("seasons.id"), nullable=False)
    group_name: Mapped[str | None] = mapped_column(String(50))  # "A", "B", etc.
    is_disqualified: Mapped[bool] = mapped_column(Boolean, default=False)
    fine_points: Mapped[int] = mapped_column(Integer, default=0)
    stadium_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("stadiums.id"))
    sort_order: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    # Relationships
    team: Mapped["Team"] = relationship("Team")
    season: Mapped["Season"] = relationship("Season")
    stadium: Mapped["Stadium"] = relationship("Stadium")
