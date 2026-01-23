from datetime import datetime
import enum
from sqlalchemy import Integer, String, DateTime, Enum as SQLEnum, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class CoachRole(str, enum.Enum):
    """Coach role/position."""
    head_coach = "head_coach"
    assistant = "assistant"
    goalkeeper_coach = "goalkeeper_coach"
    fitness_coach = "fitness_coach"
    other = "other"


class Coach(Base):
    """Team coach/staff member."""
    __tablename__ = "coaches"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    first_name: Mapped[str] = mapped_column(String(100), nullable=False)
    last_name: Mapped[str] = mapped_column(String(100), nullable=False)
    first_name_kz: Mapped[str | None] = mapped_column(String(100))
    first_name_ru: Mapped[str | None] = mapped_column(String(100))
    first_name_en: Mapped[str | None] = mapped_column(String(100))
    last_name_kz: Mapped[str | None] = mapped_column(String(100))
    last_name_ru: Mapped[str | None] = mapped_column(String(100))
    last_name_en: Mapped[str | None] = mapped_column(String(100))
    photo_url: Mapped[str | None] = mapped_column(String(500))
    country_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("countries.id"), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    # Relationships
    country: Mapped["Country"] = relationship("Country", back_populates="coaches")
    team_assignments: Mapped[list["TeamCoach"]] = relationship(
        "TeamCoach", back_populates="coach"
    )


class TeamCoach(Base):
    """Association between team and coach for a season."""
    __tablename__ = "team_coaches"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    team_id: Mapped[int] = mapped_column(Integer, ForeignKey("teams.id"), nullable=False)
    coach_id: Mapped[int] = mapped_column(Integer, ForeignKey("coaches.id"), nullable=False)
    season_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("seasons.id"))
    role: Mapped[CoachRole] = mapped_column(
        SQLEnum(CoachRole, name='coachrole', create_type=False),
        default=CoachRole.head_coach
    )
    is_active: Mapped[bool] = mapped_column(default=True)
    start_date: Mapped[datetime | None] = mapped_column(DateTime)
    end_date: Mapped[datetime | None] = mapped_column(DateTime)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    # Relationships
    team: Mapped["Team"] = relationship("Team", back_populates="coaches")
    coach: Mapped["Coach"] = relationship("Coach", back_populates="team_assignments")
    season: Mapped["Season"] = relationship("Season")
