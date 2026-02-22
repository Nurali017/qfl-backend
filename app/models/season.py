from datetime import datetime, date
from sqlalchemy import Integer, String, Date, DateTime, ForeignKey, Boolean, JSON
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class Season(Base):
    __tablename__ = "seasons"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    legacy_id: Mapped[int | None] = mapped_column(Integer, unique=True, index=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)  # Russian (default)
    name_kz: Mapped[str | None] = mapped_column(String(100))
    name_en: Mapped[str | None] = mapped_column(String(100))
    championship_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("championships.id"), index=True
    )
    date_start: Mapped[date | None] = mapped_column(Date)
    date_end: Mapped[date | None] = mapped_column(Date)
    sync_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="true")
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    # Tournament configuration (managed via admin panel)
    frontend_code: Mapped[str | None] = mapped_column(String(20), index=True)
    tournament_type: Mapped[str | None] = mapped_column(String(30))
    tournament_format: Mapped[str | None] = mapped_column(String(30))
    has_table: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="false")
    has_bracket: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="false")
    sponsor_name: Mapped[str | None] = mapped_column(String(200))
    sponsor_name_kz: Mapped[str | None] = mapped_column(String(200))
    logo: Mapped[str | None] = mapped_column(String(500))
    current_round: Mapped[int | None] = mapped_column(Integer)
    total_rounds: Mapped[int | None] = mapped_column(Integer)
    sort_order: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    colors: Mapped[dict | None] = mapped_column(JSON)
    final_stage_ids: Mapped[list[int] | None] = mapped_column(JSON)

    # Relationships
    championship: Mapped["Championship"] = relationship("Championship", back_populates="seasons")
    games: Mapped[list["Game"]] = relationship("Game", back_populates="season")
    player_teams: Mapped[list["PlayerTeam"]] = relationship(
        "PlayerTeam", back_populates="season"
    )
    score_table_entries: Mapped[list["ScoreTable"]] = relationship(
        "ScoreTable", back_populates="season"
    )
    team_stats: Mapped[list["TeamSeasonStats"]] = relationship(
        "TeamSeasonStats", back_populates="season"
    )
    player_stats: Mapped[list["PlayerSeasonStats"]] = relationship(
        "PlayerSeasonStats", back_populates="season"
    )
    stages: Mapped[list["Stage"]] = relationship("Stage", back_populates="season")
