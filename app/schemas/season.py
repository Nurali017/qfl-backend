from datetime import date
from pydantic import BaseModel


class SeasonBase(BaseModel):
    id: int
    name: str
    tournament_id: int | None = None
    date_start: date | None = None
    date_end: date | None = None


class SeasonResponse(SeasonBase):
    tournament_name: str | None = None

    class Config:
        from_attributes = True


class SeasonListResponse(BaseModel):
    items: list[SeasonResponse]
    total: int


class SeasonFromSOTA(BaseModel):
    id: int
    name: str
    tournament_id: int | None = None
    tournament_name: str | None = None
    date_start: date | None = None
    date_end: date | None = None


class SeasonStatisticsResponse(BaseModel):
    """Aggregated statistics for a season (tournament-level)."""
    season_id: int
    season_name: str | None = None

    # Match results
    matches_played: int = 0
    wins: int = 0
    draws: int = 0

    # Attendance
    total_attendance: int = 0
    average_attendance: float = 0.0

    # Goals
    total_goals: int = 0
    goals_per_match: float = 0.0

    # Penalties
    penalties: int = 0
    penalties_scored: int = 0

    # Fouls
    fouls_per_match: float = 0.0

    # Cards
    yellow_cards: int = 0
    second_yellow_cards: int = 0
    red_cards: int = 0

    class Config:
        from_attributes = True


class GoalPeriodItem(BaseModel):
    """Goals grouped by minute period."""
    period: str
    goals: int = 0
    home: int = 0
    away: int = 0


class GoalsByPeriodMeta(BaseModel):
    """Data quality metadata for goals-by-period chart."""
    matches_played: int = 0
    matches_with_goal_events: int = 0
    coverage_pct: float = 0.0


class SeasonGoalsByPeriodResponse(BaseModel):
    """Goals by minute buckets for a season."""
    season_id: int
    period_size_minutes: int = 15
    periods: list[GoalPeriodItem]
    meta: GoalsByPeriodMeta
