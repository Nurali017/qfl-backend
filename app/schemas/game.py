from datetime import date
from datetime import time as time_type
from uuid import UUID
from pydantic import BaseModel

from app.schemas.team import TeamInGame
from app.utils.file_urls import FileUrl


class GameBase(BaseModel):
    id: UUID
    date: date
    time: time_type | None = None
    tour: int | None = None
    season_id: int | None = None
    home_score: int | None = None
    away_score: int | None = None
    has_stats: bool = False
    stadium: str | None = None
    visitors: int | None = None
    video_url: str | None = None


class GameResponse(GameBase):
    home_team: TeamInGame | None = None
    away_team: TeamInGame | None = None
    season_name: str | None = None
    tournament_name: str | None = None
    status: str | None = None
    has_score: bool = False

    class Config:
        from_attributes = True


class GameListResponse(BaseModel):
    items: list[GameResponse]
    total: int


class GameDetailResponse(GameResponse):
    pass


class HomeAwayTeamFromSOTA(BaseModel):
    id: int
    name: str
    logo: str | None = None
    score: int | None = None


class GameFromSOTA(BaseModel):
    id: str
    date: date
    time: time_type | None = None
    tournament_id: int | None = None
    tournament_name: str | None = None
    home_team: HomeAwayTeamFromSOTA | None = None
    away_team: HomeAwayTeamFromSOTA | None = None
    tour: int | None = None
    has_stats: bool = False
    season_id: int | None = None
    season_name: str | None = None
    visitors: int | None = None
    stadium: str | None = None


# Match Center Schemas

class StadiumInfo(BaseModel):
    """Stadium information for match center display."""
    id: int | None = None
    name: str | None = None
    city: str | None = None
    capacity: int | None = None

    class Config:
        from_attributes = True


class TeamInMatchCenter(BaseModel):
    """Team information for match center with localized fields."""
    id: int
    name: str
    name_kz: str | None = None
    name_en: str | None = None
    logo_url: FileUrl = None

    class Config:
        from_attributes = True


class MatchCenterGame(BaseModel):
    """Match information for match center display."""
    id: UUID
    date: date
    time: time_type | None = None
    tour: int | None = None
    season_id: int | None = None

    home_team: TeamInMatchCenter
    away_team: TeamInMatchCenter

    home_score: int | None = None
    away_score: int | None = None

    stadium: StadiumInfo | None = None
    visitors: int | None = None

    # Status indicators
    is_live: bool = False
    has_stats: bool = False
    has_lineup: bool = False

    # Computed status field
    status: str  # "upcoming", "live", or "finished"

    # Optional ticket URL
    ticket_url: str | None = None

    # Optional video replay URL
    video_url: str | None = None

    class Config:
        from_attributes = True


class MatchCenterDateGroup(BaseModel):
    """Group of matches for a single date."""
    date: date
    date_label: str  # Formatted date string for display (e.g., "Пятница, 27 февраля 2026")
    games: list[MatchCenterGame]


class MatchCenterResponse(BaseModel):
    """Response format with games grouped by date."""
    groups: list[MatchCenterDateGroup]
    total: int
