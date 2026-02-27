from datetime import date
from datetime import time as time_type
from pydantic import BaseModel

from app.schemas.team import TeamInGame, TeamStadiumInfo, TeamWithScore


class GameBase(BaseModel):
    id: int
    date: date
    time: time_type | None = None
    tour: int | None = None
    season_id: int | None = None
    stage_id: int | None = None
    home_score: int | None = None
    away_score: int | None = None
    home_penalty_score: int | None = None
    away_penalty_score: int | None = None
    has_stats: bool = False
    is_technical: bool = False
    is_schedule_tentative: bool = False
    visitors: int | None = None
    video_url: str | None = None
    protocol_url: str | None = None


class GameResponse(GameBase):
    home_team: TeamInGame | None = None
    away_team: TeamInGame | None = None
    season_name: str | None = None
    tournament_name: str | None = None
    stage_name: str | None = None
    status: str | None = None
    has_score: bool = False

    class Config:
        from_attributes = True


class GameListResponse(BaseModel):
    items: list[GameResponse]
    total: int


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
    stadium: str | None = None  # Keep for SOTA ingestion


# Match Center Schemas

class StadiumInfo(BaseModel):
    """Stadium information for match center display."""
    id: int | None = None
    name: str | None = None
    city: str | None = None
    capacity: int | None = None
    address: str | None = None
    photo_url: str | None = None

    class Config:
        from_attributes = True


class TeamInMatchCenter(BaseModel):
    """Team information for match center with localized fields and colors."""
    id: int
    name: str
    name_kz: str | None = None
    name_en: str | None = None
    logo_url: str | None = None
    primary_color: str | None = None
    secondary_color: str | None = None
    accent_color: str | None = None

    class Config:
        from_attributes = True


class MatchCenterGame(BaseModel):
    """Match information for match center display."""
    id: int
    date: date
    time: time_type | None = None
    tour: int | None = None
    season_id: int | None = None

    home_team: TeamInMatchCenter | None = None
    away_team: TeamInMatchCenter | None = None

    home_score: int | None = None
    away_score: int | None = None
    home_penalty_score: int | None = None
    away_penalty_score: int | None = None

    # Stage info
    stage_id: int | None = None
    stage_name: str | None = None

    stadium: StadiumInfo | None = None
    visitors: int | None = None

    # Status indicators
    is_live: bool = False
    has_stats: bool = False
    has_lineup: bool = False
    is_technical: bool = False
    is_schedule_tentative: bool = False
    is_featured: bool = False

    # Computed status field
    status: str  # "upcoming", "live", or "finished"

    has_score: bool = False

    # Optional ticket URL
    ticket_url: str | None = None

    # Optional video replay URL
    video_url: str | None = None

    # Optional match protocol URL (PDF)
    protocol_url: str | None = None

    class Config:
        from_attributes = True


class MatchCenterDateGroup(BaseModel):
    """Group of matches for a single date."""
    date: date
    date_label: str
    games: list[MatchCenterGame]


class MatchCenterResponse(BaseModel):
    """Response format with games grouped by date."""
    groups: list[MatchCenterDateGroup]
    total: int
    tentative_tour_dates: dict[int, list[str]] = {}


# Per-endpoint response schemas

class GameListItem(BaseModel):
    """Game item in /games standard list response."""
    id: int
    date: date
    time: time_type | None = None
    tour: int | None = None
    season_id: int | None = None
    stage_id: int | None = None
    stage_name: str | None = None
    home_score: int | None = None
    away_score: int | None = None
    home_penalty_score: int | None = None
    away_penalty_score: int | None = None
    has_stats: bool = False
    has_lineup: bool = False
    is_live: bool = False
    is_technical: bool = False
    is_schedule_tentative: bool = False
    is_featured: bool = False
    visitors: int | None = None
    status: str
    has_score: bool = False
    ticket_url: str | None = None
    video_url: str | None = None
    protocol_url: str | None = None
    where_broadcast: str | None = None
    video_review_url: str | None = None
    home_team: TeamInMatchCenter | None = None
    away_team: TeamInMatchCenter | None = None
    stadium_info: StadiumInfo | None = None
    season_name: str | None = None


class GameDetailItem(BaseModel):
    """Game detail response for /games/{id}."""
    id: int
    date: date
    time: time_type | None = None
    tour: int | None = None
    season_id: int | None = None
    stage_id: int | None = None
    stage_name: str | None = None
    home_score: int | None = None
    away_score: int | None = None
    home_penalty_score: int | None = None
    away_penalty_score: int | None = None
    has_stats: bool = False
    has_lineup: bool = False
    is_live: bool = False
    is_technical: bool = False
    is_schedule_tentative: bool = False
    is_featured: bool = False
    stadium: StadiumInfo | None = None
    referee: str | None = None
    visitors: int | None = None
    ticket_url: str | None = None
    video_url: str | None = None
    protocol_url: str | None = None
    where_broadcast: str | None = None
    video_review_url: str | None = None
    status: str
    has_score: bool = False
    home_team: TeamInGame | None = None
    away_team: TeamInGame | None = None
    season_name: str | None = None


class SeasonGameItem(BaseModel):
    """Game item for /seasons/{id}/games."""
    id: int
    date: date
    time: time_type | None = None
    tour: int | None = None
    season_id: int | None = None
    home_score: int | None = None
    away_score: int | None = None
    has_stats: bool = False
    is_schedule_tentative: bool = False
    stadium: str | None = None
    visitors: int | None = None
    home_team: TeamWithScore | None = None
    away_team: TeamWithScore | None = None
    season_name: str | None = None


class StageGameItem(BaseModel):
    """Game item for /seasons/{id}/stages/{stage_id}/games."""
    id: int
    date: date
    time: time_type | None = None
    tour: int | None = None
    season_id: int | None = None
    stage_id: int | None = None
    home_score: int | None = None
    away_score: int | None = None
    home_penalty_score: int | None = None
    away_penalty_score: int | None = None
    has_stats: bool = False
    stadium: str | None = None
    visitors: int | None = None
    home_team: TeamWithScore | None = None
    away_team: TeamWithScore | None = None
    season_name: str | None = None


class TeamGameItem(BaseModel):
    """Game item for /teams/{id}/games."""
    id: int
    date: date
    time: time_type | None = None
    tour: int | None = None
    season_id: int | None = None
    home_score: int | None = None
    away_score: int | None = None
    has_stats: bool = False
    stadium: TeamStadiumInfo | None = None
    visitors: int | None = None
    home_team: TeamWithScore | None = None
    away_team: TeamWithScore | None = None
    season_name: str | None = None
