from datetime import date, datetime
from datetime import time as time_type
from typing import Annotated, Literal, Optional
from pydantic import BaseModel, PlainSerializer

# Serialize time as "HH:MM" (no seconds)
ShortTime = Annotated[time_type, PlainSerializer(lambda v: v.strftime("%H:%M"), return_type=str)]

from app.schemas.team import TeamInGame, TeamStadiumInfo, TeamWithScore


class BroadcasterInfo(BaseModel):
    """Broadcaster info included in game responses."""
    id: int
    name: str
    logo_url: str | None = None
    type: str | None = None
    website: str | None = None

    class Config:
        from_attributes = True


LivePhase = Literal["in_progress", "halftime"]
DecidedIn = Literal["regular", "extra_time", "penalties"]


class GameBase(BaseModel):
    id: int
    date: date
    time: ShortTime | None = None
    tour: int | None = None
    season_id: int | None = None
    stage_id: int | None = None
    home_score: int | None = None
    away_score: int | None = None
    home_penalty_score: int | None = None
    away_penalty_score: int | None = None
    # How the match was decided. Populated for finished knockout games:
    # 'regular' = full-time, 'extra_time' = decided in ET, 'penalties' = shootout.
    # None for games where it's unknown or not applicable (round-robin listings).
    decided_in: DecidedIn | None = None
    has_stats: bool = False
    is_technical: bool = False
    is_schedule_tentative: bool = False
    visitors: int | None = None
    video_review_url: str | None = None
    protocol_url: str | None = None


class GameResponse(GameBase):
    home_team: TeamInGame | None = None
    away_team: TeamInGame | None = None
    season_name: str | None = None
    tournament_name: str | None = None
    stage_name: str | None = None
    status: str | None = None
    live_phase: LivePhase | None = None
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
    time: ShortTime | None = None
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
    field_type: str | None = None
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
    time: ShortTime | None = None
    tour: int | None = None
    season_id: int | None = None

    home_team: TeamInMatchCenter | None = None
    away_team: TeamInMatchCenter | None = None

    home_score: int | None = None
    away_score: int | None = None
    home_penalty_score: int | None = None
    away_penalty_score: int | None = None
    decided_in: "DecidedIn | None" = None

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
    live_phase: LivePhase | None = None

    has_score: bool = False

    # Live minute for display
    minute: Optional[int] = None
    half: Optional[int] = None

    # Show/hide timeline and live minutes
    show_timeline: bool = True

    # Optional ticket URL
    ticket_url: str | None = None
    is_free_entry: bool = False

    # Optional video review URL
    video_review_url: str | None = None
    video_review_view_count: int | None = None

    # Optional YouTube live stream URL
    youtube_live_url: str | None = None
    youtube_live_view_count: int | None = None

    # Optional match protocol URL (PDF)
    protocol_url: str | None = None

    # Broadcasters
    broadcasters: list[BroadcasterInfo] = []

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


class DailyResultsCardTeam(BaseModel):
    id: int
    name: str
    logo_url: str | None = None


class DailyResultsCardGame(BaseModel):
    id: int
    time: ShortTime | None = None
    home_team: DailyResultsCardTeam
    away_team: DailyResultsCardTeam
    home_score: int
    away_score: int


class DailyResultsCardSection(BaseModel):
    key: str
    label: str | None = None
    games: list[DailyResultsCardGame]


class DailyResultsCardPayload(BaseModel):
    season_id: int
    frontend_code: str | None = None
    for_date: date
    locale: str
    brand_label: str
    tournament_name: str
    headline: str
    date_label: str
    tour: int | None = None
    season_logo_url: str | None = None
    sections: list[DailyResultsCardSection]
    game_count: int


# Per-endpoint response schemas

class GameListItem(BaseModel):
    """Game item in /games standard list response."""
    id: int
    date: date
    time: ShortTime | None = None
    tour: int | None = None
    season_id: int | None = None
    stage_id: int | None = None
    stage_name: str | None = None
    home_score: int | None = None
    away_score: int | None = None
    home_penalty_score: int | None = None
    away_penalty_score: int | None = None
    decided_in: "DecidedIn | None" = None
    has_stats: bool = False
    has_lineup: bool = False
    is_live: bool = False
    is_technical: bool = False
    is_schedule_tentative: bool = False
    is_featured: bool = False
    visitors: int | None = None
    status: str
    live_phase: LivePhase | None = None
    has_score: bool = False
    minute: Optional[int] = None
    half: Optional[int] = None
    ticket_url: str | None = None
    is_free_entry: bool = False
    video_review_url: str | None = None
    video_review_view_count: int | None = None
    youtube_live_url: str | None = None
    youtube_live_view_count: int | None = None
    protocol_url: str | None = None
    where_broadcast: str | None = None
    home_team: TeamInMatchCenter | None = None
    away_team: TeamInMatchCenter | None = None
    stadium_info: StadiumInfo | None = None
    season_name: str | None = None
    broadcasters: list[BroadcasterInfo] = []


class GameDetailItem(BaseModel):
    """Game detail response for /games/{id}."""
    id: int
    date: date
    time: ShortTime | None = None
    tour: int | None = None
    season_id: int | None = None
    stage_id: int | None = None
    stage_name: str | None = None
    home_score: int | None = None
    away_score: int | None = None
    home_penalty_score: int | None = None
    away_penalty_score: int | None = None
    decided_in: "DecidedIn | None" = None
    has_stats: bool = False
    has_lineup: bool = False
    is_live: bool = False
    minute: Optional[int] = None
    half: Optional[int] = None
    live_phase: LivePhase | None = None
    is_technical: bool = False
    is_schedule_tentative: bool = False
    is_featured: bool = False
    show_timeline: bool = True
    stadium: StadiumInfo | None = None
    referee: str | None = None
    visitors: int | None = None
    ticket_url: str | None = None
    is_free_entry: bool = False
    video_review_url: str | None = None
    video_review_view_count: int | None = None
    youtube_live_url: str | None = None
    youtube_live_view_count: int | None = None
    protocol_url: str | None = None
    where_broadcast: str | None = None
    preview_ru: str | None = None
    preview_kz: str | None = None
    status: str
    has_score: bool = False
    home_team: TeamInGame | None = None
    away_team: TeamInGame | None = None
    season_name: str | None = None
    broadcasters: list[BroadcasterInfo] = []
    weather: str | None = None


class SeasonGameItem(BaseModel):
    """Game item for /seasons/{id}/games."""
    id: int
    date: date
    time: ShortTime | None = None
    tour: int | None = None
    season_id: int | None = None
    home_score: int | None = None
    away_score: int | None = None
    home_penalty_score: int | None = None
    away_penalty_score: int | None = None
    decided_in: "DecidedIn | None" = None
    has_stats: bool = False
    has_lineup: bool = False
    is_live: bool = False
    is_technical: bool = False
    is_schedule_tentative: bool = False
    show_timeline: bool = True
    status: str = "upcoming"
    minute: Optional[int] = None
    half: Optional[int] = None
    live_phase: LivePhase | None = None
    stadium: str | None = None
    visitors: int | None = None
    home_team: TeamWithScore | None = None
    away_team: TeamWithScore | None = None
    season_name: str | None = None


class StageGameItem(BaseModel):
    """Game item for /seasons/{id}/stages/{stage_id}/games."""
    id: int
    date: date
    time: ShortTime | None = None
    tour: int | None = None
    season_id: int | None = None
    stage_id: int | None = None
    home_score: int | None = None
    away_score: int | None = None
    home_penalty_score: int | None = None
    away_penalty_score: int | None = None
    decided_in: "DecidedIn | None" = None
    has_stats: bool = False
    has_lineup: bool = False
    is_live: bool = False
    is_technical: bool = False
    show_timeline: bool = True
    status: str = "upcoming"
    minute: Optional[int] = None
    half: Optional[int] = None
    live_phase: LivePhase | None = None
    stadium: str | None = None
    visitors: int | None = None
    home_team: TeamWithScore | None = None
    away_team: TeamWithScore | None = None
    season_name: str | None = None


class HomeMatchesWidgetResponse(BaseModel):
    """Response for GET /games/home-widget."""
    frontend_code: str
    season_id: int
    selected_round: int | None = None
    window_state: str  # "active_round" | "completed_window" | "fallback"
    default_tab: str  # "upcoming" | "finished"
    show_tabs: bool
    groups: list[MatchCenterDateGroup]
    finished_groups: list[MatchCenterDateGroup] | None = None
    upcoming_groups: list[MatchCenterDateGroup] | None = None
    completed_window_expires_at: datetime | None = None


class TeamGameItem(BaseModel):
    """Game item for /teams/{id}/games."""
    id: int
    date: date
    time: ShortTime | None = None
    tour: int | None = None
    season_id: int | None = None
    home_score: int | None = None
    away_score: int | None = None
    has_stats: bool = False
    has_lineup: bool = False
    is_live: bool = False
    is_technical: bool = False
    show_timeline: bool = True
    status: str = "upcoming"
    minute: Optional[int] = None
    half: Optional[int] = None
    live_phase: LivePhase | None = None
    stadium: TeamStadiumInfo | None = None
    visitors: int | None = None
    home_team: TeamWithScore | None = None
    away_team: TeamWithScore | None = None
    season_name: str | None = None
