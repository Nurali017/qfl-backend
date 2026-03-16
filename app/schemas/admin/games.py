import datetime as dt
from typing import Optional
from uuid import UUID

from pydantic import BaseModel

from app.models.game import GameStatus
from app.models.game_referee import RefereeRole
from app.schemas.admin.broadcasters import AdminGameBroadcasterItem


class AdminGameResponse(BaseModel):
    id: int
    sota_id: Optional[UUID] = None
    date: dt.date
    time: Optional[dt.time] = None
    tour: Optional[int] = None
    season_id: Optional[int] = None
    stage_id: Optional[int] = None
    home_team_id: Optional[int] = None
    away_team_id: Optional[int] = None
    home_team_name: Optional[str] = None
    away_team_name: Optional[str] = None
    home_score: Optional[int] = None
    away_score: Optional[int] = None
    home_penalty_score: Optional[int] = None
    away_penalty_score: Optional[int] = None
    status: GameStatus = GameStatus.created
    is_live: bool = False
    is_featured: bool = False
    is_free_entry: bool = False
    sync_disabled: bool = False
    show_timeline: bool = True
    has_lineup: bool = False
    has_stats: bool = False
    stadium_id: Optional[int] = None
    stadium_name: Optional[str] = None
    visitors: Optional[int] = None
    ticket_url: Optional[str] = None
    video_url: Optional[str] = None
    youtube_live_url: Optional[str] = None
    where_broadcast: Optional[str] = None
    video_review_url: Optional[str] = None
    protocol_url: Optional[str] = None
    home_formation: Optional[str] = None
    away_formation: Optional[str] = None
    updated_at: Optional[dt.datetime] = None
    weather_temp: Optional[int] = None
    weather_condition: Optional[str] = None
    weather_fetched_at: Optional[dt.datetime] = None
    preview_ru: Optional[str] = None
    preview_kz: Optional[str] = None
    broadcasters: list[AdminGameBroadcasterItem] = []


class AdminGameUpdateRequest(BaseModel):
    sota_id: Optional[UUID] = None
    date: Optional[dt.date] = None
    time: Optional[dt.time] = None
    tour: Optional[int] = None
    home_score: Optional[int] = None
    away_score: Optional[int] = None
    home_penalty_score: Optional[int] = None
    away_penalty_score: Optional[int] = None
    stadium_id: Optional[int] = None
    visitors: Optional[int] = None
    ticket_url: Optional[str] = None
    video_url: Optional[str] = None
    youtube_live_url: Optional[str] = None
    where_broadcast: Optional[str] = None
    video_review_url: Optional[str] = None
    protocol_url: Optional[str] = None
    is_featured: Optional[bool] = None
    is_free_entry: Optional[bool] = None
    sync_disabled: Optional[bool] = None
    show_timeline: Optional[bool] = None
    status: Optional[GameStatus] = None
    preview_ru: Optional[str] = None
    preview_kz: Optional[str] = None


class AdminGamesListResponse(BaseModel):
    items: list[AdminGameResponse]
    total: int


class AdminLineupItem(BaseModel):
    id: int
    player_id: int
    player_name: Optional[str] = None
    team_id: int
    lineup_type: str
    shirt_number: Optional[int] = None
    is_captain: bool = False
    amplua: Optional[str] = None
    field_position: Optional[str] = None


class AdminLineupAddRequest(BaseModel):
    player_id: int
    team_id: int
    lineup_type: str = "starter"
    shirt_number: Optional[int] = None
    is_captain: Optional[bool] = None
    amplua: Optional[str] = None
    field_position: Optional[str] = None


class AdminLineupUpdateRequest(BaseModel):
    lineup_type: Optional[str] = None
    shirt_number: Optional[int] = None
    is_captain: Optional[bool] = None
    amplua: Optional[str] = None
    field_position: Optional[str] = None


class AdminEventItem(BaseModel):
    id: int
    half: int
    minute: int
    event_type: str
    team_id: Optional[int] = None
    player_id: Optional[int] = None
    player_name: Optional[str] = None
    player_number: Optional[int] = None
    player2_id: Optional[int] = None
    player2_name: Optional[str] = None
    assist_player_id: Optional[int] = None
    assist_player_name: Optional[str] = None
    source: str = "sota"


class AdminEventAddRequest(BaseModel):
    half: int
    minute: int
    event_type: str
    team_id: Optional[int] = None
    player_id: Optional[int] = None
    player_name: Optional[str] = None
    player_number: Optional[int] = None
    player2_id: Optional[int] = None
    player2_name: Optional[str] = None
    assist_player_id: Optional[int] = None
    assist_player_name: Optional[str] = None


class AdminEventUpdateRequest(BaseModel):
    half: Optional[int] = None
    minute: Optional[int] = None
    event_type: Optional[str] = None
    team_id: Optional[int] = None
    player_id: Optional[int] = None
    player_name: Optional[str] = None
    player_number: Optional[int] = None
    player2_id: Optional[int] = None
    player2_name: Optional[str] = None
    assist_player_id: Optional[int] = None
    assist_player_name: Optional[str] = None


class AdminRefereeItem(BaseModel):
    id: int  # GameReferee.id (for delete)
    referee_id: int
    referee_name: Optional[str] = None
    role: str


class AdminRefereeAddRequest(BaseModel):
    referee_id: int
    role: RefereeRole


class AdminTeamStatsItem(BaseModel):
    id: int
    team_id: int
    team_name: Optional[str] = None
    possession: Optional[float] = None
    possession_percent: Optional[int] = None
    shots: Optional[int] = None
    shots_on_goal: Optional[int] = None
    shots_off_goal: Optional[int] = None
    passes: Optional[int] = None
    pass_accuracy: Optional[float] = None
    fouls: Optional[int] = None
    yellow_cards: Optional[int] = None
    red_cards: Optional[int] = None
    corners: Optional[int] = None
    offsides: Optional[int] = None
    shots_on_bar: Optional[int] = None
    shots_blocked: Optional[int] = None
    penalties: Optional[int] = None
    saves: Optional[int] = None
    extra_stats: Optional[dict] = None


class AdminTeamStatsUpsertRequest(BaseModel):
    possession: Optional[float] = None
    possession_percent: Optional[int] = None
    shots: Optional[int] = None
    shots_on_goal: Optional[int] = None
    shots_off_goal: Optional[int] = None
    passes: Optional[int] = None
    pass_accuracy: Optional[float] = None
    fouls: Optional[int] = None
    yellow_cards: Optional[int] = None
    red_cards: Optional[int] = None
    corners: Optional[int] = None
    offsides: Optional[int] = None
    shots_on_bar: Optional[int] = None
    shots_blocked: Optional[int] = None
    penalties: Optional[int] = None
    saves: Optional[int] = None


class AdminPlayerStatsItem(BaseModel):
    id: int
    player_id: int
    player_name: Optional[str] = None
    team_id: int
    team_name: Optional[str] = None
    minutes_played: Optional[int] = None
    started: Optional[bool] = None
    position: Optional[str] = None
    shots: int = 0
    shots_on_goal: int = 0
    shots_off_goal: int = 0
    passes: int = 0
    pass_accuracy: Optional[float] = None
    duel: int = 0
    tackle: int = 0
    corner: int = 0
    offside: int = 0
    foul: int = 0
    yellow_cards: int = 0
    red_cards: int = 0
    extra_stats: Optional[dict] = None


class AdminPlayerStatsUpsertRequest(BaseModel):
    team_id: int
    minutes_played: Optional[int] = None
    started: Optional[bool] = None
    position: Optional[str] = None
    shots: Optional[int] = None
    shots_on_goal: Optional[int] = None
    shots_off_goal: Optional[int] = None
    passes: Optional[int] = None
    pass_accuracy: Optional[float] = None
    duel: Optional[int] = None
    tackle: Optional[int] = None
    corner: Optional[int] = None
    offside: Optional[int] = None
    foul: Optional[int] = None
    yellow_cards: Optional[int] = None
    red_cards: Optional[int] = None
    extra_stats: Optional[dict] = None
