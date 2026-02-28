import datetime as dt
from typing import Optional
from uuid import UUID

from pydantic import BaseModel

from app.models.game import GameStatus
from app.models.game_referee import RefereeRole


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
    sync_disabled: bool = False
    has_lineup: bool = False
    has_stats: bool = False
    stadium_id: Optional[int] = None
    ticket_url: Optional[str] = None
    video_url: Optional[str] = None
    youtube_live_url: Optional[str] = None
    where_broadcast: Optional[str] = None
    video_review_url: Optional[str] = None
    home_formation: Optional[str] = None
    away_formation: Optional[str] = None
    updated_at: Optional[dt.datetime] = None


class AdminGameUpdateRequest(BaseModel):
    date: Optional[dt.date] = None
    time: Optional[dt.time] = None
    tour: Optional[int] = None
    home_score: Optional[int] = None
    away_score: Optional[int] = None
    home_penalty_score: Optional[int] = None
    away_penalty_score: Optional[int] = None
    stadium_id: Optional[int] = None
    ticket_url: Optional[str] = None
    video_url: Optional[str] = None
    youtube_live_url: Optional[str] = None
    where_broadcast: Optional[str] = None
    video_review_url: Optional[str] = None
    is_featured: Optional[bool] = None
    sync_disabled: Optional[bool] = None
    status: Optional[GameStatus] = None


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


class AdminRefereeItem(BaseModel):
    id: int  # GameReferee.id (for delete)
    referee_id: int
    referee_name: Optional[str] = None
    role: str


class AdminRefereeAddRequest(BaseModel):
    referee_id: int
    role: RefereeRole
