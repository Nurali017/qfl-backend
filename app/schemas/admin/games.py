import datetime as dt
from typing import Optional
from uuid import UUID

from pydantic import BaseModel

from app.models.game import GameStatus


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
    has_lineup: bool = False
    has_stats: bool = False
    stadium_id: Optional[int] = None
    ticket_url: Optional[str] = None
    video_url: Optional[str] = None
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
    where_broadcast: Optional[str] = None
    video_review_url: Optional[str] = None
    is_featured: Optional[bool] = None
    status: Optional[GameStatus] = None


class AdminGamesListResponse(BaseModel):
    items: list[AdminGameResponse]
    total: int
