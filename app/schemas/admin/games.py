import datetime as dt
from typing import Optional
from uuid import UUID

from pydantic import BaseModel


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
    is_live: bool = False
    has_lineup: bool = False
    has_stats: bool = False
    stadium: Optional[str] = None
    stadium_id: Optional[int] = None
    ticket_url: Optional[str] = None
    video_url: Optional[str] = None
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
    stadium: Optional[str] = None
    stadium_id: Optional[int] = None
    ticket_url: Optional[str] = None
    video_url: Optional[str] = None
    is_live: Optional[bool] = None


class AdminGamesListResponse(BaseModel):
    items: list[AdminGameResponse]
    total: int
