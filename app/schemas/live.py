"""
Pydantic schemas for live match data.
"""
from datetime import datetime
from pydantic import BaseModel

from app.models.game_event import GameEventType


class GameEventResponse(BaseModel):
    """Response schema for a match event."""
    id: int
    game_id: int
    half: int
    minute: int
    event_type: GameEventType
    team_id: int | None = None
    team_name: str | None = None
    player_id: int | None = None
    player_number: int | None = None
    player_name: str | None = None
    player2_id: int | None = None
    player2_number: int | None = None
    player2_name: str | None = None
    player2_team_name: str | None = None
    # Assist info (only for goal events)
    assist_player_id: int | None = None
    assist_player_name: str | None = None
    created_at: datetime

    class Config:
        from_attributes = True


class GameEventsListResponse(BaseModel):
    """Response with list of game events."""
    game_id: int
    events: list[GameEventResponse]
    total: int


class LiveSyncResponse(BaseModel):
    """Response for live sync operations."""
    game_id: int
    is_live: bool | None = None
    new_events_count: int | None = None
    error: str | None = None


class LineupSyncResponse(BaseModel):
    """Response for lineup sync operation."""
    game_id: int
    home_formation: str | None = None
    away_formation: str | None = None
    lineup_count: int
    error: str | None = None
