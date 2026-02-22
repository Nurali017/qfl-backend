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


class LivePlayerResponse(BaseModel):
    """Player in live lineup."""
    id: int | None = None
    number: int | str | None = None
    first_name: str | None = None
    last_name: str | None = None
    full_name: str | None = None
    is_goalkeeper: bool = False
    is_captain: bool = False
    position: str | None = None
    amplua: str | None = None


class LiveLineupResponse(BaseModel):
    """Response for live lineup data."""
    game_id: int
    home_formation: str | None = None
    away_formation: str | None = None
    home_starters: list[LivePlayerResponse]
    home_substitutes: list[LivePlayerResponse]
    away_starters: list[LivePlayerResponse]
    away_substitutes: list[LivePlayerResponse]


class WebSocketEventMessage(BaseModel):
    """WebSocket message for a match event."""
    type: str = "event"
    game_id: int
    data: GameEventResponse


class WebSocketStatusMessage(BaseModel):
    """WebSocket message for game status change."""
    type: str = "status"
    game_id: int
    status: str
