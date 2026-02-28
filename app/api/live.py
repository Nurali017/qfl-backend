import logging

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.api.deps import get_db
from app.models.game_event import GameEvent, GameEventType

router = APIRouter(prefix="/live", tags=["live"])
logger = logging.getLogger(__name__)


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
    assist_player_id: int | None = None
    assist_player_name: str | None = None
    created_at: datetime

    model_config = {"from_attributes": True}


class GameEventsListResponse(BaseModel):
    """Response with list of game events."""
    game_id: int
    events: list[GameEventResponse]
    total: int


@router.get("/events/{game_id}", response_model=GameEventsListResponse)
async def get_game_events(game_id: int, db: AsyncSession = Depends(get_db)):
    """Get all events for a match."""
    result = await db.execute(
        select(GameEvent)
        .where(GameEvent.game_id == game_id)
        .where(GameEvent.event_type != GameEventType.assist)
        .order_by(GameEvent.half, GameEvent.minute)
    )
    events = result.scalars().all()
    return {
        "game_id": game_id,
        "events": events,
        "total": len(events),
    }
