import logging

from fastapi import APIRouter, Depends
from fastapi.responses import Response
from pydantic import BaseModel
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.api.deps import get_db
from app.models.game_event import GameEvent, GameEventType
from app.utils.localization import get_localized_full_name, get_localized_name

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
    video_url: str | None = None
    created_at: datetime

    model_config = {"from_attributes": True}


class GameEventsListResponse(BaseModel):
    """Response with list of game events."""
    game_id: int
    events: list[GameEventResponse]
    total: int


def _localized_event(event: GameEvent, lang: str) -> GameEventResponse:
    """Build a response with player names resolved to the requested language.

    Names are denormalized RU snapshots on the event row, so we resolve the
    localized name from the linked Player/Team when available, falling back to
    the stored string for manual events without a linked row.
    """
    response = GameEventResponse.model_validate(event)

    def resolve_player(player, stored: str | None) -> str | None:
        if player is not None:
            localized = get_localized_full_name(player, lang)
            if localized:
                return localized
        return stored

    def resolve_team(team, stored: str | None) -> str | None:
        # player2's team isn't an FK, but substitutions stay within one team,
        # so event.team is the same team — safe to localize player2_team_name.
        if team is not None and stored:
            localized = get_localized_name(team, lang)
            if localized:
                return localized
        return stored

    return response.model_copy(
        update={
            "team_name": resolve_team(event.team, event.team_name),
            "player2_team_name": resolve_team(event.team, event.player2_team_name),
            "player_name": resolve_player(event.player, event.player_name),
            "player2_name": resolve_player(event.player2, event.player2_name),
            "assist_player_name": resolve_player(
                event.assist_player, event.assist_player_name
            ),
        }
    )


@router.get("/events/{game_id}", response_model=GameEventsListResponse)
async def get_game_events(
    game_id: int, lang: str = "ru", db: AsyncSession = Depends(get_db)
):
    """Get all events for a match, with player names in the requested language."""
    from app.utils.cache import cache_get, cache_set

    cache_key = f"events:{game_id}:{lang}"
    cached = cache_get(cache_key)
    if cached is not None:
        return Response(content=cached, media_type="application/json")

    result = await db.execute(
        select(GameEvent)
        .where(GameEvent.game_id == game_id)
        .where(GameEvent.event_type != GameEventType.assist)
        .order_by(GameEvent.half, GameEvent.minute)
        .options(
            selectinload(GameEvent.team),
            selectinload(GameEvent.player),
            selectinload(GameEvent.player2),
            selectinload(GameEvent.assist_player),
        )
    )
    events = result.scalars().all()
    response_data = GameEventsListResponse(
        game_id=game_id,
        events=[_localized_event(e, lang) for e in events],
        total=len(events),
    )
    json_bytes = response_data.model_dump_json().encode()
    cache_set(cache_key, json_bytes, 5)
    return Response(content=json_bytes, media_type="application/json")
