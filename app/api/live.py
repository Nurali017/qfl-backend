"""
API endpoints for live match data and WebSocket connections.
"""
import logging
from fastapi import APIRouter, Depends, WebSocket, WebSocketDisconnect, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.api.deps import get_db
from app.models import Game
from app.services.sota_client import get_sota_client, SotaClient
from app.services.live_sync_service import LiveSyncService
from app.services.websocket_manager import get_websocket_manager, ConnectionManager
from app.services.live_event_bus import publish_live_message
from app.services.sync import SyncOrchestrator
from app.schemas.live import (
    GameEventsListResponse,
    GameEventResponse,
    LiveSyncResponse,
    LineupSyncResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/live", tags=["live"])


def get_live_sync_service(
    db: AsyncSession = Depends(get_db),
    client: SotaClient = Depends(get_sota_client),
) -> LiveSyncService:
    """Dependency to get LiveSyncService instance."""
    return LiveSyncService(db, client)


# ==================== HTTP Endpoints ====================


@router.post("/start/{game_id}", response_model=LiveSyncResponse)
async def start_live_sync(
    game_id: int,
    service: LiveSyncService = Depends(get_live_sync_service),
    manager: ConnectionManager = Depends(get_websocket_manager),
):
    """
    Start live tracking for a match.

    - Syncs lineup if not already done
    - Sets is_live=True
    - Syncs initial events
    - Broadcasts game_started status to connected clients
    """
    result = await service.start_live_tracking(game_id)

    if "error" not in result:
        await manager.broadcast_game_status(game_id, "started")
        await publish_live_message({"type": "status", "game_id": game_id, "status": "started"})

    return LiveSyncResponse(**result)


@router.post("/stop/{game_id}", response_model=LiveSyncResponse)
async def stop_live_sync(
    game_id: int,
    service: LiveSyncService = Depends(get_live_sync_service),
    manager: ConnectionManager = Depends(get_websocket_manager),
):
    """
    Stop live tracking for a match.

    - Sets is_live=False
    - Broadcasts game_ended status to connected clients
    """
    result = await service.stop_live_tracking(game_id)

    if "error" not in result:
        await manager.broadcast_game_status(game_id, "ended")
        await publish_live_message({"type": "status", "game_id": game_id, "status": "ended"})

    return LiveSyncResponse(**result)


@router.post("/sync-lineup/{game_id}", response_model=LineupSyncResponse)
async def sync_lineup(
    game_id: int,
    db: AsyncSession = Depends(get_db),
    client: SotaClient = Depends(get_sota_client),
    manager: ConnectionManager = Depends(get_websocket_manager),
):
    """
    Sync pre-game lineup for a match.

    Fetches lineup from SOTA /em/ endpoints and saves to database.
    """
    try:
        details = await SyncOrchestrator(db, client).sync_pre_game_lineup(game_id)
        game_result = await db.execute(select(Game).where(Game.id == game_id))
        game = game_result.scalar_one_or_none()

        str_game_id = str(game_id)
        result = {
            "game_id": str_game_id,
            "home_formation": game.home_formation if game else None,
            "away_formation": game.away_formation if game else None,
            "lineup_count": int(details.get("lineups", 0)),
        }
        await manager.broadcast_lineup(str_game_id, result)
        await publish_live_message({"type": "lineup", "game_id": str_game_id, "data": result})
        return LineupSyncResponse(**result)
    except Exception as exc:
        return LineupSyncResponse(game_id=str(game_id), lineup_count=0, error=str(exc))


@router.post("/sync-events/{game_id}")
async def sync_events(
    game_id: int,
    service: LiveSyncService = Depends(get_live_sync_service),
    manager: ConnectionManager = Depends(get_websocket_manager),
):
    """
    Sync live events for a match.

    Fetches events from SOTA and broadcasts new ones via WebSocket.
    """
    new_events = await service.sync_live_events(game_id)

    # Broadcast each new event
    for event in new_events:
        event_data = GameEventResponse.model_validate(event).model_dump(mode="json")
        await manager.broadcast_event(game_id, event_data)
        await publish_live_message({"type": "event", "game_id": game_id, "data": event_data})

    return {
        "game_id": game_id,
        "new_events_count": len(new_events),
        "events": [GameEventResponse.model_validate(e) for e in new_events],
    }


@router.get("/events/{game_id}", response_model=GameEventsListResponse)
async def get_game_events(
    game_id: int,
    service: LiveSyncService = Depends(get_live_sync_service),
):
    """
    Get all events for a match.
    """
    events = await service.get_game_events(game_id)
    return GameEventsListResponse(
        game_id=game_id,
        events=[GameEventResponse.model_validate(e) for e in events],
        total=len(events),
    )


@router.get("/active-games")
async def get_active_games(
    service: LiveSyncService = Depends(get_live_sync_service),
):
    """
    Get list of currently live games.
    """
    games = await service.get_active_games()
    return {
        "count": len(games),
        "games": [
            {
                "id": g.id,
                "date": g.date.isoformat(),
                "time": g.time.isoformat() if g.time else None,
                "home_team_id": g.home_team_id,
                "away_team_id": g.away_team_id,
                "home_score": g.home_score,
                "away_score": g.away_score,
            }
            for g in games
        ],
    }


@router.get("/connections/{game_id}")
async def get_websocket_connections(
    game_id: int,
    manager: ConnectionManager = Depends(get_websocket_manager),
):
    """
    Get number of WebSocket connections for a game.
    """
    return {
        "game_id": game_id,
        "connections": manager.get_connection_count(game_id),
    }


# ==================== WebSocket Endpoint ====================


@router.websocket("/ws/{game_id}")
async def game_websocket(
    websocket: WebSocket,
    game_id: str,
    manager: ConnectionManager = Depends(get_websocket_manager),
):
    """
    WebSocket endpoint for live game updates.

    Connect to receive real-time events for a specific game.

    Messages sent:
    - {"type": "event", "game_id": "...", "data": {...}} - New match event
    - {"type": "lineup", "game_id": "...", "data": {...}} - Lineup update
    - {"type": "status", "game_id": "...", "status": "started|ended"} - Game status change
    """
    await manager.connect(websocket, game_id)

    try:
        # Send initial connection confirmation
        await websocket.send_json({
            "type": "connected",
            "game_id": game_id,
            "message": "Connected to live updates",
        })

        # Keep connection alive and handle incoming messages
        while True:
            # Wait for any message (ping/pong or commands)
            data = await websocket.receive_text()

            # Handle ping
            if data == "ping":
                await websocket.send_json({"type": "pong"})

    except WebSocketDisconnect:
        await manager.disconnect(websocket, game_id)
        logger.info(f"WebSocket disconnected for game {game_id}")
    except Exception as e:
        logger.error(f"WebSocket error for game {game_id}: {e}")
        await manager.disconnect(websocket, game_id)
