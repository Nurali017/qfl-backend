from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.admin.deps import require_roles
from app.api.deps import get_db
from app.config import get_settings
from app.models import AdminUser
from app.schemas.live import GameEventResponse, GameEventsListResponse, LineupSyncResponse, LiveSyncResponse
from app.schemas.sync import SyncResponse, SyncStatus
from app.services.live_event_bus import publish_live_message
from app.services.live_sync_service import LiveSyncService
from app.services.sota_client import SotaClient, get_sota_client
from app.services.sync import SyncOrchestrator
from app.services.websocket_manager import ConnectionManager, get_websocket_manager

settings = get_settings()
router = APIRouter(prefix="/ops", tags=["admin-ops"])


def get_live_sync_service(
    db: AsyncSession = Depends(get_db),
    client: SotaClient = Depends(get_sota_client),
) -> LiveSyncService:
    return LiveSyncService(db, client)


@router.post("/sync/full", response_model=SyncResponse)
async def sync_full(
    season_id: int = Query(default=None),
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "operator")),
):
    season_id = season_id or settings.current_season_id
    try:
        result = await SyncOrchestrator(db).full_sync(season_id)
        return SyncResponse(status=SyncStatus.SUCCESS, message="Full sync completed", details=result)
    except Exception as exc:
        return SyncResponse(status=SyncStatus.FAILED, message=f"Full sync failed: {exc}")


@router.post("/sync/games", response_model=SyncResponse)
async def sync_games(
    season_id: int = Query(default=None),
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "operator")),
):
    season_id = season_id or settings.current_season_id
    try:
        count = await SyncOrchestrator(db).sync_games(season_id)
        return SyncResponse(status=SyncStatus.SUCCESS, message="Games sync completed", details={"games_synced": count})
    except Exception as exc:
        return SyncResponse(status=SyncStatus.FAILED, message=f"Games sync failed: {exc}")


@router.post("/sync/teams", response_model=SyncResponse)
async def sync_teams(
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "operator")),
):
    try:
        count = await SyncOrchestrator(db).reference.sync_teams()
        return SyncResponse(status=SyncStatus.SUCCESS, message="Teams sync completed", details={"teams_synced": count})
    except Exception as exc:
        return SyncResponse(status=SyncStatus.FAILED, message=f"Teams sync failed: {exc}")


@router.post("/sync/team-logos", response_model=SyncResponse)
async def sync_team_logos(
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "operator")),
):
    try:
        count = await SyncOrchestrator(db).reference.sync_team_logos()
        return SyncResponse(status=SyncStatus.SUCCESS, message="Team logos sync completed", details={"teams_updated": count})
    except Exception as exc:
        return SyncResponse(status=SyncStatus.FAILED, message=f"Team logos sync failed: {exc}")


@router.post("/sync/players", response_model=SyncResponse)
async def sync_players(
    season_id: int = Query(default=None),
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "operator")),
):
    season_id = season_id or settings.current_season_id
    try:
        count = await SyncOrchestrator(db).sync_players(season_id)
        return SyncResponse(status=SyncStatus.SUCCESS, message="Players sync completed", details={"players_synced": count})
    except Exception as exc:
        return SyncResponse(status=SyncStatus.FAILED, message=f"Players sync failed: {exc}")


@router.post("/sync/score-table", response_model=SyncResponse)
async def sync_score_table(
    season_id: int = Query(default=None),
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "operator")),
):
    season_id = season_id or settings.current_season_id
    try:
        count = await SyncOrchestrator(db).sync_score_table(season_id)
        return SyncResponse(status=SyncStatus.SUCCESS, message="Score table sync completed", details={"rows_synced": count})
    except Exception as exc:
        return SyncResponse(status=SyncStatus.FAILED, message=f"Score table sync failed: {exc}")


@router.post("/sync/team-season-stats", response_model=SyncResponse)
async def sync_team_season_stats(
    season_id: int = Query(default=None),
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "operator")),
):
    season_id = season_id or settings.current_season_id
    try:
        count = await SyncOrchestrator(db).sync_team_season_stats(season_id)
        return SyncResponse(status=SyncStatus.SUCCESS, message="Team season stats sync completed", details={"teams_synced": count})
    except Exception as exc:
        return SyncResponse(status=SyncStatus.FAILED, message=f"Team season stats sync failed: {exc}")


@router.post("/sync/player-season-stats", response_model=SyncResponse)
async def sync_player_season_stats(
    season_id: int = Query(default=None),
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "operator")),
):
    season_id = season_id or settings.current_season_id
    try:
        count = await SyncOrchestrator(db).sync_player_stats(season_id)
        return SyncResponse(status=SyncStatus.SUCCESS, message="Player season stats sync completed", details={"players_synced": count})
    except Exception as exc:
        return SyncResponse(status=SyncStatus.FAILED, message=f"Player season stats sync failed: {exc}")


@router.post("/sync/game-stats/{game_id}", response_model=SyncResponse)
async def sync_game_stats(
    game_id: str,
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "operator")),
):
    try:
        details = await SyncOrchestrator(db).sync_game_stats(game_id)
        return SyncResponse(status=SyncStatus.SUCCESS, message="Game stats sync completed", details=details)
    except Exception as exc:
        return SyncResponse(status=SyncStatus.FAILED, message=f"Game stats sync failed: {exc}")


@router.post("/sync/game-lineup/{game_id}", response_model=SyncResponse)
async def sync_game_lineup(
    game_id: str,
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "operator")),
):
    try:
        details = await SyncOrchestrator(db).sync_pre_game_lineup(game_id)
        return SyncResponse(status=SyncStatus.SUCCESS, message="Game lineup sync completed", details=details)
    except Exception as exc:
        return SyncResponse(status=SyncStatus.FAILED, message=f"Game lineup sync failed: {exc}")


@router.post("/sync/game-events/{game_id}", response_model=SyncResponse)
async def sync_game_events(
    game_id: str,
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "operator")),
):
    try:
        details = await SyncOrchestrator(db).sync_game_events(game_id)
        return SyncResponse(status=SyncStatus.SUCCESS, message="Game events sync completed", details=details)
    except Exception as exc:
        return SyncResponse(status=SyncStatus.FAILED, message=f"Game events sync failed: {exc}")


@router.post("/sync/all-game-events", response_model=SyncResponse)
async def sync_all_game_events(
    season_id: int = Query(default=None),
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "operator")),
):
    season_id = season_id or settings.current_season_id
    try:
        details = await SyncOrchestrator(db).sync_all_game_events(season_id)
        return SyncResponse(status=SyncStatus.SUCCESS, message="All game events sync completed", details=details)
    except Exception as exc:
        return SyncResponse(status=SyncStatus.FAILED, message=f"All game events sync failed: {exc}")


@router.post("/live/start/{game_id}", response_model=LiveSyncResponse)
async def live_start(
    game_id: str,
    service: LiveSyncService = Depends(get_live_sync_service),
    manager: ConnectionManager = Depends(get_websocket_manager),
    _admin: AdminUser = Depends(require_roles("superadmin", "operator")),
):
    result = await service.start_live_tracking(game_id)
    if "error" not in result:
        await manager.broadcast_game_status(game_id, "started")
        await publish_live_message({"type": "status", "game_id": game_id, "status": "started"})
    return LiveSyncResponse(**result)


@router.post("/live/stop/{game_id}", response_model=LiveSyncResponse)
async def live_stop(
    game_id: str,
    service: LiveSyncService = Depends(get_live_sync_service),
    manager: ConnectionManager = Depends(get_websocket_manager),
    _admin: AdminUser = Depends(require_roles("superadmin", "operator")),
):
    result = await service.stop_live_tracking(game_id)
    if "error" not in result:
        await manager.broadcast_game_status(game_id, "ended")
        await publish_live_message({"type": "status", "game_id": game_id, "status": "ended"})
    return LiveSyncResponse(**result)


@router.post("/live/sync-lineup/{game_id}", response_model=LineupSyncResponse)
async def live_sync_lineup(
    game_id: str,
    service: LiveSyncService = Depends(get_live_sync_service),
    manager: ConnectionManager = Depends(get_websocket_manager),
    _admin: AdminUser = Depends(require_roles("superadmin", "operator")),
):
    result = await service.sync_pregame_lineup(game_id)
    if "error" not in result:
        await manager.broadcast_lineup(game_id, result)
        await publish_live_message({"type": "lineup", "game_id": game_id, "data": result})
    return LineupSyncResponse(**result)


@router.post("/live/sync-events/{game_id}")
async def live_sync_events(
    game_id: str,
    service: LiveSyncService = Depends(get_live_sync_service),
    manager: ConnectionManager = Depends(get_websocket_manager),
    _admin: AdminUser = Depends(require_roles("superadmin", "operator")),
):
    new_events = await service.sync_live_events(game_id)
    for event in new_events:
        event_data = GameEventResponse.model_validate(event).model_dump(mode="json")
        await manager.broadcast_event(game_id, event_data)
        await publish_live_message({"type": "event", "game_id": game_id, "data": event_data})

    return {
        "game_id": game_id,
        "new_events_count": len(new_events),
        "events": [GameEventResponse.model_validate(e) for e in new_events],
    }


@router.get("/live/events/{game_id}", response_model=GameEventsListResponse)
async def live_events(
    game_id: str,
    service: LiveSyncService = Depends(get_live_sync_service),
    _admin: AdminUser = Depends(require_roles("superadmin", "operator")),
):
    events = await service.get_game_events(game_id)
    return GameEventsListResponse(
        game_id=game_id,
        events=[GameEventResponse.model_validate(e) for e in events],
        total=len(events),
    )


@router.get("/live/active-games")
async def live_active_games(
    service: LiveSyncService = Depends(get_live_sync_service),
    _admin: AdminUser = Depends(require_roles("superadmin", "operator")),
):
    games = await service.get_active_games()
    return {
        "count": len(games),
        "games": [
            {
                "id": str(g.id),
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
