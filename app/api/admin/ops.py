from fastapi import APIRouter, Body, Depends, Query
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.admin.deps import require_roles
from app.api.deps import get_db
from app.config import get_settings
from app.models import AdminUser, Game
from app.schemas.live import GameEventResponse, GameEventsListResponse, LineupSyncResponse, LiveSyncResponse
from app.schemas.sync import SyncResponse, SyncStatus
from app.services.live_sync_service import LiveSyncService
from app.services.sota_client import SotaClient, get_sota_client
from app.services.sync import SyncOrchestrator

settings = get_settings()
router = APIRouter(prefix="/ops", tags=["admin-ops"])


class FinishedLineupsBackfillRequest(BaseModel):
    game_ids: list[str] | None = None


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
    game_id: int,
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
    game_id: int,
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "operator")),
):
    try:
        details = await SyncOrchestrator(db).sync_pre_game_lineup(game_id)
        return SyncResponse(status=SyncStatus.SUCCESS, message="Game lineup sync completed", details=details)
    except Exception as exc:
        return SyncResponse(status=SyncStatus.FAILED, message=f"Game lineup sync failed: {exc}")


@router.post("/sync/finished-lineups-positions", response_model=SyncResponse)
async def sync_finished_lineups_positions(
    payload: FinishedLineupsBackfillRequest | None = Body(default=None),
    season_id: int | None = Query(default=None),
    batch_size: int = Query(default=100, ge=1, le=1000),
    limit: int | None = Query(default=None, ge=1),
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "operator")),
):
    try:
        details = await SyncOrchestrator(db).backfill_finished_games_positions_and_kits(
            season_id=season_id,
            batch_size=batch_size,
            limit=limit,
            game_ids=payload.game_ids if payload else None,
            timeout_seconds=settings.lineup_live_refresh_timeout_seconds,
        )
        return SyncResponse(
            status=SyncStatus.SUCCESS,
            message="Finished games lineup position repair completed",
            details=details,
        )
    except Exception as exc:
        return SyncResponse(status=SyncStatus.FAILED, message=f"Finished lineup repair failed: {exc}")


@router.post("/sync/game-events/{game_id}", response_model=SyncResponse)
async def sync_game_events(
    game_id: int,
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


# ==================== Live Operations ====================


@router.post("/live/start/{game_id}", response_model=LiveSyncResponse)
async def live_start(
    game_id: int,
    service: LiveSyncService = Depends(get_live_sync_service),
    _admin: AdminUser = Depends(require_roles("superadmin", "operator")),
):
    result = await service.start_live_tracking(game_id)
    return LiveSyncResponse(**result)


@router.post("/live/stop/{game_id}", response_model=LiveSyncResponse)
async def live_stop(
    game_id: int,
    service: LiveSyncService = Depends(get_live_sync_service),
    _admin: AdminUser = Depends(require_roles("superadmin", "operator")),
):
    result = await service.stop_live_tracking(game_id)
    return LiveSyncResponse(**result)


@router.post("/live/sync-lineup/{game_id}", response_model=LineupSyncResponse)
async def live_sync_lineup(
    game_id: int,
    source: str = Query("live", description="Lineup source: 'live' (SOTA live feed with ОСНОВНЫЕ/ЗАПАСНЫЕ) or 'pregame' (SOTA pre_game_lineup API)"),
    db: AsyncSession = Depends(get_db),
    client: SotaClient = Depends(get_sota_client),
    _admin: AdminUser = Depends(require_roles("superadmin", "operator")),
):
    try:
        service = LiveSyncService(db, client)
        if source == "pregame":
            details = await service.sync_pregame_lineup(game_id)
        else:
            details = await service.sync_live_lineup(game_id)

        if details.get("error"):
            return LineupSyncResponse(game_id=game_id, lineup_count=0, error=details["error"])

        return LineupSyncResponse(
            game_id=game_id,
            home_formation=details.get("home_formation"),
            away_formation=details.get("away_formation"),
            lineup_count=int(details.get("lineup_count", 0)),
        )
    except Exception as exc:
        return LineupSyncResponse(game_id=game_id, lineup_count=0, error=str(exc))


@router.post("/live/sync-events/{game_id}")
async def live_sync_events(
    game_id: int,
    service: LiveSyncService = Depends(get_live_sync_service),
    _admin: AdminUser = Depends(require_roles("superadmin", "operator")),
):
    new_events = await service.sync_live_events(game_id)
    return {
        "game_id": game_id,
        "new_events_count": len(new_events),
        "events": [GameEventResponse.model_validate(e) for e in new_events],
    }


@router.post("/live/sync-stats/{game_id}")
async def live_sync_stats(
    game_id: int,
    service: LiveSyncService = Depends(get_live_sync_service),
    _admin: AdminUser = Depends(require_roles("superadmin", "operator")),
):
    return await service.sync_live_stats(game_id)


@router.get("/live/events/{game_id}", response_model=GameEventsListResponse)
async def live_events(
    game_id: int,
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
