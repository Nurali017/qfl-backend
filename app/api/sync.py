from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db
from app.services.sync import SyncOrchestrator
from app.schemas.sync import SyncResponse, SyncStatus
from app.config import get_settings

settings = get_settings()

router = APIRouter(prefix="/sync", tags=["sync"])


@router.post("/full", response_model=SyncResponse)
async def sync_full(
    season_id: int = Query(default=None),
    db: AsyncSession = Depends(get_db),
):
    """Perform full data synchronization from SOTA API."""
    if season_id is None:
        season_id = settings.current_season_id

    try:
        orchestrator = SyncOrchestrator(db)
        if not await orchestrator.is_sync_enabled(season_id):
            return SyncResponse(
                status=SyncStatus.SUCCESS,
                message=f"Season {season_id}: sync disabled, skipped",
                details={"skipped": True, "season_id": season_id},
            )
        results = await orchestrator.full_sync(season_id)

        return SyncResponse(
            status=SyncStatus.SUCCESS,
            message="Full synchronization completed successfully",
            details=results,
        )
    except Exception as e:
        return SyncResponse(
            status=SyncStatus.FAILED,
            message=f"Synchronization failed: {str(e)}",
            details=None,
        )


@router.post("/games", response_model=SyncResponse)
async def sync_games(
    season_id: int = Query(default=None),
    db: AsyncSession = Depends(get_db),
):
    """Sync games for a specific season."""
    if season_id is None:
        season_id = settings.current_season_id

    try:
        orchestrator = SyncOrchestrator(db)
        if not await orchestrator.is_sync_enabled(season_id):
            return SyncResponse(
                status=SyncStatus.SUCCESS,
                message=f"Season {season_id}: sync disabled, skipped",
                details={"skipped": True, "season_id": season_id},
            )
        count = await orchestrator.sync_games(season_id)

        return SyncResponse(
            status=SyncStatus.SUCCESS,
            message=f"Games synchronization completed: {count} games synced",
            details={"games_synced": count},
        )
    except Exception as e:
        return SyncResponse(
            status=SyncStatus.FAILED,
            message=f"Games synchronization failed: {str(e)}",
            details=None,
        )


@router.post("/teams", response_model=SyncResponse)
async def sync_teams(db: AsyncSession = Depends(get_db)):
    """Sync teams from SOTA API."""
    try:
        orchestrator = SyncOrchestrator(db)
        count = await orchestrator.reference.sync_teams()

        return SyncResponse(
            status=SyncStatus.SUCCESS,
            message=f"Teams synchronization completed: {count} teams synced",
            details={"teams_synced": count},
        )
    except Exception as e:
        return SyncResponse(
            status=SyncStatus.FAILED,
            message=f"Teams synchronization failed: {str(e)}",
            details=None,
        )


@router.post("/team-logos", response_model=SyncResponse)
async def sync_team_logos(db: AsyncSession = Depends(get_db)):
    """Sync team logos from MinIO storage to database."""
    try:
        orchestrator = SyncOrchestrator(db)
        count = await orchestrator.reference.sync_team_logos()

        return SyncResponse(
            status=SyncStatus.SUCCESS,
            message=f"Team logos synchronization completed: {count} teams updated",
            details={"teams_updated": count},
        )
    except Exception as e:
        return SyncResponse(
            status=SyncStatus.FAILED,
            message=f"Team logos synchronization failed: {str(e)}",
            details=None,
        )


@router.post("/players", response_model=SyncResponse)
async def sync_players(
    season_id: int = Query(default=None),
    db: AsyncSession = Depends(get_db),
):
    """Sync players for a specific season."""
    if season_id is None:
        season_id = settings.current_season_id

    try:
        orchestrator = SyncOrchestrator(db)
        if not await orchestrator.is_sync_enabled(season_id):
            return SyncResponse(
                status=SyncStatus.SUCCESS,
                message=f"Season {season_id}: sync disabled, skipped",
                details={"skipped": True, "season_id": season_id},
            )
        count = await orchestrator.sync_players(season_id)

        return SyncResponse(
            status=SyncStatus.SUCCESS,
            message=f"Players synchronization completed: {count} players synced",
            details={"players_synced": count},
        )
    except Exception as e:
        return SyncResponse(
            status=SyncStatus.FAILED,
            message=f"Players synchronization failed: {str(e)}",
            details=None,
        )


@router.post("/score-table", response_model=SyncResponse)
async def sync_score_table(
    season_id: int = Query(default=None),
    db: AsyncSession = Depends(get_db),
):
    """Sync league table for a specific season."""
    if season_id is None:
        season_id = settings.current_season_id

    try:
        orchestrator = SyncOrchestrator(db)
        if not await orchestrator.is_sync_enabled(season_id):
            return SyncResponse(
                status=SyncStatus.SUCCESS,
                message=f"Season {season_id}: sync disabled, skipped",
                details={"skipped": True, "season_id": season_id},
            )
        count = await orchestrator.sync_score_table(season_id)

        return SyncResponse(
            status=SyncStatus.SUCCESS,
            message=f"Score table synchronization completed: {count} entries synced",
            details={"entries_synced": count},
        )
    except Exception as e:
        return SyncResponse(
            status=SyncStatus.FAILED,
            message=f"Score table synchronization failed: {str(e)}",
            details=None,
        )


@router.post("/game-stats/{game_id}", response_model=SyncResponse)
async def sync_game_stats(game_id: int, db: AsyncSession = Depends(get_db)):
    """Sync statistics for a specific game."""
    try:
        orchestrator = SyncOrchestrator(db)
        results = await orchestrator.sync_game_stats(game_id)

        return SyncResponse(
            status=SyncStatus.SUCCESS,
            message="Game stats synchronization completed",
            details=results,
        )
    except Exception as e:
        return SyncResponse(
            status=SyncStatus.FAILED,
            message=f"Game stats synchronization failed: {str(e)}",
            details=None,
        )


@router.post("/game-lineup/{game_id}", response_model=SyncResponse)
async def sync_game_lineup(game_id: int, db: AsyncSession = Depends(get_db)):
    """Sync pre-game lineup (referees, coaches, lineups) for a specific game."""
    try:
        orchestrator = SyncOrchestrator(db)
        results = await orchestrator.sync_pre_game_lineup(game_id)

        return SyncResponse(
            status=SyncStatus.SUCCESS,
            message="Game lineup synchronization completed",
            details=results,
        )
    except Exception as e:
        return SyncResponse(
            status=SyncStatus.FAILED,
            message=f"Game lineup synchronization failed: {str(e)}",
            details=None,
        )


@router.post("/team-season-stats", response_model=SyncResponse)
async def sync_team_season_stats(
    season_id: int = Query(default=None),
    db: AsyncSession = Depends(get_db),
):
    """Sync team season stats for ALL teams from SOTA API v2 (92 metrics)."""
    if season_id is None:
        season_id = settings.current_season_id

    try:
        orchestrator = SyncOrchestrator(db)
        if not await orchestrator.is_sync_enabled(season_id):
            return SyncResponse(
                status=SyncStatus.SUCCESS,
                message=f"Season {season_id}: sync disabled, skipped",
                details={"skipped": True, "season_id": season_id},
            )
        count = await orchestrator.sync_team_season_stats(season_id)

        return SyncResponse(
            status=SyncStatus.SUCCESS,
            message=f"Team season stats synchronization completed: {count} teams synced",
            details={"teams_synced": count, "season_id": season_id},
        )
    except Exception as e:
        return SyncResponse(
            status=SyncStatus.FAILED,
            message=f"Team season stats synchronization failed: {str(e)}",
            details=None,
        )


@router.post("/player-season-stats", response_model=SyncResponse)
async def sync_player_season_stats(
    season_id: int = Query(default=None),
    db: AsyncSession = Depends(get_db),
):
    """Sync player season stats for ALL players from SOTA API v2 (50+ metrics)."""
    if season_id is None:
        season_id = settings.current_season_id

    try:
        orchestrator = SyncOrchestrator(db)
        if not await orchestrator.is_sync_enabled(season_id):
            return SyncResponse(
                status=SyncStatus.SUCCESS,
                message=f"Season {season_id}: sync disabled, skipped",
                details={"skipped": True, "season_id": season_id},
            )
        count = await orchestrator.sync_player_stats(season_id)

        return SyncResponse(
            status=SyncStatus.SUCCESS,
            message=f"Player season stats synchronization completed: {count} players synced",
            details={"players_synced": count, "season_id": season_id},
        )
    except Exception as e:
        return SyncResponse(
            status=SyncStatus.FAILED,
            message=f"Player season stats synchronization failed: {str(e)}",
            details=None,
        )


@router.post("/game-events/{game_id}", response_model=SyncResponse)
async def sync_game_events(game_id: int, db: AsyncSession = Depends(get_db)):
    """Sync events (goals, cards, substitutions) for a specific game."""
    try:
        orchestrator = SyncOrchestrator(db)
        results = await orchestrator.sync_game_events(game_id)

        return SyncResponse(
            status=SyncStatus.SUCCESS,
            message="Game events synchronization completed",
            details=results,
        )
    except Exception as e:
        return SyncResponse(
            status=SyncStatus.FAILED,
            message=f"Game events synchronization failed: {str(e)}",
            details=None,
        )


@router.post("/all-game-events", response_model=SyncResponse)
async def sync_all_game_events(
    season_id: int = Query(default=None),
    db: AsyncSession = Depends(get_db),
):
    """Sync events for all games in a season."""
    if season_id is None:
        season_id = settings.current_season_id

    try:
        orchestrator = SyncOrchestrator(db)
        if not await orchestrator.is_sync_enabled(season_id):
            return SyncResponse(
                status=SyncStatus.SUCCESS,
                message=f"Season {season_id}: sync disabled, skipped",
                details={"skipped": True, "season_id": season_id},
            )
        results = await orchestrator.sync_all_game_events(season_id)

        return SyncResponse(
            status=SyncStatus.SUCCESS,
            message="All game events synchronization completed",
            details=results,
        )
    except Exception as e:
        return SyncResponse(
            status=SyncStatus.FAILED,
            message=f"All game events synchronization failed: {str(e)}",
            details=None,
        )
