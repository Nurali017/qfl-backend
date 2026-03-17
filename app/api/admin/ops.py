from datetime import datetime

from fastapi import APIRouter, Body, Depends, File, Form, HTTPException, Query, UploadFile
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from app.api.admin.deps import require_roles
from app.api.deps import get_db
from app.models import AdminUser, Broadcaster, Game, GameReferee, Referee, RefereeRole, Stadium
from app.models.game import GameStatus
from app.services.poster_parser import PosterParserService
from app.services.referee_parser import RefereeParserService
from app.schemas.live import GameEventResponse, GameEventsListResponse, LineupSyncResponse, LiveSyncResponse
from app.schemas.sync import SyncResponse, SyncStatus
from app.services.live_sync_service import LiveSyncService
from app.services.season_visibility import get_current_season_id
from app.services.sota_client import SotaClient, get_sota_client
from app.services.sync import GameSyncService, SyncOrchestrator
from app.tasks.sync_tasks import resync_extended_stats_task, backfill_player_tour_stats_task
router = APIRouter(prefix="/ops", tags=["admin-ops"])


async def _require_sync_enabled(game_id: int, db: AsyncSession) -> Game:
    """Fetch game and raise 400 if sync is disabled for it."""
    result = await db.execute(select(Game).where(Game.id == game_id))
    game = result.scalar_one_or_none()
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")
    if game.sync_disabled:
        raise HTTPException(status_code=400, detail="Sync is disabled for this game")
    return game


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
    force: bool = Query(default=False),
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "operator")),
):
    season_id = season_id or await get_current_season_id(db)
    try:
        result = await SyncOrchestrator(db).full_sync(season_id, force=force)
        return SyncResponse(status=SyncStatus.SUCCESS, message="Full sync completed", details=result)
    except Exception as exc:
        return SyncResponse(status=SyncStatus.FAILED, message=f"Full sync failed: {exc}")


@router.post("/sync/games", response_model=SyncResponse)
async def sync_games(
    season_id: int = Query(default=None),
    force: bool = Query(default=False),
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "operator")),
):
    season_id = season_id or await get_current_season_id(db)
    try:
        count = await SyncOrchestrator(db).sync_games(season_id, force=force)
        return SyncResponse(status=SyncStatus.SUCCESS, message="Games sync completed", details={"games_synced": count})
    except Exception as exc:
        return SyncResponse(status=SyncStatus.FAILED, message=f"Games sync failed: {exc}")


@router.post("/sync/team-season-stats", response_model=SyncResponse)
async def sync_team_season_stats(
    season_id: int = Query(default=None),
    force: bool = Query(default=False),
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "operator")),
):
    season_id = season_id or await get_current_season_id(db)
    try:
        count = await SyncOrchestrator(db).sync_team_season_stats(season_id, force=force)
        return SyncResponse(status=SyncStatus.SUCCESS, message="Team season stats sync completed", details={"teams_synced": count})
    except Exception as exc:
        return SyncResponse(status=SyncStatus.FAILED, message=f"Team season stats sync failed: {exc}")


@router.post("/sync/best-players", response_model=SyncResponse)
async def sync_best_players(
    season_id: int = Query(default=None),
    force: bool = Query(default=False),
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "operator")),
):
    season_id = season_id or await get_current_season_id(db)
    try:
        count = await SyncOrchestrator(db).sync_best_players(season_id, force=force)
        return SyncResponse(status=SyncStatus.SUCCESS, message="Best players sync completed", details={"players_synced": count})
    except Exception as exc:
        return SyncResponse(status=SyncStatus.FAILED, message=f"Best players sync failed: {exc}")


@router.post("/sync/player-season-stats", response_model=SyncResponse)
async def sync_player_season_stats(
    season_id: int = Query(default=None),
    force: bool = Query(default=False),
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "operator")),
):
    season_id = season_id or await get_current_season_id(db)
    try:
        count = await SyncOrchestrator(db).sync_player_stats(season_id, force=force)
        return SyncResponse(status=SyncStatus.SUCCESS, message="Player season stats sync completed", details={"players_synced": count})
    except Exception as exc:
        return SyncResponse(status=SyncStatus.FAILED, message=f"Player season stats sync failed: {exc}")


@router.post("/sync/team-of-week", response_model=SyncResponse)
async def sync_team_of_week(
    season_id: int = Query(default=None),
    force: bool = Query(default=False),
    tour_keys: list[str] | None = Query(default=None),
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "operator")),
):
    season_id = season_id or await get_current_season_id(db)
    try:
        details = await SyncOrchestrator(db).sync_team_of_week(
            season_id, force=force, tour_keys=tour_keys
        )
        return SyncResponse(
            status=SyncStatus.SUCCESS,
            message="Team of week sync completed",
            details=details,
        )
    except Exception as exc:
        return SyncResponse(status=SyncStatus.FAILED, message=f"Team of week sync failed: {exc}")


@router.post("/sync/game-stats/{game_id}", response_model=SyncResponse)
async def sync_game_stats(
    game_id: int,
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "operator", "editor")),
):
    await _require_sync_enabled(game_id, db)
    try:
        details = await SyncOrchestrator(db).sync_game_stats(game_id)
        return SyncResponse(status=SyncStatus.SUCCESS, message="Game stats sync completed", details=details)
    except Exception as exc:
        return SyncResponse(status=SyncStatus.FAILED, message=f"Game stats sync failed: {exc}")


@router.post("/sync/game-lineup/{game_id}", response_model=SyncResponse)
async def sync_game_lineup(
    game_id: int,
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "operator", "editor")),
):
    await _require_sync_enabled(game_id, db)
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
    _admin: AdminUser = Depends(require_roles("superadmin", "operator", "editor")),
):
    try:
        details = await SyncOrchestrator(db).sync_game_events(game_id)
        return SyncResponse(status=SyncStatus.SUCCESS, message="Game events sync completed", details=details)
    except Exception as exc:
        return SyncResponse(status=SyncStatus.FAILED, message=f"Game events sync failed: {exc}")


@router.post("/sync/all-game-events", response_model=SyncResponse)
async def sync_all_game_events(
    season_id: int = Query(default=None),
    force: bool = Query(default=False),
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "operator")),
):
    season_id = season_id or await get_current_season_id(db)
    try:
        details = await SyncOrchestrator(db).sync_all_game_events(season_id, force=force)
        return SyncResponse(status=SyncStatus.SUCCESS, message="All game events sync completed", details=details)
    except Exception as exc:
        return SyncResponse(status=SyncStatus.FAILED, message=f"All game events sync failed: {exc}")


class ResyncExtendedStatsRequest(BaseModel):
    game_ids: list[int] | None = None


@router.post("/resync-extended-stats", response_model=SyncResponse)
async def resync_extended_stats(
    payload: ResyncExtendedStatsRequest | None = Body(default=None),
    season_id: int | None = Query(default=None),
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "operator")),
):
    """Queue resync of extended stats via Celery (avoids 504 timeout)."""
    game_ids = payload.game_ids if payload else None
    if not game_ids and not season_id:
        raise HTTPException(400, "Provide game_ids or season_id")

    # 1. Find games
    if game_ids:
        result = await db.execute(
            select(Game).where(Game.id.in_(game_ids), Game.sync_disabled == False)
        )
    else:
        result = await db.execute(
            select(Game).where(
                Game.season_id == season_id,
                Game.status == GameStatus.finished,
                Game.sota_id.isnot(None),
                Game.sync_disabled == False,
            )
        )
    games = list(result.scalars().all())

    if not games:
        return SyncResponse(status=SyncStatus.SUCCESS, message="No games found to resync")

    # 2. Reset extended_stats_synced_at flag
    resolved_ids = [g.id for g in games]
    for game in games:
        game.extended_stats_synced_at = None
    await db.commit()

    # 3. Queue Celery task
    resync_extended_stats_task.delay(resolved_ids)

    return SyncResponse(
        status=SyncStatus.SUCCESS,
        message=f"Queued {len(resolved_ids)} games for resync",
        details={"game_ids": resolved_ids},
    )


@router.post("/sync/player-tour-stats", response_model=SyncResponse)
async def sync_player_tour_stats(
    season_id: int = Query(default=None),
    tour: int = Query(...),
    force: bool = Query(default=False),
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "operator")),
):
    """Sync player tour stats for a single tour."""
    season_id = season_id or await get_current_season_id(db)
    try:
        count = await SyncOrchestrator(db).sync_player_tour_stats(
            season_id, tour, force=force
        )
        return SyncResponse(
            status=SyncStatus.SUCCESS,
            message="Player tour stats sync completed",
            details={"season_id": season_id, "tour": tour, "players_synced": count},
        )
    except Exception as exc:
        return SyncResponse(
            status=SyncStatus.FAILED,
            message=f"Player tour stats sync failed: {exc}",
        )


@router.post("/sync/backfill-player-tour-stats", response_model=SyncResponse)
async def backfill_player_tour_stats(
    season_id: int = Query(...),
    max_tour: int = Query(...),
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "operator")),
):
    """Queue backfill of player tour stats via Celery."""
    backfill_player_tour_stats_task.delay(season_id, max_tour)
    return SyncResponse(
        status=SyncStatus.SUCCESS,
        message=f"Queued backfill for season {season_id}, tours 1..{max_tour}",
        details={"season_id": season_id, "max_tour": max_tour},
    )


@router.post("/backfill-player-stats/{season_id}", response_model=SyncResponse)
async def backfill_player_stats(
    season_id: int,
    db: AsyncSession = Depends(get_db),
    client: SotaClient = Depends(get_sota_client),
    _admin: AdminUser = Depends(require_roles("superadmin", "operator")),
):
    """Fill minutes_played and pass_accuracy from extra_stats JSONB for a season."""
    try:
        service = GameSyncService(db, client)
        updated = await service.backfill_player_stats_from_extra(season_id)
        return SyncResponse(
            status=SyncStatus.SUCCESS,
            message=f"Backfilled {updated} player stats rows",
            details={"season_id": season_id, "updated": updated},
        )
    except Exception as exc:
        return SyncResponse(status=SyncStatus.FAILED, message=f"Backfill failed: {exc}")


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


@router.post("/live/half2/{game_id}")
async def start_second_half(
    game_id: int,
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "operator")),
):
    game = await db.get(Game, game_id)
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")
    game.half2_started_at = datetime.utcnow()
    await db.commit()
    return {"game_id": game_id, "half2_started_at": game.half2_started_at}


@router.post("/live/sync-lineup/{game_id}", response_model=LineupSyncResponse)
async def live_sync_lineup(
    game_id: int,
    source: str = Query("live", description="Lineup source: 'live' (SOTA live feed with ОСНОВНЫЕ/ЗАПАСНЫЕ) or 'pregame' (SOTA pre_game_lineup API)"),
    db: AsyncSession = Depends(get_db),
    client: SotaClient = Depends(get_sota_client),
    _admin: AdminUser = Depends(require_roles("superadmin", "operator", "editor")),
):
    await _require_sync_enabled(game_id, db)
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
    db: AsyncSession = Depends(get_db),
    service: LiveSyncService = Depends(get_live_sync_service),
    _admin: AdminUser = Depends(require_roles("superadmin", "operator", "editor")),
):
    await _require_sync_enabled(game_id, db)
    result = await service.sync_live_events(game_id)
    return {
        "game_id": game_id,
        **result,
    }


@router.post("/live/sync-stats/{game_id}")
async def live_sync_stats(
    game_id: int,
    db: AsyncSession = Depends(get_db),
    service: LiveSyncService = Depends(get_live_sync_service),
    _admin: AdminUser = Depends(require_roles("superadmin", "operator", "editor")),
):
    await _require_sync_enabled(game_id, db)
    return await service.sync_live_stats(game_id)


@router.get("/live/events/{game_id}", response_model=GameEventsListResponse)
async def live_events(
    game_id: int,
    service: LiveSyncService = Depends(get_live_sync_service),
    _admin: AdminUser = Depends(require_roles("superadmin", "operator", "editor")),
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


# ==================== AI Preview ====================


@router.post("/generate-preview/{game_id}", response_model=SyncResponse)
async def generate_preview(
    game_id: int,
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "operator")),
):
    """Generate AI preview for a single game."""
    from app.services.match_preview import MatchPreviewGenerator

    generator = MatchPreviewGenerator()
    if not generator.enabled:
        return SyncResponse(status=SyncStatus.FAILED, message="Anthropic API not configured")

    game = await db.get(Game, game_id)
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")

    try:
        preview_ru = await generator.generate(game_id, "ru", db)
        preview_kz = await generator.generate(game_id, "kz", db)

        if preview_ru or preview_kz:
            game.preview_ru = preview_ru
            game.preview_kz = preview_kz
            game.preview_generated_at = datetime.utcnow()
            await db.commit()

        return SyncResponse(
            status=SyncStatus.SUCCESS,
            message="Preview generated",
            details={
                "preview_ru": (preview_ru[:100] + "...") if preview_ru else None,
                "preview_kz": (preview_kz[:100] + "...") if preview_kz else None,
            },
        )
    except Exception as exc:
        return SyncResponse(status=SyncStatus.FAILED, message=f"Preview generation failed: {exc}")


# ==================== Poster Parser ====================


@router.post("/parse-poster")
async def parse_poster(
    image: UploadFile = File(...),
    season_id: int = Form(...),
    tour: int = Form(...),
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "operator")),
):
    """Parse a match schedule poster image to extract stadiums and broadcasters."""
    service = PosterParserService()
    if not service.enabled:
        raise HTTPException(status_code=400, detail="No AI API configured")

    # Load games for season+tour with team names
    result = await db.execute(
        select(Game)
        .options(joinedload(Game.home_team), joinedload(Game.away_team))
        .where(Game.season_id == season_id, Game.tour == tour)
    )
    games = list(result.unique().scalars().all())
    if not games:
        raise HTTPException(status_code=404, detail="No games found for this season/tour")

    # Build proxies with all team name variants
    def _team_names(team) -> list[str]:
        if not team:
            return []
        return [n for n in [team.name, team.name_kz, getattr(team, 'name_en', None)] if n]

    class _GameProxy:
        def __init__(self, g: Game):
            self.id = g.id
            self.stadium_id = g.stadium_id
            self.home_team_names = _team_names(g.home_team)
            self.away_team_names = _team_names(g.away_team)
            self.home_team_name = g.home_team.name if g.home_team else None
            self.away_team_name = g.away_team.name if g.away_team else None

    game_proxies = [_GameProxy(g) for g in games]
    game_map = {g.id: g for g in game_proxies}

    # Load stadiums and broadcasters
    stadiums = list((await db.execute(select(Stadium))).scalars().all())
    broadcasters = list((await db.execute(select(Broadcaster))).scalars().all())
    stadium_map = {s.id: s for s in stadiums}
    broadcaster_map = {b.id: b for b in broadcasters}

    # AI parses poster + matches to DB context in one call
    image_bytes = await image.read()
    parsed = await service.parse_poster(image_bytes, game_proxies, stadiums, broadcasters)

    # Build response with human-readable names
    results = []
    for m in parsed.get("matches", []):
        game_id = m.get("game_id")
        stadium_id = m.get("stadium_id")
        bc_ids = m.get("broadcaster_ids") or []

        game = game_map.get(game_id) if game_id else None
        stadium = stadium_map.get(stadium_id) if stadium_id else None

        results.append({
            "home_team_name": game.home_team_name if game else m.get("poster_home", "?"),
            "away_team_name": game.away_team_name if game else m.get("poster_away", "?"),
            "parsed_stadium": m.get("poster_stadium", ""),
            "parsed_broadcasters": m.get("poster_broadcasters", []),
            "game_id": game_id,
            "current_stadium_id": game.stadium_id if game else None,
            "matched_stadium_id": stadium_id,
            "matched_stadium_name": stadium.name if stadium else None,
            "matched_broadcaster_ids": [bid for bid in bc_ids if bid in broadcaster_map],
            "matched_broadcaster_names": [broadcaster_map[bid].name for bid in bc_ids if bid in broadcaster_map],
        })

    return {"matches": results}


# ==================== Referee Parser ====================


class RefereeAssignmentItem(BaseModel):
    referee_id: int
    role: RefereeRole


class GameRefereeAssignmentItem(BaseModel):
    game_id: int
    referees: list[RefereeAssignmentItem]


class ApplyRefereesRequest(BaseModel):
    matches: list[GameRefereeAssignmentItem]


@router.post("/parse-referees")
async def parse_referees(
    text: str = Body(..., embed=True),
    season_id: int = Query(...),
    tour: int = Query(...),
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "operator")),
):
    """Parse a referee assignment text to match referees to games."""
    service = RefereeParserService()
    if not service.enabled:
        raise HTTPException(status_code=400, detail="No AI API configured")

    # Load games for season+tour with team names
    result = await db.execute(
        select(Game)
        .options(joinedload(Game.home_team), joinedload(Game.away_team))
        .where(Game.season_id == season_id, Game.tour == tour)
    )
    games = list(result.unique().scalars().all())
    if not games:
        raise HTTPException(status_code=404, detail="No games found for this season/tour")

    # Load all referees
    ref_result = await db.execute(select(Referee).order_by(Referee.last_name))
    referees = list(ref_result.scalars().all())

    # Build context dicts for AI
    def _team_names(team) -> list[str]:
        if not team:
            return []
        return [n for n in [team.name, team.name_kz, getattr(team, "name_en", None)] if n]

    games_ctx = [
        {
            "game_id": g.id,
            "home_team": _team_names(g.home_team),
            "away_team": _team_names(g.away_team),
        }
        for g in games
    ]

    referees_ctx = [
        {
            "referee_id": r.id,
            "names": [
                n
                for n in [
                    f"{r.last_name} {r.first_name}",
                    f"{r.last_name_kz} {r.first_name_kz}" if r.last_name_kz and r.first_name_kz else None,
                    f"{r.last_name_ru} {r.first_name_ru}" if r.last_name_ru and r.first_name_ru else None,
                    f"{r.last_name_en} {r.first_name_en}" if r.last_name_en and r.first_name_en else None,
                ]
                if n
            ],
        }
        for r in referees
    ]

    game_map = {g.id: g for g in games}
    referee_map = {r.id: r for r in referees}

    parsed = await service.parse_referees(text, games_ctx, referees_ctx)

    # Enrich response with DB names for verification
    results = []
    for m in parsed.get("matches", []):
        game_id = m.get("game_id")
        game = game_map.get(game_id) if game_id else None

        enriched_referees = []
        for ref in m.get("referees", []):
            ref_id = ref.get("referee_id")
            db_ref = referee_map.get(ref_id) if ref_id else None
            enriched_referees.append({
                "role": ref.get("role"),
                "parsed_name": ref.get("parsed_name", ""),
                "parsed_city": ref.get("parsed_city", ""),
                "referee_id": ref_id,
                "matched_name": f"{db_ref.last_name} {db_ref.first_name}" if db_ref else None,
                "matched": ref_id is not None and db_ref is not None,
            })

        results.append({
            "game_id": game_id,
            "home_team_parsed": m.get("home_team_parsed", ""),
            "away_team_parsed": m.get("away_team_parsed", ""),
            "home_team_name": game.home_team.name if game and game.home_team else m.get("home_team_parsed", "?"),
            "away_team_name": game.away_team.name if game and game.away_team else m.get("away_team_parsed", "?"),
            "matched": game_id is not None and game is not None,
            "referees": enriched_referees,
        })

    return {"matches": results}


@router.post("/apply-referees")
async def apply_referees(
    payload: ApplyRefereesRequest,
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "operator")),
):
    """Batch apply referee assignments. Replaces all existing referees for each game."""
    total = 0

    for match in payload.matches:
        # Delete existing referee assignments for this game
        existing = await db.execute(
            select(GameReferee).where(GameReferee.game_id == match.game_id)
        )
        for row in existing.scalars().all():
            await db.delete(row)

        # Insert new assignments
        for ref in match.referees:
            db.add(GameReferee(
                game_id=match.game_id,
                referee_id=ref.referee_id,
                role=ref.role,
            ))
            total += 1

    await db.commit()
    return {"total_assignments": total, "matches_updated": len(payload.matches)}
