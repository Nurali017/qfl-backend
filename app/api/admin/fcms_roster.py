"""Admin API for FCMS Roster Sync — view logs, trigger sync, create/link/dismiss players."""

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.admin.deps import require_roles
from app.api.deps import get_db
from app.models import AdminUser, Player, PlayerTeam
from app.models.fcms_roster_sync_log import FcmsRosterSyncLog
from app.schemas.admin.fcms_roster import (
    FcmsCreatePlayerRequest,
    FcmsLinkPlayerRequest,
    FcmsResolveRequest,
    FcmsRosterLogDetail,
    FcmsRosterLogListItem,
    FcmsRosterLogsListResponse,
    FcmsTriggerResponse,
)
from app.services.telegram import send_telegram_message

router = APIRouter(prefix="/fcms-roster", tags=["admin-fcms-roster"])


@router.get("/logs", response_model=FcmsRosterLogsListResponse)
async def list_logs(
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "operator")),
):
    total = (
        await db.execute(select(func.count()).select_from(FcmsRosterSyncLog))
    ).scalar() or 0

    result = await db.execute(
        select(FcmsRosterSyncLog)
        .order_by(FcmsRosterSyncLog.started_at.desc())
        .offset(offset)
        .limit(limit)
    )
    logs = result.scalars().all()

    return FcmsRosterLogsListResponse(
        items=[FcmsRosterLogListItem.model_validate(log) for log in logs],
        total=total,
    )


@router.get("/logs/{log_id}", response_model=FcmsRosterLogDetail)
async def get_log(
    log_id: int,
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "operator")),
):
    result = await db.execute(
        select(FcmsRosterSyncLog).where(FcmsRosterSyncLog.id == log_id)
    )
    log = result.scalar_one_or_none()
    if log is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Log not found")
    return FcmsRosterLogDetail.model_validate(log)


@router.post("/trigger", response_model=FcmsTriggerResponse)
async def trigger_sync(
    _admin: AdminUser = Depends(require_roles("superadmin")),
):
    from app.utils.redis_lock import is_lock_held

    if await is_lock_held("qfl:fcms-roster-sync"):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="FCMS roster sync is already running",
        )

    from app.tasks.fcms_tasks import sync_fcms_rosters
    task = sync_fcms_rosters.delay(triggered_by=f"manual:{_admin.email}")

    return FcmsTriggerResponse(
        task_id=task.id,
        message="FCMS roster sync triggered",
    )


@router.post("/logs/{log_id}/resolve")
async def resolve_item(
    log_id: int,
    payload: FcmsResolveRequest,
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "operator")),
):
    result = await db.execute(
        select(FcmsRosterSyncLog).where(FcmsRosterSyncLog.id == log_id)
    )
    log = result.scalar_one_or_none()
    if log is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Log not found")

    resolved = dict(log.resolved_items or {})
    resolved[payload.item_key] = {
        "action": payload.action,
        "actor": _admin.email,
        "at": datetime.now(timezone.utc).isoformat(),
    }
    log.resolved_items = resolved
    await db.commit()

    return {"status": "ok", "item_key": payload.item_key, "action": payload.action}


@router.post("/create-player")
async def create_player(
    payload: FcmsCreatePlayerRequest,
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin")),
):
    # Validate log exists
    log_result = await db.execute(
        select(FcmsRosterSyncLog).where(FcmsRosterSyncLog.id == payload.log_id)
    )
    log = log_result.scalar_one_or_none()
    if log is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Log not found")

    # Create Player
    # Invariant: _replace_team_bindings is safe here because player is brand new (no existing bindings)
    from app.api.admin.players import _replace_team_bindings
    from app.schemas.admin.players import AdminPlayerTeamBindingInput

    player = Player(
        first_name=payload.first_name,
        last_name=payload.last_name,
        first_name_en=payload.first_name_en,
        last_name_en=payload.last_name_en,
        birthday=payload.birthday,
        fcms_person_id=payload.fcms_person_id,
        country_id=payload.country_id,
    )
    db.add(player)
    await db.flush()

    binding = AdminPlayerTeamBindingInput(
        team_id=payload.team_id,
        season_id=payload.season_id,
        number=payload.number,
        amplua=payload.amplua,
        is_active=True,
        role=1,
    )
    await _replace_team_bindings(db, player.id, [binding])

    # Update resolved_items
    resolved = dict(log.resolved_items or {})
    resolved[payload.item_key] = {
        "action": "created",
        "player_id": player.id,
        "actor": _admin.email,
        "at": datetime.now(timezone.utc).isoformat(),
    }
    log.resolved_items = resolved

    await db.commit()

    # Telegram notification
    player_name = f"{payload.last_name} {payload.first_name}".strip()
    await send_telegram_message(
        f"👤 Игрок <b>создан</b> — {player_name}\n"
        f"🏷 ID: {player.id}\n"
        f"📋 FCMS Roster Sync\n"
        f"👨‍💼 Админ: {_admin.email}"
    )

    return {
        "status": "ok",
        "player_id": player.id,
        "item_key": payload.item_key,
        "action": "created",
    }


@router.post("/link-player")
async def link_player(
    payload: FcmsLinkPlayerRequest,
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin")),
):
    # Validate log exists
    log_result = await db.execute(
        select(FcmsRosterSyncLog).where(FcmsRosterSyncLog.id == payload.log_id)
    )
    log = log_result.scalar_one_or_none()
    if log is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Log not found")

    # Find player
    player_result = await db.execute(select(Player).where(Player.id == payload.player_id))
    player = player_result.scalar_one_or_none()
    if player is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Player not found")

    # Set fcms_person_id
    player.fcms_person_id = payload.fcms_person_id

    # Upsert PlayerTeam — DO NOT use _replace_team_bindings (it deletes all existing bindings!)
    pt_result = await db.execute(
        select(PlayerTeam).where(
            PlayerTeam.player_id == payload.player_id,
            PlayerTeam.team_id == payload.team_id,
            PlayerTeam.season_id == payload.season_id,
        )
    )
    existing_pt = pt_result.scalar_one_or_none()

    if existing_pt:
        if not existing_pt.is_active:
            existing_pt.is_active = True
            existing_pt.is_hidden = False
            existing_pt.left_at = None
        existing_pt.number = payload.number
        existing_pt.amplua = payload.amplua
    else:
        db.add(PlayerTeam(
            player_id=payload.player_id,
            team_id=payload.team_id,
            season_id=payload.season_id,
            number=payload.number,
            amplua=payload.amplua,
            is_active=True,
            role=1,
        ))

    # Update resolved_items
    resolved = dict(log.resolved_items or {})
    resolved[payload.item_key] = {
        "action": "linked",
        "player_id": payload.player_id,
        "actor": _admin.email,
        "at": datetime.now(timezone.utc).isoformat(),
    }
    log.resolved_items = resolved

    await db.commit()

    return {
        "status": "ok",
        "player_id": payload.player_id,
        "item_key": payload.item_key,
        "action": "linked",
    }
