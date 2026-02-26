import logging

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.admin.deps import get_current_admin, require_roles
from app.api.deps import get_db
from app.caching import invalidate_pattern
from app.models import AdminUser
from app.schemas.admin.cup_draw import (
    AddPairRequest,
    CupDrawListResponse,
    CupDrawResponse,
    DrawPairResponse,
    ParticipantTeamResponse,
)
from app.schemas.cup_draw import CupDrawTeamBrief
from app.services.cup_draw import (
    add_pair,
    complete_draw,
    delete_pair,
    get_draw,
    get_draws_for_season,
    get_participant_teams,
    publish_pair,
    _load_teams_by_id,
)

router = APIRouter(
    prefix="/cup-draw",
    tags=["admin-cup-draw"],
    dependencies=[Depends(require_roles("superadmin", "operator"))],
)

logger = logging.getLogger(__name__)


async def _invalidate_cup_related_cache() -> None:
    await invalidate_pattern("*app.api.seasons*")
    await invalidate_pattern("*app.api.cup*")


def _draw_to_response(draw, teams_by_id: dict | None = None) -> CupDrawResponse:
    pairs = []
    for p in (draw.pairs or []):
        team1 = teams_by_id.get(p["team1_id"]) if teams_by_id else None
        team2 = teams_by_id.get(p["team2_id"]) if teams_by_id else None
        # Legacy fallback: if is_published/side missing
        is_published = p.get("is_published", draw.status == "published")
        side = p.get("side", "center")
        pairs.append(DrawPairResponse(
            team1_id=p["team1_id"],
            team2_id=p["team2_id"],
            sort_order=p.get("sort_order", 0),
            side=side,
            is_published=is_published,
            team1=CupDrawTeamBrief(id=team1.id, name=team1.name, logo_url=team1.logo_url) if team1 else None,
            team2=CupDrawTeamBrief(id=team2.id, name=team2.name, logo_url=team2.logo_url) if team2 else None,
        ))
    return CupDrawResponse(
        id=draw.id,
        season_id=draw.season_id,
        round_key=draw.round_key,
        status=draw.status,
        pairs=pairs,
        created_at=draw.created_at,
        updated_at=draw.updated_at,
    )


async def _enrich_draw(db: AsyncSession, draw) -> CupDrawResponse:
    team_ids: set[int] = set()
    for p in (draw.pairs or []):
        team_ids.add(p["team1_id"])
        team_ids.add(p["team2_id"])
    teams_by_id = await _load_teams_by_id(db, team_ids)
    return _draw_to_response(draw, teams_by_id)


@router.get("/draws", response_model=CupDrawListResponse)
async def list_draws(
    season_id: int = Query(...),
    db: AsyncSession = Depends(get_db),
):
    draws = await get_draws_for_season(db, season_id)
    # Collect all team ids
    all_team_ids: set[int] = set()
    for d in draws:
        for p in (d.pairs or []):
            all_team_ids.add(p["team1_id"])
            all_team_ids.add(p["team2_id"])
    teams_by_id = await _load_teams_by_id(db, all_team_ids)
    items = [_draw_to_response(d, teams_by_id) for d in draws]
    return CupDrawListResponse(items=items)


@router.get("/draws/{season_id}/{round_key}", response_model=CupDrawResponse)
async def get_draw_endpoint(
    season_id: int,
    round_key: str,
    db: AsyncSession = Depends(get_db),
):
    from fastapi import HTTPException
    draw = await get_draw(db, season_id, round_key)
    if draw is None:
        raise HTTPException(status_code=404, detail="Draw not found")
    return await _enrich_draw(db, draw)


@router.post("/draws/{season_id}/{round_key}/pairs", response_model=CupDrawResponse)
async def add_pair_endpoint(
    season_id: int,
    round_key: str,
    body: AddPairRequest,
    db: AsyncSession = Depends(get_db),
    current_admin: AdminUser = Depends(get_current_admin),
):
    draw = await add_pair(
        db, current_admin, season_id, round_key,
        body.team1_id, body.team2_id, body.sort_order, body.side,
    )
    logger.info(
        "cup_draw_action action=add_pair season_id=%s round_key=%s sort_order=%s admin_user_id=%s",
        season_id, round_key, body.sort_order, current_admin.id,
    )
    await _invalidate_cup_related_cache()
    return await _enrich_draw(db, draw)


@router.post(
    "/draws/{season_id}/{round_key}/pairs/{sort_order}/publish",
    response_model=CupDrawResponse,
)
async def publish_pair_endpoint(
    season_id: int,
    round_key: str,
    sort_order: int,
    side: str | None = Query(default=None),
    db: AsyncSession = Depends(get_db),
    current_admin: AdminUser = Depends(get_current_admin),
):
    draw = await publish_pair(db, current_admin, season_id, round_key, sort_order, side)
    logger.info(
        "cup_draw_action action=publish_pair season_id=%s round_key=%s sort_order=%s admin_user_id=%s",
        season_id, round_key, sort_order, current_admin.id,
    )
    await _invalidate_cup_related_cache()
    return await _enrich_draw(db, draw)


@router.delete(
    "/draws/{season_id}/{round_key}/pairs/{sort_order}",
    response_model=CupDrawResponse,
)
async def delete_pair_endpoint(
    season_id: int,
    round_key: str,
    sort_order: int,
    side: str | None = Query(default=None),
    db: AsyncSession = Depends(get_db),
    current_admin: AdminUser = Depends(get_current_admin),
):
    draw = await delete_pair(db, season_id, round_key, sort_order, side)
    logger.info(
        "cup_draw_action action=delete_pair season_id=%s round_key=%s sort_order=%s admin_user_id=%s",
        season_id, round_key, sort_order, current_admin.id,
    )
    await _invalidate_cup_related_cache()
    return await _enrich_draw(db, draw)


@router.post("/draws/{season_id}/{round_key}/complete", response_model=CupDrawResponse)
async def complete_draw_endpoint(
    season_id: int,
    round_key: str,
    db: AsyncSession = Depends(get_db),
    current_admin: AdminUser = Depends(get_current_admin),
):
    draw = await complete_draw(db, current_admin, season_id, round_key)
    logger.info(
        "cup_draw_action action=complete season_id=%s round_key=%s admin_user_id=%s",
        season_id, round_key, current_admin.id,
    )
    await _invalidate_cup_related_cache()
    return await _enrich_draw(db, draw)


@router.get("/participants", response_model=list[ParticipantTeamResponse])
async def list_participants(
    season_id: int = Query(...),
    db: AsyncSession = Depends(get_db),
):
    teams = await get_participant_teams(db, season_id)
    return [ParticipantTeamResponse(**t) for t in teams]
