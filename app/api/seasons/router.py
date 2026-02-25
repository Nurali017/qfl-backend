"""Base season endpoints: list, detail, sync, stages, bracket, teams, groups."""

from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.api.deps import get_db
from app.models import Season, Game, Stage, SeasonParticipant
from app.services.season_participants import resolve_season_participants
from app.services.cup_rounds import build_playoff_bracket_from_rounds, build_schedule_rounds
from app.services.season_visibility import ensure_visible_season_or_404, is_season_visible_clause
from app.utils.localization import get_localized_field
from app.schemas.season import SeasonListResponse, SeasonResponse, SeasonSyncUpdate
from app.schemas.stage import StageResponse, StageListResponse
from app.schemas.playoff_bracket import PlayoffBracketResponse
from app.schemas.season_participant import (
    SeasonParticipantResponse,
    SeasonParticipantListResponse,
    SeasonGroupsResponse,
)

router = APIRouter(prefix="/seasons", tags=["seasons"])

_ensure_visible_season = ensure_visible_season_or_404


def _build_season_response(s: Season) -> SeasonResponse:
    """Build a SeasonResponse from a Season ORM object (with championship loaded)."""
    return SeasonResponse(
        id=s.id,
        name=s.name,
        championship_id=s.championship_id,
        date_start=s.date_start,
        date_end=s.date_end,
        sync_enabled=s.sync_enabled,
        championship_name=s.championship.name if s.championship else None,
        frontend_code=s.frontend_code,
        tournament_type=s.tournament_type,
        tournament_format=s.tournament_format,
        has_table=s.has_table,
        has_bracket=s.has_bracket,
        sponsor_name=s.sponsor_name,
        sponsor_name_kz=s.sponsor_name_kz,
        logo=s.logo,
        current_round=s.current_round,
        total_rounds=s.total_rounds,
        sort_order=s.sort_order,
        colors=s.colors,
        final_stage_ids=s.final_stage_ids,
    )


GOAL_PERIOD_LABELS = ("0-15", "16-30", "31-45+", "46-60", "61-75", "76-90+")


def _get_goal_period_index(half: int | None, minute: int | None) -> int:
    """
    Map a goal event to one of 6 minute buckets.

    Buckets:
    - 0-15, 16-30, 31-45+ (first half with stoppage time)
    - 46-60, 61-75, 76-90+ (second half with stoppage time)
    """
    safe_minute = max(int(minute or 0), 0)

    if half == 1:
        if safe_minute <= 15:
            return 0
        if safe_minute <= 30:
            return 1
        return 2

    if half == 2:
        if safe_minute <= 60:
            return 3
        if safe_minute <= 75:
            return 4
        return 5

    # Fallback to absolute minute buckets if half is unavailable/invalid.
    if safe_minute <= 15:
        return 0
    if safe_minute <= 30:
        return 1
    if safe_minute <= 45:
        return 2
    if safe_minute <= 60:
        return 3
    if safe_minute <= 75:
        return 4
    return 5


@router.get("", response_model=SeasonListResponse)
async def get_seasons(db: AsyncSession = Depends(get_db)):
    """Get all seasons."""
    result = await db.execute(
        select(Season)
        .where(is_season_visible_clause())
        .options(selectinload(Season.championship))
        .order_by(Season.date_start.desc())
    )
    seasons = result.scalars().all()

    items = []
    for s in seasons:
        items.append(_build_season_response(s))

    return SeasonListResponse(items=items, total=len(items))


@router.patch("/{season_id}/sync", response_model=SeasonResponse)
async def update_season_sync(
    season_id: int,
    body: SeasonSyncUpdate,
    db: AsyncSession = Depends(get_db),
):
    """Enable or disable SOTA sync for a season. When disabled, local data is source of truth."""
    result = await db.execute(
        select(Season)
        .where(
            Season.id == season_id,
            is_season_visible_clause(),
        )
        .options(selectinload(Season.championship))
    )
    season = result.scalar_one_or_none()
    if not season:
        raise HTTPException(status_code=404, detail="Season not found")

    season.sync_enabled = body.sync_enabled
    await db.commit()
    await db.refresh(season)

    return _build_season_response(season)


@router.get("/{season_id}", response_model=SeasonResponse)
async def get_season(season_id: int, db: AsyncSession = Depends(get_db)):
    """Get season by ID."""
    result = await db.execute(
        select(Season)
        .where(
            Season.id == season_id,
            is_season_visible_clause(),
        )
        .options(selectinload(Season.championship))
    )
    season = result.scalar_one_or_none()

    if not season:
        raise HTTPException(status_code=404, detail="Season not found")

    return _build_season_response(season)


# ──────────────────────────────────────────
#  Season sub-resources: Stages, Bracket, Teams/Groups
# ──────────────────────────────────────────


@router.get("/{season_id}/stages", response_model=StageListResponse)
async def get_season_stages(
    season_id: int,
    lang: str = Query(default="kz", pattern="^(kz|ru|en)$"),
    db: AsyncSession = Depends(get_db),
):
    """Get stages/tours for a season."""
    await _ensure_visible_season(db, season_id)

    result = await db.execute(
        select(Stage)
        .where(Stage.season_id == season_id)
        .order_by(Stage.sort_order, Stage.stage_number, Stage.id)
    )
    stages = result.scalars().all()

    items = [
        StageResponse(
            id=s.id,
            season_id=s.season_id,
            name=get_localized_field(s, "name", lang),
            stage_number=s.stage_number,
            sort_order=s.sort_order,
        )
        for s in stages
    ]

    return StageListResponse(items=items, total=len(items))


@router.get("/{season_id}/bracket", response_model=PlayoffBracketResponse)
async def get_season_bracket(
    season_id: int,
    lang: str = Query(default="kz", pattern="^(kz|ru|en)$"),
    db: AsyncSession = Depends(get_db),
):
    """Get playoff bracket for a season, derived from games and stages."""
    await _ensure_visible_season(db, season_id)

    stage_result = await db.execute(
        select(Stage)
        .where(Stage.season_id == season_id)
        .order_by(Stage.sort_order, Stage.id)
    )
    stages = list(stage_result.scalars().all())

    games_result = await db.execute(
        select(Game)
        .where(Game.season_id == season_id)
        .options(
            selectinload(Game.home_team),
            selectinload(Game.away_team),
            selectinload(Game.stage),
        )
        .order_by(Game.date, Game.time)
    )
    games = list(games_result.scalars().all())

    rounds = build_schedule_rounds(
        games=games,
        stages=stages,
        lang=lang,
        today=date.today(),
        include_games=True,
    )
    bracket = build_playoff_bracket_from_rounds(season_id, rounds)
    return bracket or PlayoffBracketResponse(season_id=season_id, rounds=[])


@router.get("/{season_id}/teams", response_model=SeasonParticipantListResponse)
async def get_season_teams(
    season_id: int,
    lang: str = Query(default="kz", pattern="^(kz|ru|en)$"),
    db: AsyncSession = Depends(get_db),
):
    """Get all teams participating in a season."""
    await _ensure_visible_season(db, season_id)

    participants = await resolve_season_participants(db, season_id, lang)
    items = [
        SeasonParticipantResponse(
            id=p.entry_id if p.entry_id is not None else -p.team_id,
            team_id=p.team_id,
            team_name=get_localized_field(p.team, "name", lang),
            team_logo=p.team.logo_url,
            season_id=season_id,
            group_name=p.group_name,
            is_disqualified=p.is_disqualified,
            fine_points=p.fine_points,
            sort_order=p.sort_order,
        )
        for p in participants
    ]

    return SeasonParticipantListResponse(items=items, total=len(items))


@router.get("/{season_id}/groups", response_model=SeasonGroupsResponse)
async def get_season_groups(
    season_id: int,
    lang: str = Query(default="kz", pattern="^(kz|ru|en)$"),
    db: AsyncSession = Depends(get_db),
):
    """Get teams grouped by group_name for a season."""
    await _ensure_visible_season(db, season_id)

    result = await db.execute(
        select(SeasonParticipant)
        .where(SeasonParticipant.season_id == season_id)
        .options(selectinload(SeasonParticipant.team))
        .order_by(SeasonParticipant.group_name, SeasonParticipant.sort_order, SeasonParticipant.id)
    )
    entries = result.scalars().all()

    groups: dict[str, list[SeasonParticipantResponse]] = {}
    for tt in entries:
        group_key = tt.group_name or "default"
        item = SeasonParticipantResponse(
            id=tt.id,
            team_id=tt.team_id,
            team_name=get_localized_field(tt.team, "name", lang) if tt.team else None,
            team_logo=tt.team.logo_url if tt.team else None,
            season_id=tt.season_id,
            group_name=tt.group_name,
            is_disqualified=tt.is_disqualified,
            fine_points=tt.fine_points,
            sort_order=tt.sort_order,
        )
        groups.setdefault(group_key, []).append(item)

    return SeasonGroupsResponse(season_id=season_id, groups=groups)
