from datetime import date as date_type

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import func, select
from sqlalchemy.orm import selectinload

from app.api.deps import get_db
from app.models import Championship, Game, Season
from app.services.season_visibility import is_season_visible_clause
from app.utils.localization import get_localized_field
from app.schemas.championship import (
    ChampionshipResponse,
    ChampionshipListResponse,
    ChampionshipTreeResponse,
    ChampionshipTreeListResponse,
    SeasonBrief,
)
from app.schemas.front_map import FrontMapEntry, FrontMapResponse, SeasonOption
from fastapi_cache.decorator import cache

router = APIRouter(prefix="/championships", tags=["championships"])


def _season_key(season: Season) -> tuple[date_type, int]:
    return (season.date_start or date_type.min, season.id)


def _pick_current_season(seasons: list[Season]) -> Season | None:
    if not seasons:
        return None

    today = date_type.today()
    active_seasons = [
        season
        for season in seasons
        if (season.date_start is None or season.date_start <= today)
        and (season.date_end is None or season.date_end >= today)
    ]
    candidates = active_seasons or seasons
    return max(candidates, key=_season_key)


@router.get("", response_model=ChampionshipListResponse)
async def get_championships(
    lang: str = Query(default="kz", pattern="^(kz|ru|en)$"),
    db: AsyncSession = Depends(get_db),
):
    """Get all championships sorted by sort_order."""
    result = await db.execute(
        select(Championship)
        .where(Championship.is_active == True)
        .order_by(Championship.sort_order, Championship.id)
    )
    championships = result.scalars().all()

    items = []
    for c in championships:
        items.append(
            ChampionshipResponse(
                id=c.id,
                name=get_localized_field(c, "name", lang),
                short_name=get_localized_field(c, "short_name", lang),
                slug=c.slug,
                sort_order=c.sort_order,
                is_active=c.is_active,
            )
        )

    return ChampionshipListResponse(items=items, total=len(items))


@router.get("/tree", response_model=ChampionshipTreeListResponse)
@cache(expire=14400)
async def get_championships_tree(
    lang: str = Query(default="kz", pattern="^(kz|ru|en)$"),
    db: AsyncSession = Depends(get_db),
):
    """Get full tree: Championship → Seasons."""
    result = await db.execute(
        select(Championship)
        .where(Championship.is_active == True)
        .options(selectinload(Championship.seasons))
        .order_by(Championship.sort_order, Championship.id)
    )
    championships = result.scalars().all()

    items = []
    for c in championships:
        seasons = [
            SeasonBrief(
                id=s.id,
                name=get_localized_field(s, "name", lang),
                date_start=s.date_start,
                date_end=s.date_end,
                sync_enabled=s.sync_enabled,
                frontend_code=s.frontend_code,
                tournament_type=s.tournament_type,
            )
            for s in sorted(c.seasons, key=lambda s: (s.date_start or date_type.min, s.id), reverse=True)
            if s.is_visible
        ]

        items.append(
            ChampionshipTreeResponse(
                id=c.id,
                name=get_localized_field(c, "name", lang),
                short_name=get_localized_field(c, "short_name", lang),
                slug=c.slug,
                seasons=seasons,
            )
        )

    return ChampionshipTreeListResponse(items=items, total=len(items))


@router.get("/front-map", response_model=FrontMapResponse)
@cache(expire=14400)
async def get_front_map(
    lang: str = Query(default="kz", pattern="^(kz|ru|en)$"),
    db: AsyncSession = Depends(get_db),
):
    """Return current season mapping for frontend tournament IDs.

    Uses the `frontend_code` column on seasons to build the map.
    For each frontend_code, picks the current (active) season.
    """
    result = await db.execute(
        select(Season)
        .where(
            Season.frontend_code.isnot(None),
            is_season_visible_clause(),
        )
        .options(selectinload(Season.championship))
    )
    all_seasons = result.scalars().all()

    # Group seasons by frontend_code
    by_code: dict[str, list[Season]] = {}
    for s in all_seasons:
        by_code.setdefault(s.frontend_code, []).append(s)

    selected_by_code: dict[str, Season] = {}
    for code, seasons in by_code.items():
        selected = _pick_current_season(seasons)
        if selected is not None:
            selected_by_code[code] = selected

    max_tour_by_season: dict[int, int] = {}
    selected_season_ids = [season.id for season in selected_by_code.values()]
    if selected_season_ids:
        max_tour_result = await db.execute(
            select(
                Game.season_id,
                func.max(Game.tour).label("max_tour"),
            )
            .where(
                Game.season_id.in_(selected_season_ids),
                Game.tour.isnot(None),
            )
            .group_by(Game.season_id)
        )
        max_tour_by_season = {
            season_id: int(max_tour)
            for season_id, max_tour in max_tour_result.all()
            if season_id is not None and max_tour is not None
        }

    front_map_items: dict[str, FrontMapEntry] = {}
    for code, selected in selected_by_code.items():
        seasons = by_code[code]

        sponsor = get_localized_field(selected, "sponsor_name", lang)
        total_rounds = (
            selected.total_rounds
            if selected.total_rounds is not None
            else max_tour_by_season.get(selected.id)
        )

        season_options = []
        for s in sorted(seasons, key=_season_key, reverse=True):
            year = s.date_start.year if s.date_start else None
            if year:
                season_options.append(
                    SeasonOption(season_id=s.id, year=year, name=get_localized_field(s, "name", lang))
                )

        front_map_items[code] = FrontMapEntry(
            season_id=selected.id,
            name=get_localized_field(selected, "name", lang),
            tournament_type=selected.tournament_type,
            tournament_format=selected.tournament_format,
            has_table=selected.has_table,
            has_bracket=selected.has_bracket,
            sponsor_name=sponsor,
            logo=selected.logo,
            colors=selected.colors,
            final_stage_ids=selected.final_stage_ids,
            current_round=selected.current_round,
            total_rounds=total_rounds,
            sort_order=selected.sort_order,
            seasons=season_options,
        )

    return FrontMapResponse(items=front_map_items)


@router.get("/{championship_id}", response_model=ChampionshipResponse)
async def get_championship(
    championship_id: int,
    lang: str = Query(default="kz", pattern="^(kz|ru|en)$"),
    db: AsyncSession = Depends(get_db),
):
    """Get championship by ID."""
    result = await db.execute(
        select(Championship).where(Championship.id == championship_id)
    )
    c = result.scalar_one_or_none()

    if not c:
        raise HTTPException(status_code=404, detail="Championship not found")

    return ChampionshipResponse(
        id=c.id,
        name=get_localized_field(c, "name", lang),
        short_name=get_localized_field(c, "short_name", lang),
        slug=c.slug,
        sort_order=c.sort_order,
        is_active=c.is_active,
    )
