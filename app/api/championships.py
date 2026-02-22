from datetime import date as date_type

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.api.deps import get_db
from app.models import Championship, Season
from app.utils.localization import get_localized_field
from app.schemas.championship import (
    ChampionshipResponse,
    ChampionshipListResponse,
    ChampionshipTreeResponse,
    ChampionshipTreeListResponse,
    SeasonBrief,
)
from app.schemas.front_map import FrontMapEntry, FrontMapResponse

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
    lang: str = Query(default="ru", pattern="^(kz|ru|en)$"),
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
async def get_championships_tree(
    lang: str = Query(default="ru", pattern="^(kz|ru|en)$"),
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
                name=s.name,
                date_start=s.date_start,
                date_end=s.date_end,
                sync_enabled=s.sync_enabled,
                frontend_code=s.frontend_code,
                tournament_type=s.tournament_type,
            )
            for s in sorted(c.seasons, key=lambda s: s.date_start or s.id, reverse=True)
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
async def get_front_map(
    lang: str = Query(default="ru", pattern="^(kz|ru|en)$"),
    db: AsyncSession = Depends(get_db),
):
    """Return current season mapping for frontend tournament IDs.

    Uses the `frontend_code` column on seasons to build the map.
    For each frontend_code, picks the current (active) season.
    """
    result = await db.execute(
        select(Season)
        .where(Season.frontend_code.isnot(None))
        .options(selectinload(Season.championship))
    )
    all_seasons = result.scalars().all()

    # Group seasons by frontend_code
    by_code: dict[str, list[Season]] = {}
    for s in all_seasons:
        by_code.setdefault(s.frontend_code, []).append(s)

    front_map_items: dict[str, FrontMapEntry] = {}
    for code, seasons in by_code.items():
        selected = _pick_current_season(seasons)
        if selected is None:
            continue

        sponsor = (
            get_localized_field(selected, "sponsor_name", lang)
            if lang == "kz" and selected.sponsor_name_kz
            else selected.sponsor_name
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
            current_round=selected.current_round,
            total_rounds=selected.total_rounds,
            sort_order=selected.sort_order,
        )

    return FrontMapResponse(items=front_map_items)


@router.get("/{championship_id}", response_model=ChampionshipResponse)
async def get_championship(
    championship_id: int,
    lang: str = Query(default="ru", pattern="^(kz|ru|en)$"),
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
