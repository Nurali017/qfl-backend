import re
from datetime import date as date_type

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.api.deps import get_db
from app.models import Championship, Tournament, Season
from app.utils.localization import get_localized_field
from app.schemas.championship import (
    ChampionshipResponse,
    ChampionshipListResponse,
    ChampionshipTreeResponse,
    ChampionshipTreeListResponse,
    TournamentInChampionship,
    SeasonBrief,
)
from app.schemas.front_map import FrontMapEntry, FrontMapResponse

router = APIRouter(prefix="/championships", tags=["championships"])

FRONT_MAP_KEYS = ("pl", "1l", "cup", "2l", "el")

CHAMPIONSHIP_CODE_PATTERNS: dict[str, tuple[str, ...]] = {
    "pl": ("premier", "премьер", "premier-league", "qpl"),
    "1l": ("first", "первая", "бірінші", "first-league", "1-liga"),
    "cup": ("cup", "кубок", "кубогы"),
    "2l": ("second", "вторая", "екінші", "second-league", "2-liga"),
    "el": ("women", "жен", "әйел", "women-league", "female"),
}


def _season_key(season: Season) -> tuple[date_type, int]:
    return (season.date_start or date_type.min, season.id)


def _is_better_season(new: Season, current: Season | None) -> bool:
    if current is None:
        return True
    return _season_key(new) > _season_key(current)


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


def _detect_front_code(championship: Championship) -> str | None:
    haystack = " ".join(
        filter(
            None,
            [
                championship.slug,
                championship.name,
                championship.name_kz,
                championship.name_en,
            ],
        )
    ).lower()

    for code, patterns in CHAMPIONSHIP_CODE_PATTERNS.items():
        if any(pattern in haystack for pattern in patterns):
            return code
    return None


def _infer_second_league_stage(season: Season) -> str | None:
    haystack = " ".join(
        filter(None, [season.name, season.name_kz, season.name_en])
    ).lower()

    if any(token in haystack for token in ("финал", "final", "қорытынды")):
        return "final"

    if re.search(r"(group|группа|топ|подгруппа)\s*[-:]?\s*[aа]\b", haystack):
        return "a"
    if re.search(r"(group|группа|топ|подгруппа)\s*[-:]?\s*[bв]\b", haystack):
        return "b"

    if any(token in haystack for token in ("south", "оңтүстік", "юг")):
        return "a"
    if any(token in haystack for token in ("north", "солтүстік", "север")):
        return "b"

    return None


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
    """Get full tree: Championship → Tournaments → Seasons."""
    result = await db.execute(
        select(Championship)
        .where(Championship.is_active == True)
        .options(
            selectinload(Championship.tournaments).selectinload(Tournament.seasons)
        )
        .order_by(Championship.sort_order, Championship.id)
    )
    championships = result.scalars().all()

    items = []
    for c in championships:
        tournaments = []
        for t in c.tournaments:
            seasons = [
                SeasonBrief(
                    id=s.id,
                    name=s.name,
                    date_start=s.date_start,
                    date_end=s.date_end,
                    sync_enabled=s.sync_enabled,
                )
                for s in sorted(t.seasons, key=lambda s: s.date_start or s.id, reverse=True)
            ]
            tournaments.append(
                TournamentInChampionship(
                    id=t.id,
                    name=get_localized_field(t, "name", lang),
                    seasons=seasons,
                )
            )

        items.append(
            ChampionshipTreeResponse(
                id=c.id,
                name=get_localized_field(c, "name", lang),
                short_name=get_localized_field(c, "short_name", lang),
                slug=c.slug,
                tournaments=tournaments,
            )
        )

    return ChampionshipTreeListResponse(items=items, total=len(items))


@router.get("/front-map", response_model=FrontMapResponse)
async def get_front_map(db: AsyncSession = Depends(get_db)):
    """Return current season mapping for frontend tournament IDs."""
    result = await db.execute(
        select(Championship)
        .where(Championship.is_active == True)
        .options(selectinload(Championship.tournaments).selectinload(Tournament.seasons))
    )
    championships = result.scalars().all()

    season_candidates: dict[str, Season | None] = {key: None for key in FRONT_MAP_KEYS}
    second_league_stage_candidates: dict[str, Season | None] = {
        "a": None,
        "b": None,
        "final": None,
    }
    second_league_default: Season | None = None

    for championship in championships:
        code = _detect_front_code(championship)
        if code is None:
            continue

        seasons = [season for tournament in championship.tournaments for season in tournament.seasons]
        if not seasons:
            continue

        selected = _pick_current_season(seasons)
        if selected is None:
            continue

        if code == "2l":
            if _is_better_season(selected, second_league_default):
                second_league_default = selected

            for season in seasons:
                stage = _infer_second_league_stage(season)
                if stage is None:
                    continue
                if _is_better_season(season, second_league_stage_candidates[stage]):
                    second_league_stage_candidates[stage] = season
            continue

        if _is_better_season(selected, season_candidates[code]):
            season_candidates[code] = selected

    front_map_items: dict[str, FrontMapEntry] = {
        key: FrontMapEntry(season_id=None) for key in FRONT_MAP_KEYS
    }

    for code in ("pl", "1l", "cup", "el"):
        season = season_candidates[code]
        if season is not None:
            front_map_items[code] = FrontMapEntry(season_id=season.id)

    second_stage_map = {
        stage: season.id
        for stage, season in second_league_stage_candidates.items()
        if season is not None
    }
    second_default_id = (
        second_stage_map.get("a")
        or (second_league_default.id if second_league_default is not None else None)
    )
    front_map_items["2l"] = FrontMapEntry(
        season_id=second_default_id,
        stages=second_stage_map or None,
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
