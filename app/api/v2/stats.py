from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db
from app.models import Country, Player, PlayerSeasonStats, PlayerTeam, Team, TeamSeasonStats
from app.schemas.country import CountryInPlayer
from app.schemas.stats_v2 import (
    PlayerStatsTableEntryV2,
    PlayerStatsTableResponseV2,
    PlayerStatsV2,
    StatsCatalogResponseV2,
    TeamStatsTableEntryV2,
    TeamStatsTableResponseV2,
    TeamStatsV2,
)
from app.services.season_filters import get_group_team_ids
from app.services.season_visibility import ensure_visible_season_or_404, resolve_visible_season_id
from app.services.stats_v2 import (
    PLAYER_V2_SORT_FIELDS,
    TEAM_V2_SORT_FIELDS,
    build_stats_catalog_payload,
    get_player_detail_payload_with_ranks,
    get_team_detail_payload_with_ranks,
    build_player_stats_payload,
    build_team_stats_payload,
    get_team_clean_sheets_map,
    sort_player_stats_items,
    sort_team_stats_items,
)
from app.utils.localization import get_localized_field
from app.utils.team_logo_fallback import resolve_team_logo_url

router = APIRouter(tags=["stats-v2"])

AMPLUA_TO_POSITION = {1: "GK", 2: "DEF", 3: "MID", 4: "FWD"}
POSITION_TO_AMPLUA = {v: k for k, v in AMPLUA_TO_POSITION.items()}


@router.get("/stats/catalog", response_model=StatsCatalogResponseV2)
async def get_stats_catalog_v2():
    return StatsCatalogResponseV2(**build_stats_catalog_payload())


@router.get("/players/{player_id}/stats", response_model=PlayerStatsV2 | None)
async def get_player_stats_v2(
    player_id: int,
    season_id: int | None = Query(default=None),
    db: AsyncSession = Depends(get_db),
):
    season_id = await resolve_visible_season_id(db, season_id)
    payload = await get_player_detail_payload_with_ranks(
        db,
        season_id=season_id,
        player_id=player_id,
    )
    if not payload:
        return None

    return PlayerStatsV2(**payload)


@router.get("/teams/{team_id}/stats", response_model=TeamStatsV2 | None)
async def get_team_stats_v2(
    team_id: int,
    season_id: int | None = Query(default=None),
    db: AsyncSession = Depends(get_db),
):
    season_id = await resolve_visible_season_id(db, season_id)
    payload = await get_team_detail_payload_with_ranks(
        db,
        season_id=season_id,
        team_id=team_id,
    )
    if not payload:
        return None

    return TeamStatsV2(**payload)


@router.get("/seasons/{season_id}/player-stats", response_model=PlayerStatsTableResponseV2)
async def get_player_stats_table_v2(
    season_id: int,
    sort_by: str = Query(default="goal"),
    team_id: int | None = Query(default=None),
    group: str | None = Query(default=None),
    position_code: str | None = Query(default=None, pattern="^(GK|DEF|MID|FWD)$"),
    nationality: str | None = Query(default=None, pattern="^(kz|foreign)$"),
    limit: int = Query(default=50, le=100),
    offset: int = Query(default=0, ge=0),
    lang: str = Query(default="kz", pattern="^(kz|ru|en)$"),
    db: AsyncSession = Depends(get_db),
):
    await ensure_visible_season_or_404(db, season_id)

    if sort_by not in PLAYER_V2_SORT_FIELDS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid sort_by field. Available: {', '.join(PLAYER_V2_SORT_FIELDS)}",
        )

    group_team_ids: list[int] | None = None
    if group:
        group_team_ids = await get_group_team_ids(db, season_id, group)
        if not group_team_ids:
            return PlayerStatsTableResponseV2(
                season_id=season_id,
                sort_by=sort_by,
                items=[],
                total=0,
            )

    contract_photo_subq = (
        select(PlayerTeam.photo_url)
        .where(
            PlayerTeam.player_id == PlayerSeasonStats.player_id,
            PlayerTeam.team_id == PlayerSeasonStats.team_id,
            PlayerTeam.season_id == PlayerSeasonStats.season_id,
        )
        .limit(1)
        .correlate(PlayerSeasonStats)
        .scalar_subquery()
    )

    contract_amplua_subq = (
        select(PlayerTeam.amplua)
        .where(
            PlayerTeam.player_id == PlayerSeasonStats.player_id,
            PlayerTeam.team_id == PlayerSeasonStats.team_id,
            PlayerTeam.season_id == PlayerSeasonStats.season_id,
        )
        .limit(1)
        .correlate(PlayerSeasonStats)
        .scalar_subquery()
    )

    filters = [PlayerSeasonStats.season_id == season_id]
    if team_id is not None:
        filters.append(PlayerSeasonStats.team_id == team_id)
    if group_team_ids is not None:
        filters.append(PlayerSeasonStats.team_id.in_(group_team_ids))
    if position_code:
        filters.append(contract_amplua_subq == POSITION_TO_AMPLUA[position_code])
    if nationality == "kz":
        filters.append(func.upper(Country.code) == "KZ")
    elif nationality == "foreign":
        filters.append(Country.code.is_not(None))
        filters.append(func.upper(Country.code) != "KZ")

    result = await db.execute(
        select(
            PlayerSeasonStats,
            Player,
            Team,
            Country,
            contract_photo_subq.label("contract_photo"),
            contract_amplua_subq.label("contract_amplua"),
        )
        .join(Player, PlayerSeasonStats.player_id == Player.id)
        .outerjoin(Team, PlayerSeasonStats.team_id == Team.id)
        .outerjoin(Country, Player.country_id == Country.id)
        .where(*filters)
    )
    rows = result.all()

    items: list[PlayerStatsTableEntryV2] = []
    for stats, player, team, country, contract_photo, contract_amplua in rows:
        country_data = None
        if country:
            country_data = CountryInPlayer(
                id=country.id,
                code=country.code,
                name=get_localized_field(country, "name", lang),
                flag_url=country.flag_url,
            )

        items.append(
            PlayerStatsTableEntryV2(
                first_name=get_localized_field(player, "first_name", lang),
                last_name=get_localized_field(player, "last_name", lang),
                photo_url=contract_photo or player.photo_url,
                country=country_data,
                team_name=get_localized_field(team, "name", lang) if team else None,
                team_logo=resolve_team_logo_url(team),
                player_type=player.player_type,
                position_code=AMPLUA_TO_POSITION.get(contract_amplua),
                **build_player_stats_payload(stats),
            )
        )

    sorted_items = sort_player_stats_items(items, sort_by)
    paged_items = sorted_items[offset : offset + limit]

    return PlayerStatsTableResponseV2(
        season_id=season_id,
        sort_by=sort_by,
        items=paged_items,
        total=len(sorted_items),
    )


@router.get("/seasons/{season_id}/team-stats", response_model=TeamStatsTableResponseV2)
async def get_team_stats_table_v2(
    season_id: int,
    sort_by: str = Query(default="points"),
    group: str | None = Query(default=None),
    limit: int = Query(default=50, le=100),
    offset: int = Query(default=0, ge=0),
    lang: str = Query(default="kz", pattern="^(kz|ru|en)$"),
    db: AsyncSession = Depends(get_db),
):
    await ensure_visible_season_or_404(db, season_id)

    if sort_by not in TEAM_V2_SORT_FIELDS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid sort_by field. Available: {', '.join(TEAM_V2_SORT_FIELDS)}",
        )

    group_team_ids: list[int] | None = None
    if group:
        group_team_ids = await get_group_team_ids(db, season_id, group)
        if not group_team_ids:
            return TeamStatsTableResponseV2(
                season_id=season_id,
                sort_by=sort_by,
                items=[],
                total=0,
            )

    filters = [TeamSeasonStats.season_id == season_id]
    if group_team_ids is not None:
        filters.append(TeamSeasonStats.team_id.in_(group_team_ids))

    result = await db.execute(
        select(TeamSeasonStats, Team)
        .join(Team, TeamSeasonStats.team_id == Team.id)
        .where(*filters)
    )
    rows = result.all()

    team_ids = [team.id for _, team in rows]
    clean_sheets_map = await get_team_clean_sheets_map(db, season_id, team_ids=team_ids)

    items = [
        TeamStatsTableEntryV2(
            team_name=get_localized_field(team, "name", lang),
            team_logo=resolve_team_logo_url(team),
            **build_team_stats_payload(
                stats,
                clean_sheets=clean_sheets_map.get(team.id, 0),
            ),
        )
        for stats, team in rows
    ]

    sorted_items = sort_team_stats_items(items, sort_by)
    paged_items = sorted_items[offset : offset + limit]

    return TeamStatsTableResponseV2(
        season_id=season_id,
        sort_by=sort_by,
        items=paged_items,
        total=len(sorted_items),
    )
