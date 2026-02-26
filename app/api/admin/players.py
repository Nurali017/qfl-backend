from collections import defaultdict

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import delete, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.api.admin.deps import require_roles
from app.api.deps import get_db
from app.caching import invalidate_pattern
from app.models import (
    AdminUser,
    Country,
    GameEvent,
    GameLineup,
    GamePlayerStats,
    Player,
    PlayerSeasonStats,
    PlayerTeam,
    Season,
    Team,
)
from app.services.season_visibility import ensure_visible_season_or_404, is_season_visible_clause
from app.schemas.admin.players import (
    AdminMetaCountry,
    AdminMetaSeason,
    AdminMetaTeam,
    AdminPlayerCreateRequest,
    AdminPlayerResponse,
    AdminPlayersListResponse,
    AdminPlayersMetaResponse,
    AdminPlayerTeamBindingInput,
    AdminPlayerTeamBindingResponse,
    AdminPlayerUpdateRequest,
)

router = APIRouter(prefix="/players", tags=["admin-players"])


async def _ensure_unique_sota_id(
    db: AsyncSession,
    sota_id,
    *,
    exclude_player_id: int | None = None,
) -> None:
    if sota_id is None:
        return

    query = select(Player.id).where(Player.sota_id == sota_id)
    if exclude_player_id is not None:
        query = query.where(Player.id != exclude_player_id)
    existing = await db.execute(query)
    if existing.scalar_one_or_none() is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Player with this sota_id already exists",
        )


async def _validate_binding_refs(
    db: AsyncSession,
    team_bindings: list[AdminPlayerTeamBindingInput],
) -> None:
    if not team_bindings:
        return

    team_ids = sorted({binding.team_id for binding in team_bindings})
    season_ids = sorted({binding.season_id for binding in team_bindings})

    existing_team_ids = set(
        (
            await db.execute(
                select(Team.id).where(Team.id.in_(team_ids))
            )
        ).scalars().all()
    )
    missing_teams = sorted(set(team_ids) - existing_team_ids)
    if missing_teams:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unknown team_id values: {missing_teams}",
        )

    existing_season_ids = set(
        (
            await db.execute(
                select(Season.id).where(
                    Season.id.in_(season_ids),
                    is_season_visible_clause(),
                )
            )
        ).scalars().all()
    )
    missing_seasons = sorted(set(season_ids) - existing_season_ids)
    if missing_seasons:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unknown season_id values: {missing_seasons}",
        )


async def _replace_team_bindings(
    db: AsyncSession,
    player_id: int,
    team_bindings: list[AdminPlayerTeamBindingInput],
) -> None:
    await db.execute(delete(PlayerTeam).where(PlayerTeam.player_id == player_id))
    if not team_bindings:
        return

    await _validate_binding_refs(db, team_bindings)

    unique_bindings: dict[tuple[int, int, int], AdminPlayerTeamBindingInput] = {}
    for binding in team_bindings:
        role_key = binding.role if binding.role is not None else 1
        unique_bindings[(binding.team_id, binding.season_id, role_key)] = binding

    for binding in unique_bindings.values():
        db.add(
            PlayerTeam(
                player_id=player_id,
                team_id=binding.team_id,
                season_id=binding.season_id,
                number=binding.number,
                is_active=binding.is_active,
                is_hidden=binding.is_hidden,
                photo_url=binding.photo_url,
                role=binding.role if binding.role is not None else 1,
                amplua=binding.amplua,
                position_ru=binding.position_ru,
                position_kz=binding.position_kz,
                position_en=binding.position_en,
            )
        )


async def _get_player_bindings(
    db: AsyncSession,
    player_ids: list[int],
) -> dict[int, list[AdminPlayerTeamBindingResponse]]:
    if not player_ids:
        return {}

    result = await db.execute(
        select(PlayerTeam, Team, Season)
        .join(Team, Team.id == PlayerTeam.team_id)
        .join(Season, Season.id == PlayerTeam.season_id)
        .where(is_season_visible_clause())
        .where(PlayerTeam.player_id.in_(player_ids))
        .order_by(PlayerTeam.player_id, PlayerTeam.season_id.desc(), PlayerTeam.team_id.asc())
    )

    bindings_map: dict[int, list[AdminPlayerTeamBindingResponse]] = defaultdict(list)
    for player_team, team, season in result.all():
        bindings_map[player_team.player_id].append(
            AdminPlayerTeamBindingResponse(
                team_id=player_team.team_id,
                season_id=player_team.season_id,
                number=player_team.number,
                is_active=player_team.is_active,
                is_hidden=player_team.is_hidden,
                photo_url=player_team.photo_url,
                role=player_team.role,
                amplua=player_team.amplua,
                position_ru=player_team.position_ru,
                position_kz=player_team.position_kz,
                position_en=player_team.position_en,
                team_name=team.name if team else None,
                season_name=season.name if season else None,
            )
        )
    return bindings_map


def _serialize_player(player: Player, bindings: list[AdminPlayerTeamBindingResponse]) -> AdminPlayerResponse:
    return AdminPlayerResponse(
        id=player.id,
        sota_id=player.sota_id,
        first_name=player.first_name,
        first_name_kz=player.first_name_kz,
        first_name_en=player.first_name_en,
        last_name=player.last_name,
        last_name_kz=player.last_name_kz,
        last_name_en=player.last_name_en,
        nickname=player.nickname,
        nickname_kz=player.nickname_kz,
        nickname_en=player.nickname_en,
        bio=player.bio,
        bio_kz=player.bio_kz,
        bio_en=player.bio_en,
        genius_id=player.genius_id,
        vsporte_id=player.vsporte_id,
        birthday=player.birthday,
        player_type=player.player_type,
        country_id=player.country_id,
        photo_url=player.photo_url,
        age=player.age,
        top_role=player.top_role,
        top_role_kz=player.top_role_kz,
        top_role_en=player.top_role_en,
        team_bindings=bindings,
    )


@router.get("/meta", response_model=AdminPlayersMetaResponse)
async def get_players_meta(
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "editor")),
):
    countries_result = await db.execute(select(Country).order_by(Country.name.asc()))
    countries = countries_result.scalars().all()

    teams_result = await db.execute(select(Team).order_by(Team.name.asc()))
    teams = teams_result.scalars().all()

    seasons_result = await db.execute(
        select(Season)
        .where(is_season_visible_clause())
        .order_by(Season.id.desc())
    )
    seasons = seasons_result.scalars().all()

    return AdminPlayersMetaResponse(
        countries=[
            AdminMetaCountry(id=country.id, code=country.code, name=country.name)
            for country in countries
        ],
        teams=[AdminMetaTeam(id=team.id, name=team.name) for team in teams],
        seasons=[AdminMetaSeason(id=season.id, name=season.name) for season in seasons],
    )


@router.get("", response_model=AdminPlayersListResponse)
async def list_players(
    search: str | None = Query(default=None),
    season_id: int | None = Query(default=None),
    team_id: int | None = Query(default=None),
    linked_to_sota: bool | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "editor")),
):
    if season_id is not None:
        await ensure_visible_season_or_404(db, season_id)

    query = select(Player).options(selectinload(Player.country))

    if season_id is not None or team_id is not None:
        subquery = select(PlayerTeam.player_id).distinct()
        if season_id is not None:
            subquery = subquery.where(PlayerTeam.season_id == season_id)
        if team_id is not None:
            subquery = subquery.where(PlayerTeam.team_id == team_id)
        query = query.where(Player.id.in_(subquery))

    if linked_to_sota is not None:
        query = query.where(Player.sota_id.is_not(None) if linked_to_sota else Player.sota_id.is_(None))

    if search:
        normalized = f"%{search.strip()}%"
        query = query.where(
            or_(
                Player.first_name.ilike(normalized),
                Player.last_name.ilike(normalized),
                Player.first_name_kz.ilike(normalized),
                Player.last_name_kz.ilike(normalized),
                Player.first_name_en.ilike(normalized),
                Player.last_name_en.ilike(normalized),
            )
        )

    total = (
        await db.execute(
            select(func.count()).select_from(query.subquery())
        )
    ).scalar() or 0

    result = await db.execute(
        query.order_by(Player.last_name.asc(), Player.first_name.asc(), Player.id.asc())
        .offset(offset)
        .limit(limit)
    )
    players = list(result.scalars().all())

    bindings_map = await _get_player_bindings(db, [player.id for player in players])

    return AdminPlayersListResponse(
        items=[_serialize_player(player, bindings_map.get(player.id, [])) for player in players],
        total=total,
    )


@router.get("/{player_id}", response_model=AdminPlayerResponse)
async def get_player(
    player_id: int,
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "editor")),
):
    result = await db.execute(
        select(Player)
        .where(Player.id == player_id)
        .options(selectinload(Player.country))
    )
    player = result.scalar_one_or_none()
    if player is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Player not found")

    bindings_map = await _get_player_bindings(db, [player_id])
    return _serialize_player(player, bindings_map.get(player_id, []))


@router.post("", response_model=AdminPlayerResponse, status_code=status.HTTP_201_CREATED)
async def create_player(
    payload: AdminPlayerCreateRequest,
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "editor")),
):
    await _ensure_unique_sota_id(db, payload.sota_id)

    player = Player(
        sota_id=payload.sota_id,
        first_name=payload.first_name,
        first_name_kz=payload.first_name_kz,
        first_name_en=payload.first_name_en,
        last_name=payload.last_name,
        last_name_kz=payload.last_name_kz,
        last_name_en=payload.last_name_en,
        nickname=payload.nickname,
        nickname_kz=payload.nickname_kz,
        nickname_en=payload.nickname_en,
        bio=payload.bio,
        bio_kz=payload.bio_kz,
        bio_en=payload.bio_en,
        genius_id=payload.genius_id,
        vsporte_id=payload.vsporte_id,
        birthday=payload.birthday,
        player_type=payload.player_type,
        country_id=payload.country_id,
        photo_url=payload.photo_url,
        top_role=payload.top_role,
        top_role_kz=payload.top_role_kz,
        top_role_en=payload.top_role_en,
    )
    db.add(player)
    await db.flush()

    await _replace_team_bindings(db, player.id, payload.team_bindings)
    await db.commit()
    await db.refresh(player)
    await invalidate_pattern("*app.api.seasons*")

    bindings_map = await _get_player_bindings(db, [player.id])
    return _serialize_player(player, bindings_map.get(player.id, []))


@router.patch("/{player_id}", response_model=AdminPlayerResponse)
async def update_player(
    player_id: int,
    payload: AdminPlayerUpdateRequest,
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "editor")),
):
    result = await db.execute(select(Player).where(Player.id == player_id))
    player = result.scalar_one_or_none()
    if player is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Player not found")

    await _ensure_unique_sota_id(db, payload.sota_id, exclude_player_id=player_id)

    update_data = payload.model_dump(exclude_unset=True, exclude={"team_bindings"})
    for field_name, value in update_data.items():
        setattr(player, field_name, value)

    if payload.team_bindings is not None:
        await _replace_team_bindings(db, player_id, payload.team_bindings)

    await db.commit()
    await db.refresh(player)
    await invalidate_pattern("*app.api.seasons*")

    bindings_map = await _get_player_bindings(db, [player.id])
    return _serialize_player(player, bindings_map.get(player.id, []))


@router.delete("/{player_id}")
async def delete_player(
    player_id: int,
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "editor")),
):
    result = await db.execute(select(Player).where(Player.id == player_id))
    player = result.scalar_one_or_none()
    if player is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Player not found")

    game_stats_count = (
        await db.execute(
            select(func.count()).select_from(GamePlayerStats).where(GamePlayerStats.player_id == player_id)
        )
    ).scalar() or 0
    lineups_count = (
        await db.execute(
            select(func.count()).select_from(GameLineup).where(GameLineup.player_id == player_id)
        )
    ).scalar() or 0
    season_stats_count = (
        await db.execute(
            select(func.count()).select_from(PlayerSeasonStats).where(PlayerSeasonStats.player_id == player_id)
        )
    ).scalar() or 0
    game_events_count = (
        await db.execute(
            select(func.count()).select_from(GameEvent).where(
                or_(
                    GameEvent.player_id == player_id,
                    GameEvent.player2_id == player_id,
                    GameEvent.assist_player_id == player_id,
                )
            )
        )
    ).scalar() or 0

    dependency_total = game_stats_count + lineups_count + season_stats_count + game_events_count
    if dependency_total > 0:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                "Cannot delete player with dependencies: "
                f"game_player_stats={game_stats_count}, "
                f"game_lineups={lineups_count}, "
                f"player_season_stats={season_stats_count}, "
                f"game_events={game_events_count}"
            ),
        )

    await db.execute(delete(PlayerTeam).where(PlayerTeam.player_id == player_id))
    await db.delete(player)
    await db.commit()
    await invalidate_pattern("*app.api.seasons*")
    return {"message": "Player deleted"}
