from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.admin.deps import require_roles
from app.api.deps import get_db
from app.caching import invalidate_pattern
from app.models import AdminUser, Championship, Player, PlayerTeam, Season, Team
from app.schemas.admin.contracts import (
    AdminContractCreateRequest,
    AdminContractListItem,
    AdminContractMetaPlayer,
    AdminContractMetaResponse,
    AdminContractMetaSeason,
    AdminContractMetaTeam,
    AdminContractResponse,
    AdminContractUpdateRequest,
    AdminContractsListResponse,
)
from app.utils.file_urls import to_object_name

router = APIRouter(prefix="/contracts", tags=["admin-contracts"])


async def _fetch_contract_row(db: AsyncSession, contract_id: int):
    result = await db.execute(
        select(PlayerTeam, Player, Team, Season)
        .join(Player, Player.id == PlayerTeam.player_id)
        .join(Team, Team.id == PlayerTeam.team_id)
        .join(Season, Season.id == PlayerTeam.season_id)
        .where(PlayerTeam.id == contract_id)
    )
    return result.one_or_none()


def _build_contract_item(pt: PlayerTeam, player: Player, team: Team, season: Season) -> AdminContractListItem:
    return AdminContractListItem(
        id=pt.id,
        player_id=pt.player_id,
        player_last_name=player.last_name if player else None,
        player_first_name=player.first_name if player else None,
        player_sota_id=player.sota_id if player else None,
        team_id=pt.team_id,
        team_name=team.name if team else None,
        season_id=pt.season_id,
        season_name=season.name if season else None,
        role=pt.role,
        amplua=pt.amplua,
        number=pt.number,
        position_ru=pt.position_ru,
        position_kz=pt.position_kz,
        position_en=pt.position_en,
        photo_url=pt.photo_url,
        is_active=pt.is_active,
        is_hidden=pt.is_hidden,
    )


@router.get("/meta", response_model=AdminContractMetaResponse)
async def get_contracts_meta(
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "editor")),
):
    players_result = await db.execute(
        select(Player).order_by(Player.last_name.asc(), Player.first_name.asc())
    )
    players = players_result.scalars().all()

    teams_result = await db.execute(select(Team).order_by(Team.name.asc()))
    teams = teams_result.scalars().all()

    seasons_result = await db.execute(
        select(Season, Championship)
        .join(Championship, Championship.id == Season.championship_id)
        .order_by(Season.id.desc())
    )
    seasons_rows = seasons_result.all()

    return AdminContractMetaResponse(
        players=[
            AdminContractMetaPlayer(
                id=p.id,
                last_name=p.last_name,
                first_name=p.first_name,
                sota_id=p.sota_id,
            )
            for p in players
        ],
        teams=[AdminContractMetaTeam(id=t.id, name=t.name) for t in teams],
        seasons=[
            AdminContractMetaSeason(
                id=s.id,
                name=s.name,
                championship_name=c.name if c else None,
            )
            for s, c in seasons_rows
        ],
    )


@router.get("", response_model=AdminContractsListResponse)
async def list_contracts(
    player_id: int | None = Query(default=None),
    team_id: int | None = Query(default=None),
    season_id: int | None = Query(default=None),
    role: int | None = Query(default=None),
    is_active: bool | None = Query(default=None),
    is_hidden: bool | None = Query(default=None),
    has_sota_id: bool | None = Query(default=None),
    limit: int = Query(default=25, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "editor")),
):
    query = (
        select(PlayerTeam, Player, Team, Season)
        .join(Player, Player.id == PlayerTeam.player_id)
        .join(Team, Team.id == PlayerTeam.team_id)
        .join(Season, Season.id == PlayerTeam.season_id)
    )

    if player_id is not None:
        query = query.where(PlayerTeam.player_id == player_id)
    if team_id is not None:
        query = query.where(PlayerTeam.team_id == team_id)
    if season_id is not None:
        query = query.where(PlayerTeam.season_id == season_id)
    if role is not None:
        query = query.where(PlayerTeam.role == role)
    if is_active is not None:
        query = query.where(PlayerTeam.is_active == is_active)
    if is_hidden is not None:
        query = query.where(PlayerTeam.is_hidden == is_hidden)
    if has_sota_id is not None:
        if has_sota_id:
            query = query.where(Player.sota_id.is_not(None))
        else:
            query = query.where(Player.sota_id.is_(None))

    total = (
        await db.execute(select(func.count()).select_from(query.subquery()))
    ).scalar() or 0

    query = query.order_by(
        PlayerTeam.player_id.desc(),
        PlayerTeam.season_id.desc(),
        PlayerTeam.is_active.desc(),
    )
    result = await db.execute(query.offset(offset).limit(limit))
    rows = result.all()

    return AdminContractsListResponse(
        items=[_build_contract_item(pt, player, team, season) for pt, player, team, season in rows],
        total=total,
    )


@router.get("/{contract_id}", response_model=AdminContractResponse)
async def get_contract(
    contract_id: int,
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "editor")),
):
    result = await db.execute(
        select(PlayerTeam, Player, Team, Season)
        .join(Player, Player.id == PlayerTeam.player_id)
        .join(Team, Team.id == PlayerTeam.team_id)
        .join(Season, Season.id == PlayerTeam.season_id)
        .where(PlayerTeam.id == contract_id)
    )
    row = result.one_or_none()
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Contract not found")
    pt, player, team, season = row
    return _build_contract_item(pt, player, team, season)


@router.post("", response_model=AdminContractResponse, status_code=status.HTTP_201_CREATED)
async def create_contract(
    payload: AdminContractCreateRequest,
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "editor")),
):
    # Validate references
    player = (await db.execute(select(Player).where(Player.id == payload.player_id))).scalar_one_or_none()
    if player is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="player_id not found")

    team = (await db.execute(select(Team).where(Team.id == payload.team_id))).scalar_one_or_none()
    if team is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="team_id not found")

    season = (await db.execute(select(Season).where(Season.id == payload.season_id))).scalar_one_or_none()
    if season is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="season_id not found")

    # Check uniqueness: (player_id, team_id, season_id, role)
    existing = (
        await db.execute(
            select(PlayerTeam.id).where(
                PlayerTeam.player_id == payload.player_id,
                PlayerTeam.team_id == payload.team_id,
                PlayerTeam.season_id == payload.season_id,
                PlayerTeam.role == payload.role,
            )
        )
    ).scalar_one_or_none()
    if existing is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Contract with this (player_id, team_id, season_id, role) already exists",
        )

    pt = PlayerTeam(
        player_id=payload.player_id,
        team_id=payload.team_id,
        season_id=payload.season_id,
        role=payload.role,
        amplua=payload.amplua,
        number=payload.number,
        position_ru=payload.position_ru,
        position_kz=payload.position_kz,
        position_en=payload.position_en,
        photo_url=to_object_name(payload.photo_url) if payload.photo_url else None,
        is_active=payload.is_active,
        is_hidden=payload.is_hidden,
    )
    db.add(pt)
    await db.flush()
    pt_id = pt.id
    await db.commit()
    await invalidate_pattern("*app.api.seasons*")

    row = await _fetch_contract_row(db, pt_id)
    pt2, player2, team2, season2 = row
    return _build_contract_item(pt2, player2, team2, season2)


@router.patch("/{contract_id}", response_model=AdminContractResponse)
async def update_contract(
    contract_id: int,
    payload: AdminContractUpdateRequest,
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "editor")),
):
    result = await db.execute(
        select(PlayerTeam, Player, Team, Season)
        .join(Player, Player.id == PlayerTeam.player_id)
        .join(Team, Team.id == PlayerTeam.team_id)
        .join(Season, Season.id == PlayerTeam.season_id)
        .where(PlayerTeam.id == contract_id)
    )
    row = result.one_or_none()
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Contract not found")
    pt, player, team, season = row

    update_data = payload.model_dump(exclude_unset=True)

    # Validate new FK references if provided
    if "player_id" in update_data:
        player = (await db.execute(select(Player).where(Player.id == update_data["player_id"]))).scalar_one_or_none()
        if player is None:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="player_id not found")
    if "team_id" in update_data:
        team = (await db.execute(select(Team).where(Team.id == update_data["team_id"]))).scalar_one_or_none()
        if team is None:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="team_id not found")
    if "season_id" in update_data:
        season = (await db.execute(select(Season).where(Season.id == update_data["season_id"]))).scalar_one_or_none()
        if season is None:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="season_id not found")

    # Handle photo_url separately
    if "photo_url" in update_data:
        update_data["photo_url"] = to_object_name(update_data["photo_url"]) if update_data["photo_url"] else None

    for field, value in update_data.items():
        setattr(pt, field, value)

    await db.commit()
    await invalidate_pattern("*app.api.seasons*")

    row = await _fetch_contract_row(db, contract_id)
    pt2, player2, team2, season2 = row
    return _build_contract_item(pt2, player2, team2, season2)


@router.delete("/{contract_id}")
async def delete_contract(
    contract_id: int,
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "editor")),
):
    result = await db.execute(select(PlayerTeam).where(PlayerTeam.id == contract_id))
    pt = result.scalar_one_or_none()
    if pt is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Contract not found")

    await db.delete(pt)
    await db.commit()
    await invalidate_pattern("*app.api.seasons*")
    return {"message": "Contract deleted"}
