from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.admin.deps import require_roles
from app.api.deps import get_db
from app.caching import invalidate_pattern
from app.models import AdminUser, Championship, Player, PlayerTeam, Season, Team
from app.schemas.admin.contracts import (
    AdminContractBulkCopyItem,
    AdminContractBulkCopyRequest,
    AdminContractBulkCopyResponse,
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
from app.services.telegram import notify_contract_change, send_telegram_message
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
        player_photo_url=player.photo_url if player else None,
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


@router.post("/bulk-copy", response_model=AdminContractBulkCopyResponse)
async def bulk_copy_contracts(
    payload: AdminContractBulkCopyRequest,
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "editor")),
):
    team = (await db.execute(select(Team).where(Team.id == payload.team_id))).scalar_one_or_none()
    if team is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="team_id not found")

    source_season = (await db.execute(select(Season).where(Season.id == payload.source_season_id))).scalar_one_or_none()
    if source_season is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="source_season_id not found")

    target_season = (await db.execute(select(Season).where(Season.id == payload.target_season_id))).scalar_one_or_none()
    if target_season is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="target_season_id not found")

    source_result = await db.execute(
        select(PlayerTeam).where(
            PlayerTeam.season_id == payload.source_season_id,
            PlayerTeam.team_id == payload.team_id,
        )
    )
    source_contracts = source_result.scalars().all()

    overrides_map: dict[tuple[int, int], AdminContractBulkCopyItem] = {
        (ov.player_id, ov.role): ov for ov in payload.overrides
    }
    excluded_set = set(payload.excluded_player_ids)

    to_copy = [pt for pt in source_contracts if pt.player_id not in excluded_set]
    excluded_count = len(source_contracts) - len(to_copy)

    created = 0
    skipped = 0
    for src in to_copy:
        exists = (
            await db.execute(
                select(PlayerTeam.id).where(
                    PlayerTeam.player_id == src.player_id,
                    PlayerTeam.team_id == src.team_id,
                    PlayerTeam.season_id == payload.target_season_id,
                    PlayerTeam.role == src.role,
                )
            )
        ).scalar_one_or_none()
        if exists is not None:
            skipped += 1
            continue

        ov = overrides_map.get((src.player_id, src.role or 1))
        new_pt = PlayerTeam(
            player_id=src.player_id,
            team_id=src.team_id,
            season_id=payload.target_season_id,
            role=src.role,
            amplua=ov.amplua if ov is not None and ov.amplua is not None else src.amplua,
            number=ov.number if ov is not None and ov.number is not None else src.number,
            position_ru=ov.position_ru if ov is not None and ov.position_ru is not None else src.position_ru,
            position_kz=ov.position_kz if ov is not None and ov.position_kz is not None else src.position_kz,
            position_en=ov.position_en if ov is not None and ov.position_en is not None else src.position_en,
            photo_url=ov.photo_url if ov is not None and ov.photo_url is not None else src.photo_url,
            is_active=True,
            is_hidden=False,
        )
        db.add(new_pt)
        created += 1

    await db.commit()
    await invalidate_pattern("*app.api.seasons*")

    await send_telegram_message(
        f"\U0001f4cb Контракт <b>массовый перенос</b>\n\n"
        f"\U0001f3df Команда: {team.name}\n"
        f"\U0001f4c5 Сезон (источник): {source_season.name}\n"
        f"\U0001f4c5 Сезон (цель): {target_season.name}\n"
        f"\U0001f468\u200d\U0001f4bc Админ: {_admin.email}\n\n"
        f"Создано: {created}, пропущено: {skipped}, исключено: {excluded_count}"
    )

    return AdminContractBulkCopyResponse(created=created, skipped=skipped, excluded=excluded_count)


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

    await notify_contract_change(
        action="создан",
        player_name=f"{player.last_name} {player.first_name}",
        team_name=team.name,
        season_name=season.name,
        admin_email=_admin.email,
        contract_id=pt_id,
    )

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

    await notify_contract_change(
        action="изменён",
        player_name=f"{player2.last_name} {player2.first_name}",
        team_name=team2.name,
        season_name=season2.name,
        admin_email=_admin.email,
        contract_id=contract_id,
    )

    return _build_contract_item(pt2, player2, team2, season2)


@router.delete("/{contract_id}")
async def delete_contract(
    contract_id: int,
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "editor")),
):
    row = await _fetch_contract_row(db, contract_id)
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Contract not found")
    pt, player, team, season = row

    await db.delete(pt)
    await db.commit()
    await invalidate_pattern("*app.api.seasons*")

    await notify_contract_change(
        action="удалён",
        player_name=f"{player.last_name} {player.first_name}",
        team_name=team.name,
        season_name=season.name,
        admin_email=_admin.email,
        contract_id=contract_id,
    )

    return {"message": "Contract deleted"}
