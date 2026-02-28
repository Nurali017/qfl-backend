from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, func, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.api.deps import get_db
from app.api.admin.deps import require_roles
from app.models import Game, GameStatus, Team, GameLineup, GameEvent, LineupType, GameEventType, Referee, GameReferee, RefereeRole, Broadcaster, GameBroadcaster
from app.schemas.admin.games import (
    AdminGameResponse,
    AdminGameUpdateRequest,
    AdminGamesListResponse,
    AdminLineupItem,
    AdminLineupAddRequest,
    AdminLineupUpdateRequest,
    AdminEventItem,
    AdminEventAddRequest,
    AdminRefereeItem,
    AdminRefereeAddRequest,
)
from app.schemas.admin.broadcasters import (
    AdminGameBroadcasterItem,
    AdminGameBroadcasterAddRequest,
)

router = APIRouter(
    prefix="/games",
    tags=["admin-games"],
    dependencies=[Depends(require_roles("superadmin", "editor", "operator"))],
)


def _game_to_response(game: Game) -> AdminGameResponse:
    broadcaster_items = []
    if hasattr(game, "broadcasters") and game.broadcasters:
        for gb in sorted(game.broadcasters, key=lambda x: x.sort_order):
            if gb.broadcaster:
                broadcaster_items.append(AdminGameBroadcasterItem(
                    id=gb.id,
                    broadcaster_id=gb.broadcaster_id,
                    broadcaster_name=gb.broadcaster.name,
                    logo_url=gb.broadcaster.logo_url,
                    sort_order=gb.sort_order,
                ))
    return AdminGameResponse(
        id=game.id,
        sota_id=game.sota_id,
        date=game.date,
        time=game.time,
        tour=game.tour,
        season_id=game.season_id,
        stage_id=game.stage_id,
        home_team_id=game.home_team_id,
        away_team_id=game.away_team_id,
        home_team_name=game.home_team.name if game.home_team else None,
        away_team_name=game.away_team.name if game.away_team else None,
        home_score=game.home_score,
        away_score=game.away_score,
        home_penalty_score=game.home_penalty_score,
        away_penalty_score=game.away_penalty_score,
        status=game.status,
        is_live=game.is_live,
        is_featured=game.is_featured,
        sync_disabled=game.sync_disabled,
        has_lineup=game.has_lineup,
        has_stats=game.has_stats,
        stadium_id=game.stadium_id,
        ticket_url=game.ticket_url,
        video_url=game.video_url,
        youtube_live_url=game.youtube_live_url,
        where_broadcast=game.where_broadcast,
        video_review_url=game.video_review_url,
        home_formation=game.home_formation,
        away_formation=game.away_formation,
        updated_at=game.updated_at,
        broadcasters=broadcaster_items,
    )


@router.get("", response_model=AdminGamesListResponse)
async def list_games(
    season_id: int | None = Query(default=None),
    status: str | None = Query(default=None, description="upcoming, live, finished, postponed, cancelled"),
    team_id: int | None = Query(default=None),
    date_from: date | None = Query(default=None),
    date_to: date | None = Query(default=None),
    limit: int = Query(default=50, le=200),
    offset: int = Query(default=0),
    db: AsyncSession = Depends(get_db),
):
    query = select(Game).options(
        selectinload(Game.home_team),
        selectinload(Game.away_team),
        selectinload(Game.broadcasters).selectinload(GameBroadcaster.broadcaster),
    )
    count_query = select(func.count()).select_from(Game)

    filters = []
    if season_id is not None:
        filters.append(Game.season_id == season_id)
    if team_id is not None:
        filters.append(
            (Game.home_team_id == team_id) | (Game.away_team_id == team_id)
        )
    if date_from is not None:
        filters.append(Game.date >= date_from)
    if date_to is not None:
        filters.append(Game.date <= date_to)

    today = date.today()
    if status == "live":
        filters.append(Game.status == GameStatus.live)
    elif status == "upcoming":
        filters.append(and_(
            Game.status.in_([GameStatus.created]),
            Game.date >= today,
            Game.home_score.is_(None),
        ))
    elif status == "finished":
        filters.append(Game.status == GameStatus.finished)
    elif status == "postponed":
        filters.append(Game.status == GameStatus.postponed)
    elif status == "cancelled":
        filters.append(Game.status == GameStatus.cancelled)

    if filters:
        query = query.where(*filters)
        count_query = count_query.where(*filters)

    count_result = await db.execute(count_query)
    total = count_result.scalar() or 0

    result = await db.execute(
        query.order_by(func.abs(Game.date - func.current_date()), Game.time.asc().nullslast())
        .offset(offset)
        .limit(limit)
    )
    games = result.scalars().all()
    items = [_game_to_response(g) for g in games]
    return AdminGamesListResponse(items=items, total=total)


# --- Referee search (must be before /{game_id} to avoid path conflict) ---

@router.get("/referees/search")
async def search_referees(
    q: str = Query(default="", max_length=100),
    limit: int = Query(default=30, le=200),
    db: AsyncSession = Depends(get_db),
):
    query = select(Referee)
    if q.strip():
        pattern = f"%{q.strip()}%"
        query = query.where(
            or_(
                Referee.last_name.ilike(pattern),
                Referee.first_name.ilike(pattern),
            )
        )
    query = query.order_by(Referee.last_name, Referee.first_name).limit(limit)
    result = await db.execute(query)
    referees = result.scalars().all()
    return [
        {"id": r.id, "name": " ".join(filter(None, [r.last_name, r.first_name]))}
        for r in referees
    ]


@router.get("/{game_id}", response_model=AdminGameResponse)
async def get_game(game_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(Game)
        .options(
            selectinload(Game.home_team),
            selectinload(Game.away_team),
            selectinload(Game.broadcasters).selectinload(GameBroadcaster.broadcaster),
        )
        .where(Game.id == game_id)
    )
    game = result.scalar_one_or_none()
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")
    return _game_to_response(game)


@router.patch("/{game_id}", response_model=AdminGameResponse)
async def update_game(
    game_id: int,
    body: AdminGameUpdateRequest,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(Game)
        .options(
            selectinload(Game.home_team),
            selectinload(Game.away_team),
            selectinload(Game.broadcasters).selectinload(GameBroadcaster.broadcaster),
        )
        .where(Game.id == game_id)
    )
    game = result.scalar_one_or_none()
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")

    # NOT NULL fields — silently skip if caller sends null
    NOT_NULLABLE = {"date", "status", "is_featured", "sync_disabled"}
    update_data = {
        k: v for k, v in body.model_dump(exclude_unset=True).items()
        if v is not None or k not in NOT_NULLABLE
    }
    for field, value in update_data.items():
        setattr(game, field, value)

    await db.commit()
    result = await db.execute(
        select(Game)
        .options(
            selectinload(Game.home_team),
            selectinload(Game.away_team),
            selectinload(Game.broadcasters).selectinload(GameBroadcaster.broadcaster),
        )
        .where(Game.id == game_id)
    )
    game = result.scalar_one()
    return _game_to_response(game)


# --- Lineup endpoints ---

@router.get("/{game_id}/lineup", response_model=list[AdminLineupItem])
async def list_lineup(game_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(Game).where(Game.id == game_id)
    )
    game = result.scalar_one_or_none()
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")

    lineup_result = await db.execute(
        select(GameLineup)
        .options(selectinload(GameLineup.player))
        .where(GameLineup.game_id == game_id)
        .order_by(GameLineup.team_id, GameLineup.lineup_type, GameLineup.shirt_number)
    )
    lineups = lineup_result.scalars().all()
    return [
        AdminLineupItem(
            id=entry.id,
            player_id=entry.player_id,
            player_name=" ".join(filter(None, [entry.player.last_name, entry.player.first_name])) if entry.player else None,
            team_id=entry.team_id,
            lineup_type=entry.lineup_type.value,
            shirt_number=entry.shirt_number,
            is_captain=entry.is_captain,
            amplua=entry.amplua,
            field_position=entry.field_position,
        )
        for entry in lineups
    ]


@router.post("/{game_id}/lineup", response_model=AdminLineupItem, status_code=201)
async def add_lineup(game_id: int, body: AdminLineupAddRequest, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Game).where(Game.id == game_id))
    if not result.scalar_one_or_none():
        raise HTTPException(status_code=404, detail="Game not found")

    entry = GameLineup(
        game_id=game_id,
        player_id=body.player_id,
        team_id=body.team_id,
        lineup_type=LineupType(body.lineup_type),
        shirt_number=body.shirt_number,
        is_captain=body.is_captain if body.is_captain is not None else False,
        amplua=body.amplua,
        field_position=body.field_position,
    )
    db.add(entry)
    try:
        await db.commit()
    except Exception:
        await db.rollback()
        raise HTTPException(status_code=409, detail="Player already in lineup for this game")
    await db.refresh(entry)

    # Load player name
    player_result = await db.execute(
        select(GameLineup).options(selectinload(GameLineup.player)).where(GameLineup.id == entry.id)
    )
    entry = player_result.scalar_one()

    return AdminLineupItem(
        id=entry.id,
        player_id=entry.player_id,
        player_name=" ".join(filter(None, [entry.player.last_name, entry.player.first_name])) if entry.player else None,
        team_id=entry.team_id,
        lineup_type=entry.lineup_type.value,
        shirt_number=entry.shirt_number,
        is_captain=entry.is_captain,
        amplua=entry.amplua,
        field_position=entry.field_position,
    )


@router.patch("/{game_id}/lineup/{lineup_id}", response_model=AdminLineupItem)
async def update_lineup(game_id: int, lineup_id: int, body: AdminLineupUpdateRequest, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(GameLineup)
        .options(selectinload(GameLineup.player))
        .where(GameLineup.id == lineup_id, GameLineup.game_id == game_id)
    )
    entry = result.scalar_one_or_none()
    if not entry:
        raise HTTPException(status_code=404, detail="Lineup entry not found")
    if body.lineup_type is not None:
        entry.lineup_type = LineupType(body.lineup_type)
    if body.shirt_number is not None:
        entry.shirt_number = body.shirt_number
    if body.is_captain is not None:
        entry.is_captain = body.is_captain
    if body.amplua is not None:
        entry.amplua = body.amplua if body.amplua != "" else None
    if body.field_position is not None:
        entry.field_position = body.field_position if body.field_position != "" else None
    await db.commit()
    await db.refresh(entry)

    player_result = await db.execute(
        select(GameLineup).options(selectinload(GameLineup.player)).where(GameLineup.id == entry.id)
    )
    entry = player_result.scalar_one()

    return AdminLineupItem(
        id=entry.id,
        player_id=entry.player_id,
        player_name=" ".join(filter(None, [entry.player.last_name, entry.player.first_name])) if entry.player else None,
        team_id=entry.team_id,
        lineup_type=entry.lineup_type.value,
        shirt_number=entry.shirt_number,
        is_captain=entry.is_captain,
        amplua=entry.amplua,
        field_position=entry.field_position,
    )


@router.delete("/{game_id}/lineup/{lineup_id}")
async def delete_lineup(game_id: int, lineup_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(GameLineup).where(GameLineup.id == lineup_id, GameLineup.game_id == game_id)
    )
    entry = result.scalar_one_or_none()
    if not entry:
        raise HTTPException(status_code=404, detail="Lineup entry not found")
    await db.delete(entry)
    await db.commit()
    return {"ok": True}


# --- Events endpoints ---

@router.get("/{game_id}/events", response_model=list[AdminEventItem])
async def list_events(game_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Game).where(Game.id == game_id))
    if not result.scalar_one_or_none():
        raise HTTPException(status_code=404, detail="Game not found")

    events_result = await db.execute(
        select(GameEvent)
        .where(GameEvent.game_id == game_id)
        .order_by(GameEvent.half, GameEvent.minute)
    )
    events = events_result.scalars().all()
    return [
        AdminEventItem(
            id=ev.id,
            half=ev.half,
            minute=ev.minute,
            event_type=ev.event_type.value,
            team_id=ev.team_id,
            player_id=ev.player_id,
            player_name=ev.player_name,
            player_number=ev.player_number,
            player2_id=ev.player2_id,
            player2_name=ev.player2_name,
            assist_player_id=ev.assist_player_id,
            assist_player_name=ev.assist_player_name,
        )
        for ev in events
    ]


@router.post("/{game_id}/events", response_model=AdminEventItem, status_code=201)
async def add_event(game_id: int, body: AdminEventAddRequest, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Game).where(Game.id == game_id))
    if not result.scalar_one_or_none():
        raise HTTPException(status_code=404, detail="Game not found")

    ev = GameEvent(
        game_id=game_id,
        half=body.half,
        minute=body.minute,
        event_type=GameEventType(body.event_type),
        team_id=body.team_id,
        player_id=body.player_id,
        player_name=body.player_name,
        player_number=body.player_number,
        player2_id=body.player2_id,
        player2_name=body.player2_name,
        assist_player_id=body.assist_player_id,
        assist_player_name=body.assist_player_name,
    )
    db.add(ev)
    await db.commit()
    await db.refresh(ev)

    return AdminEventItem(
        id=ev.id,
        half=ev.half,
        minute=ev.minute,
        event_type=ev.event_type.value,
        team_id=ev.team_id,
        player_id=ev.player_id,
        player_name=ev.player_name,
        player_number=ev.player_number,
        player2_id=ev.player2_id,
        player2_name=ev.player2_name,
        assist_player_id=ev.assist_player_id,
        assist_player_name=ev.assist_player_name,
    )


@router.delete("/{game_id}/events/{event_id}")
async def delete_event(game_id: int, event_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(GameEvent).where(GameEvent.id == event_id, GameEvent.game_id == game_id)
    )
    ev = result.scalar_one_or_none()
    if not ev:
        raise HTTPException(status_code=404, detail="Event not found")
    await db.delete(ev)
    await db.commit()
    return {"ok": True}


# --- Referee endpoints ---

@router.get("/{game_id}/referees", response_model=list[AdminRefereeItem])
async def list_referees(game_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Game).where(Game.id == game_id))
    if not result.scalar_one_or_none():
        raise HTTPException(status_code=404, detail="Game not found")

    refs_result = await db.execute(
        select(GameReferee)
        .options(selectinload(GameReferee.referee))
        .where(GameReferee.game_id == game_id)
        .order_by(GameReferee.role)
    )
    entries = refs_result.scalars().all()
    return [
        AdminRefereeItem(
            id=entry.id,
            referee_id=entry.referee_id,
            referee_name=" ".join(filter(None, [entry.referee.last_name, entry.referee.first_name])) if entry.referee else None,
            role=entry.role.value,
        )
        for entry in entries
    ]


@router.post("/{game_id}/referees", response_model=AdminRefereeItem, status_code=201)
async def add_referee(game_id: int, body: AdminRefereeAddRequest, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Game).where(Game.id == game_id))
    if not result.scalar_one_or_none():
        raise HTTPException(status_code=404, detail="Game not found")

    entry = GameReferee(
        game_id=game_id,
        referee_id=body.referee_id,
        role=body.role,
    )
    db.add(entry)
    try:
        await db.commit()
    except Exception:
        await db.rollback()
        raise HTTPException(status_code=409, detail="Referee already assigned with this role")
    await db.refresh(entry)

    ref_result = await db.execute(
        select(GameReferee).options(selectinload(GameReferee.referee)).where(GameReferee.id == entry.id)
    )
    entry = ref_result.scalar_one()

    return AdminRefereeItem(
        id=entry.id,
        referee_id=entry.referee_id,
        referee_name=" ".join(filter(None, [entry.referee.last_name, entry.referee.first_name])) if entry.referee else None,
        role=entry.role.value,
    )


@router.delete("/{game_id}/referees/{entry_id}")
async def delete_referee(game_id: int, entry_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(GameReferee).where(GameReferee.id == entry_id, GameReferee.game_id == game_id)
    )
    entry = result.scalar_one_or_none()
    if not entry:
        raise HTTPException(status_code=404, detail="Referee entry not found")
    await db.delete(entry)
    await db.commit()
    return {"ok": True}


# --- Broadcaster endpoints ---

@router.get("/{game_id}/broadcasters", response_model=list[AdminGameBroadcasterItem])
async def list_game_broadcasters(game_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Game).where(Game.id == game_id))
    if not result.scalar_one_or_none():
        raise HTTPException(status_code=404, detail="Game not found")

    entries_result = await db.execute(
        select(GameBroadcaster)
        .options(selectinload(GameBroadcaster.broadcaster))
        .where(GameBroadcaster.game_id == game_id)
        .order_by(GameBroadcaster.sort_order)
    )
    entries = entries_result.scalars().all()
    return [
        AdminGameBroadcasterItem(
            id=entry.id,
            broadcaster_id=entry.broadcaster_id,
            broadcaster_name=entry.broadcaster.name if entry.broadcaster else "",
            logo_url=entry.broadcaster.logo_url if entry.broadcaster else None,
            sort_order=entry.sort_order,
        )
        for entry in entries
    ]


@router.post("/{game_id}/broadcasters", response_model=AdminGameBroadcasterItem, status_code=201)
async def add_game_broadcaster(
    game_id: int,
    body: AdminGameBroadcasterAddRequest,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(Game).where(Game.id == game_id))
    if not result.scalar_one_or_none():
        raise HTTPException(status_code=404, detail="Game not found")

    broadcaster_result = await db.execute(
        select(Broadcaster).where(Broadcaster.id == body.broadcaster_id)
    )
    broadcaster = broadcaster_result.scalar_one_or_none()
    if not broadcaster:
        raise HTTPException(status_code=404, detail="Broadcaster not found")

    entry = GameBroadcaster(
        game_id=game_id,
        broadcaster_id=body.broadcaster_id,
        sort_order=body.sort_order,
    )
    db.add(entry)
    try:
        await db.commit()
    except Exception:
        await db.rollback()
        raise HTTPException(status_code=409, detail="Broadcaster already added to this game")
    await db.refresh(entry)

    # Reload with broadcaster
    entry_result = await db.execute(
        select(GameBroadcaster)
        .options(selectinload(GameBroadcaster.broadcaster))
        .where(GameBroadcaster.id == entry.id)
    )
    entry = entry_result.scalar_one()

    return AdminGameBroadcasterItem(
        id=entry.id,
        broadcaster_id=entry.broadcaster_id,
        broadcaster_name=entry.broadcaster.name if entry.broadcaster else "",
        logo_url=entry.broadcaster.logo_url if entry.broadcaster else None,
        sort_order=entry.sort_order,
    )


@router.delete("/{game_id}/broadcasters/{entry_id}")
async def delete_game_broadcaster(game_id: int, entry_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(GameBroadcaster).where(
            GameBroadcaster.id == entry_id,
            GameBroadcaster.game_id == game_id,
        )
    )
    entry = result.scalar_one_or_none()
    if not entry:
        raise HTTPException(status_code=404, detail="Broadcaster entry not found")
    await db.delete(entry)
    await db.commit()
    return {"ok": True}
