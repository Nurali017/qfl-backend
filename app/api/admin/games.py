from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, func, and_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.api.deps import get_db
from app.api.admin.deps import require_roles
from app.models import Game, GameStatus, Team
from app.schemas.admin.games import (
    AdminGameResponse,
    AdminGameUpdateRequest,
    AdminGamesListResponse,
)

router = APIRouter(
    prefix="/games",
    tags=["admin-games"],
    dependencies=[Depends(require_roles("superadmin", "editor", "operator"))],
)


def _game_to_response(game: Game) -> AdminGameResponse:
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
        has_lineup=game.has_lineup,
        has_stats=game.has_stats,
        stadium_id=game.stadium_id,
        ticket_url=game.ticket_url,
        video_url=game.video_url,
        where_broadcast=game.where_broadcast,
        video_review_url=game.video_review_url,
        home_formation=game.home_formation,
        away_formation=game.away_formation,
        updated_at=game.updated_at,
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
        query.order_by(Game.date.desc(), Game.time.desc().nullslast())
        .offset(offset)
        .limit(limit)
    )
    games = result.scalars().all()
    items = [_game_to_response(g) for g in games]
    return AdminGamesListResponse(items=items, total=total)


@router.get("/{game_id}", response_model=AdminGameResponse)
async def get_game(game_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(Game)
        .options(selectinload(Game.home_team), selectinload(Game.away_team))
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
        .options(selectinload(Game.home_team), selectinload(Game.away_team))
        .where(Game.id == game_id)
    )
    game = result.scalar_one_or_none()
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")

    update_data = body.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(game, field, value)

    await db.commit()
    await db.refresh(game)
    return _game_to_response(game)
