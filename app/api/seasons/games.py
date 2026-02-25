"""Season games endpoints: season games list, stage games."""

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.api.deps import get_db
from app.models import Game
from app.services.season_visibility import ensure_visible_season_or_404
from app.utils.localization import get_localized_field

router = APIRouter(prefix="/seasons", tags=["seasons"])

_ensure_visible_season = ensure_visible_season_or_404


@router.get("/{season_id}/games")
async def get_season_games(
    season_id: int,
    tour: int | None = None,
    lang: str = Query(default="kz", pattern="^(kz|ru|en)$"),
    db: AsyncSession = Depends(get_db),
):
    """Get games for a season."""
    await _ensure_visible_season(db, season_id)

    query = (
        select(Game)
        .where(Game.season_id == season_id)
        .options(
            selectinload(Game.home_team),
            selectinload(Game.away_team),
            selectinload(Game.season),
        )
        .order_by(Game.date.desc(), Game.time.desc())
    )

    if tour is not None:
        query = query.where(Game.tour == tour)

    result = await db.execute(query)
    games = result.scalars().all()

    items = []
    for g in games:
        home_team = None
        away_team = None
        if g.home_team:
            home_team = {
                "id": g.home_team.id,
                "name": get_localized_field(g.home_team, "name", lang),
                "logo_url": g.home_team.logo_url,
                "score": g.home_score,
            }
        if g.away_team:
            away_team = {
                "id": g.away_team.id,
                "name": get_localized_field(g.away_team, "name", lang),
                "logo_url": g.away_team.logo_url,
                "score": g.away_score,
            }

        items.append({
            "id": g.id,
            "date": g.date.isoformat() if g.date else None,
            "time": g.time.isoformat() if g.time else None,
            "tour": g.tour,
            "season_id": g.season_id,
            "home_score": g.home_score,
            "away_score": g.away_score,
            "has_stats": g.has_stats,
            "is_schedule_tentative": g.is_schedule_tentative,
            "stadium": g.stadium,
            "visitors": g.visitors,
            "home_team": home_team,
            "away_team": away_team,
            "season_name": g.season.name if g.season else None,
        })

    return {"items": items, "total": len(items)}


@router.get("/{season_id}/stages/{stage_id}/games")
async def get_stage_games(
    season_id: int,
    stage_id: int,
    lang: str = Query(default="kz", pattern="^(kz|ru|en)$"),
    db: AsyncSession = Depends(get_db),
):
    """Get games for a specific stage/tour."""
    await _ensure_visible_season(db, season_id)

    result = await db.execute(
        select(Game)
        .where(Game.season_id == season_id, Game.stage_id == stage_id)
        .options(
            selectinload(Game.home_team),
            selectinload(Game.away_team),
            selectinload(Game.season),
        )
        .order_by(Game.date, Game.time)
    )
    games = result.scalars().all()

    items = []
    for g in games:
        home_team = None
        away_team = None
        if g.home_team:
            home_team = {
                "id": g.home_team.id,
                "name": get_localized_field(g.home_team, "name", lang),
                "logo_url": g.home_team.logo_url,
                "score": g.home_score,
            }
        if g.away_team:
            away_team = {
                "id": g.away_team.id,
                "name": get_localized_field(g.away_team, "name", lang),
                "logo_url": g.away_team.logo_url,
                "score": g.away_score,
            }

        items.append({
            "id": g.id,
            "date": g.date.isoformat() if g.date else None,
            "time": g.time.isoformat() if g.time else None,
            "tour": g.tour,
            "season_id": g.season_id,
            "stage_id": g.stage_id,
            "home_score": g.home_score,
            "away_score": g.away_score,
            "home_penalty_score": g.home_penalty_score,
            "away_penalty_score": g.away_penalty_score,
            "has_stats": g.has_stats,
            "stadium": g.stadium,
            "visitors": g.visitors,
            "home_team": home_team,
            "away_team": away_team,
            "season_name": g.season.name if g.season else None,
        })

    return {"items": items, "total": len(items)}
