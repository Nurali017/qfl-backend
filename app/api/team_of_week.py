from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.api.deps import get_db
from app.models.team_of_week import TeamOfWeek
from app.services.season_visibility import ensure_visible_season_or_404
from app.schemas.team_of_week import TeamOfWeekResponse, TeamOfWeekListResponse

router = APIRouter(prefix="/team-of-week", tags=["team-of-week"])


@router.get("", response_model=TeamOfWeekListResponse)
async def get_team_of_week(
    season_id: int = Query(...),
    tour_key: str | None = Query(default=None),
    locale: str = Query(default="kz"),
    db: AsyncSession = Depends(get_db),
):
    """Get team of the week / season entries."""
    await ensure_visible_season_or_404(db, season_id)

    query = select(TeamOfWeek).where(
        TeamOfWeek.season_id == season_id,
        TeamOfWeek.locale == locale,
    )

    if tour_key is not None:
        query = query.where(TeamOfWeek.tour_key == tour_key)

    query = query.order_by(TeamOfWeek.tour_key)

    result = await db.execute(query)
    items = [TeamOfWeekResponse.model_validate(r) for r in result.scalars().all()]

    return TeamOfWeekListResponse(items=items, total=len(items))
