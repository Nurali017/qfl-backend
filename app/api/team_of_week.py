from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, distinct

from app.api.deps import get_db
from app.models.player_team import PlayerTeam
from app.models.team_of_week import TeamOfWeek
from app.services.season_visibility import ensure_visible_season_or_404
from app.schemas.team_of_week import TeamOfWeekResponse, TeamOfWeekListResponse

router = APIRouter(prefix="/team-of-week", tags=["team-of-week"])


async def _resolve_live_photos(
    db: AsyncSession, items: list[TeamOfWeekResponse], season_id: int
) -> None:
    """Overwrite each payload player's photo with the player's current photo.

    The payload stores a snapshot of the player's photo captured when the tour
    was generated. That snapshot goes stale — a season's photos live on
    ``player_teams`` (not ``players.photo_url``, which is largely empty now), and
    they change over time — so recent tours rendered silhouettes while older tours
    kept outdated photos. Resolving live from ``PlayerTeam`` for this season,
    preferring the squad (leaderboard) variant, fixes every tour at once and never
    drifts. The stored snapshot is kept as a fallback when a player currently has
    no season photo. ``photo_url_*`` are ``FileUrlType`` columns, already returned
    as full URLs.
    """
    person_ids = {
        p.person_id
        for entry in items
        for p in entry.payload
        if p.person_id is not None
    }
    if not person_ids:
        return

    rows = await db.execute(
        select(
            PlayerTeam.player_id,
            PlayerTeam.photo_url_leaderboard,
            PlayerTeam.photo_url_avatar,
            PlayerTeam.photo_url,
        ).where(
            PlayerTeam.player_id.in_(person_ids),
            PlayerTeam.season_id == season_id,
        )
    )
    photo_by_id: dict[int, str] = {}
    for player_id, leaderboard, avatar, base in rows.all():
        photo = leaderboard or avatar or base
        if photo:
            photo_by_id.setdefault(player_id, photo)

    for entry in items:
        for p in entry.payload:
            if p.person_id is None:
                continue
            live = photo_by_id.get(p.person_id)
            if live:
                p.photo = live


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

    await _resolve_live_photos(db, items, season_id)

    return TeamOfWeekListResponse(items=items, total=len(items))


@router.get("/available-tours")
async def get_available_tours(
    season_id: int = Query(...),
    locale: str = Query(default="kz"),
    db: AsyncSession = Depends(get_db),
):
    """Get list of distinct tour_key values for the season."""
    await ensure_visible_season_or_404(db, season_id)

    query = (
        select(distinct(TeamOfWeek.tour_key))
        .where(
            TeamOfWeek.season_id == season_id,
            TeamOfWeek.locale == locale,
        )
        .order_by(TeamOfWeek.tour_key)
    )

    result = await db.execute(query)
    tours = [row[0] for row in result.all()]

    return {"items": tours, "total": len(tours)}
