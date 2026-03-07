from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.api.deps import get_db
from app.models import Player, PlayerTeam, Season, Team
from app.services.season_visibility import (
    is_season_visible_clause,
    resolve_visible_season_id,
)
from app.utils.localization import get_localized_field, get_localized_name
from app.utils.positions import infer_position_code
from app.utils.team_logo_fallback import resolve_team_logo_url
from fastapi_cache.decorator import cache

router = APIRouter(prefix="/teams", tags=["teams"])

_AMPLUA_TO_POSITION = {1: "GK", 2: "DEF", 3: "MID", 4: "FWD"}


def _build_player_info(pt: PlayerTeam, lang: str) -> dict:
    p = pt.player
    country_data = None
    if p.country:
        country_data = {
            "id": p.country.id,
            "code": p.country.code,
            "name": get_localized_name(p.country, lang),
            "flag_url": p.country.flag_url,
        }
    position = (
        infer_position_code(pt.position_ru or pt.position_kz, pt.position_en)
        or infer_position_code(p.player_type, p.top_role)
        or _AMPLUA_TO_POSITION.get(pt.amplua)
    )
    return {
        "id": p.id,
        "first_name": get_localized_field(p, "first_name", lang),
        "last_name": get_localized_field(p, "last_name", lang),
        "photo_url": pt.photo_url or p.photo_url,
        "position": position,
        "age": p.age,
        "country": country_data,
        "number": pt.number,
    }


def _build_team_info(team: Team, lang: str) -> dict:
    return {
        "id": team.id,
        "name": get_localized_name(team, lang),
        "logo_url": resolve_team_logo_url(team),
    }


@router.get("/{team_id}/transfers")
@cache(expire=3600)
async def get_team_transfers(
    team_id: int,
    season_id: int = Query(default=None),
    lang: str = Query(default="kz", description="Language: kz, ru, or en"),
    db: AsyncSession = Depends(get_db),
):
    """Get transfers (arrivals/departures) for a team by comparing rosters between seasons."""
    season_id = await resolve_visible_season_id(db, season_id)

    # Load current season to get championship_id
    cur_season = await db.get(Season, season_id)
    if not cur_season:
        return {
            "season_id": season_id,
            "previous_season_id": None,
            "has_previous_season": False,
            "arrivals": [],
            "departures": [],
        }

    # Find previous season: same championship, visible, ordered by date_start DESC
    result = await db.execute(
        select(Season)
        .where(
            Season.championship_id == cur_season.championship_id,
            is_season_visible_clause(),
        )
        .order_by(Season.date_start.desc())
    )
    champ_seasons = result.scalars().all()

    # Find current season index, then take the next one (previous chronologically)
    prev_season = None
    found_current = False
    for s in champ_seasons:
        if found_current:
            prev_season = s
            break
        if s.id == season_id:
            found_current = True

    if not prev_season:
        return {
            "season_id": season_id,
            "previous_season_id": None,
            "has_previous_season": False,
            "arrivals": [],
            "departures": [],
        }

    # Load player_teams for current and previous seasons (role=1 = players only)
    # Include all registered (non-hidden) players, not just is_active,
    # so that mid-season arrivals/departures are tracked correctly.
    pt_options = selectinload(PlayerTeam.player).selectinload(Player.country)

    cur_result = await db.execute(
        select(PlayerTeam)
        .where(
            PlayerTeam.team_id == team_id,
            PlayerTeam.season_id == season_id,
            PlayerTeam.role == 1,
            PlayerTeam.is_hidden == False,
        )
        .options(pt_options)
    )
    cur_pts = cur_result.scalars().all()

    prev_result = await db.execute(
        select(PlayerTeam)
        .where(
            PlayerTeam.team_id == team_id,
            PlayerTeam.season_id == prev_season.id,
            PlayerTeam.role == 1,
            PlayerTeam.is_hidden == False,
        )
        .options(pt_options)
    )
    prev_pts = prev_result.scalars().all()

    cur_player_ids = {pt.player_id for pt in cur_pts}
    prev_player_ids = {pt.player_id for pt in prev_pts}

    arrival_ids = cur_player_ids - prev_player_ids
    departure_ids = prev_player_ids - cur_player_ids

    cur_pt_by_player = {pt.player_id: pt for pt in cur_pts}
    prev_pt_by_player = {pt.player_id: pt for pt in prev_pts}

    # For arrivals: find where they came from (any team in previous season)
    from_teams: dict[int, Team | None] = {}
    if arrival_ids:
        from_result = await db.execute(
            select(PlayerTeam)
            .where(
                PlayerTeam.player_id.in_(arrival_ids),
                PlayerTeam.season_id == prev_season.id,
                PlayerTeam.role == 1,
                PlayerTeam.is_hidden == False,
            )
            .options(selectinload(PlayerTeam.team))
        )
        for pt in from_result.scalars().all():
            from_teams[pt.player_id] = pt.team

    # For departures: find where they went (any team in current season)
    to_teams: dict[int, Team | None] = {}
    if departure_ids:
        to_result = await db.execute(
            select(PlayerTeam)
            .where(
                PlayerTeam.player_id.in_(departure_ids),
                PlayerTeam.season_id == season_id,
                PlayerTeam.role == 1,
                PlayerTeam.is_hidden == False,
            )
            .options(selectinload(PlayerTeam.team))
        )
        for pt in to_result.scalars().all():
            to_teams[pt.player_id] = pt.team

    # Build response
    arrivals = []
    for pid in arrival_ids:
        pt = cur_pt_by_player[pid]
        from_team = from_teams.get(pid)
        arrivals.append({
            "player": _build_player_info(pt, lang),
            "from_team": _build_team_info(from_team, lang) if from_team else None,
            "is_active": pt.is_active,
            "left_at": pt.left_at.isoformat() if pt.left_at else None,
        })

    departures = []
    for pid in departure_ids:
        pt = prev_pt_by_player[pid]
        to_team = to_teams.get(pid)
        departures.append({
            "player": _build_player_info(pt, lang),
            "to_team": _build_team_info(to_team, lang) if to_team else None,
            "is_active": pt.is_active,
            "left_at": pt.left_at.isoformat() if pt.left_at else None,
        })

    # Sort by last name
    arrivals.sort(key=lambda x: x["player"]["last_name"] or "")
    departures.sort(key=lambda x: x["player"]["last_name"] or "")

    return {
        "season_id": season_id,
        "previous_season_id": prev_season.id,
        "has_previous_season": True,
        "arrivals": arrivals,
        "departures": departures,
    }
