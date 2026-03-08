"""Season attendance statistics endpoint."""

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, case, literal_column
from sqlalchemy.orm import selectinload

from app.api.deps import get_db
from app.models import Game, GameStatus, Stadium, Team
from app.services.season_visibility import ensure_visible_season_or_404
from app.utils.localization import get_localized_field, get_localized_city
from app.utils.team_logo_fallback import resolve_team_logo_url
from app.schemas.attendance import (
    AttendanceResponse,
    AttendanceSummary,
    TopAttendanceMatch,
    AttendanceByTeam,
    AttendanceByStadium,
    AttendanceByTour,
)

router = APIRouter(prefix="/seasons", tags=["seasons"])


@router.get("/{season_id}/attendance", response_model=AttendanceResponse)
async def get_season_attendance(
    season_id: int,
    lang: str = Query(default="kz", pattern="^(kz|ru|en)$"),
    db: AsyncSession = Depends(get_db),
):
    """Get detailed attendance statistics for a season."""
    await ensure_visible_season_or_404(db, season_id)

    # Base filter: finished games with attendance data
    base_filter = [
        Game.season_id == season_id,
        Game.visitors.isnot(None),
        Game.visitors > 0,
        Game.status == GameStatus.finished,
    ]

    # --- 1. Summary ---
    summary_q = select(
        func.count().label("total_matches"),
        func.coalesce(func.sum(Game.visitors), 0).label("total_attendance"),
    ).where(*base_filter)

    summary_result = await db.execute(summary_q)
    summary_row = summary_result.one()
    total_matches = summary_row.total_matches or 0
    total_attendance = int(summary_row.total_attendance or 0)
    avg_attendance = round(total_attendance / total_matches, 1) if total_matches > 0 else 0.0

    summary = AttendanceSummary(
        total_matches=total_matches,
        total_attendance=total_attendance,
        average_attendance=avg_attendance,
    )

    # --- 2. Top 10 matches by attendance ---
    top_q = (
        select(Game)
        .where(*base_filter)
        .options(
            selectinload(Game.home_team),
            selectinload(Game.away_team),
            selectinload(Game.stadium_rel),
        )
        .order_by(Game.visitors.desc())
        .limit(10)
    )

    top_result = await db.execute(top_q)
    top_games = top_result.scalars().all()

    top_matches = []
    for g in top_games:
        top_matches.append(TopAttendanceMatch(
            game_id=g.id,
            tour=g.tour,
            date=g.date.isoformat() if g.date else None,
            home_team_id=g.home_team.id if g.home_team else None,
            home_team_name=get_localized_field(g.home_team, "name", lang) if g.home_team else None,
            home_team_logo=resolve_team_logo_url(g.home_team),
            home_score=g.home_score,
            away_team_id=g.away_team.id if g.away_team else None,
            away_team_name=get_localized_field(g.away_team, "name", lang) if g.away_team else None,
            away_team_logo=resolve_team_logo_url(g.away_team),
            away_score=g.away_score,
            visitors=g.visitors,
            stadium_name=get_localized_field(g.stadium_rel, "name", lang) if g.stadium_rel else None,
            city=get_localized_city(g.stadium_rel, lang) if g.stadium_rel else None,
        ))

    # --- 3. Attendance by team (home + away) ---
    home_q = (
        select(
            Game.home_team_id.label("team_id"),
            func.count().label("matches"),
            func.coalesce(func.sum(Game.visitors), 0).label("attendance"),
        )
        .where(*base_filter, Game.home_team_id.isnot(None))
        .group_by(Game.home_team_id)
    )

    away_q = (
        select(
            Game.away_team_id.label("team_id"),
            func.count().label("matches"),
            func.coalesce(func.sum(Game.visitors), 0).label("attendance"),
        )
        .where(*base_filter, Game.away_team_id.isnot(None))
        .group_by(Game.away_team_id)
    )

    home_result = await db.execute(home_q)
    away_result = await db.execute(away_q)

    home_data = {row.team_id: row for row in home_result.all()}
    away_data = {row.team_id: row for row in away_result.all()}

    all_team_ids = set(home_data.keys()) | set(away_data.keys())

    # Fetch team objects for names/logos
    if all_team_ids:
        teams_q = select(Team).where(Team.id.in_(all_team_ids))
        teams_result = await db.execute(teams_q)
        teams_map = {t.id: t for t in teams_result.scalars().all()}
    else:
        teams_map = {}

    by_team = []
    for tid in all_team_ids:
        team_obj = teams_map.get(tid)
        h = home_data.get(tid)
        a = away_data.get(tid)
        home_matches = h.matches if h else 0
        home_att = int(h.attendance) if h else 0
        away_matches = a.matches if a else 0
        away_att = int(a.attendance) if a else 0
        total_m = home_matches + away_matches
        total_a = home_att + away_att
        by_team.append(AttendanceByTeam(
            team_id=tid,
            team_name=get_localized_field(team_obj, "name", lang) if team_obj else None,
            team_logo=resolve_team_logo_url(team_obj),
            home_matches=home_matches,
            home_attendance=home_att,
            away_matches=away_matches,
            away_attendance=away_att,
            total_matches=total_m,
            total_attendance=total_a,
            average_attendance=round(total_a / total_m, 1) if total_m > 0 else 0.0,
        ))
    by_team.sort(key=lambda x: x.total_attendance, reverse=True)

    # --- 4. Attendance by stadium ---
    stadium_q = (
        select(
            Game.stadium_id,
            func.count().label("matches"),
            func.coalesce(func.sum(Game.visitors), 0).label("total"),
        )
        .where(*base_filter, Game.stadium_id.isnot(None))
        .group_by(Game.stadium_id)
        .order_by(func.sum(Game.visitors).desc())
    )

    stadium_result = await db.execute(stadium_q)
    stadium_rows = stadium_result.all()

    stadium_ids = [r.stadium_id for r in stadium_rows]
    if stadium_ids:
        stadiums_q = select(Stadium).where(Stadium.id.in_(stadium_ids))
        stadiums_result = await db.execute(stadiums_q)
        stadiums_map = {s.id: s for s in stadiums_result.scalars().all()}
    else:
        stadiums_map = {}

    by_stadium = []
    for row in stadium_rows:
        s = stadiums_map.get(row.stadium_id)
        matches = row.matches
        total = int(row.total)
        by_stadium.append(AttendanceByStadium(
            stadium_id=row.stadium_id,
            stadium_name=get_localized_field(s, "name", lang) if s else None,
            city=get_localized_city(s, lang) if s else None,
            matches=matches,
            total_attendance=total,
            average_attendance=round(total / matches, 1) if matches > 0 else 0.0,
        ))

    # --- 5. Attendance by tour ---
    tour_q = (
        select(
            Game.tour,
            func.count().label("matches"),
            func.coalesce(func.sum(Game.visitors), 0).label("total"),
        )
        .where(*base_filter, Game.tour.isnot(None))
        .group_by(Game.tour)
        .order_by(Game.tour)
    )

    tour_result = await db.execute(tour_q)
    by_tour = []
    for row in tour_result.all():
        matches = row.matches
        total = int(row.total)
        by_tour.append(AttendanceByTour(
            tour=row.tour,
            matches=matches,
            total_attendance=total,
            average_attendance=round(total / matches, 1) if matches > 0 else 0.0,
        ))

    return AttendanceResponse(
        season_id=season_id,
        summary=summary,
        top_matches=top_matches,
        by_team=by_team,
        by_stadium=by_stadium,
        by_tour=by_tour,
    )
