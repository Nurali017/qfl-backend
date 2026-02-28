from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.admin.deps import require_roles
from app.api.deps import get_db
from app.caching import invalidate_pattern
from app.models import AdminUser, Championship, Coach, CoachRole, Country, Season, Team, TeamCoach
from app.schemas.admin.coaches import (
    AdminCoachAssignmentCreateRequest,
    AdminCoachAssignmentListItem,
    AdminCoachAssignmentResponse,
    AdminCoachAssignmentsListResponse,
    AdminCoachAssignmentUpdateRequest,
    AdminCoachBulkCopyRequest,
    AdminCoachBulkCopyResponse,
    AdminCoachMetaCoach,
    AdminCoachMetaCountry,
    AdminCoachMetaResponse,
    AdminCoachMetaSeason,
    AdminCoachMetaTeam,
)
from app.services.telegram import notify_coach_change, notify_coach_updated, send_telegram_message
from app.utils.file_urls import to_object_name

router = APIRouter(prefix="/coaches", tags=["admin-coaches"])


def _parse_date(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except (ValueError, TypeError):
        return None


def _format_date(value: datetime | None) -> str | None:
    if not value:
        return None
    return value.isoformat()


async def _fetch_assignment_row(db: AsyncSession, assignment_id: int):
    result = await db.execute(
        select(TeamCoach, Coach, Team, Season)
        .join(Coach, Coach.id == TeamCoach.coach_id)
        .join(Team, Team.id == TeamCoach.team_id)
        .outerjoin(Season, Season.id == TeamCoach.season_id)
        .where(TeamCoach.id == assignment_id)
    )
    return result.one_or_none()


def _build_assignment_item(tc: TeamCoach, coach: Coach, team: Team, season: Season | None) -> AdminCoachAssignmentListItem:
    return AdminCoachAssignmentListItem(
        id=tc.id,
        coach_id=tc.coach_id,
        coach_first_name=coach.first_name if coach else None,
        coach_last_name=coach.last_name if coach else None,
        coach_photo_url=coach.photo_url if coach else None,
        team_id=tc.team_id,
        team_name=team.name if team else None,
        season_id=tc.season_id,
        season_name=season.name if season else None,
        role=tc.role.value if tc.role else "head_coach",
        is_active=tc.is_active,
        start_date=_format_date(tc.start_date),
        end_date=_format_date(tc.end_date),
    )


@router.get("/meta", response_model=AdminCoachMetaResponse)
async def get_coaches_meta(
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "editor")),
):
    coaches_result = await db.execute(
        select(Coach).order_by(Coach.last_name.asc(), Coach.first_name.asc())
    )
    coaches = coaches_result.scalars().all()

    teams_result = await db.execute(select(Team).order_by(Team.name.asc()))
    teams = teams_result.scalars().all()

    seasons_result = await db.execute(
        select(Season, Championship)
        .join(Championship, Championship.id == Season.championship_id)
        .order_by(Season.id.desc())
    )
    seasons_rows = seasons_result.all()

    countries_result = await db.execute(select(Country).order_by(Country.name.asc()))
    countries = countries_result.scalars().all()

    return AdminCoachMetaResponse(
        coaches=[
            AdminCoachMetaCoach(
                id=c.id,
                last_name=c.last_name,
                first_name=c.first_name,
                photo_url=c.photo_url,
            )
            for c in coaches
        ],
        teams=[AdminCoachMetaTeam(id=t.id, name=t.name) for t in teams],
        seasons=[
            AdminCoachMetaSeason(
                id=s.id,
                name=s.name,
                championship_name=ch.name if ch else None,
            )
            for s, ch in seasons_rows
        ],
        countries=[
            AdminCoachMetaCountry(
                id=c.id,
                code=c.code if hasattr(c, "code") else None,
                name=c.name,
            )
            for c in countries
        ],
    )


@router.get("", response_model=AdminCoachAssignmentsListResponse)
async def list_coach_assignments(
    team_id: int | None = Query(default=None),
    season_id: int | None = Query(default=None),
    coach_id: int | None = Query(default=None),
    role: str | None = Query(default=None),
    is_active: bool | None = Query(default=None),
    limit: int = Query(default=25, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "editor")),
):
    query = (
        select(TeamCoach, Coach, Team, Season)
        .join(Coach, Coach.id == TeamCoach.coach_id)
        .join(Team, Team.id == TeamCoach.team_id)
        .outerjoin(Season, Season.id == TeamCoach.season_id)
    )

    if team_id is not None:
        query = query.where(TeamCoach.team_id == team_id)
    if season_id is not None:
        query = query.where(TeamCoach.season_id == season_id)
    if coach_id is not None:
        query = query.where(TeamCoach.coach_id == coach_id)
    if role is not None:
        query = query.where(TeamCoach.role == role)
    if is_active is not None:
        query = query.where(TeamCoach.is_active == is_active)

    total = (
        await db.execute(select(func.count()).select_from(query.subquery()))
    ).scalar() or 0

    query = query.order_by(
        TeamCoach.id.desc(),
    )
    result = await db.execute(query.offset(offset).limit(limit))
    rows = result.all()

    return AdminCoachAssignmentsListResponse(
        items=[_build_assignment_item(tc, coach, team, season) for tc, coach, team, season in rows],
        total=total,
    )


@router.post("", response_model=AdminCoachAssignmentResponse, status_code=status.HTTP_201_CREATED)
async def create_coach_assignment(
    payload: AdminCoachAssignmentCreateRequest,
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "editor")),
):
    # Determine coach_id: either from payload or create inline
    coach_id = payload.coach_id
    if payload.inline_coach is not None:
        inline = payload.inline_coach
        new_coach = Coach(
            first_name=inline.first_name,
            last_name=inline.last_name,
            first_name_kz=inline.first_name_kz,
            first_name_ru=inline.first_name_ru,
            first_name_en=inline.first_name_en,
            last_name_kz=inline.last_name_kz,
            last_name_ru=inline.last_name_ru,
            last_name_en=inline.last_name_en,
            photo_url=to_object_name(inline.photo_url) if inline.photo_url else None,
            country_id=inline.country_id,
        )
        db.add(new_coach)
        await db.flush()
        coach_id = new_coach.id
    elif coach_id is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Укажите coach_id или inline_coach",
        )

    # Validate references
    coach = (await db.execute(select(Coach).where(Coach.id == coach_id))).scalar_one_or_none()
    if coach is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="coach_id not found")

    team = (await db.execute(select(Team).where(Team.id == payload.team_id))).scalar_one_or_none()
    if team is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="team_id not found")

    season = None
    if payload.season_id is not None:
        season = (await db.execute(select(Season).where(Season.id == payload.season_id))).scalar_one_or_none()
        if season is None:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="season_id not found")

    # Validate role
    try:
        coach_role = CoachRole(payload.role)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid role: {payload.role}. Valid: {[r.value for r in CoachRole]}",
        )

    # Check uniqueness: (team_id, coach_id, season_id, role)
    existing_query = select(TeamCoach.id).where(
        TeamCoach.team_id == payload.team_id,
        TeamCoach.coach_id == coach_id,
        TeamCoach.role == coach_role,
    )
    if payload.season_id is not None:
        existing_query = existing_query.where(TeamCoach.season_id == payload.season_id)
    else:
        existing_query = existing_query.where(TeamCoach.season_id.is_(None))

    existing = (await db.execute(existing_query)).scalar_one_or_none()
    if existing is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Назначение для этого тренера в данной команде, сезоне и роли уже существует",
        )

    tc = TeamCoach(
        team_id=payload.team_id,
        coach_id=coach_id,
        season_id=payload.season_id,
        role=coach_role,
        is_active=payload.is_active,
        start_date=_parse_date(payload.start_date),
        end_date=_parse_date(payload.end_date),
    )
    db.add(tc)
    await db.flush()
    tc_id = tc.id
    await db.commit()
    await invalidate_pattern("*app.api.teams*")
    await invalidate_pattern("*app.api.seasons*")

    coach_name = f"{coach.last_name} {coach.first_name}"
    await notify_coach_change(
        action="создано",
        coach_name=coach_name,
        team_name=team.name,
        season_name=season.name if season else "—",
        admin_email=_admin.email,
        assignment_id=tc_id,
        role=payload.role,
        is_active=payload.is_active,
    )

    row = await _fetch_assignment_row(db, tc_id)
    tc2, coach2, team2, season2 = row
    return _build_assignment_item(tc2, coach2, team2, season2)


@router.get("/{assignment_id}", response_model=AdminCoachAssignmentResponse)
async def get_coach_assignment(
    assignment_id: int,
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "editor")),
):
    row = await _fetch_assignment_row(db, assignment_id)
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Assignment not found")
    tc, coach, team, season = row
    return _build_assignment_item(tc, coach, team, season)


@router.patch("/{assignment_id}", response_model=AdminCoachAssignmentResponse)
async def update_coach_assignment(
    assignment_id: int,
    payload: AdminCoachAssignmentUpdateRequest,
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "editor")),
):
    row = await _fetch_assignment_row(db, assignment_id)
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Assignment not found")
    tc, coach, team, season = row

    update_data = payload.model_dump(exclude_unset=True)

    # Validate new FK references if provided
    if "coach_id" in update_data:
        coach = (await db.execute(select(Coach).where(Coach.id == update_data["coach_id"]))).scalar_one_or_none()
        if coach is None:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="coach_id not found")
    if "team_id" in update_data:
        team = (await db.execute(select(Team).where(Team.id == update_data["team_id"]))).scalar_one_or_none()
        if team is None:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="team_id not found")
    if "season_id" in update_data:
        if update_data["season_id"] is not None:
            season = (await db.execute(select(Season).where(Season.id == update_data["season_id"]))).scalar_one_or_none()
            if season is None:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="season_id not found")
        else:
            season = None

    # Validate role if provided
    if "role" in update_data:
        try:
            update_data["role"] = CoachRole(update_data["role"])
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid role: {update_data['role']}",
            )

    # Handle date fields
    if "start_date" in update_data:
        update_data["start_date"] = _parse_date(update_data["start_date"])
    if "end_date" in update_data:
        update_data["end_date"] = _parse_date(update_data["end_date"])

    # Track changes for notification
    changes: dict[str, tuple] = {}
    for field, value in update_data.items():
        old_value = getattr(tc, field, None)
        if old_value != value:
            changes[field] = (old_value, value)
        setattr(tc, field, value)

    await db.commit()
    await invalidate_pattern("*app.api.teams*")
    await invalidate_pattern("*app.api.seasons*")

    row = await _fetch_assignment_row(db, assignment_id)
    tc2, coach2, team2, season2 = row

    await notify_coach_updated(
        coach_name=f"{coach2.last_name} {coach2.first_name}",
        team_name=team2.name,
        season_name=season2.name if season2 else "—",
        admin_email=_admin.email,
        assignment_id=assignment_id,
        changes=changes,
    )

    return _build_assignment_item(tc2, coach2, team2, season2)


@router.delete("/{assignment_id}")
async def delete_coach_assignment(
    assignment_id: int,
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin", "editor")),
):
    row = await _fetch_assignment_row(db, assignment_id)
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Assignment not found")
    tc, coach, team, season = row

    await db.delete(tc)
    await db.commit()
    await invalidate_pattern("*app.api.teams*")
    await invalidate_pattern("*app.api.seasons*")

    await notify_coach_change(
        action="удалено",
        coach_name=f"{coach.last_name} {coach.first_name}",
        team_name=team.name,
        season_name=season.name if season else "—",
        admin_email=_admin.email,
        assignment_id=assignment_id,
        role=tc.role.value if tc.role else "head_coach",
        is_active=tc.is_active,
    )

    return {"message": "Assignment deleted"}


@router.post("/bulk-copy", response_model=AdminCoachBulkCopyResponse)
async def bulk_copy_coach_assignments(
    payload: AdminCoachBulkCopyRequest,
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
        select(TeamCoach).where(
            TeamCoach.season_id == payload.source_season_id,
            TeamCoach.team_id == payload.team_id,
        )
    )
    source_assignments = source_result.scalars().all()

    excluded_set = set(payload.excluded_coach_ids)
    to_copy = [tc for tc in source_assignments if tc.coach_id not in excluded_set]
    excluded_count = len(source_assignments) - len(to_copy)

    # Fetch coach names for notification
    coach_ids = list({tc.coach_id for tc in to_copy})
    coaches_map: dict[int, str] = {}
    if coach_ids:
        coaches_result = await db.execute(
            select(Coach.id, Coach.last_name, Coach.first_name).where(Coach.id.in_(coach_ids))
        )
        for cid, last_name, first_name in coaches_result.all():
            coaches_map[cid] = f"{last_name or ''} {first_name or ''}".strip() or f"#{cid}"

    created = 0
    skipped = 0
    created_names: list[str] = []
    skipped_names: list[str] = []
    for src in to_copy:
        role = CoachRole(payload.override_role) if payload.override_role else src.role

        # Check if assignment already exists in target
        exists = (
            await db.execute(
                select(TeamCoach.id).where(
                    TeamCoach.coach_id == src.coach_id,
                    TeamCoach.team_id == src.team_id,
                    TeamCoach.season_id == payload.target_season_id,
                    TeamCoach.role == role,
                )
            )
        ).scalar_one_or_none()
        if exists is not None:
            skipped += 1
            skipped_names.append(coaches_map.get(src.coach_id, f"#{src.coach_id}"))
            continue

        new_tc = TeamCoach(
            coach_id=src.coach_id,
            team_id=src.team_id,
            season_id=payload.target_season_id,
            role=role,
            is_active=True,
        )
        db.add(new_tc)
        try:
            async with db.begin_nested():
                await db.flush()
        except Exception:
            skipped += 1
            skipped_names.append(coaches_map.get(src.coach_id, f"#{src.coach_id}"))
            continue
        created += 1
        created_names.append(coaches_map.get(src.coach_id, f"#{src.coach_id}"))

    await db.commit()
    await invalidate_pattern("*app.api.teams*")
    await invalidate_pattern("*app.api.seasons*")

    # Build detailed notification
    msg_parts = [
        "\U0001f9d1\u200d\U0001f3eb Тренеры <b>массовый перенос</b>\n",
        f"\U0001f3df Команда: {team.name}",
        f"\U0001f4c5 Сезон (источник): {source_season.name}",
        f"\U0001f4c5 Сезон (цель): {target_season.name}",
        f"\U0001f468\u200d\U0001f4bc Админ: {_admin.email}\n",
        f"\u2705 Создано: {created}",
    ]
    if created_names:
        msg_parts.append("  " + ", ".join(created_names))
    msg_parts.append(f"\u23ed Пропущено: {skipped}")
    if skipped_names:
        msg_parts.append("  " + ", ".join(skipped_names))
    msg_parts.append(f"\u274c Исключено: {excluded_count}")

    await send_telegram_message("\n".join(msg_parts))

    return AdminCoachBulkCopyResponse(created=created, skipped=skipped, excluded=excluded_count)
