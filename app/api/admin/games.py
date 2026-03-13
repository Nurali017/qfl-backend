import asyncio
import logging
from datetime import date, datetime
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File
from sqlalchemy import select, delete, func, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.api.deps import get_db
from app.api.admin.deps import require_roles
from app.models import Game, GameStatus, Team, GameLineup, GameEvent, LineupType, GameEventType, Referee, GameReferee, RefereeRole, Broadcaster, GameBroadcaster, GameTeamStats, GamePlayerStats, Player
from app.models.player_team import PlayerTeam
from app.schemas.admin.games import (
    AdminGameResponse,
    AdminGameUpdateRequest,
    AdminGamesListResponse,
    AdminLineupItem,
    AdminLineupAddRequest,
    AdminLineupUpdateRequest,
    AdminEventItem,
    AdminEventAddRequest,
    AdminEventUpdateRequest,
    AdminRefereeItem,
    AdminRefereeAddRequest,
    AdminTeamStatsItem,
    AdminTeamStatsUpsertRequest,
    AdminPlayerStatsItem,
    AdminPlayerStatsUpsertRequest,
)
from app.schemas.admin.prematch import PrematchImportResponse, PrematchPlayerMatch, PrematchTeamResult
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
        is_free_entry=game.is_free_entry,
        sync_disabled=game.sync_disabled,
        has_lineup=game.has_lineup,
        has_stats=game.has_stats,
        stadium_id=game.stadium_id,
        stadium_name=game.stadium_rel.name if game.stadium_rel else None,
        visitors=game.visitors,
        ticket_url=game.ticket_url,
        video_url=game.video_url,
        youtube_live_url=game.youtube_live_url,
        where_broadcast=game.where_broadcast,
        video_review_url=game.video_review_url,
        protocol_url=game.protocol_url,
        home_formation=game.home_formation,
        away_formation=game.away_formation,
        updated_at=game.updated_at,
        weather_temp=game.weather_temp,
        weather_condition=game.weather_condition,
        weather_fetched_at=game.weather_fetched_at,
        preview_ru=game.preview_ru,
        preview_kz=game.preview_kz,
        broadcasters=broadcaster_items,
    )


@router.get("", response_model=AdminGamesListResponse)
async def list_games(
    season_id: int | None = Query(default=None),
    tour: int | None = Query(default=None),
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
        selectinload(Game.stadium_rel),
        selectinload(Game.broadcasters).selectinload(GameBroadcaster.broadcaster),
    )
    count_query = select(func.count()).select_from(Game)

    filters = []
    if season_id is not None:
        filters.append(Game.season_id == season_id)
    if tour is not None:
        filters.append(Game.tour == tour)
    if team_id is not None:
        filters.append(
            (Game.home_team_id == team_id) | (Game.away_team_id == team_id)
        )
    if date_from is not None:
        filters.append(Game.date >= date_from)
    if date_to is not None:
        filters.append(Game.date <= date_to)

    today = datetime.now(ZoneInfo("Asia/Almaty")).date()
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
    limit: int = Query(default=30, le=500),
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
            selectinload(Game.stadium_rel),
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
            selectinload(Game.stadium_rel),
            selectinload(Game.broadcasters).selectinload(GameBroadcaster.broadcaster),
        )
        .where(Game.id == game_id)
    )
    game = result.scalar_one_or_none()
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")

    # NOT NULL fields — silently skip if caller sends null
    NOT_NULLABLE = {"date", "status", "is_featured", "is_free_entry", "sync_disabled"}
    update_data = {
        k: v for k, v in body.model_dump(exclude_unset=True).items()
        if v is not None or k not in NOT_NULLABLE
    }
    for field, value in update_data.items():
        setattr(game, field, value)

    # Auto-extract attendance from protocol PDF
    if "protocol_url" in update_data and update_data["protocol_url"] and "visitors" not in update_data:
        try:
            from app.minio_client import get_minio_client
            from app.config import get_settings
            from app.utils.file_urls import to_object_name
            from app.utils.protocol_parser import extract_attendance_from_protocol

            _settings = get_settings()
            object_name = to_object_name(update_data["protocol_url"])
            if object_name:
                attendance = extract_attendance_from_protocol(
                    get_minio_client(), _settings.minio_bucket, object_name
                )
                if attendance is not None:
                    game.visitors = attendance
        except Exception:
            logging.getLogger(__name__).warning(
                "Failed to extract attendance from protocol for game %s", game_id, exc_info=True
            )

    await db.commit()
    result = await db.execute(
        select(Game)
        .options(
            selectinload(Game.home_team),
            selectinload(Game.away_team),
            selectinload(Game.stadium_rel),
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


@router.delete("/{game_id}/lineup")
async def clear_lineup(game_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Game).where(Game.id == game_id))
    game = result.scalar_one_or_none()
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")
    await db.execute(delete(GameLineup).where(GameLineup.game_id == game_id))
    game.has_lineup = False
    await db.commit()
    return {"ok": True}


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


# --- Pre-match PDF import ---

_NAME_TRANSLATION = str.maketrans({
    "ё": "е", "ә": "а", "ғ": "г", "қ": "к", "ң": "н",
    "ө": "о", "ұ": "у", "ү": "у", "һ": "х", "і": "и",
})


def _norm(s: str | None) -> str:
    if not s:
        return ""
    return s.casefold().translate(_NAME_TRANSLATION).strip()


def _match_player(
    pdf_first: str,
    pdf_last: str,
    pdf_number: int,
    name_index: dict[str, list[PlayerTeam]],
    number_index: dict[int, PlayerTeam],
) -> tuple[int | None, str | None, str | None]:
    """Match a parsed player against team roster. Returns (player_id, db_name, method)."""
    full_key = _norm(f"{pdf_first} {pdf_last}")
    # Also try reversed name order (last first)
    reversed_key = _norm(f"{pdf_last} {pdf_first}")

    # Step 1: name + number
    for key in (full_key, reversed_key):
        entries = name_index.get(key, [])
        for pt in entries:
            if pt.number == pdf_number:
                p = pt.player
                name = " ".join(filter(None, [p.last_name, p.first_name]))
                return p.id, name, "name+number"

    # Step 2: name only (accept if single match)
    for key in (full_key, reversed_key):
        entries = name_index.get(key, [])
        if len(entries) == 1:
            p = entries[0].player
            name = " ".join(filter(None, [p.last_name, p.first_name]))
            return p.id, name, "name"

    # Step 3: shirt number fallback
    pt = number_index.get(pdf_number)
    if pt and pt.player:
        p = pt.player
        name = " ".join(filter(None, [p.last_name, p.first_name]))
        return p.id, name, "shirt_number"

    # Step 4: last name only fallback
    last_key = _norm(pdf_last)
    entries = name_index.get(f"__last__{last_key}", [])
    if len(entries) == 1:
        p = entries[0].player
        name = " ".join(filter(None, [p.last_name, p.first_name]))
        return p.id, name, "last_name"
    # Also try with reversed (pdf_first as last name)
    first_as_last = _norm(pdf_first)
    entries = name_index.get(f"__last__{first_as_last}", [])
    if len(entries) == 1:
        p = entries[0].player
        name = " ".join(filter(None, [p.last_name, p.first_name]))
        return p.id, name, "last_name"

    return None, None, None


def _build_roster_indexes(
    roster_entries: list[PlayerTeam], team_id: int,
) -> tuple[dict[str, list[PlayerTeam]], dict[int, PlayerTeam]]:
    name_index: dict[str, list[PlayerTeam]] = {}
    number_index: dict[int, PlayerTeam] = {}

    for pt in roster_entries:
        if pt.team_id != team_id or not pt.player:
            continue
        p = pt.player

        # Full name: "first last"
        full = _norm(f"{p.first_name} {p.last_name}")
        if full:
            name_index.setdefault(full, []).append(pt)

        # Also index as "last first" for reversed matching
        rev = _norm(f"{p.last_name} {p.first_name}")
        if rev and rev != full:
            name_index.setdefault(rev, []).append(pt)

        # Kazakh name variants
        full_kz = _norm(f"{p.first_name_kz} {p.last_name_kz}")
        if full_kz and full_kz != full:
            name_index.setdefault(full_kz, []).append(pt)

        # Last name only index (for fallback)
        for ln in (p.last_name, p.last_name_kz):
            key = _norm(ln)
            if key:
                name_index.setdefault(f"__last__{key}", []).append(pt)

        if pt.number is not None:
            number_index[pt.number] = pt

    return name_index, number_index


@router.post("/{game_id}/import-prematch", response_model=PrematchImportResponse)
async def import_prematch_lineup(
    game_id: int,
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
):
    """Import lineup from a pre-match report PDF."""
    # Validate game
    result = await db.execute(
        select(Game)
        .options(selectinload(Game.home_team), selectinload(Game.away_team))
        .where(Game.id == game_id)
    )
    game = result.scalar_one_or_none()
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")
    if not game.home_team_id or not game.away_team_id:
        raise HTTPException(status_code=400, detail="Game must have both home and away teams assigned")
    if not game.season_id:
        raise HTTPException(status_code=400, detail="Game must have a season assigned")

    # Read and parse PDF
    pdf_bytes = await file.read()
    if not pdf_bytes:
        raise HTTPException(status_code=400, detail="Empty file")

    from app.utils.prematch_parser import parse_prematch_pdf
    try:
        parse_result = await asyncio.to_thread(parse_prematch_pdf, pdf_bytes)
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Failed to parse PDF: {e}")

    if not parse_result.home.starters and not parse_result.away.starters:
        raise HTTPException(status_code=422, detail="No players found in PDF. Check format.")

    # Load team rosters
    roster_result = await db.execute(
        select(PlayerTeam)
        .options(selectinload(PlayerTeam.player))
        .where(
            PlayerTeam.season_id == game.season_id,
            PlayerTeam.team_id.in_([game.home_team_id, game.away_team_id]),
        )
    )
    roster_entries = roster_result.scalars().all()

    warnings: list[str] = []

    # Build indexes per team
    home_name_idx, home_num_idx = _build_roster_indexes(roster_entries, game.home_team_id)
    away_name_idx, away_num_idx = _build_roster_indexes(roster_entries, game.away_team_id)

    # Warn if team names don't match
    if parse_result.home.team_name and game.home_team:
        pdf_name = parse_result.home.team_name.upper()
        db_name = game.home_team.name.upper()
        if pdf_name not in db_name and db_name not in pdf_name:
            warnings.append(f"Home team in PDF '{parse_result.home.team_name}' may not match DB '{game.home_team.name}'")
    if parse_result.away.team_name and game.away_team:
        pdf_name = parse_result.away.team_name.upper()
        db_name = game.away_team.name.upper()
        if pdf_name not in db_name and db_name not in pdf_name:
            warnings.append(f"Away team in PDF '{parse_result.away.team_name}' may not match DB '{game.away_team.name}'")

    # Match players
    def _process_team(parsed_team, team_id, name_idx, num_idx):
        results = []
        for lineup_type, players in [("starter", parsed_team.starters), ("substitute", parsed_team.substitutes)]:
            for pp in players:
                pid, db_name, method = _match_player(
                    pp.first_name, pp.last_name, pp.shirt_number, name_idx, num_idx,
                )
                results.append(PrematchPlayerMatch(
                    pdf_name=f"{pp.first_name} {pp.last_name}",
                    pdf_shirt_number=pp.shirt_number,
                    lineup_type=lineup_type,
                    is_goalkeeper=pp.is_goalkeeper,
                    is_captain=pp.is_captain,
                    matched=pid is not None,
                    player_id=pid,
                    player_name=db_name,
                    match_method=method,
                ))
        return results

    home_matches = _process_team(parse_result.home, game.home_team_id, home_name_idx, home_num_idx)
    away_matches = _process_team(parse_result.away, game.away_team_id, away_name_idx, away_num_idx)

    # Delete existing lineup
    await db.execute(delete(GameLineup).where(GameLineup.game_id == game_id))

    # Create new lineup entries
    created = 0
    seen_player_ids: set[int] = set()
    for team_id, matches in [(game.home_team_id, home_matches), (game.away_team_id, away_matches)]:
        for pm in matches:
            if pm.player_id and pm.player_id not in seen_player_ids:
                seen_player_ids.add(pm.player_id)
                entry = GameLineup(
                    game_id=game_id,
                    team_id=team_id,
                    player_id=pm.player_id,
                    lineup_type=LineupType(pm.lineup_type),
                    shirt_number=pm.pdf_shirt_number,
                    is_captain=pm.is_captain,
                    amplua="Gk" if pm.is_goalkeeper else None,
                    field_position=None,
                )
                db.add(entry)
                created += 1

    # Update game metadata
    if created > 0:
        game.has_lineup = True
        game.lineup_source = "prematch_report"

    await db.commit()

    # Add warnings for unmatched
    for pm in home_matches + away_matches:
        if not pm.matched:
            warnings.append(f"Unmatched: #{pm.pdf_shirt_number} {pm.pdf_name}")

    home_matched = sum(1 for m in home_matches if m.matched)
    away_matched = sum(1 for m in away_matches if m.matched)

    return PrematchImportResponse(
        game_id=game_id,
        home=PrematchTeamResult(
            team_id=game.home_team_id,
            team_name=game.home_team.name if game.home_team else None,
            players=home_matches,
            matched_count=home_matched,
            unmatched_count=len(home_matches) - home_matched,
        ),
        away=PrematchTeamResult(
            team_id=game.away_team_id,
            team_name=game.away_team.name if game.away_team else None,
            players=away_matches,
            matched_count=away_matched,
            unmatched_count=len(away_matches) - away_matched,
        ),
        total_matched=home_matched + away_matched,
        total_unmatched=(len(home_matches) - home_matched) + (len(away_matches) - away_matched),
        lineup_created=created > 0,
        warnings=warnings,
    )


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
            source=ev.source,
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
        source="manual",
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
        source=ev.source,
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


@router.patch("/{game_id}/events/{event_id}", response_model=AdminEventItem)
async def update_event(game_id: int, event_id: int, body: AdminEventUpdateRequest, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(GameEvent).where(GameEvent.id == event_id, GameEvent.game_id == game_id)
    )
    ev = result.scalar_one_or_none()
    if not ev:
        raise HTTPException(status_code=404, detail="Event not found")
    for field in ["half", "minute", "team_id", "player_id", "player_name",
                  "player_number", "player2_id", "player2_name",
                  "assist_player_id", "assist_player_name"]:
        val = getattr(body, field)
        if val is not None:
            setattr(ev, field, val)
    if body.event_type is not None:
        ev.event_type = GameEventType(body.event_type)
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
        source=ev.source,
    )


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


# --- Team Stats endpoints ---

def _team_stats_to_item(ts: GameTeamStats) -> AdminTeamStatsItem:
    return AdminTeamStatsItem(
        id=ts.id,
        team_id=ts.team_id,
        team_name=ts.team.name if ts.team else None,
        possession=float(ts.possession) if ts.possession is not None else None,
        possession_percent=ts.possession_percent,
        shots=ts.shots,
        shots_on_goal=ts.shots_on_goal,
        shots_off_goal=ts.shots_off_goal,
        passes=ts.passes,
        pass_accuracy=float(ts.pass_accuracy) if ts.pass_accuracy is not None else None,
        fouls=ts.fouls,
        yellow_cards=ts.yellow_cards,
        red_cards=ts.red_cards,
        corners=ts.corners,
        offsides=ts.offsides,
        shots_on_bar=ts.shots_on_bar,
        shots_blocked=ts.shots_blocked,
        penalties=ts.penalties,
        saves=ts.saves,
        extra_stats=ts.extra_stats,
    )


@router.get("/{game_id}/team-stats", response_model=list[AdminTeamStatsItem])
async def list_team_stats(game_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Game).where(Game.id == game_id))
    if not result.scalar_one_or_none():
        raise HTTPException(status_code=404, detail="Game not found")

    stats_result = await db.execute(
        select(GameTeamStats)
        .options(selectinload(GameTeamStats.team))
        .where(GameTeamStats.game_id == game_id)
        .order_by(GameTeamStats.team_id)
    )
    stats = stats_result.scalars().all()
    return [_team_stats_to_item(ts) for ts in stats]


@router.put("/{game_id}/team-stats/{team_id}", response_model=AdminTeamStatsItem)
async def upsert_team_stats(
    game_id: int,
    team_id: int,
    body: AdminTeamStatsUpsertRequest,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(GameTeamStats)
        .where(GameTeamStats.game_id == game_id, GameTeamStats.team_id == team_id)
    )
    ts = result.scalar_one_or_none()
    if not ts:
        ts = GameTeamStats(game_id=game_id, team_id=team_id)
        db.add(ts)

    for field, value in body.model_dump(exclude_unset=True).items():
        setattr(ts, field, value)

    await db.commit()
    await db.refresh(ts)

    # Reload with team
    reload = await db.execute(
        select(GameTeamStats)
        .options(selectinload(GameTeamStats.team))
        .where(GameTeamStats.id == ts.id)
    )
    ts = reload.scalar_one()
    return _team_stats_to_item(ts)


# --- Player Stats endpoints ---

def _player_stats_to_item(ps: GamePlayerStats) -> AdminPlayerStatsItem:
    player_name = None
    if ps.player:
        player_name = " ".join(filter(None, [ps.player.last_name, ps.player.first_name]))
    return AdminPlayerStatsItem(
        id=ps.id,
        player_id=ps.player_id,
        player_name=player_name,
        team_id=ps.team_id,
        team_name=ps.team.name if ps.team else None,
        minutes_played=ps.minutes_played,
        started=ps.started,
        position=ps.position,
        shots=ps.shots,
        shots_on_goal=ps.shots_on_goal,
        shots_off_goal=ps.shots_off_goal,
        passes=ps.passes,
        pass_accuracy=float(ps.pass_accuracy) if ps.pass_accuracy is not None else None,
        duel=ps.duel,
        tackle=ps.tackle,
        corner=ps.corner,
        offside=ps.offside,
        foul=ps.foul,
        yellow_cards=ps.yellow_cards,
        red_cards=ps.red_cards,
        extra_stats=ps.extra_stats,
    )


@router.get("/{game_id}/player-stats", response_model=list[AdminPlayerStatsItem])
async def list_player_stats(game_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Game).where(Game.id == game_id))
    if not result.scalar_one_or_none():
        raise HTTPException(status_code=404, detail="Game not found")

    stats_result = await db.execute(
        select(GamePlayerStats)
        .options(selectinload(GamePlayerStats.player), selectinload(GamePlayerStats.team))
        .where(GamePlayerStats.game_id == game_id)
        .order_by(GamePlayerStats.team_id, GamePlayerStats.started.desc(), GamePlayerStats.minutes_played.desc().nullslast())
    )
    stats = stats_result.scalars().all()
    return [_player_stats_to_item(ps) for ps in stats]


@router.put("/{game_id}/player-stats/{player_id}", response_model=AdminPlayerStatsItem)
async def upsert_player_stats(
    game_id: int,
    player_id: int,
    body: AdminPlayerStatsUpsertRequest,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(GamePlayerStats)
        .where(GamePlayerStats.game_id == game_id, GamePlayerStats.player_id == player_id)
    )
    ps = result.scalar_one_or_none()
    if not ps:
        ps = GamePlayerStats(game_id=game_id, player_id=player_id, team_id=body.team_id)
        db.add(ps)

    for field, value in body.model_dump(exclude_unset=True).items():
        setattr(ps, field, value)

    await db.commit()
    await db.refresh(ps)

    # Reload with player and team
    reload = await db.execute(
        select(GamePlayerStats)
        .options(selectinload(GamePlayerStats.player), selectinload(GamePlayerStats.team))
        .where(GamePlayerStats.id == ps.id)
    )
    ps = reload.scalar_one()
    return _player_stats_to_item(ps)
