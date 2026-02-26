from datetime import date as date_type

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.orm.attributes import flag_modified

from app.models import AdminUser, CupDraw, Season, SeasonParticipant, Team
from app.schemas.cup_draw import CupDrawTeamBrief
from app.schemas.playoff_bracket import (
    ROUND_LABELS,
    BracketGameBrief,
    BracketGameTeam,
    PlayoffBracketEntry,
    PlayoffBracketResponse,
    PlayoffRound,
)
from app.services.season_visibility import is_season_visible_clause
from app.utils.timestamps import utcnow

PLAYOFF_ROUND_ORDER = ["1_32", "1_16", "1_8", "1_4", "1_2", "3rd_place", "final"]


async def _get_cup_season_or_404(db: AsyncSession, season_id: int) -> Season:
    result = await db.execute(
        select(Season).where(
            Season.id == season_id,
            is_season_visible_clause(),
        )
    )
    season = result.scalar_one_or_none()
    if season is None:
        raise HTTPException(status_code=404, detail="Season not found")
    if season.frontend_code != "cup":
        raise HTTPException(status_code=400, detail="Cup draw is available only for cup seasons")
    return season


async def get_draws_for_season(db: AsyncSession, season_id: int) -> list[CupDraw]:
    await _get_cup_season_or_404(db, season_id)
    result = await db.execute(
        select(CupDraw)
        .where(CupDraw.season_id == season_id)
        .order_by(CupDraw.created_at)
    )
    return list(result.scalars().all())


async def get_draw(db: AsyncSession, season_id: int, round_key: str) -> CupDraw | None:
    result = await db.execute(
        select(CupDraw).where(
            CupDraw.season_id == season_id,
            CupDraw.round_key == round_key,
        )
    )
    return result.scalar_one_or_none()


async def add_pair(
    db: AsyncSession,
    admin: AdminUser,
    season_id: int,
    round_key: str,
    team1_id: int,
    team2_id: int,
    sort_order: int,
    side: str,
) -> CupDraw:
    await _get_cup_season_or_404(db, season_id)

    if team1_id == team2_id:
        raise HTTPException(status_code=400, detail="A team cannot play against itself")

    # Validate teams are season participants
    participant_ids = await _get_participant_team_ids(db, season_id)
    participant_set = set(participant_ids)
    if team1_id not in participant_set:
        raise HTTPException(status_code=400, detail=f"Team {team1_id} is not a season participant")
    if team2_id not in participant_set:
        raise HTTPException(status_code=400, detail=f"Team {team2_id} is not a season participant")

    # Get or create draw
    draw = await get_draw(db, season_id, round_key)
    if draw is None:
        draw = CupDraw(
            season_id=season_id,
            round_key=round_key,
            status="active",
            pairs=[],
            created_by=admin.id,
        )
        db.add(draw)
        await db.flush()
    elif draw.status == "completed":
        raise HTTPException(status_code=400, detail="Cannot add pairs to completed draw")

    existing_pairs = draw.pairs or []

    # Check for duplicate teams in existing pairs
    existing_team_ids: set[int] = set()
    for p in existing_pairs:
        existing_team_ids.add(p["team1_id"])
        existing_team_ids.add(p["team2_id"])
    if team1_id in existing_team_ids or team2_id in existing_team_ids:
        raise HTTPException(status_code=400, detail="Duplicate team in pairs")

    # Check for sort_order conflict within the same side
    for p in existing_pairs:
        if p.get("sort_order") == sort_order and p.get("side") == side:
            raise HTTPException(status_code=400, detail=f"sort_order {sort_order} already exists for side '{side}'")

    new_pair = {
        "team1_id": team1_id,
        "team2_id": team2_id,
        "sort_order": sort_order,
        "side": side,
        "is_published": False,
    }
    existing_pairs.append(new_pair)
    draw.pairs = existing_pairs
    flag_modified(draw, "pairs")
    draw.updated_at = utcnow()

    await db.commit()
    await db.refresh(draw)
    return draw


async def publish_pair(
    db: AsyncSession,
    admin: AdminUser,
    season_id: int,
    round_key: str,
    sort_order: int,
    side: str | None = None,
) -> CupDraw:
    draw = await get_draw(db, season_id, round_key)
    if draw is None:
        raise HTTPException(status_code=404, detail="Draw not found")

    pairs = draw.pairs or []
    found = False
    for p in pairs:
        if p.get("sort_order") == sort_order and (side is None or p.get("side") == side):
            p["is_published"] = True
            found = True
            break
    if not found:
        raise HTTPException(status_code=404, detail=f"Pair with sort_order {sort_order} not found")

    draw.pairs = pairs
    flag_modified(draw, "pairs")
    draw.updated_at = utcnow()

    await db.commit()
    await db.refresh(draw)
    return draw


async def delete_pair(
    db: AsyncSession,
    season_id: int,
    round_key: str,
    sort_order: int,
    side: str | None = None,
) -> CupDraw:
    draw = await get_draw(db, season_id, round_key)
    if draw is None:
        raise HTTPException(status_code=404, detail="Draw not found")

    pairs = draw.pairs or []
    target = None
    for p in pairs:
        if p.get("sort_order") == sort_order and (side is None or p.get("side") == side):
            target = p
            break
    if target is None:
        raise HTTPException(status_code=404, detail=f"Pair with sort_order {sort_order} not found")

    pairs.remove(target)
    draw.pairs = pairs
    flag_modified(draw, "pairs")
    draw.updated_at = utcnow()

    await db.commit()
    await db.refresh(draw)
    return draw


async def complete_draw(
    db: AsyncSession,
    admin: AdminUser,
    season_id: int,
    round_key: str,
) -> CupDraw:
    draw = await get_draw(db, season_id, round_key)
    if draw is None:
        raise HTTPException(status_code=404, detail="Draw not found")
    if draw.status == "completed":
        return draw

    draw.status = "completed"
    draw.published_by = admin.id
    draw.published_at = utcnow()
    draw.updated_at = utcnow()

    await db.commit()
    await db.refresh(draw)
    return draw


async def _get_participant_team_ids(db: AsyncSession, season_id: int) -> list[int]:
    result = await db.execute(
        select(SeasonParticipant.team_id)
        .where(
            SeasonParticipant.season_id == season_id,
            SeasonParticipant.is_disqualified.is_(False),
        )
        .order_by(SeasonParticipant.sort_order, SeasonParticipant.id)
    )
    raw_ids = [team_id for team_id in result.scalars().all() if team_id is not None]
    seen: set[int] = set()
    ordered: list[int] = []
    for team_id in raw_ids:
        if team_id not in seen:
            seen.add(team_id)
            ordered.append(team_id)
    return ordered


async def get_participant_teams(db: AsyncSession, season_id: int) -> list[dict]:
    await _get_cup_season_or_404(db, season_id)
    result = await db.execute(
        select(SeasonParticipant)
        .where(
            SeasonParticipant.season_id == season_id,
            SeasonParticipant.is_disqualified.is_(False),
        )
        .options(selectinload(SeasonParticipant.team))
        .order_by(SeasonParticipant.sort_order, SeasonParticipant.id)
    )
    participants = result.scalars().all()
    teams = []
    seen: set[int] = set()
    for sp in participants:
        if sp.team_id is None or sp.team_id in seen:
            continue
        seen.add(sp.team_id)
        team = sp.team
        teams.append({
            "team_id": sp.team_id,
            "team_name": team.name if team else f"Team #{sp.team_id}",
            "team_logo": team.logo_url if team else None,
        })
    return teams


def _team_brief_from_team(team: Team) -> CupDrawTeamBrief:
    return CupDrawTeamBrief(
        id=team.id,
        name=team.name,
        logo_url=team.logo_url,
    )


async def _load_teams_by_id(db: AsyncSession, team_ids: set[int]) -> dict[int, Team]:
    if not team_ids:
        return {}
    result = await db.execute(select(Team).where(Team.id.in_(team_ids)))
    teams = result.scalars().all()
    return {t.id: t for t in teams}


def _normalize_pair(pair: dict, draw_status: str) -> dict:
    """Add side/is_published defaults for legacy pairs that lack them."""
    return {
        **pair,
        "side": pair.get("side", "center"),
        "is_published": pair.get("is_published", draw_status == "published"),
    }


async def build_bracket_from_cup_draws(
    db: AsyncSession,
    season_id: int,
) -> PlayoffBracketResponse | None:
    """Build a PlayoffBracketResponse from active/completed/published CupDraw records."""
    result = await db.execute(
        select(CupDraw).where(
            CupDraw.season_id == season_id,
            CupDraw.status.in_(["active", "completed", "published"]),
        )
    )
    draws = list(result.scalars().all())
    if not draws:
        return None

    # Collect all team ids from published pairs only
    all_team_ids: set[int] = set()
    for draw in draws:
        for pair in (draw.pairs or []):
            normalized = _normalize_pair(pair, draw.status)
            if normalized["is_published"]:
                all_team_ids.add(pair["team1_id"])
                all_team_ids.add(pair["team2_id"])

    if not all_team_ids:
        return None

    teams_by_id = await _load_teams_by_id(db, all_team_ids)

    return _build_bracket_response(season_id, draws, teams_by_id)


def _build_bracket_response(
    season_id: int,
    draws: list[CupDraw],
    teams_by_id: dict[int, Team],
) -> PlayoffBracketResponse | None:
    rounds_by_key: dict[str, list[PlayoffBracketEntry]] = {}
    synthetic_id = 1

    for draw in draws:
        pairs = draw.pairs or []
        entries: list[PlayoffBracketEntry] = []

        for pair in pairs:
            normalized = _normalize_pair(pair, draw.status)

            # Only show published pairs in the bracket
            if not normalized["is_published"]:
                continue

            team1 = teams_by_id.get(pair["team1_id"])
            team2 = teams_by_id.get(pair["team2_id"])

            home_team = BracketGameTeam(
                id=team1.id, name=team1.name, logo_url=team1.logo_url
            ) if team1 else None
            away_team = BracketGameTeam(
                id=team2.id, name=team2.name, logo_url=team2.logo_url
            ) if team2 else None

            # Use side from the pair JSON directly
            side = normalized["side"]

            game = BracketGameBrief(
                id=synthetic_id,
                date=date_type.today(),
                home_team=home_team,
                away_team=away_team,
                home_score=None,
                away_score=None,
                status=None,
            )
            entries.append(
                PlayoffBracketEntry(
                    id=synthetic_id,
                    round_name=draw.round_key,
                    side=side,
                    sort_order=pair.get("sort_order", synthetic_id),
                    is_third_place=draw.round_key == "3rd_place",
                    game=game,
                )
            )
            synthetic_id += 1

        entries.sort(key=lambda e: e.sort_order)
        rounds_by_key.setdefault(draw.round_key, []).extend(entries)

    # Build full path: from the earliest round with data through to final
    # e.g. if 1/8 has pairs → show 1/8, 1/4, 1/2, final (even if empty)
    numeric_rounds = ["1_32", "1_16", "1_8", "1_4", "1_2"]
    has_data = {rk for rk in rounds_by_key if rounds_by_key[rk]}

    # Find earliest numeric round that has data
    earliest_idx = len(numeric_rounds)
    for i, rk in enumerate(numeric_rounds):
        if rk in has_data:
            earliest_idx = i
            break

    # Path: from earliest round to 1_2, then always include final
    path_rounds = numeric_rounds[earliest_idx:] if earliest_idx < len(numeric_rounds) else []

    response_rounds: list[PlayoffRound] = []
    for rk in path_rounds:
        response_rounds.append(
            PlayoffRound(
                round_name=rk,
                round_label=ROUND_LABELS.get(rk, rk),
                entries=rounds_by_key.get(rk, []),
            )
        )

    # Always include 3rd_place if it has data
    if "3rd_place" in rounds_by_key:
        response_rounds.append(
            PlayoffRound(
                round_name="3rd_place",
                round_label=ROUND_LABELS.get("3rd_place", "3rd_place"),
                entries=rounds_by_key["3rd_place"],
            )
        )

    # Always include final if there are any rounds
    if response_rounds or "final" in has_data:
        response_rounds.append(
            PlayoffRound(
                round_name="final",
                round_label=ROUND_LABELS.get("final", "final"),
                entries=rounds_by_key.get("final", []),
            )
        )

    if not response_rounds:
        return None

    return PlayoffBracketResponse(season_id=season_id, rounds=response_rounds)
