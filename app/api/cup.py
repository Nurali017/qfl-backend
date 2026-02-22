"""Public Cup API — aggregated endpoints for cup/knockout tournament pages."""

import re
from datetime import date as date_type

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.api.deps import get_db
from app.api.games import compute_game_status
from app.api.seasons import calculate_dynamic_table
from app.models import (
    Championship,
    Game,
    PlayoffBracket,
    Season,
    Stage,
    SeasonParticipant,
)
from app.schemas.cup import (
    CupGameBrief,
    CupGroup,
    CupGroupStandingEntry,
    CupOverviewResponse,
    CupRound,
    CupScheduleResponse,
    CupTeamBrief,
)
from app.schemas.playoff_bracket import (
    ROUND_LABELS,
    BracketGameBrief,
    BracketGameTeam,
    PlayoffBracketEntry,
    PlayoffBracketResponse,
    PlayoffRound,
)
from app.utils.localization import get_localized_field

router = APIRouter(prefix="/cup", tags=["cup"])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_cup_game(game: Game, lang: str, today: date_type) -> CupGameBrief:
    """Build a CupGameBrief from a Game ORM object."""
    home_team = None
    away_team = None
    if game.home_team:
        home_team = CupTeamBrief(
            id=game.home_team.id,
            name=get_localized_field(game.home_team, "name", lang),
            logo_url=game.home_team.logo_url,
        )
    if game.away_team:
        away_team = CupTeamBrief(
            id=game.away_team.id,
            name=get_localized_field(game.away_team, "name", lang),
            logo_url=game.away_team.logo_url,
        )

    stage_name = None
    if game.stage:
        stage_name = get_localized_field(game.stage, "name", lang)

    status = compute_game_status(game, today)
    return CupGameBrief(
        id=game.id,
        date=game.date,
        time=game.time,
        stage_name=stage_name,
        home_team=home_team,
        away_team=away_team,
        home_score=game.home_score,
        away_score=game.away_score,
        home_penalty_score=game.home_penalty_score,
        away_penalty_score=game.away_penalty_score,
        status=status,
        is_live=game.is_live,
    )


def _infer_round_key(stage: Stage) -> str:
    """Infer a URL-friendly round key from stage name.

    Examples:
        "1/8 финала"  -> "1_8"
        "1/4 финала"  -> "1_4"
        "1/2 финала"  -> "1_2"
        "Финал"       -> "final"
        "За 3-е место"-> "3rd_place"
        "Тур 1"       -> "group_1"
        "Группа A"    -> "group_a"
        anything else -> slugified stage name
    """
    name = stage.name or ""
    name_lower = name.lower().strip()

    # Fraction rounds: "1/8", "1/4", "1/2"
    m = re.match(r"1\s*/\s*(\d+)", name_lower)
    if m:
        return f"1_{m.group(1)}"

    if "финал" in name_lower and "полу" not in name_lower and "1/" not in name_lower:
        return "final"
    if "полуфинал" in name_lower:
        return "1_2"
    if "четвертьфинал" in name_lower:
        return "1_4"
    if "3" in name_lower and "мест" in name_lower:
        return "3rd_place"

    # Group stage tours: "Тур 1", "Тур 2"
    m = re.match(r"тур\s*(\d+)", name_lower)
    if m:
        return f"group_{m.group(1)}"

    # "Группа A" etc
    m = re.match(r"групп[аы]\s*(\w+)", name_lower)
    if m:
        return f"group_{m.group(1).lower()}"

    # Fallback: slugify
    slug = re.sub(r"[^a-z0-9]+", "_", name_lower).strip("_")
    return slug or f"stage_{stage.id}"


def _determine_current_round(rounds: list[CupRound]) -> CupRound | None:
    """Pick the current round: live games > first incomplete > last round."""
    # 1. Any round with live games
    for r in rounds:
        if any(g.is_live for g in r.games):
            return r

    # 2. First round that is not fully played
    for r in rounds:
        if r.total_games > 0 and r.played_games < r.total_games:
            return r

    # 3. Last round with games
    for r in reversed(rounds):
        if r.total_games > 0:
            return r

    return None


async def _build_bracket(
    db: AsyncSession, season_id: int, lang: str
) -> PlayoffBracketResponse | None:
    """Build playoff bracket response (same logic as seasons.py)."""
    result = await db.execute(
        select(PlayoffBracket)
        .where(PlayoffBracket.season_id == season_id, PlayoffBracket.is_visible == True)
        .options(
            selectinload(PlayoffBracket.game).selectinload(Game.home_team),
            selectinload(PlayoffBracket.game).selectinload(Game.away_team),
        )
        .order_by(PlayoffBracket.sort_order)
    )
    brackets = result.scalars().all()

    if not brackets:
        return None

    rounds_map: dict[str, list[PlayoffBracketEntry]] = {}
    for b in brackets:
        game_brief = None
        if b.game:
            home_team = None
            away_team = None
            if b.game.home_team:
                home_team = BracketGameTeam(
                    id=b.game.home_team.id,
                    name=get_localized_field(b.game.home_team, "name", lang),
                    logo_url=b.game.home_team.logo_url,
                )
            if b.game.away_team:
                away_team = BracketGameTeam(
                    id=b.game.away_team.id,
                    name=get_localized_field(b.game.away_team, "name", lang),
                    logo_url=b.game.away_team.logo_url,
                )
            game_brief = BracketGameBrief(
                id=b.game.id,
                date=b.game.date,
                time=b.game.time,
                home_team=home_team,
                away_team=away_team,
                home_score=b.game.home_score,
                away_score=b.game.away_score,
                home_penalty_score=b.game.home_penalty_score,
                away_penalty_score=b.game.away_penalty_score,
                status=compute_game_status(b.game),
            )

        entry = PlayoffBracketEntry(
            id=b.id,
            round_name=b.round_name,
            side=b.side,
            sort_order=b.sort_order,
            is_third_place=b.is_third_place,
            game=game_brief,
        )
        rounds_map.setdefault(b.round_name, []).append(entry)

    round_order = ["1_16", "1_8", "1_4", "1_2", "3rd_place", "final"]
    rounds = []
    for round_name in round_order:
        if round_name in rounds_map:
            rounds.append(
                PlayoffRound(
                    round_name=round_name,
                    round_label=ROUND_LABELS.get(round_name, round_name),
                    entries=rounds_map[round_name],
                )
            )
    for round_name, entries in rounds_map.items():
        if round_name not in round_order:
            rounds.append(
                PlayoffRound(
                    round_name=round_name,
                    round_label=ROUND_LABELS.get(round_name, round_name),
                    entries=entries,
                )
            )

    return PlayoffBracketResponse(season_id=season_id, rounds=rounds)


async def _load_season(db: AsyncSession, season_id: int, lang: str) -> Season:
    """Load season with championship, or 404."""
    result = await db.execute(
        select(Season)
        .where(Season.id == season_id)
        .options(selectinload(Season.championship))
    )
    season = result.scalar_one_or_none()
    if not season:
        raise HTTPException(status_code=404, detail="Season not found")
    return season


async def _load_games(db: AsyncSession, season_id: int) -> list[Game]:
    """Load all games for a season with teams and stage."""
    result = await db.execute(
        select(Game)
        .where(Game.season_id == season_id)
        .options(
            selectinload(Game.home_team),
            selectinload(Game.away_team),
            selectinload(Game.stage),
        )
        .order_by(Game.date, Game.time)
    )
    return list(result.scalars().all())


def _build_rounds(
    games: list[Game], stages: list[Stage], lang: str, today: date_type,
    include_games: bool = True,
) -> list[CupRound]:
    """Group games by stage into CupRound objects."""
    # Map stage_id -> stage
    stage_map = {s.id: s for s in stages}

    # Group games by stage_id
    games_by_stage: dict[int | None, list[Game]] = {}
    for g in games:
        games_by_stage.setdefault(g.stage_id, []).append(g)

    rounds: list[CupRound] = []
    # Use stages order (sort_order, then id)
    sorted_stages = sorted(stages, key=lambda s: (s.sort_order, s.id))

    for stage in sorted_stages:
        stage_games = games_by_stage.get(stage.id, [])
        played = sum(
            1 for g in stage_games
            if compute_game_status(g, today) == "finished"
        )
        cup_games = []
        if include_games:
            cup_games = [_build_cup_game(g, lang, today) for g in stage_games]

        rounds.append(CupRound(
            stage_id=stage.id,
            round_name=get_localized_field(stage, "name", lang) or f"Stage {stage.id}",
            round_key=_infer_round_key(stage),
            is_current=False,
            total_games=len(stage_games),
            played_games=played,
            games=cup_games,
        ))

    # Games without a stage (shouldn't happen, but be safe)
    orphan_games = games_by_stage.get(None, [])
    if orphan_games:
        played = sum(
            1 for g in orphan_games
            if compute_game_status(g, today) == "finished"
        )
        cup_games = []
        if include_games:
            cup_games = [_build_cup_game(g, lang, today) for g in orphan_games]
        rounds.append(CupRound(
            round_name="Other",
            round_key="other",
            total_games=len(orphan_games),
            played_games=played,
            games=cup_games,
        ))

    return rounds


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/{season_id}/overview", response_model=CupOverviewResponse)
async def get_cup_overview(
    season_id: int,
    lang: str = Query(default="ru", pattern="^(kz|ru|en)$"),
    recent_limit: int = Query(default=5, ge=1, le=20),
    upcoming_limit: int = Query(default=5, ge=1, le=20),
    db: AsyncSession = Depends(get_db),
):
    """Aggregated cup overview: current round, groups, bracket, recent/upcoming."""
    today = date_type.today()

    # 1. Load season + championship
    season = await _load_season(db, season_id, lang)
    championship = season.championship

    # 2. Load stages
    stage_result = await db.execute(
        select(Stage)
        .where(Stage.season_id == season_id)
        .order_by(Stage.sort_order, Stage.id)
    )
    stages = list(stage_result.scalars().all())

    # 3. Load all games
    all_games = await _load_games(db, season_id)

    # 4. Build rounds (with games for current_round detection)
    rounds_with_games = _build_rounds(all_games, stages, lang, today, include_games=True)

    # 5. Determine current round
    current_round = _determine_current_round(rounds_with_games)
    if current_round:
        current_round.is_current = True

    # 6. Build navigation rounds (without games to keep response smaller)
    nav_rounds = []
    for r in rounds_with_games:
        nav_rounds.append(CupRound(
            stage_id=r.stage_id,
            round_name=r.round_name,
            round_key=r.round_key,
            is_current=r is current_round,
            total_games=r.total_games,
            played_games=r.played_games,
            games=[],  # no games in nav
        ))

    # 7. Recent results + upcoming games (from all games)
    finished_games = [
        g for g in all_games if compute_game_status(g, today) == "finished"
    ]
    upcoming_games_raw = [
        g for g in all_games if compute_game_status(g, today) == "upcoming"
    ]
    live_games = [
        g for g in all_games if compute_game_status(g, today) == "live"
    ]

    # Recent: last N finished (most recent first)
    finished_games.sort(key=lambda g: (g.date, g.time or ""), reverse=True)
    recent_results = [
        _build_cup_game(g, lang, today) for g in finished_games[:recent_limit]
    ]

    # Upcoming: live games first, then next N upcoming (soonest first)
    upcoming_games_raw.sort(key=lambda g: (g.date, g.time or ""))
    upcoming_built = [_build_cup_game(g, lang, today) for g in live_games]
    upcoming_built += [
        _build_cup_game(g, lang, today) for g in upcoming_games_raw[:upcoming_limit]
    ]

    # 8. Groups (from SeasonParticipant entries)
    groups = None
    sp_result = await db.execute(
        select(SeasonParticipant)
        .where(SeasonParticipant.season_id == season_id)
        .options(selectinload(SeasonParticipant.team))
    )
    season_participant_entries = sp_result.scalars().all()

    group_names = sorted({
        tt.group_name for tt in season_participant_entries if tt.group_name
    })
    if group_names:
        groups = []
        for group_name in group_names:
            group_team_ids = [
                tt.team_id for tt in season_participant_entries
                if tt.group_name == group_name and not tt.is_disqualified
            ]
            table = await calculate_dynamic_table(
                db, season_id,
                tour_from=None, tour_to=None, home_away=None,
                lang=lang, group_team_ids=group_team_ids,
            )
            standings = [
                CupGroupStandingEntry(**row) for row in table
            ]
            groups.append(CupGroup(group_name=group_name, standings=standings))

    # 9. Bracket
    bracket = await _build_bracket(db, season_id, lang)

    return CupOverviewResponse(
        season_id=season_id,
        season_name=get_localized_field(season, "name", lang),
        championship_name=get_localized_field(championship, "name", lang) if championship else None,
        current_round=current_round,
        groups=groups,
        bracket=bracket,
        recent_results=recent_results,
        upcoming_games=upcoming_built,
        rounds=nav_rounds,
    )


@router.get("/{season_id}/schedule", response_model=CupScheduleResponse)
async def get_cup_schedule(
    season_id: int,
    lang: str = Query(default="ru", pattern="^(kz|ru|en)$"),
    round_key: str | None = Query(default=None, description="Filter by round key (e.g. '1_4', 'group_1')"),
    db: AsyncSession = Depends(get_db),
):
    """Full schedule grouped by rounds, with optional round filter."""
    today = date_type.today()

    # Verify season exists
    await _load_season(db, season_id, lang)

    # Load stages + games
    stage_result = await db.execute(
        select(Stage)
        .where(Stage.season_id == season_id)
        .order_by(Stage.sort_order, Stage.id)
    )
    stages = list(stage_result.scalars().all())

    all_games = await _load_games(db, season_id)

    # Build rounds with full game lists
    rounds = _build_rounds(all_games, stages, lang, today, include_games=True)

    # Optional filter by round_key
    if round_key:
        rounds = [r for r in rounds if r.round_key == round_key]

    total_games = sum(r.total_games for r in rounds)

    return CupScheduleResponse(
        season_id=season_id,
        rounds=rounds,
        total_games=total_games,
    )
