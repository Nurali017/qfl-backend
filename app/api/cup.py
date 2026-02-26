"""Public Cup API — aggregated endpoints for cup/knockout tournament pages."""

from datetime import date as date_type

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.api.deps import get_db
from app.utils.game_status import compute_game_status
from app.api.seasons import calculate_dynamic_table
from app.models import (
    Game,
    Season,
    Stage,
    SeasonParticipant,
)
from app.schemas.cup import (
    CupGroup,
    CupGroupStandingEntry,
    CupOverviewResponse,
    CupRound,
    CupScheduleResponse,
)
from app.services.cup_rounds import (
    build_cup_game,
    build_playoff_bracket_from_rounds,
    build_schedule_rounds,
    determine_current_round,
)
from app.schemas.playoff_bracket import PlayoffBracketResponse
from app.services.cup_draw import build_bracket_from_cup_draws
from app.services.season_visibility import is_season_visible_clause
from app.utils.localization import get_localized_field

router = APIRouter(prefix="/cup", tags=["cup"])


async def _load_season(db: AsyncSession, season_id: int, lang: str) -> Season:
    """Load season with championship, or 404."""
    result = await db.execute(
        select(Season)
        .where(
            Season.id == season_id,
            is_season_visible_clause(),
        )
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


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/{season_id}/overview", response_model=CupOverviewResponse)
async def get_cup_overview(
    season_id: int,
    lang: str = Query(default="kz", pattern="^(kz|ru|en)$"),
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
    rounds_with_games = build_schedule_rounds(all_games, stages, lang, today, include_games=True)

    # 5. Determine current round
    current_round = determine_current_round(rounds_with_games)
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
        build_cup_game(g, lang, today) for g in finished_games[:recent_limit]
    ]

    # Upcoming: live games first, then next N upcoming (soonest first)
    upcoming_games_raw.sort(key=lambda g: (g.date, g.time or ""))
    upcoming_built = [build_cup_game(g, lang, today) for g in live_games]
    upcoming_built += [
        build_cup_game(g, lang, today) for g in upcoming_games_raw[:upcoming_limit]
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

    # 9. Bracket (merge game-based rounds with draw-based rounds)
    game_bracket = build_playoff_bracket_from_rounds(season_id, rounds_with_games)
    draw_bracket = await build_bracket_from_cup_draws(db, season_id)

    if game_bracket and game_bracket.rounds and draw_bracket and draw_bracket.rounds:
        # Build draw lookup: round_key -> {frozenset(team_ids): entry}
        draw_round_map: dict[str, dict[str, list]] = {}
        for dr in draw_bracket.rounds:
            draw_round_map[dr.round_name] = {}
            for de in dr.entries:
                team_ids = set()
                if de.game and de.game.home_team:
                    team_ids.add(de.game.home_team.id)
                if de.game and de.game.away_team:
                    team_ids.add(de.game.away_team.id)
                key = frozenset(team_ids)
                draw_round_map[dr.round_name][str(key)] = de

        # Enrich game entries with draw side/sort_order
        for gr in game_bracket.rounds:
            draw_entries = draw_round_map.get(gr.round_name, {})
            for ge in gr.entries:
                team_ids = set()
                if ge.game and ge.game.home_team:
                    team_ids.add(ge.game.home_team.id)
                if ge.game and ge.game.away_team:
                    team_ids.add(ge.game.away_team.id)
                key = str(frozenset(team_ids))
                if key in draw_entries:
                    ge.side = draw_entries[key].side
                    ge.sort_order = draw_entries[key].sort_order

        # Merge: game rounds (now enriched) + draw-only rounds
        game_round_keys = {r.round_name for r in game_bracket.rounds}
        merged_rounds = list(game_bracket.rounds)
        for dr in draw_bracket.rounds:
            if dr.round_name not in game_round_keys:
                merged_rounds.append(dr)
        # Sort by playoff order
        order = {rk: i for i, rk in enumerate(
            ["1_32", "1_16", "1_8", "1_4", "1_2", "3rd_place", "final"]
        )}
        merged_rounds.sort(key=lambda r: order.get(r.round_name, 999))
        bracket = PlayoffBracketResponse(season_id=season_id, rounds=merged_rounds)
    elif game_bracket and game_bracket.rounds:
        bracket = game_bracket
    else:
        bracket = draw_bracket

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
    lang: str = Query(default="kz", pattern="^(kz|ru|en)$"),
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
    rounds = build_schedule_rounds(all_games, stages, lang, today, include_games=True)

    # Optional filter by round_key
    if round_key:
        rounds = [r for r in rounds if r.round_key == round_key]

    total_games = sum(r.total_games for r in rounds)

    return CupScheduleResponse(
        season_id=season_id,
        rounds=rounds,
        total_games=total_games,
    )

