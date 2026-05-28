"""Season table endpoints: standings table, results grid, league performance."""

from collections import defaultdict

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import Response
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import func, select
from sqlalchemy.orm import selectinload

from app.utils.cache import cache_get_or_compute

from app.api.deps import get_db
from app.database import WebAsyncSessionLocal
from app.models import Game, GameStatus, ScoreTable, Season, Team
from app.services.season_filters import get_group_team_ids, get_final_stage_ids, get_group_for_team
from app.services.standings import (
    _primary_sort_key,
    calculate_dynamic_table,
    compute_table_from_games,
    fetch_card_stats,
    read_score_table,
    get_next_games_for_teams,
)
from app.services.table_zones import resolve_table_zone
from app.services.season_visibility import ensure_visible_season_or_404
from app.utils.localization import get_localized_field
from app.utils.team_logo_fallback import resolve_team_logo_url
from app.schemas.stats import (
    ScoreTableResponse,
    ScoreTableEntryResponse,
    ScoreTableFilters,
    ResultsGridResponse,
    TeamResultsGridEntry,
    LiveMatchInline,
)
router = APIRouter(prefix="/seasons", tags=["seasons"])

_ensure_visible_season = ensure_visible_season_or_404


@router.get("/{season_id}/table")
async def get_season_table(
    season_id: int,
    group: str | None = Query(default=None, description="Filter by group name (e.g. 'A', 'B')"),
    final: bool = Query(default=False, description="Show only final stage matches"),
    tour_from: int | None = Query(default=None, description="From matchweek (inclusive)"),
    tour_to: int | None = Query(default=None, description="To matchweek (inclusive)"),
    home_away: str | None = Query(default=None, pattern="^(home|away)$", description="Filter home/away games"),
    include_live: bool = Query(default=True, description="If false, exclude live games from standings (Flashscore-style snapshot)"),
    team_id: int | None = Query(default=None, description="Auto-resolve group from this team's SeasonParticipant entry"),
    lang: str = Query(default="kz", pattern="^(kz|ru|en)$"),
):
    """
    Get league table for a season.

    Filters:
    - group: Filter by group name (from SeasonParticipant.group_name)
    - tour_from: Starting matchweek (inclusive)
    - tour_to: Ending matchweek (inclusive)
    - home_away: "home" for home games only, "away" for away games only

    Session lifecycle deliberately bypasses Depends(get_db). The default
    FastAPI pattern would hold one session for the entire handler — and so
    every concurrent caller waiting on cache_get_or_compute's per-key lock
    would also hold a pool slot in idle-in-transaction. Instead we run
    validation + group/final resolution in a SHORT explicit session, close
    it, then enter the singleflight wait holding NO connection. The actual
    work (executed by exactly one caller) opens its own fresh session
    inside _compute. The other 29-of-30 callers wait on an asyncio.Lock
    with no DB occupancy at all — they read the cached bytes when the
    leader releases the lock.
    """
    # Phase A: validation + group/final resolution in a short session.
    async with WebAsyncSessionLocal() as db:
        try:
            await _ensure_visible_season(db, season_id)

            if group and final:
                raise HTTPException(status_code=400, detail="group and final filters are mutually exclusive")

            # Auto-resolve group from team_id when caller supplies a team but no
            # explicit group / final filter — used by widgets on the team /
            # match pages so the table is scoped to the team's conference.
            if team_id is not None and not group and not final:
                resolved = await get_group_for_team(db, season_id, team_id)
                if resolved:
                    group = resolved

            group_team_ids: list[int] | None = None
            if group:
                group_team_ids = await get_group_team_ids(db, season_id, group)
                if not group_team_ids:
                    filters = ScoreTableFilters(tour_from=tour_from, tour_to=tour_to, home_away=home_away)
                    await db.commit()
                    return ScoreTableResponse(season_id=season_id, filters=filters, table=[])

            final_stage_ids_list: list[int] | None = None
            if final:
                final_stage_ids_list = await get_final_stage_ids(db, season_id)
                if not final_stage_ids_list:
                    filters = ScoreTableFilters(tour_from=tour_from, tour_to=tour_to, home_away=home_away)
                    await db.commit()
                    return ScoreTableResponse(season_id=season_id, filters=filters, table=[])

            await db.commit()
        except Exception:
            await db.rollback()
            raise
    # Phase A session released — we now hold no DB connection.

    filters = ScoreTableFilters(tour_from=tour_from, tour_to=tour_to, home_away=home_away)

    # Singleflight + TTL cache. Under HT/FT/goal bursts dozens of SSR
    # requests hit /seasons/{id}/table at once. Without coalescing each one
    # ran the full handler in parallel — ~330 PG queries from one burst,
    # backend CPU saturated, RU /table latency 5-12s (2026-05-28 incident).
    # Resolved `group` is used in the key (not raw team_id) so widgets on
    # team-pages share cache with the explicit ?group= query.
    cache_key = (
        f"season_table:v1:{season_id}:{group or ''}:{int(final)}:"
        f"{tour_from or ''}:{tour_to or ''}:{home_away or ''}:"
        f"{int(include_live)}:{lang}"
    )

    async def _compute() -> bytes:
        # Heavy work session is scoped to the leader caller only — the 29
        # waiters never touch the pool. Open here, close here.
        async with WebAsyncSessionLocal() as compute_db:
            try:
                response = await _build_season_table_response(
                    db=compute_db, season_id=season_id, lang=lang,
                    include_live=include_live, home_away=home_away,
                    group_team_ids=group_team_ids,
                    final_stage_ids_list=final_stage_ids_list,
                    tour_from=tour_from, tour_to=tour_to, filters=filters,
                )
                await compute_db.commit()
                return response.model_dump_json().encode()
            except Exception:
                await compute_db.rollback()
                raise

    json_bytes = await cache_get_or_compute(cache_key, ttl=30, compute=_compute)
    return Response(content=json_bytes, media_type="application/json")


async def _build_season_table_response(
    *,
    db: AsyncSession,
    season_id: int,
    lang: str,
    include_live: bool,
    home_away: str | None,
    group_team_ids: list[int] | None,
    final_stage_ids_list: list[int] | None,
    tour_from: int | None,
    tour_to: int | None,
    filters: ScoreTableFilters,
) -> ScoreTableResponse:
    """Build the full /table response. Extracted into a helper so the
    handler stays small and can wrap this body in cache_get_or_compute."""
    # Load all standings-relevant games (live + finished + technical defeat)
    # ONCE. We previously ran three near-identical SELECTs:
    #   1. live_games_full (status=live)
    #   2. calculate_dynamic_table(include_live=True)
    #   3. calculate_dynamic_table(include_live=False)  ← only during live
    # Each with selectinload(home_team, away_team) over ~150-200 games. Under
    # HT/FT/goal bursts that was ~330 queries to PG in parallel (2026-05-28
    # incident). Loading once and splitting in-memory cuts ~60% of the SQL.
    standings_statuses = [GameStatus.finished, GameStatus.technical_defeat, GameStatus.live]
    all_games_q = (
        select(Game)
        .where(
            Game.season_id == season_id,
            Game.status.in_(standings_statuses),
        )
        .options(
            selectinload(Game.home_team),
            selectinload(Game.away_team),
        )
        .order_by(Game.tour, Game.date, Game.time)
    )
    if tour_from is not None:
        all_games_q = all_games_q.where(Game.tour >= tour_from)
    if tour_to is not None:
        all_games_q = all_games_q.where(Game.tour <= tour_to)
    if group_team_ids is not None:
        all_games_q = all_games_q.where(
            Game.home_team_id.in_(group_team_ids),
            Game.away_team_id.in_(group_team_ids),
        )
    if final_stage_ids_list is not None:
        all_games_q = all_games_q.where(Game.stage_id.in_(final_stage_ids_list))

    all_games = list((await db.execute(all_games_q)).scalars().all())

    finished_or_td = (GameStatus.finished, GameStatus.technical_defeat)
    live_games_full = [g for g in all_games if g.status == GameStatus.live]
    finished_games = [g for g in all_games if g.status in finished_or_td]

    # Single card-stats fetch — covers both the live and pre-live computations.
    card_stats = await fetch_card_stats(db, [g.id for g in all_games])

    live_team_ids = list({
        tid
        for g in live_games_full
        for tid in (g.home_team_id, g.away_team_id)
        if tid
    })
    live_count = len(live_games_full)

    live_match_by_team: dict[int, LiveMatchInline] = {}
    for g in live_games_full:
        if g.home_team_id is None or g.away_team_id is None:
            continue
        home_score = g.home_score or 0
        away_score = g.away_score or 0
        if g.home_team is not None:
            live_match_by_team[g.home_team_id] = LiveMatchInline(
                match_id=g.id,
                opponent_id=g.away_team_id,
                opponent_name=get_localized_field(g.away_team, "name", lang) if g.away_team else None,
                opponent_logo=resolve_team_logo_url(g.away_team) if g.away_team else None,
                is_home=True,
                score_for=home_score,
                score_against=away_score,
                minute=g.live_minute,
                half=g.live_half,
                status_text=g.live_phase,
            )
        if g.away_team is not None:
            live_match_by_team[g.away_team_id] = LiveMatchInline(
                match_id=g.id,
                opponent_id=g.home_team_id,
                opponent_name=get_localized_field(g.home_team, "name", lang) if g.home_team else None,
                opponent_logo=resolve_team_logo_url(g.home_team) if g.home_team else None,
                is_home=False,
                score_for=away_score,
                score_against=home_score,
                minute=g.live_minute,
                half=g.live_half,
                status_text=g.live_phase,
            )

    # Always calculate table dynamically with full tiebreakers (H2H + cards).
    # Uses the games we already loaded above — no extra SQL.
    table_games = all_games if (bool(live_count) and include_live) else finished_games
    table_data = compute_table_from_games(table_games, card_stats, home_away, lang)

    # Merge with score_table for: team list (teams with 0 games) + notes (point penalties)
    base_table = await read_score_table(db, season_id, group_team_ids, lang)
    if table_data:
        if base_table:
            # Transfer notes (point penalties) from score_table
            note_map = {e["team_id"]: e.get("note") for e in base_table if e.get("note")}
            for entry in table_data:
                if entry["team_id"] in note_map:
                    entry["note"] = note_map[entry["team_id"]]
            # Add teams without matches in this filter range
            if len(table_data) < len(base_table):
                dynamic_ids = {e["team_id"] for e in table_data}
                for entry in base_table:
                    if entry["team_id"] not in dynamic_ids:
                        table_data.append({
                            "team_id": entry["team_id"],
                            "team_name": entry["team_name"],
                            "team_logo": entry.get("team_logo"),
                            "games_played": 0, "wins": 0, "draws": 0,
                            "losses": 0, "goals_scored": 0, "goals_conceded": 0,
                            "goal_difference": 0, "points": 0, "form": None,
                            "note": entry.get("note"),
                            "total_red_cards": 0, "total_yellow_cards": 0,
                        })
                # Stable sort preserves H2H ordering from calculate_dynamic_table
                table_data.sort(key=_primary_sort_key)
                for i, entry in enumerate(table_data, 1):
                    entry["position"] = i
    elif base_table:
        table_data = base_table
    else:
        table_data = []

    # Compute position_change only during LIVE matches — compares current live
    # standings vs standings before live games started (Flashscore-style).
    # When no live games, standings are static → no indicators shown.
    # Reuses finished_games + card_stats from the single SQL load above.
    position_change_map: dict[int, int] = {}
    if live_count and include_live and not home_away:
        pre_live_table = compute_table_from_games(finished_games, card_stats, None, lang)
        position_change_map = {e["team_id"]: e["position"] for e in pre_live_table}

    team_ids = [entry["team_id"] for entry in table_data]
    next_games = await get_next_games_for_teams(db, season_id, team_ids)
    total_rows = len(table_data)

    season_config_result = await db.execute(
        select(
            Season.champion_spots,
            Season.euro_cup_spots,
            Season.relegation_spots,
        ).where(Season.id == season_id)
    )
    season_config = season_config_result.one_or_none()
    champion_spots = season_config.champion_spots if season_config else 0
    euro_cup_spots = season_config.euro_cup_spots if season_config else 0
    relegation_spots = season_config.relegation_spots if season_config else 0

    table = []
    for entry in table_data:
        prev_pos = position_change_map.get(entry["team_id"])
        pos_change = (prev_pos - entry["position"]) if prev_pos and entry.get("position") else None
        table.append(
            ScoreTableEntryResponse(
                position=entry["position"],
                team_id=entry["team_id"],
                team_name=entry["team_name"],
                team_logo=entry.get("team_logo") or entry.get("logo_url"),
                games_played=entry["games_played"],
                wins=entry["wins"],
                draws=entry["draws"],
                losses=entry["losses"],
                goals_scored=entry["goals_scored"],
                goals_conceded=entry["goals_conceded"],
                goal_difference=entry["goal_difference"],
                points=entry["points"],
                form=entry["form"],
                note=entry.get("note"),
                total_red_cards=entry.get("total_red_cards"),
                total_yellow_cards=entry.get("total_yellow_cards"),
                zone=resolve_table_zone(
                    position=entry["position"],
                    total_rows=total_rows,
                    champion_spots=champion_spots,
                    euro_cup_spots=euro_cup_spots,
                    relegation_spots=relegation_spots,
                ),
                next_game=next_games.get(entry["team_id"]),
                position_change=pos_change,
                live_match=live_match_by_team.get(entry["team_id"]) if include_live else None,
            )
        )

    return ScoreTableResponse(
        season_id=season_id, filters=filters, table=table,
        has_live=bool(live_count), live_team_ids=live_team_ids,
    )


@router.get("/{season_id}/results-grid", response_model=ResultsGridResponse)
async def get_results_grid(
    season_id: int,
    group: str | None = Query(default=None, description="Filter by group name (e.g. 'A', 'B')"),
    final: bool = Query(default=False, description="Show only final stage matches"),
    lang: str = Query(default="kz", pattern="^(kz|ru|en)$"),
    db: AsyncSession = Depends(get_db),
):
    """
    Get results grid - W/D/L for each team in each tour.

    Returns a matrix where each team has an array of results for each matchweek.
    """
    await _ensure_visible_season(db, season_id)

    if group and final:
        raise HTTPException(status_code=400, detail="group and final filters are mutually exclusive")

    # Resolve group team_ids if group filter is specified
    group_team_ids: list[int] | None = None
    if group:
        group_team_ids = await get_group_team_ids(db, season_id, group)
        if not group_team_ids:
            return ResultsGridResponse(season_id=season_id, total_tours=0, teams=[])

    final_stage_ids_list: list[int] | None = None
    if final:
        final_stage_ids_list = await get_final_stage_ids(db, season_id)
        if not final_stage_ids_list:
            return ResultsGridResponse(season_id=season_id, total_tours=0, teams=[])

    # Get teams from score_table (sorted by position) for stable ordering
    score_query = (
        select(ScoreTable)
        .where(ScoreTable.season_id == season_id)
        .options(selectinload(ScoreTable.team))
    )
    if group_team_ids is not None:
        score_query = score_query.where(ScoreTable.team_id.in_(group_team_ids))
    score_query = score_query.order_by(ScoreTable.position)

    score_result = await db.execute(score_query)
    score_entries = score_result.scalars().all()

    # Get all played games for the selected phase
    games_query = (
        select(Game)
        .where(
            Game.season_id == season_id,
            Game.status.in_([GameStatus.finished, GameStatus.technical_defeat]),
            Game.tour.isnot(None),
        )
    )
    if group_team_ids is not None:
        games_query = games_query.where(
            Game.home_team_id.in_(group_team_ids),
            Game.away_team_id.in_(group_team_ids),
        )
    if final_stage_ids_list is not None:
        games_query = games_query.where(Game.stage_id.in_(final_stage_ids_list))
    games_query = games_query.order_by(Game.tour)

    games_result = await db.execute(games_query)
    games = games_result.scalars().all()

    if not score_entries and not games:
        return ResultsGridResponse(season_id=season_id, total_tours=0, teams=[])

    # Find max tour from ALL matches (including upcoming) so the tour slider
    # shows the full season, not just played tours.
    season = await db.get(Season, season_id)
    all_tours_max = await db.scalar(
        select(func.max(Game.tour)).where(
            Game.season_id == season_id,
            Game.tour.isnot(None),
        )
    )
    max_tour = (
        (season.total_rounds if season and season.total_rounds else None)
        or all_tours_max
        or max((g.tour for g in games), default=0)
    )

    score_by_team_id: dict[int, ScoreTable] = {
        entry.team_id: entry for entry in score_entries
    }
    played_team_ids: set[int] = {
        team_id
        for game in games
        for team_id in (game.home_team_id, game.away_team_id)
        if team_id is not None
    }

    if final_stage_ids_list is not None:
        ordered_team_ids = [entry.team_id for entry in score_entries if entry.team_id in played_team_ids]
        remaining_ids = sorted(played_team_ids - set(ordered_team_ids))
        ordered_team_ids.extend(remaining_ids)
    elif score_entries:
        ordered_team_ids = [entry.team_id for entry in score_entries]
    else:
        ordered_team_ids = sorted(played_team_ids)

    if not ordered_team_ids:
        return ResultsGridResponse(season_id=season_id, total_tours=max_tour, teams=[])

    # Build results dict: team_id -> [result for each tour]
    team_results: dict[int, list[str | None]] = {
        team_id: [None] * max_tour
        for team_id in ordered_team_ids
    }

    # Fill in results from games
    for game in games:
        tour_idx = game.tour - 1
        home_id = game.home_team_id
        away_id = game.away_team_id

        if game.home_score > game.away_score:
            home_result, away_result = "W", "L"
        elif game.home_score < game.away_score:
            home_result, away_result = "L", "W"
        else:
            home_result, away_result = "D", "D"

        if home_id in team_results and tour_idx < len(team_results[home_id]):
            team_results[home_id][tour_idx] = home_result
        if away_id in team_results and tour_idx < len(team_results[away_id]):
            team_results[away_id][tour_idx] = away_result

    missing_team_ids = [
        team_id
        for team_id in ordered_team_ids
        if score_by_team_id.get(team_id) is None or score_by_team_id[team_id].team is None
    ]
    teams_lookup: dict[int, Team] = {}
    if missing_team_ids:
        teams_result = await db.execute(
            select(Team).where(Team.id.in_(missing_team_ids))
        )
        teams_lookup = {team.id: team for team in teams_result.scalars().all()}

    # Build response
    phase_filtered = group_team_ids is not None or final_stage_ids_list is not None
    teams = []
    for idx, team_id in enumerate(ordered_team_ids, 1):
        score_entry = score_by_team_id.get(team_id)
        team = score_entry.team if score_entry and score_entry.team else teams_lookup.get(team_id)
        position = (
            idx
            if phase_filtered
            else (score_entry.position if score_entry and score_entry.position is not None else idx)
        )
        teams.append(
            TeamResultsGridEntry(
                position=position,
                team_id=team_id,
                team_name=get_localized_field(team, "name", lang) if team else None,
                team_logo=resolve_team_logo_url(team),
                results=team_results.get(team_id, []),
            )
        )

    return ResultsGridResponse(season_id=season_id, total_tours=max_tour, teams=teams)


@router.get("/{season_id}/league-performance")
async def get_league_performance(
    season_id: int,
    team_ids: str | None = Query(default=None, description="Comma-separated team IDs to filter"),
    lang: str = Query(default="kz", pattern="^(kz|ru|en)$"),
    db: AsyncSession = Depends(get_db),
):
    """
    Get league position for each team at each matchweek.
    Returns positions over time for the league performance chart.
    """
    await _ensure_visible_season(db, season_id)

    season = await db.get(Season, season_id)
    total_rounds = season.total_rounds if season else None

    filter_team_ids: set[int] | None = None
    if team_ids:
        filter_team_ids = {
            int(x.strip()) for x in team_ids.split(",") if x.strip().isdigit()
        }

    # Load both terminal games (for scoring) and postponed games (for the
    # makeup-mapping below).  A rescheduled fixture played in a far-off tour
    # slot should appear in the dynamics chart at the original tour it was
    # supposed to be played in, not the placeholder slot it currently sits
    # at (PL-2026: Qairat vs Kaspiy game #1081 in tour 25 is the makeup for
    # postponed tour 6 game #926).
    query = (
        select(Game)
        .where(
            Game.season_id == season_id,
            Game.status.in_([
                GameStatus.finished,
                GameStatus.technical_defeat,
                GameStatus.live,
                GameStatus.postponed,
            ]),
            Game.tour.isnot(None),
        )
        .options(selectinload(Game.home_team), selectinload(Game.away_team))
        .order_by(Game.tour, Game.date, Game.time)
    )

    result = await db.execute(query)
    games = result.scalars().all()

    if not games:
        return {"season_id": season_id, "max_tour": 0, "total_rounds": total_rounds, "teams": []}

    # Build makeup map: earliest postponed-tour for each team pair.  If a
    # later-tour terminal game between the same two teams exists, it is
    # almost always the rescheduled makeup and belongs in the earlier slot.
    postponed_tour_by_pair: dict[frozenset[int], int] = {}
    for g in games:
        if g.status != GameStatus.postponed:
            continue
        if g.home_team_id is None or g.away_team_id is None:
            continue
        pair = frozenset((g.home_team_id, g.away_team_id))
        existing = postponed_tour_by_pair.get(pair)
        if existing is None or g.tour < existing:
            postponed_tour_by_pair[pair] = g.tour

    def _effective_tour(g) -> int:
        if g.home_team_id is None or g.away_team_id is None:
            return g.tour
        pair = frozenset((g.home_team_id, g.away_team_id))
        makeup_tour = postponed_tour_by_pair.get(pair)
        if makeup_tour is not None and g.tour > makeup_tour:
            return makeup_tour
        return g.tour

    # Terminal games go into their effective tour; postponed games contribute
    # nothing to positions (no score) so they are skipped entirely.
    games_by_tour: dict[int, list] = defaultdict(list)
    for g in games:
        if g.status == GameStatus.postponed:
            continue
        games_by_tour[_effective_tour(g)].append(g)

    if not games_by_tour:
        return {"season_id": season_id, "max_tour": 0, "total_rounds": total_rounds, "teams": []}

    max_tour = max(games_by_tour.keys())

    # Collect all team IDs and info
    team_info: dict[int, dict] = {}
    for game in games:
        if game.home_team_id not in team_info and game.home_team:
            team_info[game.home_team_id] = {
                "name": get_localized_field(game.home_team, "name", lang),
                "logo": resolve_team_logo_url(game.home_team),
            }
        if game.away_team_id not in team_info and game.away_team:
            team_info[game.away_team_id] = {
                "name": get_localized_field(game.away_team, "name", lang),
                "logo": resolve_team_logo_url(game.away_team),
            }

    # Initialize cumulative stats for all teams
    cumulative: dict[int, dict] = {
        tid: {"points": 0, "gd": 0, "gs": 0, "wins": 0}
        for tid in team_info
    }
    positions_by_team: dict[int, list[int]] = {tid: [] for tid in team_info}

    for tour in range(1, max_tour + 1):
        for game in games_by_tour.get(tour, []):
            # Skip live games without scores yet
            if game.home_score is None and game.away_score is None:
                continue
            h_id, a_id = game.home_team_id, game.away_team_id
            h_score = game.home_score if game.home_score is not None else 0
            a_score = game.away_score if game.away_score is not None else 0

            cumulative[h_id]["gs"] += h_score
            cumulative[h_id]["gd"] += h_score - a_score
            cumulative[a_id]["gs"] += a_score
            cumulative[a_id]["gd"] += a_score - h_score

            if h_score > a_score:
                cumulative[h_id]["points"] += 3
                cumulative[h_id]["wins"] += 1
            elif h_score < a_score:
                cumulative[a_id]["points"] += 3
                cumulative[a_id]["wins"] += 1
            else:
                cumulative[h_id]["points"] += 1
                cumulative[a_id]["points"] += 1

        standings = sorted(
            cumulative.items(),
            key=lambda x: (-x[1]["points"], -x[1]["gd"], -x[1]["wins"], -x[1]["gs"]),
        )
        for pos, (tid, _) in enumerate(standings, 1):
            positions_by_team[tid].append(pos)

    teams_result = []
    for tid, positions in positions_by_team.items():
        if filter_team_ids and tid not in filter_team_ids:
            continue
        info = team_info.get(tid, {"name": None, "logo": None})
        teams_result.append({
            "team_id": tid,
            "team_name": info["name"],
            "team_logo": info["logo"],
            "positions": positions,
        })

    teams_result.sort(
        key=lambda t: t["positions"][-1] if t["positions"] else 999
    )

    return {"season_id": season_id, "max_tour": max_tour, "total_rounds": total_rounds, "teams": teams_result}
