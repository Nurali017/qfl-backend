from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from sqlalchemy.orm import selectinload

from app.api.deps import get_db
from app.models import Player, PlayerTeam, Game, GamePlayerStats, PlayerSeasonStats, Team, Season
from app.models.game_event import GameEvent, GameEventType
from app.schemas.player import (
    PlayerResponse,
    PlayerListResponse,
    PlayerDetailResponse,
    PlayerSeasonStatsResponse,
    PlayerTeammateResponse,
    PlayerTeammatesListResponse,
    PlayerTournamentHistoryEntry,
    PlayerTournamentHistoryResponse,
    PlayerMatchHistoryEntry,
    PlayerMatchHistoryResponse,
    PlayerMatchHistoryTeam,
)
from app.schemas.game import GameResponse, GameListResponse
from app.schemas.team import TeamInGame
from app.models.game_lineup import GameLineup, LineupType
from app.services.default_season import pick_default_season
from app.services.season_visibility import ensure_visible_season_or_404, resolve_visible_season_id
from app.utils.localization import get_localized_field, get_localized_name
from app.utils.numbers import sanitize_non_finite_numbers
from app.utils.positions import (
    aggregate_lineup_positions,
    fallback_positions_from_top_role,
    infer_position_code,
)
from app.utils.team_logo_fallback import resolve_team_logo_url
from app.utils.has_stats import enrich_games_has_stats

router = APIRouter(prefix="/players", tags=["players"])


def _resolve_top_role(
    player: Player,
    lang: str,
    player_team: PlayerTeam | None = None,
) -> str | None:
    """
    Return localized player top_role with fallback to PlayerTeam.position.

    Priority:
    1. Player.top_role_{lang} / top_role (set manually via admin API).
    2. PlayerTeam.position_{lang} / position_ru fallback from the supplied
       contract, or from the most recent contract on player.player_teams.
    """
    top_role = get_localized_field(player, "top_role", lang)
    if top_role:
        return top_role

    if player_team is not None:
        pt_position = get_localized_field(player_team, "position", lang)
        if pt_position:
            return pt_position
        return None

    player_teams = getattr(player, "player_teams", None) or []
    for pt in sorted(player_teams, key=lambda p: p.season_id, reverse=True):
        pt_position = get_localized_field(pt, "position", lang)
        if pt_position:
            return pt_position

    return None


@router.get("", response_model=PlayerListResponse)
async def get_players(
    season_id: int | None = None,
    team_id: int | None = None,
    limit: int = Query(default=50, le=100),
    offset: int = Query(default=0),
    lang: str = Query(default="kz", description="Language: kz, ru, or en"),
    db: AsyncSession = Depends(get_db),
):
    """Get players, optionally filtered by season and team."""
    if season_id is not None:
        await ensure_visible_season_or_404(db, season_id)

    query = select(Player).options(selectinload(Player.country), selectinload(Player.player_teams))

    if season_id or team_id:
        subquery = select(PlayerTeam.player_id)
        if season_id:
            subquery = subquery.where(PlayerTeam.season_id == season_id)
        if team_id:
            subquery = subquery.where(PlayerTeam.team_id == team_id)
        query = query.where(Player.id.in_(subquery.distinct()))

    # Count total
    count_query = select(func.count()).select_from(query.subquery())
    total_result = await db.execute(count_query)
    total = total_result.scalar() or 0

    # Get paginated results
    query = query.order_by(Player.last_name, Player.first_name).offset(offset).limit(limit)
    result = await db.execute(query)
    players = result.scalars().all()

    # Build localized response
    items = []
    for p in players:
        country_data = None
        if p.country:
            country_data = {
                "id": p.country.id,
                "code": p.country.code,
                "name": get_localized_name(p.country, lang),
                "flag_url": p.country.flag_url,
            }
        items.append({
            "id": p.id,
            "first_name": get_localized_field(p, "first_name", lang),
            "last_name": get_localized_field(p, "last_name", lang),
            "birthday": p.birthday,
            "player_type": p.player_type,
            "country": country_data,
            "photo_url": next((pt.photo_url for pt in p.player_teams if pt.photo_url and (not season_id or pt.season_id == season_id) and (not team_id or pt.team_id == team_id)), None) or p.photo_url,
            "age": p.age,
            "top_role": _resolve_top_role(p, lang),
        })

    return {"items": items, "total": total}


@router.get("/{player_id}", response_model=PlayerDetailResponse)
async def get_player(
    player_id: int,
    season_id: int | None = Query(default=None),
    lang: str = Query(default="kz", description="Language: kz, ru, or en"),
    db: AsyncSession = Depends(get_db),
):
    """Get player by ID."""
    if season_id is not None:
        await ensure_visible_season_or_404(db, season_id)

    result = await db.execute(
        select(Player)
        .where(Player.id == player_id)
        .options(
            selectinload(Player.player_teams),
            selectinload(Player.country),
        )
    )
    player = result.scalar_one_or_none()

    if not player:
        raise HTTPException(status_code=404, detail="Player not found")

    # Filter teams by season if season_id provided
    if season_id is not None:
        player_teams_filtered = [pt for pt in player.player_teams if pt.season_id == season_id]
    else:
        player_teams_filtered = list(player.player_teams)

    teams = list(set(pt.team_id for pt in player_teams_filtered))

    # Fallback to latest season contracts if current season has none
    if player_teams_filtered:
        latest_pts = player_teams_filtered
    else:
        all_pts_sorted = sorted(player.player_teams, key=lambda pt: pt.season_id, reverse=True)
        if all_pts_sorted:
            latest_season_id = all_pts_sorted[0].season_id
            latest_pts = [pt for pt in all_pts_sorted if pt.season_id == latest_season_id]
            teams = list(set(pt.team_id for pt in latest_pts))
        else:
            latest_pts = []

    jersey_number = latest_pts[0].number if latest_pts else None

    # Extract contract end date from active records only
    contract_end = None
    active_with_contract = [
        pt for pt in latest_pts
        if pt.is_active and pt.contract_end_date
    ]
    if active_with_contract:
        contract_end = active_with_contract[0].contract_end_date.isoformat()

    # Build country response
    country_data = None
    if player.country:
        country_data = {
            "id": player.country.id,
            "code": player.country.code,
            "name": get_localized_name(player.country, lang),
            "flag_url": player.country.flag_url,
        }

    resolved_top_role = _resolve_top_role(
        player,
        lang,
        player_team=latest_pts[0] if latest_pts else None,
    )

    # Aggregate positions from starter lineups in the player's CURRENT season
    # (derived from the latest PlayerTeam contract). If fewer than 3 such
    # lineups exist (e.g. early in a new season), fall back to the last 20
    # across all seasons so new/inactive players still get a label.
    current_season_id = (
        max(pt.season_id for pt in latest_pts) if latest_pts else None
    )

    lineup_rows: list[tuple[str | None, str | None]] = []
    if current_season_id is not None:
        season_lineup_result = await db.execute(
            select(GameLineup.amplua, GameLineup.field_position)
            .join(Game, Game.id == GameLineup.game_id)
            .where(
                GameLineup.player_id == player_id,
                GameLineup.lineup_type == LineupType.starter,
                GameLineup.amplua.isnot(None),
                Game.season_id == current_season_id,
            )
            .order_by(Game.date.desc().nullslast(), Game.id.desc())
        )
        lineup_rows = list(season_lineup_result.all())

    if len(lineup_rows) < 3:
        fallback_result = await db.execute(
            select(GameLineup.amplua, GameLineup.field_position)
            .join(Game, Game.id == GameLineup.game_id)
            .where(
                GameLineup.player_id == player_id,
                GameLineup.lineup_type == LineupType.starter,
                GameLineup.amplua.isnot(None),
            )
            .order_by(Game.date.desc().nullslast(), Game.id.desc())
            .limit(20)
        )
        lineup_rows = list(fallback_result.all())

    aggregated = aggregate_lineup_positions(lineup_rows)
    if aggregated.source == "unknown":
        aggregated = fallback_positions_from_top_role(player.player_type, resolved_top_role)

    return {
        "id": player.id,
        "first_name": get_localized_field(player, "first_name", lang),
        "last_name": get_localized_field(player, "last_name", lang),
        "birthday": player.birthday,
        "player_type": player.player_type,
        "country": country_data,
        "photo_url": (latest_pts[0].photo_url if latest_pts else None) or player.photo_url,
        "photo_url_avatar": latest_pts[0].photo_url_avatar if latest_pts else None,
        "photo_url_leaderboard": latest_pts[0].photo_url_leaderboard if latest_pts else None,
        "photo_url_player_page": latest_pts[0].photo_url_player_page if latest_pts else None,
        "age": player.age,
        "top_role": resolved_top_role,
        "teams": teams,
        "jersey_number": jersey_number,
        "height": player.height,
        "weight": player.weight,
        "gender": player.gender,
        "contract_end": contract_end,
        "position_code": infer_position_code(player.player_type, resolved_top_role),
        "positions": {
            "primary": aggregated.primary,
            "secondary": list(aggregated.secondary),
            "sample_size": aggregated.sample_size,
            "source": aggregated.source,
        },
    }


@router.get("/{player_id}/stats", response_model=PlayerSeasonStatsResponse | None)
async def get_player_stats(
    player_id: int,
    season_id: int = Query(default=None),
    db: AsyncSession = Depends(get_db),
):
    """
    Get player statistics for a season.

    Returns 50+ metrics from SOTA API v2 including:
    - xG, xG per 90
    - Goals, assists
    - Duels, dribbles, tackles
    - Passes, key passes
    - And more in extra_stats
    """
    season_id = await resolve_visible_season_id(db, season_id)

    # Get from player_season_stats table
    result = await db.execute(
        select(PlayerSeasonStats).where(
            PlayerSeasonStats.player_id == player_id,
            PlayerSeasonStats.season_id == season_id,
        )
    )
    stats = result.scalar_one_or_none()

    if not stats:
        return None

    payload = PlayerSeasonStatsResponse.model_validate(stats).model_dump()
    return sanitize_non_finite_numbers(payload)


@router.get("/{player_id}/games", response_model=GameListResponse)
async def get_player_games(
    player_id: int,
    season_id: int = Query(default=None),
    limit: int = Query(default=50, le=100),
    lang: str = Query(default="kz", description="Language: kz, ru, or en"),
    db: AsyncSession = Depends(get_db),
):
    """Get games a player participated in."""
    season_id = await resolve_visible_season_id(db, season_id)

    # Get game IDs where player has stats
    game_ids_result = await db.execute(
        select(GamePlayerStats.game_id).where(GamePlayerStats.player_id == player_id)
    )
    game_ids = [g[0] for g in game_ids_result.fetchall()]

    if not game_ids:
        return GameListResponse(items=[], total=0)

    # Get games
    result = await db.execute(
        select(Game)
        .where(Game.id.in_(game_ids), Game.season_id == season_id)
        .options(
            selectinload(Game.home_team),
            selectinload(Game.away_team),
            selectinload(Game.season),
        )
        .order_by(Game.date.desc())
        .limit(limit)
    )
    games = list(result.scalars().all())
    await enrich_games_has_stats(db, games)

    items = []
    for g in games:
        home_team = None
        away_team = None
        if g.home_team:
            home_team = TeamInGame(
                id=g.home_team.id,
                name=get_localized_field(g.home_team, "name", lang),
                logo_url=resolve_team_logo_url(g.home_team),
                score=g.home_score,
            )
        if g.away_team:
            away_team = TeamInGame(
                id=g.away_team.id,
                name=get_localized_field(g.away_team, "name", lang),
                logo_url=resolve_team_logo_url(g.away_team),
                score=g.away_score,
            )

        items.append(
            GameResponse(
                id=g.id,
                date=g.date,
                time=g.time,
                tour=g.tour,
                season_id=g.season_id,
                home_score=g.home_score,
                away_score=g.away_score,
                has_stats=g.has_stats,
                visitors=g.visitors,
                home_team=home_team,
                away_team=away_team,
                season_name=get_localized_field(g.season, "name", lang) if g.season else None,
            )
        )

    return GameListResponse(items=items, total=len(items))


@router.get("/{player_id}/teammates", response_model=PlayerTeammatesListResponse)
async def get_player_teammates(
    player_id: int,
    season_id: int = Query(default=None),
    limit: int = Query(default=10, le=50),
    lang: str = Query(default="kz", description="Language: kz, ru, or en"),
    db: AsyncSession = Depends(get_db),
):
    """
    Get teammates of a player from the same team in the current season.
    Excludes the player themselves from the result.
    """
    season_id = await resolve_visible_season_id(db, season_id)

    # 1. Find player's team_id in the current season
    player_team_result = await db.execute(
        select(PlayerTeam).where(
            PlayerTeam.player_id == player_id,
            PlayerTeam.season_id == season_id,
        )
    )
    player_team = player_team_result.scalars().first()

    if not player_team:
        return PlayerTeammatesListResponse(items=[], total=0)

    team_id = player_team.team_id

    # 2. Get all players from the same team (excluding the current player)
    teammates_result = await db.execute(
        select(PlayerTeam)
        .where(
            PlayerTeam.team_id == team_id,
            PlayerTeam.season_id == season_id,
            PlayerTeam.player_id != player_id,
        )
        .options(selectinload(PlayerTeam.player))
        .limit(limit)
    )
    teammate_teams = teammates_result.scalars().all()

    items = []
    for pt in teammate_teams:
        if pt.player:
            items.append(
                PlayerTeammateResponse(
                    player_id=pt.player.id,
                    first_name=get_localized_field(pt.player, "first_name", lang),
                    last_name=get_localized_field(pt.player, "last_name", lang),
                    jersey_number=pt.number,
                    position=_resolve_top_role(pt.player, lang, player_team=pt),
                    age=pt.player.age,
                    photo_url=pt.photo_url,
                )
            )

    return PlayerTeammatesListResponse(items=items, total=len(items))


@router.get("/{player_id}/match-history", response_model=PlayerMatchHistoryResponse)
async def get_player_match_history(
    player_id: int,
    season_id: int | None = Query(default=None),
    limit: int = Query(default=50, le=200),
    lang: str = Query(default="kz", description="Language: kz, ru, or en"),
    db: AsyncSession = Depends(get_db),
):
    """
    Get player's per-match history, enriched with per-match stats
    from GamePlayerStats and goal/assist counts derived from game_events.

    Unlike /games (which returns raw Game objects), this endpoint joins
    GamePlayerStats for minutes/shots/cards and aggregates goals/assists
    from the game_events table for the specific player_id.
    """
    if season_id is not None:
        await ensure_visible_season_or_404(db, season_id)

    # 1. Fetch per-game stats rows for the player, joined with Game + teams + season.
    # Skip rows where v2 enrichment (minutes_played / pass accuracy / duels /
    # tackles) has not landed yet — otherwise the table renders misleading
    # zeros for a match that was just played and is still pending sync from
    # SOTA. Once enrichment runs, minutes_played becomes non-null and the
    # match appears automatically.
    stats_query = (
        select(GamePlayerStats)
        .where(
            GamePlayerStats.player_id == player_id,
            GamePlayerStats.minutes_played.isnot(None),
        )
        .options(
            selectinload(GamePlayerStats.game).selectinload(Game.home_team),
            selectinload(GamePlayerStats.game).selectinload(Game.away_team),
            selectinload(GamePlayerStats.game).selectinload(Game.season),
        )
    )
    if season_id is not None:
        stats_query = stats_query.join(Game, GamePlayerStats.game_id == Game.id).where(
            Game.season_id == season_id
        )

    stats_result = await db.execute(stats_query)
    per_game_stats = list(stats_result.scalars().all())

    if not per_game_stats:
        return PlayerMatchHistoryResponse(items=[], total=0)

    game_ids = [ps.game_id for ps in per_game_stats]

    # 2. Count goals/assists per game_id for this player from game_events
    events_result = await db.execute(
        select(
            GameEvent.game_id,
            GameEvent.event_type,
            func.count(GameEvent.id).label("cnt"),
        )
        .where(
            GameEvent.game_id.in_(game_ids),
            (
                (GameEvent.player_id == player_id)
                | (GameEvent.assist_player_id == player_id)
            ),
        )
        .group_by(GameEvent.game_id, GameEvent.event_type)
    )

    goals_by_game: dict[int, int] = {}
    assists_by_game: dict[int, int] = {}
    for row in events_result.all():
        gid, event_type, cnt = row
        if event_type in (GameEventType.goal, GameEventType.penalty):
            # Only count if player_id matches (filter above may have matched via assist_player_id)
            goals_by_game[gid] = goals_by_game.get(gid, 0) + int(cnt or 0)
        elif event_type == GameEventType.assist:
            assists_by_game[gid] = assists_by_game.get(gid, 0) + int(cnt or 0)

    # Re-query to accurately split goals (by player_id) from assists (by assist_player_id)
    # The single grouped query above may double-count; run precise queries instead.
    goals_result = await db.execute(
        select(GameEvent.game_id, func.count(GameEvent.id))
        .where(
            GameEvent.game_id.in_(game_ids),
            GameEvent.player_id == player_id,
            GameEvent.event_type.in_(
                [GameEventType.goal, GameEventType.penalty]
            ),
        )
        .group_by(GameEvent.game_id)
    )
    goals_by_game = {gid: int(cnt or 0) for gid, cnt in goals_result.all()}

    assists_result = await db.execute(
        select(GameEvent.game_id, func.count(GameEvent.id))
        .where(
            GameEvent.game_id.in_(game_ids),
            GameEvent.assist_player_id == player_id,
            GameEvent.event_type.in_(
                [GameEventType.goal, GameEventType.penalty]
            ),
        )
        .group_by(GameEvent.game_id)
    )
    assists_by_game = {gid: int(cnt or 0) for gid, cnt in assists_result.all()}

    # 3. Build response items, sorted newest first
    per_game_stats.sort(key=lambda ps: (ps.game.date if ps.game else None, ps.game_id), reverse=True)
    per_game_stats = per_game_stats[:limit]

    items: list[PlayerMatchHistoryEntry] = []
    for ps in per_game_stats:
        game = ps.game
        if game is None:
            continue

        home_team_obj = game.home_team
        away_team_obj = game.away_team

        home_team = PlayerMatchHistoryTeam(
            id=home_team_obj.id if home_team_obj else None,
            name=get_localized_field(home_team_obj, "name", lang) if home_team_obj else None,
            logo_url=resolve_team_logo_url(home_team_obj) if home_team_obj else None,
            score=game.home_score,
        )
        away_team = PlayerMatchHistoryTeam(
            id=away_team_obj.id if away_team_obj else None,
            name=get_localized_field(away_team_obj, "name", lang) if away_team_obj else None,
            logo_url=resolve_team_logo_url(away_team_obj) if away_team_obj else None,
            score=game.away_score,
        )

        items.append(
            PlayerMatchHistoryEntry(
                game_id=game.id,
                date=game.date.isoformat() if game.date else None,
                tour=game.tour,
                season_id=game.season_id,
                season_name=get_localized_field(game.season, "name", lang) if game.season else None,
                home_team=home_team,
                away_team=away_team,
                player_team_id=ps.team_id,
                position=ps.position,
                minutes_played=ps.minutes_played,
                started=ps.started,
                goals=goals_by_game.get(game.id, 0),
                assists=assists_by_game.get(game.id, 0),
                shots=ps.shots or 0,
                shots_on_goal=ps.shots_on_goal or 0,
                shots_off_goal=ps.shots_off_goal or 0,
                passes=ps.passes or 0,
                pass_accuracy=float(ps.pass_accuracy) if ps.pass_accuracy is not None else None,
                duel=ps.duel or 0,
                tackle=ps.tackle or 0,
                corner=ps.corner or 0,
                offside=ps.offside or 0,
                foul=ps.foul or 0,
                yellow_cards=ps.yellow_cards or 0,
                red_cards=ps.red_cards or 0,
                extra_stats=ps.extra_stats or None,
            )
        )

    return PlayerMatchHistoryResponse(items=items, total=len(items))


@router.get("/{player_id}/tournaments", response_model=PlayerTournamentHistoryResponse)
async def get_player_tournament_history(
    player_id: int,
    lang: str = Query(default="kz", description="Language: kz, ru, or en"),
    db: AsyncSession = Depends(get_db),
):
    """
    Get player's tournament history (stats by season).
    Returns all seasons where the player has stats OR is in a roster.
    """
    # 1. Seasons with stats
    stats_result = await db.execute(
        select(PlayerSeasonStats)
        .where(PlayerSeasonStats.player_id == player_id)
        .options(
            selectinload(PlayerSeasonStats.season).selectinload(Season.championship),
            selectinload(PlayerSeasonStats.team),
        )
    )
    all_stats = stats_result.scalars().all()

    items = []
    stats_season_ids = set()

    for stat in all_stats:
        season = stat.season
        if season is None or not season.is_visible:
            continue
        stats_season_ids.add(stat.season_id)

        team_name = get_localized_field(stat.team, "name", lang) if stat.team else None
        season_name = get_localized_field(season, "name", lang)
        championship_name = (
            get_localized_field(season.championship, "name", lang)
            if season.championship else None
        )

        items.append(
            PlayerTournamentHistoryEntry(
                season_id=stat.season_id,
                season_name=season_name,
                championship_name=championship_name,
                frontend_code=season.frontend_code,
                season_year=season.date_start.year if season.date_start else None,
                team_id=stat.team_id,
                team_name=team_name,
                position=None,
                games_played=stat.games_played,
                games_starting=stat.games_starting,
                time_on_field_total=stat.time_on_field_total,
                goal=stat.goal,
                goal_pass=stat.goal_pass,
                shot=stat.shot,
                shots_on_goal=stat.shots_on_goal,
                passes=stat.passes,
                pass_ratio=float(stat.pass_ratio) if stat.pass_ratio is not None else None,
                key_pass=stat.key_pass,
                duel=stat.duel,
                duel_success=stat.duel_success,
                tackle=stat.tackle,
                interception=stat.interception,
                recovery=stat.recovery,
                dribble=stat.dribble,
                xg=float(stat.xg) if stat.xg is not None else None,
                xg_per_90=float(stat.xg_per_90) if stat.xg_per_90 is not None else None,
                corner=stat.corner,
                offside=stat.offside,
                foul=stat.foul,
                yellow_cards=stat.yellow_cards,
                red_cards=stat.red_cards,
                extra_stats=stat.extra_stats or None,
            )
        )

    # 2. Seasons from roster (PlayerTeam) without stats
    roster_query = (
        select(PlayerTeam)
        .where(
            PlayerTeam.player_id == player_id,
            PlayerTeam.is_active.is_(True),
            PlayerTeam.is_hidden.is_(False),
        )
        .options(
            selectinload(PlayerTeam.season).selectinload(Season.championship),
            selectinload(PlayerTeam.team),
        )
    )
    if stats_season_ids:
        roster_query = roster_query.where(
            PlayerTeam.season_id.not_in(stats_season_ids)
        )
    roster_result = await db.execute(roster_query)
    roster_entries = roster_result.scalars().all()

    # Deduplicate by season_id (player may be in multiple teams per season)
    seen_roster_seasons: set[int] = set()
    for pt in roster_entries:
        season = pt.season
        if season is None or not season.is_visible:
            continue
        if pt.season_id in seen_roster_seasons:
            continue
        seen_roster_seasons.add(pt.season_id)

        team_name = get_localized_field(pt.team, "name", lang) if pt.team else None
        season_name = get_localized_field(season, "name", lang)
        championship_name = (
            get_localized_field(season.championship, "name", lang)
            if season.championship else None
        )

        items.append(
            PlayerTournamentHistoryEntry(
                season_id=pt.season_id,
                season_name=season_name,
                championship_name=championship_name,
                frontend_code=season.frontend_code,
                season_year=season.date_start.year if season.date_start else None,
                team_id=pt.team_id,
                team_name=team_name,
                position=None,
            )
        )

    # Sort by season_id descending
    items.sort(key=lambda x: x.season_id, reverse=True)

    default_season_id = pick_default_season(
        (it.season_id, it.season_year, it.frontend_code) for it in items
    )

    return PlayerTournamentHistoryResponse(
        items=items,
        total=len(items),
        default_season_id=default_season_id,
    )
