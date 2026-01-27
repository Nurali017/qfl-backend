from uuid import UUID
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from sqlalchemy.orm import selectinload

from app.api.deps import get_db
from app.models import Player, PlayerTeam, Game, GamePlayerStats, PlayerSeasonStats, Team, Season
from app.schemas.player import (
    PlayerResponse,
    PlayerListResponse,
    PlayerDetailResponse,
    PlayerSeasonStatsResponse,
    PlayerTeammateResponse,
    PlayerTeammatesListResponse,
    PlayerTournamentHistoryEntry,
    PlayerTournamentHistoryResponse,
)
from app.schemas.game import GameResponse, GameListResponse
from app.schemas.team import TeamInGame
from app.config import get_settings
from app.utils.localization import get_localized_field, get_localized_name

settings = get_settings()

router = APIRouter(prefix="/players", tags=["players"])


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
    query = select(Player).options(selectinload(Player.country))

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
            "photo_url": p.photo_url,
            "age": p.age,
            "top_role": get_localized_field(p, "top_role", lang),
        })

    return {"items": items, "total": total}


@router.get("/{player_id}")
async def get_player(
    player_id: UUID,
    season_id: int | None = Query(default=None),
    lang: str = Query(default="kz", description="Language: kz, ru, or en"),
    db: AsyncSession = Depends(get_db),
):
    """Get player by ID."""
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
        teams = list(set(pt.team_id for pt in player.player_teams if pt.season_id == season_id))
    else:
        teams = list(set(pt.team_id for pt in player.player_teams))

    # Build country response
    country_data = None
    if player.country:
        country_data = {
            "id": player.country.id,
            "code": player.country.code,
            "name": get_localized_name(player.country, lang),
            "flag_url": player.country.flag_url,
        }

    return {
        "id": player.id,
        "first_name": get_localized_field(player, "first_name", lang),
        "last_name": get_localized_field(player, "last_name", lang),
        "birthday": player.birthday,
        "player_type": player.player_type,
        "country": country_data,
        "photo_url": player.photo_url,
        "age": player.age,
        "top_role": get_localized_field(player, "top_role", lang),
        "teams": teams,
    }


@router.get("/{player_id}/stats", response_model=PlayerSeasonStatsResponse)
async def get_player_stats(
    player_id: UUID,
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
    if season_id is None:
        season_id = settings.current_season_id

    # Get from player_season_stats table
    result = await db.execute(
        select(PlayerSeasonStats).where(
            PlayerSeasonStats.player_id == player_id,
            PlayerSeasonStats.season_id == season_id,
        )
    )
    stats = result.scalar_one_or_none()

    if not stats:
        raise HTTPException(
            status_code=404,
            detail="Stats not found. Run /sync/player-season-stats first.",
        )

    return PlayerSeasonStatsResponse.model_validate(stats)


@router.get("/{player_id}/games", response_model=GameListResponse)
async def get_player_games(
    player_id: UUID,
    season_id: int = Query(default=None),
    limit: int = Query(default=50, le=100),
    db: AsyncSession = Depends(get_db),
):
    """Get games a player participated in."""
    if season_id is None:
        season_id = settings.current_season_id

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
    games = result.scalars().all()

    items = []
    for g in games:
        home_team = None
        away_team = None
        if g.home_team:
            home_team = TeamInGame(
                id=g.home_team.id,
                name=g.home_team.name,
                logo_url=g.home_team.logo_url,
                score=g.home_score,
            )
        if g.away_team:
            away_team = TeamInGame(
                id=g.away_team.id,
                name=g.away_team.name,
                logo_url=g.away_team.logo_url,
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
                stadium=g.stadium,
                visitors=g.visitors,
                home_team=home_team,
                away_team=away_team,
                season_name=g.season.name if g.season else None,
            )
        )

    return GameListResponse(items=items, total=len(items))


@router.get("/{player_id}/teammates", response_model=PlayerTeammatesListResponse)
async def get_player_teammates(
    player_id: UUID,
    season_id: int = Query(default=None),
    limit: int = Query(default=10, le=50),
    lang: str = Query(default="kz", description="Language: kz, ru, or en"),
    db: AsyncSession = Depends(get_db),
):
    """
    Get teammates of a player from the same team in the current season.
    Excludes the player themselves from the result.
    """
    if season_id is None:
        season_id = settings.current_season_id

    # 1. Find player's team_id in the current season
    player_team_result = await db.execute(
        select(PlayerTeam).where(
            PlayerTeam.player_id == player_id,
            PlayerTeam.season_id == season_id,
        )
    )
    player_team = player_team_result.scalar_one_or_none()

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
                    position=get_localized_field(pt.player, "top_role", lang),
                    age=pt.player.age,
                    photo_url=pt.player.photo_url,
                )
            )

    return PlayerTeammatesListResponse(items=items, total=len(items))


@router.get("/{player_id}/tournaments", response_model=PlayerTournamentHistoryResponse)
async def get_player_tournament_history(
    player_id: UUID,
    lang: str = Query(default="kz", description="Language: kz, ru, or en"),
    db: AsyncSession = Depends(get_db),
):
    """
    Get player's tournament history (stats by season).
    Returns all seasons where the player has stats.
    """
    # Get all PlayerSeasonStats for this player
    stats_result = await db.execute(
        select(PlayerSeasonStats)
        .where(PlayerSeasonStats.player_id == player_id)
        .order_by(PlayerSeasonStats.season_id.desc())
    )
    all_stats = stats_result.scalars().all()

    if not all_stats:
        return PlayerTournamentHistoryResponse(items=[], total=0)

    # Get season and team info for each stat entry
    items = []
    for stat in all_stats:
        # Get season info
        season_result = await db.execute(
            select(Season)
            .where(Season.id == stat.season_id)
            .options(selectinload(Season.tournament))
        )
        season = season_result.scalar_one_or_none()

        # Get team info
        team_name = None
        if stat.team_id:
            team_result = await db.execute(
                select(Team).where(Team.id == stat.team_id)
            )
            team = team_result.scalar_one_or_none()
            if team:
                team_name = get_localized_field(team, "name", lang) if hasattr(team, "name_kz") else team.name

        tournament_name = None
        season_name = None
        if season:
            season_name = get_localized_field(season, "name", lang) if hasattr(season, "name_kz") else season.name
            if season.tournament:
                tournament_name = get_localized_field(season.tournament, "name", lang) if hasattr(season.tournament, "name_kz") else season.tournament.name

        items.append(
            PlayerTournamentHistoryEntry(
                season_id=stat.season_id,
                season_name=season_name,
                tournament_name=tournament_name,
                team_id=stat.team_id,
                team_name=team_name,
                position=None,  # Can be added later if needed
                games_played=stat.games_played,
                minutes_played=stat.minutes_played,
                goals=stat.goals,
                assists=stat.assists,
                yellow_cards=stat.yellow_cards,
                red_cards=stat.red_cards,
            )
        )

    return PlayerTournamentHistoryResponse(items=items, total=len(items))
