"""Game statistics endpoint."""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from sqlalchemy.orm import selectinload

from app.api.deps import get_db
from app.models import (
    Game, GameTeamStats, GamePlayerStats, GameEvent, GameEventType, Player,
)
from app.utils.numbers import to_finite_float
from app.utils.team_logo_fallback import resolve_team_logo_url
from app.utils.game_grouping import get_player_names_fallback

router = APIRouter(prefix="/games", tags=["games"])


@router.get("/{game_id}/stats")
async def get_game_stats(game_id: int, db: AsyncSession = Depends(get_db)):
    """Get statistics for a game."""
    # Early return for technical wins — no real stats exist
    tech_result = await db.execute(
        select(Game.is_technical).where(Game.id == game_id)
    )
    is_technical = tech_result.scalar()
    if is_technical is None:
        raise HTTPException(status_code=404, detail="Game not found")
    if is_technical:
        return {
            "game_id": game_id,
            "is_technical": True,
            "team_stats": [],
            "player_stats": [],
            "events": [],
        }

    # Get team stats
    team_stats_result = await db.execute(
        select(GameTeamStats)
        .where(GameTeamStats.game_id == game_id)
        .options(selectinload(GameTeamStats.team))
    )
    team_stats = team_stats_result.scalars().all()

    team_stats_response = []
    for ts in team_stats:
        team_stats_response.append({
            "team_id": ts.team_id,
            "team_name": ts.team.name if ts.team else None,
            "logo_url": resolve_team_logo_url(ts.team),
            "primary_color": ts.team.primary_color if ts.team else None,
            "secondary_color": ts.team.secondary_color if ts.team else None,
            "accent_color": ts.team.accent_color if ts.team else None,
            "possession": to_finite_float(ts.possession),
            "possession_percent": ts.possession_percent,
            "shots": ts.shots,
            "shots_on_goal": ts.shots_on_goal,
            "passes": ts.passes,
            "pass_accuracy": to_finite_float(ts.pass_accuracy),
            "fouls": ts.fouls,
            "yellow_cards": ts.yellow_cards,
            "red_cards": ts.red_cards,
            "corners": ts.corners,
            "offsides": ts.offsides,
            "extra_stats": ts.extra_stats,
        })

    # Get goals and assists from game_events (single source of truth)
    goals_result = await db.execute(
        select(GameEvent.player_id, func.count(GameEvent.id).label("count"))
        .where(GameEvent.game_id == game_id, GameEvent.event_type == GameEventType.goal)
        .group_by(GameEvent.player_id)
    )
    player_goals = {row.player_id: row.count for row in goals_result}

    assists_result = await db.execute(
        select(GameEvent.player_id, func.count(GameEvent.id).label("count"))
        .where(GameEvent.game_id == game_id, GameEvent.event_type == GameEventType.assist)
        .group_by(GameEvent.player_id)
    )
    player_assists = {row.player_id: row.count for row in assists_result}

    # Get player stats
    player_stats_result = await db.execute(
        select(GamePlayerStats)
        .where(GamePlayerStats.game_id == game_id)
        .options(
            selectinload(GamePlayerStats.player).selectinload(Player.country),
            selectinload(GamePlayerStats.team),
        )
        .order_by(GamePlayerStats.team_id, GamePlayerStats.started.desc())
    )
    player_stats = player_stats_result.scalars().all()

    # Get fallback names from GameEvent
    player_ids = [ps.player_id for ps in player_stats]
    fallback_names = await get_player_names_fallback(db, game_id, player_ids)

    player_stats_response = []
    for ps in player_stats:
        # Determine names with fallback
        if ps.player and ps.player.first_name and ps.player.last_name:
            first_name = ps.player.first_name
            last_name = ps.player.last_name
        elif ps.player_id in fallback_names:
            first_name, last_name = fallback_names[ps.player_id]
        else:
            first_name = ps.player.first_name if ps.player else None
            last_name = ps.player.last_name if ps.player else None

        # Build country data
        country_data = None
        if ps.player and ps.player.country:
            country_data = {
                "id": ps.player.country.id,
                "code": ps.player.country.code,
                "name": ps.player.country.name,
                "flag_url": ps.player.country.flag_url,
            }

        player_stats_response.append({
            "player_id": ps.player_id,
            "first_name": first_name,
            "last_name": last_name,
            "country": country_data,
            "team_id": ps.team_id,
            "team_name": ps.team.name if ps.team else None,
            "team_primary_color": ps.team.primary_color if ps.team else None,
            "team_secondary_color": ps.team.secondary_color if ps.team else None,
            "team_accent_color": ps.team.accent_color if ps.team else None,
            "position": ps.position,
            "minutes_played": ps.minutes_played,
            "started": ps.started,
            "goals": player_goals.get(ps.player_id, 0),
            "assists": player_assists.get(ps.player_id, 0),
            "shots": ps.shots,
            "passes": ps.passes,
            "pass_accuracy": to_finite_float(ps.pass_accuracy),
            "yellow_cards": ps.yellow_cards,
            "red_cards": ps.red_cards,
            "extra_stats": ps.extra_stats,
        })

    # Get game events
    events_result = await db.execute(
        select(GameEvent)
        .where(GameEvent.game_id == game_id)
        .order_by(GameEvent.half, GameEvent.minute)
    )
    events = events_result.scalars().all()

    events_response = []
    for e in events:
        events_response.append({
            "id": e.id,
            "half": e.half,
            "minute": e.minute,
            "event_type": e.event_type.value,
            "team_id": e.team_id,
            "team_name": e.team_name,
            "player_id": e.player_id,
            "player_name": e.player_name,
            "player_number": e.player_number,
            "player2_id": e.player2_id,
            "player2_name": e.player2_name,
            "player2_number": e.player2_number,
        })

    return {
        "game_id": game_id,
        "is_technical": False,
        "team_stats": team_stats_response,
        "player_stats": player_stats_response,
        "events": events_response,
    }
