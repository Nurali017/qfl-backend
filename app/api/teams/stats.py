from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db
from app.models import (
    Game,
    TeamSeasonStats,
)
from app.schemas.team import TeamSeasonStatsResponse
from app.services.season_visibility import resolve_visible_season_id
from app.utils.error_messages import get_error_message

router = APIRouter(prefix="/teams", tags=["teams"])


@router.get("/{team_id}/stats", response_model=TeamSeasonStatsResponse)
async def get_team_stats(
    team_id: int,
    season_id: int = Query(default=None),
    lang: str = Query(default="kz", pattern="^(kz|ru|en)$"),
    db: AsyncSession = Depends(get_db),
):
    """Get team statistics for a season from local DB."""
    season_id = await resolve_visible_season_id(db, season_id)

    # Fetch from team_season_stats table
    result = await db.execute(
        select(TeamSeasonStats).where(
            TeamSeasonStats.team_id == team_id,
            TeamSeasonStats.season_id == season_id,
        )
    )
    stats = result.scalar_one_or_none()

    if not stats:
        raise HTTPException(status_code=404, detail=get_error_message("stats_not_found", lang))

    # Calculate goal_difference for response
    goal_diff = None
    if stats.goals_scored is not None and stats.goals_conceded is not None:
        goal_diff = stats.goals_scored - stats.goals_conceded

    # Calculate clean sheets from games (matches where opponent scored 0)
    clean_sheets = 0
    cs_query = (
        select(Game)
        .where(
            Game.season_id == season_id,
            or_(Game.home_team_id == team_id, Game.away_team_id == team_id),
            Game.home_score.is_not(None),
        )
    )
    cs_result = await db.execute(cs_query)
    cs_games = cs_result.scalars().all()
    for game in cs_games:
        if game.home_team_id == team_id and game.away_score == 0:
            clean_sheets += 1
        elif game.away_team_id == team_id and game.home_score == 0:
            clean_sheets += 1

    def _f(val):
        """Convert Decimal to float."""
        return float(val) if val is not None else None

    return TeamSeasonStatsResponse(
        team_id=stats.team_id,
        season_id=stats.season_id,
        games_played=stats.games_played,
        wins=stats.wins,
        draws=stats.draws,
        losses=stats.losses,
        goals_scored=stats.goals_scored,
        goals_conceded=stats.goals_conceded,
        goal_difference=goal_diff,
        points=stats.points,
        # xG
        xg=_f(stats.xg),
        xg_per_match=_f(stats.xg_per_match),
        opponent_xg=_f(stats.opponent_xg),
        # Shots
        shots=stats.shots,
        shots_on_goal=stats.shots_on_goal,
        shots_off_goal=stats.shots_off_goal,
        shot_per_match=_f(stats.shot_per_match),
        goal_to_shot_ratio=_f(stats.goal_to_shot_ratio),
        # Possession & Passes
        possession_avg=_f(stats.possession_avg),
        passes=stats.passes,
        pass_accuracy_avg=_f(stats.pass_accuracy_avg),
        pass_per_match=_f(stats.pass_per_match),
        pass_forward=stats.pass_forward,
        pass_long=stats.pass_long,
        pass_long_ratio=_f(stats.pass_long_ratio),
        pass_progressive=stats.pass_progressive,
        pass_cross=stats.pass_cross,
        pass_cross_ratio=_f(stats.pass_cross_ratio),
        pass_to_box=stats.pass_to_box,
        pass_to_3rd=stats.pass_to_3rd,
        key_pass=stats.key_pass,
        key_pass_per_match=_f(stats.key_pass_per_match),
        goal_pass=stats.goal_pass,
        # Defense
        tackle=stats.tackle,
        tackle_per_match=_f(stats.tackle_per_match),
        interception=stats.interception,
        interception_per_match=_f(stats.interception_per_match),
        recovery=stats.recovery,
        recovery_per_match=_f(stats.recovery_per_match),
        # Duels
        duel=stats.duel,
        duel_ratio=_f(stats.duel_ratio),
        aerial_duel_offence=stats.aerial_duel_offence,
        aerial_duel_offence_ratio=_f(stats.aerial_duel_offence_ratio),
        ground_duel_offence=stats.ground_duel_offence,
        ground_duel_offence_ratio=_f(stats.ground_duel_offence_ratio),
        # Dribbles
        dribble=stats.dribble,
        dribble_per_match=_f(stats.dribble_per_match),
        dribble_ratio=_f(stats.dribble_ratio),
        # Discipline
        fouls=stats.fouls,
        foul_taken=stats.foul_taken,
        yellow_cards=stats.yellow_cards,
        second_yellow_cards=stats.second_yellow_cards,
        red_cards=stats.red_cards,
        # Set pieces
        corners=stats.corners,
        corner_per_match=_f(stats.corner_per_match),
        offsides=stats.offsides,
        # Penalty
        penalty=stats.penalty,
        penalty_ratio=_f(stats.penalty_ratio),
        # Other
        clean_sheets=clean_sheets,
        extra_stats=stats.extra_stats,
    )
