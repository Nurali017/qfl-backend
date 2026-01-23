from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, or_
from sqlalchemy.orm import selectinload

from app.api.deps import get_db
from app.models import Team, Player, PlayerTeam, Game, GamePlayerStats, TeamSeasonStats, ScoreTable
from app.schemas.team import TeamResponse, TeamListResponse, TeamDetailResponse, TeamSeasonStatsResponse
from app.schemas.player import PlayerWithTeamResponse
from app.schemas.game import GameResponse, GameListResponse
from app.schemas.team import TeamInGame
from app.schemas.head_to_head import (
    HeadToHeadResponse,
    H2HOverallStats,
    FormGuide,
    FormGuideMatch,
    SeasonTableEntry,
    PreviousMeeting,
)
from app.config import get_settings
from app.utils.localization import get_localized_name, get_localized_city, get_localized_field

settings = get_settings()

router = APIRouter(prefix="/teams", tags=["teams"])


@router.get("")
async def get_teams(
    season_id: int | None = None,
    lang: str = Query(default="kz", description="Language: kz, ru, or en"),
    db: AsyncSession = Depends(get_db),
):
    """Get all teams, optionally filtered by season."""
    if season_id:
        # Get teams that have players in this season
        subquery = (
            select(PlayerTeam.team_id)
            .where(PlayerTeam.season_id == season_id)
            .distinct()
        )
        query = select(Team).where(Team.id.in_(subquery))
    else:
        query = select(Team)

    query = query.order_by(Team.name)
    result = await db.execute(query)
    teams = result.scalars().all()

    items = []
    for t in teams:
        items.append({
            "id": t.id,
            "name": get_localized_name(t, lang),
            "logo_url": t.logo_url,
            "primary_color": t.primary_color,
            "secondary_color": t.secondary_color,
            "accent_color": t.accent_color,
        })

    return {"items": items, "total": len(items)}


@router.get("/{team_id}")
async def get_team(
    team_id: int,
    lang: str = Query(default="kz", description="Language: kz, ru, or en"),
    db: AsyncSession = Depends(get_db),
):
    """Get team by ID."""
    result = await db.execute(select(Team).where(Team.id == team_id))
    team = result.scalar_one_or_none()

    if not team:
        raise HTTPException(status_code=404, detail="Team not found")

    return {
        "id": team.id,
        "name": get_localized_name(team, lang),
        "city": get_localized_city(team, lang),
        "logo_url": team.logo_url,
        "primary_color": team.primary_color,
        "secondary_color": team.secondary_color,
        "accent_color": team.accent_color,
    }


@router.get("/{team_id}/players")
async def get_team_players(
    team_id: int,
    season_id: int = Query(default=None),
    lang: str = Query(default="kz", description="Language: kz, ru, or en"),
    db: AsyncSession = Depends(get_db),
):
    """Get players for a team in a specific season."""
    if season_id is None:
        season_id = settings.current_season_id

    result = await db.execute(
        select(PlayerTeam)
        .where(PlayerTeam.team_id == team_id, PlayerTeam.season_id == season_id)
        .options(
            selectinload(PlayerTeam.player).selectinload(Player.country)
        )
    )
    player_teams = result.scalars().all()

    items = []
    for pt in player_teams:
        p = pt.player
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
            "top_role": p.top_role,
            "team_id": pt.team_id,
            "number": pt.number,
        })

    return {"items": items, "total": len(items)}


@router.get("/{team_id}/games")
async def get_team_games(
    team_id: int,
    season_id: int = Query(default=None),
    lang: str = Query(default="kz", description="Language: kz, ru, or en"),
    db: AsyncSession = Depends(get_db),
):
    """Get games for a team."""
    if season_id is None:
        season_id = settings.current_season_id

    query = (
        select(Game)
        .where(
            Game.season_id == season_id,
            or_(Game.home_team_id == team_id, Game.away_team_id == team_id),
        )
        .options(
            selectinload(Game.home_team),
            selectinload(Game.away_team),
            selectinload(Game.season),
        )
        .order_by(Game.date.desc())
    )

    result = await db.execute(query)
    games = result.scalars().all()

    items = []
    for g in games:
        home_team = None
        away_team = None
        if g.home_team:
            home_team = {
                "id": g.home_team.id,
                "name": get_localized_name(g.home_team, lang),
                "logo_url": g.home_team.logo_url,
                "score": g.home_score,
            }
        if g.away_team:
            away_team = {
                "id": g.away_team.id,
                "name": get_localized_name(g.away_team, lang),
                "logo_url": g.away_team.logo_url,
                "score": g.away_score,
            }

        items.append({
            "id": g.id,
            "date": g.date.isoformat() if g.date else None,
            "time": g.time.isoformat() if g.time else None,
            "tour": g.tour,
            "season_id": g.season_id,
            "home_score": g.home_score,
            "away_score": g.away_score,
            "has_stats": g.has_stats,
            "stadium": g.stadium,
            "visitors": g.visitors,
            "home_team": home_team,
            "away_team": away_team,
            "season_name": get_localized_name(g.season, lang) if g.season else None,
        })

    return {"items": items, "total": len(items)}


@router.get("/{team_id}/stats", response_model=TeamSeasonStatsResponse)
async def get_team_stats(
    team_id: int,
    season_id: int = Query(default=None),
    db: AsyncSession = Depends(get_db),
):
    """Get team statistics for a season from local DB."""
    if season_id is None:
        season_id = settings.current_season_id

    # Fetch from team_season_stats table
    result = await db.execute(
        select(TeamSeasonStats).where(
            TeamSeasonStats.team_id == team_id,
            TeamSeasonStats.season_id == season_id,
        )
    )
    stats = result.scalar_one_or_none()

    if not stats:
        raise HTTPException(status_code=404, detail="Stats not found. Run sync first.")

    # Calculate goal_difference for response
    goal_diff = None
    if stats.goals_scored is not None and stats.goals_conceded is not None:
        goal_diff = stats.goals_scored - stats.goals_conceded

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
        shots=stats.shots,
        shots_on_goal=stats.shots_on_goal,
        possession_avg=float(stats.possession_avg) if stats.possession_avg else None,
        passes=stats.passes,
        pass_accuracy_avg=float(stats.pass_accuracy_avg) if stats.pass_accuracy_avg else None,
        fouls=stats.fouls,
        yellow_cards=stats.yellow_cards,
        red_cards=stats.red_cards,
        corners=stats.corners,
        offsides=stats.offsides,
        extra_stats=stats.extra_stats,
    )


@router.get("/{team1_id}/vs/{team2_id}/head-to-head")
async def get_head_to_head(
    team1_id: int,
    team2_id: int,
    season_id: int = Query(default=None),
    lang: str = Query(default="ru", description="Language: kz, ru, or en"),
    db: AsyncSession = Depends(get_db),
):
    """
    Get comprehensive head-to-head statistics between two teams.

    Returns:
    - Overall H2H stats (all-time wins/draws/losses)
    - Form guide (last 5 matches for each team in current season)
    - Season table positions
    - Previous meetings between the two teams
    """
    if season_id is None:
        season_id = settings.current_season_id

    # Validate teams exist
    team1_result = await db.execute(select(Team).where(Team.id == team1_id))
    team1 = team1_result.scalar_one_or_none()

    team2_result = await db.execute(select(Team).where(Team.id == team2_id))
    team2 = team2_result.scalar_one_or_none()

    if not team1 or not team2:
        raise HTTPException(status_code=404, detail="One or both teams not found")

    # 1. OVERALL H2H STATS (all seasons)
    overall_query = (
        select(Game)
        .where(
            or_(
                (Game.home_team_id == team1_id) & (Game.away_team_id == team2_id),
                (Game.home_team_id == team2_id) & (Game.away_team_id == team1_id)
            ),
            Game.home_score.is_not(None),  # Only finished matches
            Game.away_score.is_not(None),
        )
    )
    overall_result = await db.execute(overall_query)
    all_h2h_games = overall_result.scalars().all()

    team1_wins = 0
    team2_wins = 0
    draws = 0
    team1_goals = 0
    team2_goals = 0

    for game in all_h2h_games:
        if game.home_team_id == team1_id:
            team1_goals += game.home_score or 0
            team2_goals += game.away_score or 0
            if game.home_score > game.away_score:
                team1_wins += 1
            elif game.home_score < game.away_score:
                team2_wins += 1
            else:
                draws += 1
        else:
            team1_goals += game.away_score or 0
            team2_goals += game.home_score or 0
            if game.away_score > game.home_score:
                team1_wins += 1
            elif game.away_score < game.home_score:
                team2_wins += 1
            else:
                draws += 1

    overall_stats = H2HOverallStats(
        total_matches=len(all_h2h_games),
        team1_wins=team1_wins,
        draws=draws,
        team2_wins=team2_wins,
        team1_goals=team1_goals,
        team2_goals=team2_goals,
    )

    # 2. FORM GUIDE (last 5 matches in current season)
    async def get_team_form(team_id: int) -> FormGuide:
        form_query = (
            select(Game)
            .where(
                Game.season_id == season_id,
                or_(Game.home_team_id == team_id, Game.away_team_id == team_id),
                Game.home_score.is_not(None),
            )
            .options(
                selectinload(Game.home_team),
                selectinload(Game.away_team),
            )
            .order_by(Game.date.desc())
            .limit(5)
        )
        form_result = await db.execute(form_query)
        recent_games = form_result.scalars().all()

        matches = []
        for game in recent_games:
            is_home = game.home_team_id == team_id
            opponent = game.away_team if is_home else game.home_team
            team_score = game.home_score if is_home else game.away_score
            opponent_score = game.away_score if is_home else game.home_score

            if team_score > opponent_score:
                result = "W"
            elif team_score < opponent_score:
                result = "L"
            else:
                result = "D"

            matches.append(FormGuideMatch(
                game_id=str(game.id),
                date=game.date,
                result=result,
                opponent_id=opponent.id,
                opponent_name=get_localized_name(opponent, lang),
                opponent_logo_url=opponent.logo_url,
                home_score=game.home_score,
                away_score=game.away_score,
                was_home=is_home,
            ))

        team_obj = team1 if team_id == team1_id else team2
        return FormGuide(
            team_id=team_id,
            team_name=get_localized_name(team_obj, lang),
            matches=matches,
        )

    form_team1 = await get_team_form(team1_id)
    form_team2 = await get_team_form(team2_id)

    # 3. SEASON TABLE (from ScoreTable)
    table_query = (
        select(ScoreTable)
        .where(ScoreTable.season_id == season_id)
        .options(selectinload(ScoreTable.team))
        .order_by(ScoreTable.position.asc())
    )
    table_result = await db.execute(table_query)
    table_entries = table_result.scalars().all()

    season_table = []
    for entry in table_entries:
        # Calculate clean sheets from games
        clean_sheets = 0
        games_query = (
            select(Game)
            .where(
                Game.season_id == season_id,
                or_(
                    Game.home_team_id == entry.team_id,
                    Game.away_team_id == entry.team_id
                ),
                Game.home_score.is_not(None),
            )
        )
        games_result = await db.execute(games_query)
        team_games = games_result.scalars().all()

        for game in team_games:
            if game.home_team_id == entry.team_id and game.away_score == 0:
                clean_sheets += 1
            elif game.away_team_id == entry.team_id and game.home_score == 0:
                clean_sheets += 1

        season_table.append(SeasonTableEntry(
            position=entry.position,
            team_id=entry.team_id,
            team_name=get_localized_name(entry.team, lang),
            logo_url=entry.team.logo_url if entry.team else None,
            games_played=entry.games or 0,
            wins=entry.wins or 0,
            draws=entry.draws or 0,
            losses=entry.losses or 0,
            goals_scored=entry.goals_scored or 0,
            goals_conceded=entry.goals_conceded or 0,
            goal_difference=(entry.goals_scored or 0) - (entry.goals_conceded or 0),
            points=entry.points or 0,
            clean_sheets=clean_sheets,
        ))

    # 4. PREVIOUS MEETINGS (most recent first)
    prev_meetings_query = (
        select(Game)
        .where(
            or_(
                (Game.home_team_id == team1_id) & (Game.away_team_id == team2_id),
                (Game.home_team_id == team2_id) & (Game.away_team_id == team1_id)
            ),
            Game.home_score.is_not(None),
        )
        .options(
            selectinload(Game.home_team),
            selectinload(Game.away_team),
            selectinload(Game.season),
        )
        .order_by(Game.date.desc())
        .limit(10)  # Last 10 meetings
    )
    prev_meetings_result = await db.execute(prev_meetings_query)
    prev_games = prev_meetings_result.scalars().all()

    previous_meetings = []
    for game in prev_games:
        previous_meetings.append(PreviousMeeting(
            game_id=str(game.id),
            date=game.date,
            home_team_id=game.home_team_id,
            home_team_name=get_localized_name(game.home_team, lang),
            away_team_id=game.away_team_id,
            away_team_name=get_localized_name(game.away_team, lang),
            home_score=game.home_score,
            away_score=game.away_score,
            tour=game.tour,
            season_name=get_localized_field(game.season, "name", lang) if game.season else None,
        ))

    return HeadToHeadResponse(
        team1_id=team1_id,
        team1_name=get_localized_name(team1, lang),
        team2_id=team2_id,
        team2_name=get_localized_name(team2, lang),
        season_id=season_id,
        overall=overall_stats,
        form_guide={
            "team1": form_team1,
            "team2": form_team2,
        },
        season_table=season_table,
        previous_meetings=previous_meetings,
    )
