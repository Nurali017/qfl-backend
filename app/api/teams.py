from collections import defaultdict
from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.api.deps import get_db
from app.models import (
    Game,
    GamePlayerStats,
    Player,
    PlayerSeasonStats,
    PlayerTeam,
    ScoreTable,
    Season,
    Team,
    TeamSeasonStats,
)
from app.models.coach import Coach, TeamCoach
from app.schemas.team import (
    TeamDetailResponse,
    TeamListResponse,
    TeamOverviewCoachPreview,
    TeamOverviewFormEntry,
    TeamOverviewLeaderPlayer,
    TeamOverviewLeaders,
    TeamOverviewMatch,
    TeamOverviewMatchTeam,
    TeamOverviewMiniLeaders,
    TeamOverviewResponse,
    TeamOverviewSeason,
    TeamOverviewStadium,
    TeamOverviewStandingEntry,
    TeamOverviewSummary,
    TeamOverviewTeam,
    TeamResponse,
    TeamSeasonStatsResponse,
)
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
from app.services.season_participants import resolve_season_participants
from app.utils.localization import get_localized_name, get_localized_city, get_localized_field
from app.utils.error_messages import get_error_message

settings = get_settings()

router = APIRouter(prefix="/teams", tags=["teams"])


def _safe_int(value: int | float | None) -> int:
    return int(value) if value is not None else 0


def _match_status(game: Game) -> str:
    if game.is_live:
        return "live"
    if game.home_score is not None and game.away_score is not None:
        return "finished"
    return "upcoming"


def _build_overview_match(game: Game, lang: str) -> TeamOverviewMatch:
    home_team = game.home_team
    away_team = game.away_team

    if game.stadium_rel:
        stadium = TeamOverviewStadium(
            name=get_localized_name(game.stadium_rel, lang),
            city=get_localized_city(game.stadium_rel, lang) if hasattr(game.stadium_rel, "city") else None,
        )
    elif game.stadium:
        stadium = TeamOverviewStadium(name=game.stadium, city=None)
    else:
        stadium = None

    return TeamOverviewMatch(
        id=str(game.id),
        date=game.date,
        time=game.time,
        tour=game.tour,
        status=_match_status(game),
        home_score=game.home_score,
        away_score=game.away_score,
        has_stats=bool(game.has_stats),
        has_lineup=bool(game.has_lineup),
        home_team=TeamOverviewMatchTeam(
            id=home_team.id if home_team else (game.home_team_id or 0),
            name=get_localized_name(home_team, lang) if home_team else "—",
            logo_url=home_team.logo_url if home_team else None,
        ),
        away_team=TeamOverviewMatchTeam(
            id=away_team.id if away_team else (game.away_team_id or 0),
            name=get_localized_name(away_team, lang) if away_team else "—",
            logo_url=away_team.logo_url if away_team else None,
        ),
        stadium=stadium,
    )


def _compute_summary_from_games(team_id: int, finished_games: list[Game]) -> TeamOverviewSummary:
    wins = draws = losses = goals_scored = goals_conceded = points = 0

    for game in finished_games:
        is_home = game.home_team_id == team_id
        team_score = _safe_int(game.home_score if is_home else game.away_score)
        opp_score = _safe_int(game.away_score if is_home else game.home_score)
        goals_scored += team_score
        goals_conceded += opp_score

        if team_score > opp_score:
            wins += 1
            points += 3
        elif team_score < opp_score:
            losses += 1
        else:
            draws += 1
            points += 1

    return TeamOverviewSummary(
        games_played=len(finished_games),
        wins=wins,
        draws=draws,
        losses=losses,
        goals_scored=goals_scored,
        goals_conceded=goals_conceded,
        goal_difference=goals_scored - goals_conceded,
        points=points,
    )


def _window_around_team(
    entries: list[TeamOverviewStandingEntry],
    team_id: int,
    window: int = 5,
) -> list[TeamOverviewStandingEntry]:
    if not entries:
        return []

    idx = next((i for i, item in enumerate(entries) if item.team_id == team_id), -1)
    if idx < 0:
        return entries[:window]

    start = max(0, idx - 2)
    end = min(len(entries), start + window)
    start = max(0, end - window)
    return entries[start:end]


@router.get("")
async def get_teams(
    season_id: int | None = None,
    lang: str = Query(default="kz", description="Language: kz, ru, or en"),
    db: AsyncSession = Depends(get_db),
):
    """Get all teams, optionally filtered by season."""
    if season_id:
        participants = await resolve_season_participants(db, season_id, lang)
        if not participants:
            raise HTTPException(
                status_code=409,
                detail=get_error_message("season_teams_not_configured", lang),
            )
        items = []
        for participant in participants:
            team = participant.team
            items.append({
                "id": team.id,
                "name": get_localized_name(team, lang),
                "logo_url": team.logo_url,
                "primary_color": team.primary_color,
                "secondary_color": team.secondary_color,
                "accent_color": team.accent_color,
            })
        return {"items": items, "total": len(items)}

    result = await db.execute(select(Team).order_by(Team.name))
    teams = result.scalars().all()
    items = [
        {
            "id": t.id,
            "name": get_localized_name(t, lang),
            "logo_url": t.logo_url,
            "primary_color": t.primary_color,
            "secondary_color": t.secondary_color,
            "accent_color": t.accent_color,
        }
        for t in teams
    ]
    return {"items": items, "total": len(items)}


@router.get("/{team_id}")
async def get_team(
    team_id: int,
    lang: str = Query(default="kz", description="Language: kz, ru, or en"),
    db: AsyncSession = Depends(get_db),
):
    """Get team by ID."""
    result = await db.execute(
        select(Team)
        .where(Team.id == team_id)
        .options(
            selectinload(Team.stadium),
            selectinload(Team.club),
        )
    )
    team = result.scalar_one_or_none()

    if not team:
        raise HTTPException(status_code=404, detail=get_error_message("team_not_found", lang))

    stadium_data = None
    if team.stadium:
        stadium_data = {
            "name": get_localized_name(team.stadium, lang),
            "city": get_localized_city(team.stadium, lang) if hasattr(team.stadium, 'city') else None,
        }

    return {
        "id": team.id,
        "name": get_localized_name(team, lang),
        "city": get_localized_city(team, lang),
        "logo_url": team.logo_url,
        "primary_color": team.primary_color,
        "secondary_color": team.secondary_color,
        "accent_color": team.accent_color,
        "website": team.website,
        "stadium": stadium_data,
        "club_id": team.club_id,
        "club_name": get_localized_field(team.club, "name", lang) if team.club else None,
    }


@router.get("/{team_id}/overview", response_model=TeamOverviewResponse)
async def get_team_overview(
    team_id: int,
    season_id: int | None = Query(default=None),
    fixtures_limit: int = Query(default=5, ge=1, le=10),
    leaders_limit: int = Query(default=8, ge=3, le=20),
    lang: str = Query(default="ru", pattern="^(kz|ru|en)$"),
    db: AsyncSession = Depends(get_db),
):
    """Get aggregated team overview data for the team page."""
    if season_id is None:
        season_id = settings.current_season_id

    team_result = await db.execute(
        select(Team)
        .where(Team.id == team_id)
        .options(selectinload(Team.stadium))
    )
    team = team_result.scalar_one_or_none()
    if not team:
        raise HTTPException(status_code=404, detail=get_error_message("team_not_found", lang))

    season_result = await db.execute(select(Season).where(Season.id == season_id))
    season = season_result.scalar_one_or_none()

    games_result = await db.execute(
        select(Game)
        .where(
            Game.season_id == season_id,
            or_(Game.home_team_id == team_id, Game.away_team_id == team_id),
        )
        .options(
            selectinload(Game.home_team),
            selectinload(Game.away_team),
            selectinload(Game.stadium_rel),
        )
        .order_by(Game.date.desc(), Game.time.desc())
    )
    team_games = games_result.scalars().all()

    finished_games = [
        game for game in team_games if game.home_score is not None and game.away_score is not None
    ]
    upcoming_games = sorted(
        [game for game in team_games if game.home_score is None or game.away_score is None],
        key=lambda game: (game.date, game.time.isoformat() if game.time else ""),
    )

    stats_result = await db.execute(
        select(TeamSeasonStats).where(
            TeamSeasonStats.team_id == team_id,
            TeamSeasonStats.season_id == season_id,
        )
    )
    stats = stats_result.scalar_one_or_none()

    if stats:
        goals_scored = _safe_int(stats.goals_scored)
        goals_conceded = _safe_int(stats.goals_conceded)
        goal_difference = (
            _safe_int(stats.goals_difference)
            if stats.goals_difference is not None
            else goals_scored - goals_conceded
        )
        summary = TeamOverviewSummary(
            games_played=_safe_int(stats.games_played),
            wins=_safe_int(stats.wins),
            draws=_safe_int(stats.draws),
            losses=_safe_int(stats.losses),
            goals_scored=goals_scored,
            goals_conceded=goals_conceded,
            goal_difference=goal_difference,
            points=_safe_int(stats.points),
        )
    else:
        summary = _compute_summary_from_games(team_id, finished_games)

    recent_match = _build_overview_match(finished_games[0], lang) if finished_games else None

    form_last5: list[TeamOverviewFormEntry] = []
    for game in finished_games[:5]:
        is_home = game.home_team_id == team_id
        opponent = game.away_team if is_home else game.home_team
        team_score = _safe_int(game.home_score if is_home else game.away_score)
        opponent_score = _safe_int(game.away_score if is_home else game.home_score)
        if team_score > opponent_score:
            result = "W"
        elif team_score < opponent_score:
            result = "L"
        else:
            result = "D"

        form_last5.append(
            TeamOverviewFormEntry(
                game_id=str(game.id),
                is_home=is_home,
                opponent_name=get_localized_name(opponent, lang),
                opponent_logo=opponent.logo_url,
                team_score=team_score,
                opponent_score=opponent_score,
                result=result,
            )
        )

    upcoming_matches = [
        _build_overview_match(game, lang) for game in upcoming_games[:fixtures_limit]
    ]

    # Standings window: score_table first, then fallback to finished games.
    score_table_result = await db.execute(
        select(ScoreTable)
        .where(ScoreTable.season_id == season_id)
        .options(selectinload(ScoreTable.team))
        .order_by(ScoreTable.position.asc())
    )
    score_table_entries = score_table_result.scalars().all()

    standings: list[TeamOverviewStandingEntry] = []
    if score_table_entries:
        for entry in score_table_entries:
            standings.append(
                TeamOverviewStandingEntry(
                    position=entry.position or 0,
                    team_id=entry.team_id,
                    team_name=get_localized_name(entry.team, lang),
                    team_logo=entry.team.logo_url if entry.team else None,
                    games_played=_safe_int(entry.games_played),
                    points=_safe_int(entry.points),
                    goal_difference=_safe_int(entry.goal_difference),
                    goals_scored=_safe_int(entry.goals_scored),
                    goals_conceded=_safe_int(entry.goals_conceded),
                )
            )
    else:
        season_games_result = await db.execute(
            select(Game)
            .where(
                Game.season_id == season_id,
                Game.home_score.is_not(None),
                Game.away_score.is_not(None),
            )
        )
        season_games = season_games_result.scalars().all()

        bucket: dict[int, dict] = {}
        for game in season_games:
            home_id = game.home_team_id
            away_id = game.away_team_id
            home_score = _safe_int(game.home_score)
            away_score = _safe_int(game.away_score)

            for current_team_id in (home_id, away_id):
                bucket.setdefault(
                    current_team_id,
                    {
                        "games_played": 0,
                        "wins": 0,
                        "draws": 0,
                        "losses": 0,
                        "goals_scored": 0,
                        "goals_conceded": 0,
                        "points": 0,
                    },
                )

            bucket[home_id]["games_played"] += 1
            bucket[away_id]["games_played"] += 1
            bucket[home_id]["goals_scored"] += home_score
            bucket[home_id]["goals_conceded"] += away_score
            bucket[away_id]["goals_scored"] += away_score
            bucket[away_id]["goals_conceded"] += home_score

            if home_score > away_score:
                bucket[home_id]["wins"] += 1
                bucket[home_id]["points"] += 3
                bucket[away_id]["losses"] += 1
            elif away_score > home_score:
                bucket[away_id]["wins"] += 1
                bucket[away_id]["points"] += 3
                bucket[home_id]["losses"] += 1
            else:
                bucket[home_id]["draws"] += 1
                bucket[away_id]["draws"] += 1
                bucket[home_id]["points"] += 1
                bucket[away_id]["points"] += 1

        if bucket:
            teams_result = await db.execute(select(Team).where(Team.id.in_(list(bucket.keys()))))
            teams = {item.id: item for item in teams_result.scalars().all()}

            sorted_rows = sorted(
                bucket.items(),
                key=lambda item: (
                    -(item[1]["points"]),
                    -(item[1]["goals_scored"] - item[1]["goals_conceded"]),
                    -(item[1]["goals_scored"]),
                ),
            )

            for position, (current_team_id, values) in enumerate(sorted_rows, start=1):
                current_team = teams.get(current_team_id)
                standings.append(
                    TeamOverviewStandingEntry(
                        position=position,
                        team_id=current_team_id,
                        team_name=get_localized_name(current_team, lang) if current_team else str(current_team_id),
                        team_logo=current_team.logo_url if current_team else None,
                        games_played=values["games_played"],
                        points=values["points"],
                        goal_difference=values["goals_scored"] - values["goals_conceded"],
                        goals_scored=values["goals_scored"],
                        goals_conceded=values["goals_conceded"],
                    )
                )

    standings_window = _window_around_team(standings, team_id, window=5)

    players_result = await db.execute(
        select(PlayerSeasonStats, Player, Team)
        .join(Player, PlayerSeasonStats.player_id == Player.id)
        .outerjoin(Team, PlayerSeasonStats.team_id == Team.id)
        .where(
            PlayerSeasonStats.season_id == season_id,
            PlayerSeasonStats.team_id == team_id,
        )
    )
    player_rows = players_result.all()

    players: list[TeamOverviewLeaderPlayer] = []
    for row_stats, row_player, row_team in player_rows:
        players.append(
            TeamOverviewLeaderPlayer(
                player_id=row_player.id,
                first_name=get_localized_field(row_player, "first_name", lang),
                last_name=get_localized_field(row_player, "last_name", lang),
                photo_url=row_player.photo_url,
                team_id=row_team.id if row_team else row_stats.team_id,
                team_name=get_localized_name(row_team, lang) if row_team else None,
                team_logo=row_team.logo_url if row_team else None,
                position=get_localized_field(row_player, "top_role", lang),
                games_played=_safe_int(row_stats.games_played),
                goals=_safe_int(row_stats.goals),
                assists=_safe_int(row_stats.assists),
                passes=_safe_int(row_stats.passes),
                save_shot=_safe_int(row_stats.save_shot),
                dry_match=_safe_int(row_stats.dry_match),
                red_cards=_safe_int(row_stats.red_cards),
            )
        )

    def sort_players(items: list[TeamOverviewLeaderPlayer], field: str) -> list[TeamOverviewLeaderPlayer]:
        return sorted(
            items,
            key=lambda item: (
                getattr(item, field) or 0,
                item.games_played,
                item.last_name or "",
                item.first_name or "",
            ),
            reverse=True,
        )

    goals_table = sort_players(players, "goals")[:leaders_limit]
    assists_table = sort_players(players, "assists")[:leaders_limit]
    leaders = TeamOverviewLeaders(
        top_scorer=goals_table[0] if goals_table else None,
        top_assister=assists_table[0] if assists_table else None,
        goals_table=goals_table,
        assists_table=assists_table,
        mini_leaders=TeamOverviewMiniLeaders(
            passes=sort_players(players, "passes")[0] if players else None,
            appearances=sort_players(players, "games_played")[0] if players else None,
            saves=sort_players(players, "save_shot")[0] if players else None,
            clean_sheets=sort_players(players, "dry_match")[0] if players else None,
            red_cards=sort_players(players, "red_cards")[0] if players else None,
        ),
    )

    coaches_result = await db.execute(
        select(TeamCoach)
        .where(
            TeamCoach.team_id == team_id,
            TeamCoach.season_id == season_id,
            TeamCoach.is_active == True,
        )
        .options(selectinload(TeamCoach.coach).selectinload(Coach.country))
    )
    team_coaches = coaches_result.scalars().all()
    role_order = {"head_coach": 0, "assistant": 1, "goalkeeper_coach": 2, "fitness_coach": 3, "other": 4}
    team_coaches.sort(key=lambda item: role_order.get(item.role.value, 99))

    staff_preview = [
        TeamOverviewCoachPreview(
            id=item.coach.id,
            first_name=get_localized_field(item.coach, "first_name", lang) or item.coach.first_name,
            last_name=get_localized_field(item.coach, "last_name", lang) or item.coach.last_name,
            photo_url=item.coach.photo_url,
            role=item.role.value,
            country_name=get_localized_name(item.coach.country, lang) if item.coach.country else None,
        )
        for item in team_coaches[:4]
    ]

    overview_team = TeamOverviewTeam(
        id=team.id,
        name=get_localized_name(team, lang),
        city=get_localized_city(team, lang),
        logo_url=team.logo_url,
        website=team.website,
        stadium=TeamOverviewStadium(
            name=get_localized_name(team.stadium, lang),
            city=get_localized_city(team.stadium, lang) if team.stadium and hasattr(team.stadium, "city") else None,
        ) if team.stadium else None,
        primary_color=team.primary_color,
        secondary_color=team.secondary_color,
        accent_color=team.accent_color,
    )
    overview_season = (
        TeamOverviewSeason(
            id=season.id,
            name=get_localized_field(season, "name", lang),
            championship_id=season.championship_id,
        ) if season else None
    )

    return TeamOverviewResponse(
        team=overview_team,
        season=overview_season,
        summary=summary,
        form_last5=form_last5,
        recent_match=recent_match,
        upcoming_matches=upcoming_matches,
        standings_window=standings_window,
        leaders=leaders,
        staff_preview=staff_preview,
    )


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
            "top_role": get_localized_field(p, "top_role", lang),
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
            selectinload(Game.stadium_rel),
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

        # Build stadium object from relationship or legacy string
        stadium_data = None
        if g.stadium_rel:
            stadium_data = {
                "name": get_localized_name(g.stadium_rel, lang),
                "city": get_localized_city(g.stadium_rel, lang) if hasattr(g.stadium_rel, 'city') else None,
            }
        elif g.stadium:
            stadium_data = {"name": g.stadium, "city": None}

        items.append({
            "id": g.id,
            "date": g.date.isoformat() if g.date else None,
            "time": g.time.isoformat() if g.time else None,
            "tour": g.tour,
            "season_id": g.season_id,
            "home_score": g.home_score,
            "away_score": g.away_score,
            "has_stats": g.has_stats,
            "stadium": stadium_data,
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
    lang: str = Query(default="ru", pattern="^(kz|ru|en)$"),
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


@router.get("/{team_id}/coaches")
async def get_team_coaches(
    team_id: int,
    season_id: int = Query(default=None),
    lang: str = Query(default="kz", description="Language: kz, ru, or en"),
    db: AsyncSession = Depends(get_db),
):
    """Get coaching staff for a team in a specific season."""
    if season_id is None:
        season_id = settings.current_season_id

    result = await db.execute(
        select(TeamCoach)
        .where(
            TeamCoach.team_id == team_id,
            TeamCoach.season_id == season_id,
            TeamCoach.is_active == True,
        )
        .options(
            selectinload(TeamCoach.coach).selectinload(Coach.country),
        )
    )
    team_coaches = result.scalars().all()

    items = []
    for tc in team_coaches:
        c = tc.coach
        country_data = None
        if c.country:
            country_data = {
                "id": c.country.id,
                "code": c.country.code,
                "name": get_localized_name(c.country, lang),
                "flag_url": c.country.flag_url,
            }
        items.append({
            "id": c.id,
            "first_name": get_localized_field(c, "first_name", lang),
            "last_name": get_localized_field(c, "last_name", lang),
            "photo_url": c.photo_url,
            "role": tc.role.value,
            "country": country_data,
        })

    # Sort: head_coach first, then by role
    role_order = {"head_coach": 0, "assistant": 1, "goalkeeper_coach": 2, "fitness_coach": 3, "other": 4}
    items.sort(key=lambda x: role_order.get(x["role"], 99))

    return {"items": items, "total": len(items)}


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
        raise HTTPException(status_code=404, detail=get_error_message("teams_not_found", lang))

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

    # Pre-calculate clean sheets for all teams in one query
    all_games_result = await db.execute(
        select(Game).where(
            Game.season_id == season_id,
            Game.home_score.is_not(None),
        )
    )
    all_season_games = all_games_result.scalars().all()

    clean_sheets_map: dict[int, int] = defaultdict(int)
    for game in all_season_games:
        if game.away_score == 0:
            clean_sheets_map[game.home_team_id] += 1
        if game.home_score == 0:
            clean_sheets_map[game.away_team_id] += 1

    season_table = []
    for entry in table_entries:
        clean_sheets = clean_sheets_map.get(entry.team_id, 0)

        season_table.append(SeasonTableEntry(
            position=entry.position,
            team_id=entry.team_id,
            team_name=get_localized_name(entry.team, lang),
            logo_url=entry.team.logo_url if entry.team else None,
            games_played=entry.games_played or 0,
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
