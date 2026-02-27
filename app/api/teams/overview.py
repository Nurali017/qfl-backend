from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.api.deps import get_db
from app.models import (
    Game,
    Player,
    PlayerTeam,
    PlayerSeasonStats,
    ScoreTable,
    Season,
    Team,
    TeamSeasonStats,
)
from app.models.coach import Coach, TeamCoach
from app.schemas.team import (
    TeamOverviewCoachPreview,
    TeamOverviewFormEntry,
    TeamOverviewLeaderPlayer,
    TeamOverviewLeaders,
    TeamOverviewMiniLeaders,
    TeamOverviewResponse,
    TeamOverviewSeason,
    TeamOverviewStadium,
    TeamOverviewStandingEntry,
    TeamOverviewSummary,
    TeamOverviewTeam,
)
from app.services.season_visibility import is_season_visible_clause, resolve_visible_season_id
from app.services.team_overview import (
    _build_overview_match,
    _compute_summary_from_games,
    _safe_int,
    _window_around_team,
)
from app.utils.localization import get_localized_name, get_localized_city, get_localized_field
from app.utils.error_messages import get_error_message
from app.utils.team_logo_fallback import resolve_team_logo_url
from fastapi_cache.decorator import cache

router = APIRouter(prefix="/teams", tags=["teams"])


@router.get("/{team_id}/overview", response_model=TeamOverviewResponse)
@cache(expire=3600)
async def get_team_overview(
    team_id: int,
    season_id: int | None = Query(default=None),
    fixtures_limit: int = Query(default=5, ge=1, le=10),
    leaders_limit: int = Query(default=8, ge=3, le=20),
    lang: str = Query(default="kz", pattern="^(kz|ru|en)$"),
    db: AsyncSession = Depends(get_db),
):
    """Get aggregated team overview data for the team page."""
    season_id = await resolve_visible_season_id(db, season_id)

    team_result = await db.execute(
        select(Team)
        .where(Team.id == team_id)
        .options(selectinload(Team.stadium))
    )
    team = team_result.scalar_one_or_none()
    if not team:
        raise HTTPException(status_code=404, detail=get_error_message("team_not_found", lang))

    season_result = await db.execute(
        select(Season).where(
            Season.id == season_id,
            is_season_visible_clause(),
        )
    )
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
                opponent_logo=resolve_team_logo_url(opponent),
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
                    team_logo=resolve_team_logo_url(entry.team),
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
                        team_logo=resolve_team_logo_url(current_team),
                        games_played=values["games_played"],
                        points=values["points"],
                        goal_difference=values["goals_scored"] - values["goals_conceded"],
                        goals_scored=values["goals_scored"],
                        goals_conceded=values["goals_conceded"],
                    )
                )

    standings_window = _window_around_team(standings, team_id, window=5)

    contract_photo_subq = (
        select(PlayerTeam.photo_url)
        .where(
            PlayerTeam.player_id == PlayerSeasonStats.player_id,
            PlayerTeam.team_id == PlayerSeasonStats.team_id,
            PlayerTeam.season_id == PlayerSeasonStats.season_id,
        )
        .limit(1)
        .correlate(PlayerSeasonStats)
        .scalar_subquery()
    )

    players_result = await db.execute(
        select(PlayerSeasonStats, Player, Team, contract_photo_subq.label("contract_photo"))
        .join(Player, PlayerSeasonStats.player_id == Player.id)
        .outerjoin(Team, PlayerSeasonStats.team_id == Team.id)
        .where(
            PlayerSeasonStats.season_id == season_id,
            PlayerSeasonStats.team_id == team_id,
        )
    )
    player_rows = players_result.all()

    players: list[TeamOverviewLeaderPlayer] = []
    for row_stats, row_player, row_team, contract_photo in player_rows:
        players.append(
            TeamOverviewLeaderPlayer(
                player_id=row_player.id,
                first_name=get_localized_field(row_player, "first_name", lang),
                last_name=get_localized_field(row_player, "last_name", lang),
                photo_url=contract_photo or row_player.photo_url,
                team_id=row_team.id if row_team else row_stats.team_id,
                team_name=get_localized_name(row_team, lang) if row_team else None,
                team_logo=resolve_team_logo_url(row_team),
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
        logo_url=resolve_team_logo_url(team),
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
