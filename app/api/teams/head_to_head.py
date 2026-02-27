from collections import defaultdict

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, selectinload

from app.api.deps import get_db
from app.models import (
    Game,
    Player,
    PlayerTeam,
    PlayerSeasonStats,
    ScoreTable,
    Team,
    TeamSeasonStats,
)
from app.models.game_team_stats import GameTeamStats
from app.models.game_event import GameEvent, GameEventType
from app.schemas.head_to_head import (
    HeadToHeadResponse,
    H2HOverallStats,
    FormGuide,
    FormGuideMatch,
    SeasonTableEntry,
    PreviousMeeting,
    H2HFunFacts,
    H2HBiggestWin,
    H2HGoalsByHalf,
    H2HAggregatedMatchStats,
    H2HTeamMatchStats,
    H2HTopPerformers,
    H2HTopPerformer,
    H2HEnhancedSeasonStats,
    H2HEnhancedSeasonTeamStats,
)
from app.services.season_visibility import resolve_visible_season_id
from app.utils.localization import get_localized_name, get_localized_field
from app.utils.error_messages import get_error_message
from app.utils.team_logo_fallback import resolve_team_logo_url
from fastapi_cache.decorator import cache

router = APIRouter(prefix="/teams", tags=["teams"])


@router.get("/{team1_id}/vs/{team2_id}/head-to-head")
@cache(expire=7200)
async def get_head_to_head(
    team1_id: int,
    team2_id: int,
    season_id: int = Query(default=None),
    lang: str = Query(default="kz", description="Language: kz, ru, or en"),
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
    season_id = await resolve_visible_season_id(db, season_id)

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
        .order_by(Game.date.asc())
    )
    overall_result = await db.execute(overall_query)
    all_h2h_games = overall_result.scalars().all()

    team1_wins = 0
    team2_wins = 0
    draws = 0
    team1_goals = 0
    team2_goals = 0
    team1_home_wins = 0
    team1_away_wins = 0
    team2_home_wins = 0
    team2_away_wins = 0

    for game in all_h2h_games:
        if game.home_team_id == team1_id:
            team1_goals += game.home_score or 0
            team2_goals += game.away_score or 0
            if game.home_score > game.away_score:
                team1_wins += 1
                team1_home_wins += 1
            elif game.home_score < game.away_score:
                team2_wins += 1
                team2_away_wins += 1
            else:
                draws += 1
        else:
            team1_goals += game.away_score or 0
            team2_goals += game.home_score or 0
            if game.away_score > game.home_score:
                team1_wins += 1
                team1_away_wins += 1
            elif game.away_score < game.home_score:
                team2_wins += 1
                team2_home_wins += 1
            else:
                draws += 1

    overall_stats = H2HOverallStats(
        total_matches=len(all_h2h_games),
        team1_wins=team1_wins,
        draws=draws,
        team2_wins=team2_wins,
        team1_goals=team1_goals,
        team2_goals=team2_goals,
        team1_home_wins=team1_home_wins,
        team1_away_wins=team1_away_wins,
        team2_home_wins=team2_home_wins,
        team2_away_wins=team2_away_wins,
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
                opponent_logo_url=resolve_team_logo_url(opponent),
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
            logo_url=resolve_team_logo_url(entry.team),
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
            home_team_logo=resolve_team_logo_url(game.home_team),
            away_team_logo=resolve_team_logo_url(game.away_team),
        ))

    # 5. FUN FACTS
    # H2H rates/streaks are computed from all-time H2H games,
    # while biggest win / worst defeat are computed from the selected tournament season.
    fun_facts = None
    if all_h2h_games:
        tournament_games_result = await db.execute(
            select(Game).where(
                Game.season_id == season_id,
                or_(
                    Game.home_team_id.in_([team1_id, team2_id]),
                    Game.away_team_id.in_([team1_id, team2_id]),
                ),
                Game.home_score.is_not(None),
                Game.away_score.is_not(None),
            )
        )
        tournament_games = tournament_games_result.scalars().all()

        def get_team_extreme_results(team_id: int) -> tuple[H2HBiggestWin | None, H2HBiggestWin | None]:
            biggest_win: H2HBiggestWin | None = None
            worst_defeat: H2HBiggestWin | None = None
            biggest_win_diff = 0
            worst_defeat_diff = 0

            for game in tournament_games:
                if game.home_team_id == team_id:
                    team_score = game.home_score or 0
                    opp_score = game.away_score or 0
                elif game.away_team_id == team_id:
                    team_score = game.away_score or 0
                    opp_score = game.home_score or 0
                else:
                    continue

                diff = team_score - opp_score
                if diff > biggest_win_diff:
                    biggest_win_diff = diff
                    biggest_win = H2HBiggestWin(
                        game_id=game.id,
                        date=game.date,
                        score=f"{team_score}-{opp_score}",
                        goal_difference=diff,
                    )

                if diff < 0 and abs(diff) > worst_defeat_diff:
                    worst_defeat_diff = abs(diff)
                    worst_defeat = H2HBiggestWin(
                        game_id=game.id,
                        date=game.date,
                        score=f"{team_score}-{opp_score}",
                        goal_difference=abs(diff),
                    )

            return biggest_win, worst_defeat

        team1_biggest_win, team1_worst_defeat = get_team_extreme_results(team1_id)
        team2_biggest_win, team2_worst_defeat = get_team_extreme_results(team2_id)

        total_goals = team1_goals + team2_goals
        total_matches = len(all_h2h_games)
        avg_goals = round(total_goals / total_matches, 2) if total_matches else 0

        over_2_5_count = 0
        btts_count = 0
        team1_streak = 0
        team2_streak = 0
        team1_max_streak = 0
        team2_max_streak = 0

        for game in all_h2h_games:
            hs = game.home_score or 0
            aws = game.away_score or 0
            total = hs + aws

            if total > 2.5:
                over_2_5_count += 1
            if hs > 0 and aws > 0:
                btts_count += 1

            # Determine team1/team2 scores
            if game.home_team_id == team1_id:
                t1_score, t2_score = hs, aws
            else:
                t1_score, t2_score = aws, hs

            diff = t1_score - t2_score

            # Unbeaten streaks (sorted by date ascending for streak calc)
            if t1_score >= t2_score:
                team1_streak += 1
                team1_max_streak = max(team1_max_streak, team1_streak)
            else:
                team1_streak = 0

            if t2_score >= t1_score:
                team2_streak += 1
                team2_max_streak = max(team2_max_streak, team2_streak)
            else:
                team2_streak = 0

        over_2_5_pct = round((over_2_5_count / total_matches) * 100, 1)
        btts_pct = round((btts_count / total_matches) * 100, 1)

        # Goals by half from GameEvent
        h2h_game_ids = [g.id for g in all_h2h_games]
        goals_by_half = None

        goal_events_query = (
            select(GameEvent)
            .where(
                GameEvent.game_id.in_(h2h_game_ids),
                GameEvent.event_type == GameEventType.goal,
            )
        )
        goal_events_result = await db.execute(goal_events_query)
        goal_events = goal_events_result.scalars().all()

        if goal_events:
            t1_1h, t1_2h, t2_1h, t2_2h = 0, 0, 0, 0
            # Build a map of game_id -> home_team_id for resolving team1/team2
            game_home_map = {g.id: g.home_team_id for g in all_h2h_games}
            for ev in goal_events:
                home_tid = game_home_map.get(ev.game_id)
                # Determine if event team is team1
                if ev.team_id == team1_id:
                    is_team1 = True
                elif ev.team_id == team2_id:
                    is_team1 = False
                else:
                    continue

                if is_team1:
                    if ev.half == 1:
                        t1_1h += 1
                    else:
                        t1_2h += 1
                else:
                    if ev.half == 1:
                        t2_1h += 1
                    else:
                        t2_2h += 1

            goals_by_half = H2HGoalsByHalf(
                team1_first_half=t1_1h,
                team1_second_half=t1_2h,
                team2_first_half=t2_1h,
                team2_second_half=t2_2h,
            )

        fun_facts = H2HFunFacts(
            avg_goals_per_match=avg_goals,
            over_2_5_percent=over_2_5_pct,
            btts_percent=btts_pct,
            team1_biggest_win=team1_biggest_win,
            team2_biggest_win=team2_biggest_win,
            team1_unbeaten_streak=team1_max_streak,
            team2_unbeaten_streak=team2_max_streak,
            goals_by_half=goals_by_half,
            team1_worst_defeat=team1_worst_defeat,
            team2_worst_defeat=team2_worst_defeat,
        )

    # 6. AGGREGATED MATCH STATS (from GameTeamStats)
    match_stats = None
    if all_h2h_games:
        h2h_game_ids = [g.id for g in all_h2h_games]
        gts_query = (
            select(GameTeamStats)
            .where(
                GameTeamStats.game_id.in_(h2h_game_ids),
                GameTeamStats.team_id.in_([team1_id, team2_id]),
            )
        )
        gts_result = await db.execute(gts_query)
        all_gts = gts_result.scalars().all()

        if all_gts:
            # Group by team
            t1_stats = [s for s in all_gts if s.team_id == team1_id]
            t2_stats = [s for s in all_gts if s.team_id == team2_id]

            def calc_team_match_stats(stats_list):
                n = len(stats_list)
                if n == 0:
                    return H2HTeamMatchStats(
                        avg_possession=None, avg_shots=None,
                        avg_shots_on_goal=None, avg_corners=None,
                        avg_fouls=None, total_yellow_cards=0, total_red_cards=0,
                    )
                poss = [s.possession_percent for s in stats_list if s.possession_percent is not None]
                shots = [s.shots for s in stats_list if s.shots is not None]
                sog = [s.shots_on_goal for s in stats_list if s.shots_on_goal is not None]
                corners = [s.corners for s in stats_list if s.corners is not None]
                fouls = [s.fouls for s in stats_list if s.fouls is not None]
                yc = sum(s.yellow_cards or 0 for s in stats_list)
                rc = sum(s.red_cards or 0 for s in stats_list)
                return H2HTeamMatchStats(
                    avg_possession=round(sum(poss) / len(poss), 1) if poss else None,
                    avg_shots=round(sum(shots) / len(shots), 1) if shots else None,
                    avg_shots_on_goal=round(sum(sog) / len(sog), 1) if sog else None,
                    avg_corners=round(sum(corners) / len(corners), 1) if corners else None,
                    avg_fouls=round(sum(fouls) / len(fouls), 1) if fouls else None,
                    total_yellow_cards=yc,
                    total_red_cards=rc,
                )

            # Count unique games that have stats
            games_with_stats = len(set(s.game_id for s in all_gts))

            match_stats = H2HAggregatedMatchStats(
                matches_with_stats=games_with_stats,
                team1=calc_team_match_stats(t1_stats),
                team2=calc_team_match_stats(t2_stats),
            )

    # 7. TOP PERFORMERS (from PlayerSeasonStats -- season leaders for both teams)
    top_performers = None
    pss_query = (
        select(PlayerSeasonStats)
        .options(
            joinedload(PlayerSeasonStats.player).selectinload(Player.player_teams),
        )
        .where(
            PlayerSeasonStats.season_id == season_id,
            PlayerSeasonStats.team_id.in_([team1_id, team2_id]),
        )
    )
    pss_result = await db.execute(pss_query)
    all_pss = pss_result.scalars().unique().all()

    if all_pss:
        # Top scorers by goals
        scorers = sorted(
            [p for p in all_pss if (p.goals or 0) > 0],
            key=lambda p: p.goals or 0, reverse=True
        )[:5]
        # Top assisters by assists
        assisters = sorted(
            [p for p in all_pss if (p.assists or 0) > 0],
            key=lambda p: p.assists or 0, reverse=True
        )[:5]

        def _player_full_name(player: Player | None) -> str:
            if not player:
                return ""
            parts = [player.first_name or "", player.last_name or ""]
            return " ".join(p for p in parts if p)

        top_scorers = [
            H2HTopPerformer(
                player_id=p.player_id,
                player_name=_player_full_name(p.player),
                team_id=p.team_id,
                photo_url=(next((pt.photo_url for pt in p.player.player_teams if pt.team_id == p.team_id and pt.season_id == season_id and pt.photo_url), None) or p.player.photo_url) if p.player else None,
                count=p.goals or 0,
            )
            for p in scorers
        ]
        top_assisters = [
            H2HTopPerformer(
                player_id=p.player_id,
                player_name=_player_full_name(p.player),
                team_id=p.team_id,
                photo_url=(next((pt.photo_url for pt in p.player.player_teams if pt.team_id == p.team_id and pt.season_id == season_id and pt.photo_url), None) or p.player.photo_url) if p.player else None,
                count=p.assists or 0,
            )
            for p in assisters
        ]

        if top_scorers or top_assisters:
            top_performers = H2HTopPerformers(
                top_scorers=top_scorers,
                top_assisters=top_assisters,
            )

    # 8. ENHANCED SEASON STATS (from TeamSeasonStats)
    enhanced_season_stats = None
    tss_query = (
        select(TeamSeasonStats)
        .where(
            TeamSeasonStats.season_id == season_id,
            TeamSeasonStats.team_id.in_([team1_id, team2_id]),
        )
    )
    tss_result = await db.execute(tss_query)
    tss_all = tss_result.scalars().all()

    if tss_all:
        tss_map = {s.team_id: s for s in tss_all}
        t1_tss = tss_map.get(team1_id)
        t2_tss = tss_map.get(team2_id)

        def to_enhanced(tss):
            if not tss:
                return None
            return H2HEnhancedSeasonTeamStats(
                xg=float(tss.xg) if tss.xg is not None else None,
                xg_per_match=float(tss.xg_per_match) if tss.xg_per_match is not None else None,
                possession_avg=float(tss.possession_avg) if tss.possession_avg is not None else None,
                pass_accuracy_avg=float(tss.pass_accuracy_avg) if tss.pass_accuracy_avg is not None else None,
                duel_ratio=float(tss.duel_ratio) if tss.duel_ratio is not None else None,
                shots_per_match=float(tss.shot_per_match) if tss.shot_per_match is not None else None,
            )

        enhanced_season_stats = H2HEnhancedSeasonStats(
            team1=to_enhanced(t1_tss),
            team2=to_enhanced(t2_tss),
        )

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
        fun_facts=fun_facts,
        match_stats=match_stats,
        top_performers=top_performers,
        enhanced_season_stats=enhanced_season_stats,
    )
