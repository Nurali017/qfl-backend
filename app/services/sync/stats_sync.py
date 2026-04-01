"""
Stats sync service.

Handles synchronization of team season statistics from SOTA API.
Score table is managed locally — no longer synced from SOTA.
"""
import logging

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from app.models import Game, ScoreTable, SeasonParticipant, TeamSeasonStats
from app.services.sync.base import BaseSyncService, TEAM_SEASON_STATS_FIELDS
from app.utils.timestamps import utcnow

logger = logging.getLogger(__name__)


class StatsSyncService(BaseSyncService):
    """
    Service for syncing team season statistics.

    Handles:
    - Team season statistics (92 metrics from v2 API)
    """

    async def sync_team_season_stats(self, season_id: int) -> int:
        """
        Sync season stats for ALL teams in a season from SOTA API v2.

        Uses v2 endpoint which provides 92 metrics including:
        - xG, xGA, xG per match
        - Possession, pass ratio, duel ratio
        - All detailed attacking/defending stats

        Args:
            season_id: Season ID to sync

        Returns:
            Number of team stats synced
        """
        # Build candidate team set from all season sources:
        # score_table + season_participants + games.
        # This prevents partial sync when score_table is incomplete (e.g. split groups).
        score_table_result = await self.db.execute(
            select(ScoreTable).where(ScoreTable.season_id == season_id)
        )
        score_table_entries = {st.team_id: st for st in score_table_result.scalars().all()}

        team_ids = set(score_table_entries.keys())

        participants_result = await self.db.execute(
            select(SeasonParticipant.team_id).where(SeasonParticipant.season_id == season_id)
        )
        for team_id in participants_result.scalars().all():
            if team_id:
                team_ids.add(team_id)

        # Cup-style seasons or partially populated participants are additionally covered by games.
        games_result = await self.db.execute(
            select(Game.home_team_id, Game.away_team_id).where(Game.season_id == season_id)
        )
        for home_id, away_id in games_result.all():
            if home_id:
                team_ids.add(home_id)
            if away_id:
                team_ids.add(away_id)

        if not team_ids:
            return 0

        # Resolve all SOTA season IDs (usually 1, but 2L has SW+NE)
        sota_season_ids = await self.get_all_sota_season_ids(season_id)

        count = 0
        for team_id in sorted(team_ids):
            try:
                # Try each SOTA season ID (team belongs to one conference)
                stats = {}
                for sid in sota_season_ids:
                    stats = await self.client.get_team_season_stats_v2(team_id, sid)
                    if stats and stats.get("games_played"):
                        break

                # Extract extra stats (fields not in our known list)
                extra_stats = {k: v for k, v in stats.items() if k not in TEAM_SEASON_STATS_FIELDS}

                stmt = insert(TeamSeasonStats).values(
                    team_id=team_id,
                    season_id=season_id,
                    # Basic stats
                    games_played=stats.get("games_played"),
                    games_total=stats.get("games_total"),
                    win=stats.get("win"),
                    draw=stats.get("draw"),
                    match_loss=stats.get("match_loss"),
                    goal=stats.get("goal"),
                    goals_conceded=stats.get("goals_conceded"),
                    goals_difference=stats.get("goals_difference"),
                    points=stats.get("points"),
                    # xG
                    xg=stats.get("xg"),
                    xg_per_match=stats.get("xg_per_match"),
                    opponent_xg=stats.get("opponent_xg"),
                    # Shots
                    shot=stats.get("shot"),
                    shots_on_goal=stats.get("shots_on_goal"),
                    shots_on_goal_per_match=stats.get("shots_on_goal_per_match"),
                    shots_off_goal=stats.get("shots_off_goal"),
                    shot_per_90=stats.get("shot_per_90"),
                    shot_per_match=stats.get("shot_per_match"),
                    shot_to_goal=stats.get("shot_to_goal"),
                    goal_to_shot_ratio=stats.get("goal_to_shot_ratio"),
                    shots_against_penalty=stats.get("shots_against_penalty"),
                    # Possession
                    possession_percent_average=stats.get("possession_percent_average"),
                    # Passes
                    passes=stats.get("pass"),
                    pass_per_match=stats.get("pass_per_match"),
                    pass_ratio=stats.get("pass_ratio"),
                    pass_forward=stats.get("pass_forward"),
                    pass_forward_per_match=stats.get("pass_forward_per_match"),
                    pass_forward_ratio=stats.get("pass_forward_ratio"),
                    pass_long=stats.get("pass_long"),
                    pass_long_per_match=stats.get("pass_long_per_match"),
                    pass_long_ratio=stats.get("pass_long_ratio"),
                    pass_progressive=stats.get("pass_progressive"),
                    pass_progressive_per_match=stats.get("pass_progressive_per_match"),
                    pass_cross=stats.get("pass_cross"),
                    pass_cross_per_match=stats.get("pass_cross_per_match"),
                    pass_cross_ratio=stats.get("pass_cross_ratio"),
                    pass_to_box=stats.get("pass_to_box"),
                    pass_to_box_per_match=stats.get("pass_to_box_per_match"),
                    pass_to_box_ratio=stats.get("pass_to_box_ratio"),
                    pass_to_3rd=stats.get("pass_to_3rd"),
                    pass_to_3rd_acc=stats.get("pass_to_3rd_acc"),
                    pass_to_3rd_per_match=stats.get("pass_to_3rd_per_match"),
                    pass_to_3rd_ratio=stats.get("pass_to_3rd_ratio"),
                    key_pass=stats.get("key_pass"),
                    key_pass_per_match=stats.get("key_pass_per_match"),
                    key_pass_ratio=stats.get("key_pass_ratio"),
                    goal_pass=stats.get("goal_pass"),
                    # Free kicks
                    freekick_pass=stats.get("freekick_pass"),
                    freekick_pass_per_match=stats.get("freekick_pass_per_match"),
                    freekick_shot=stats.get("freekick_shot"),
                    freekick_shot_per_match=stats.get("freekick_shot_per_match"),
                    # Duels
                    duel=stats.get("duel"),
                    duel_per_match=stats.get("duel_per_match"),
                    duel_ratio=stats.get("duel_ratio"),
                    aerial_duel_offence=stats.get("aerial_duel_offence"),
                    aerial_duel_offence_per_match=stats.get("aerial_duel_offence_per_match"),
                    aerial_duel_offence_ratio=stats.get("aerial_duel_offence_ratio"),
                    aerial_duel_defence=stats.get("aerial_duel_defence"),
                    aerial_duel_defence_per_match=stats.get("aerial_duel_defence_per_match"),
                    aerial_duel_defence_ratio=stats.get("aerial_duel_defence_ratio"),
                    ground_duel_offence=stats.get("ground_duel_offence"),
                    ground_duel_offence_per_match=stats.get("ground_duel_offence_per_match"),
                    ground_duel_offence_ratio=stats.get("ground_duel_offence_ratio"),
                    ground_duel_defence=stats.get("ground_duel_defence"),
                    ground_duel_defence_per_match=stats.get("ground_duel_defence_per_match"),
                    ground_duel_defence_ratio=stats.get("ground_duel_defence_ratio"),
                    # Defense
                    tackle=stats.get("tackle"),
                    tackle_per_match=stats.get("tackle_per_match"),
                    tackle1_1=stats.get("tackle1-1"),
                    tackle1_1_per_match=stats.get("tackle1-1_per_match"),
                    tackle1_1_ratio=stats.get("tackle1-1_ratio"),
                    interception=stats.get("interception"),
                    interception_per_match=stats.get("interception_per_match"),
                    recovery=stats.get("recovery"),
                    recovery_per_match=stats.get("recovery_per_match"),
                    # Dribbles
                    dribble=stats.get("dribble"),
                    dribble_per_match=stats.get("dribble_per_match"),
                    dribble_ratio=stats.get("dribble_ratio"),
                    # Penalties
                    penalty=stats.get("penalty"),
                    penalty_ratio=stats.get("penalty_ratio"),
                    save_penalty_ratio=stats.get("save_penalty_ratio"),
                    # Discipline
                    foul=stats.get("foul"),
                    foul_taken=stats.get("foul_taken"),
                    yellow_cards=stats.get("yellow_cards"),
                    second_yellow_cards=stats.get("second_yellow_cards"),
                    red_cards=stats.get("red_cards"),
                    # Set pieces
                    corner=stats.get("corner"),
                    corner_per_match=stats.get("corner_per_match"),
                    offside=stats.get("offside"),
                    out=stats.get("out"),
                    # Visitors
                    visitor_total=stats.get("visitor_total"),
                    average_visitors=stats.get("average_visitors"),
                    # Extra stats for unknown fields
                    extra_stats=extra_stats if extra_stats else None,
                    updated_at=utcnow(),
                )
                stmt = stmt.on_conflict_do_update(
                    index_elements=["team_id", "season_id"],
                    set_={
                        "games_played": stmt.excluded.games_played,
                        "games_total": stmt.excluded.games_total,
                        "win": stmt.excluded.win,
                        "draw": stmt.excluded.draw,
                        "match_loss": stmt.excluded.match_loss,
                        "goal": stmt.excluded.goal,
                        "goals_conceded": stmt.excluded.goals_conceded,
                        "goals_difference": stmt.excluded.goals_difference,
                        "points": stmt.excluded.points,
                        "xg": stmt.excluded.xg,
                        "xg_per_match": stmt.excluded.xg_per_match,
                        "opponent_xg": stmt.excluded.opponent_xg,
                        "shot": stmt.excluded.shot,
                        "shots_on_goal": stmt.excluded.shots_on_goal,
                        "shots_on_goal_per_match": stmt.excluded.shots_on_goal_per_match,
                        "shots_off_goal": stmt.excluded.shots_off_goal,
                        "shot_per_90": stmt.excluded.shot_per_90,
                        "shot_per_match": stmt.excluded.shot_per_match,
                        "shot_to_goal": stmt.excluded.shot_to_goal,
                        "goal_to_shot_ratio": stmt.excluded.goal_to_shot_ratio,
                        "shots_against_penalty": stmt.excluded.shots_against_penalty,
                        "possession_percent_average": stmt.excluded.possession_percent_average,
                        "passes": stmt.excluded.passes,
                        "pass_per_match": stmt.excluded.pass_per_match,
                        "pass_ratio": stmt.excluded.pass_ratio,
                        "pass_forward": stmt.excluded.pass_forward,
                        "pass_forward_per_match": stmt.excluded.pass_forward_per_match,
                        "pass_forward_ratio": stmt.excluded.pass_forward_ratio,
                        "pass_long": stmt.excluded.pass_long,
                        "pass_long_per_match": stmt.excluded.pass_long_per_match,
                        "pass_long_ratio": stmt.excluded.pass_long_ratio,
                        "pass_progressive": stmt.excluded.pass_progressive,
                        "pass_progressive_per_match": stmt.excluded.pass_progressive_per_match,
                        "pass_cross": stmt.excluded.pass_cross,
                        "pass_cross_per_match": stmt.excluded.pass_cross_per_match,
                        "pass_cross_ratio": stmt.excluded.pass_cross_ratio,
                        "pass_to_box": stmt.excluded.pass_to_box,
                        "pass_to_box_per_match": stmt.excluded.pass_to_box_per_match,
                        "pass_to_box_ratio": stmt.excluded.pass_to_box_ratio,
                        "pass_to_3rd": stmt.excluded.pass_to_3rd,
                        "pass_to_3rd_acc": stmt.excluded.pass_to_3rd_acc,
                        "pass_to_3rd_per_match": stmt.excluded.pass_to_3rd_per_match,
                        "pass_to_3rd_ratio": stmt.excluded.pass_to_3rd_ratio,
                        "key_pass": stmt.excluded.key_pass,
                        "key_pass_per_match": stmt.excluded.key_pass_per_match,
                        "key_pass_ratio": stmt.excluded.key_pass_ratio,
                        "goal_pass": stmt.excluded.goal_pass,
                        "freekick_pass": stmt.excluded.freekick_pass,
                        "freekick_pass_per_match": stmt.excluded.freekick_pass_per_match,
                        "freekick_shot": stmt.excluded.freekick_shot,
                        "freekick_shot_per_match": stmt.excluded.freekick_shot_per_match,
                        "duel": stmt.excluded.duel,
                        "duel_per_match": stmt.excluded.duel_per_match,
                        "duel_ratio": stmt.excluded.duel_ratio,
                        "aerial_duel_offence": stmt.excluded.aerial_duel_offence,
                        "aerial_duel_offence_per_match": stmt.excluded.aerial_duel_offence_per_match,
                        "aerial_duel_offence_ratio": stmt.excluded.aerial_duel_offence_ratio,
                        "aerial_duel_defence": stmt.excluded.aerial_duel_defence,
                        "aerial_duel_defence_per_match": stmt.excluded.aerial_duel_defence_per_match,
                        "aerial_duel_defence_ratio": stmt.excluded.aerial_duel_defence_ratio,
                        "ground_duel_offence": stmt.excluded.ground_duel_offence,
                        "ground_duel_offence_per_match": stmt.excluded.ground_duel_offence_per_match,
                        "ground_duel_offence_ratio": stmt.excluded.ground_duel_offence_ratio,
                        "ground_duel_defence": stmt.excluded.ground_duel_defence,
                        "ground_duel_defence_per_match": stmt.excluded.ground_duel_defence_per_match,
                        "ground_duel_defence_ratio": stmt.excluded.ground_duel_defence_ratio,
                        "tackle": stmt.excluded.tackle,
                        "tackle_per_match": stmt.excluded.tackle_per_match,
                        "tackle1_1": stmt.excluded.tackle1_1,
                        "tackle1_1_per_match": stmt.excluded.tackle1_1_per_match,
                        "tackle1_1_ratio": stmt.excluded.tackle1_1_ratio,
                        "interception": stmt.excluded.interception,
                        "interception_per_match": stmt.excluded.interception_per_match,
                        "recovery": stmt.excluded.recovery,
                        "recovery_per_match": stmt.excluded.recovery_per_match,
                        "dribble": stmt.excluded.dribble,
                        "dribble_per_match": stmt.excluded.dribble_per_match,
                        "dribble_ratio": stmt.excluded.dribble_ratio,
                        "penalty": stmt.excluded.penalty,
                        "penalty_ratio": stmt.excluded.penalty_ratio,
                        "save_penalty_ratio": stmt.excluded.save_penalty_ratio,
                        "foul": stmt.excluded.foul,
                        "foul_taken": stmt.excluded.foul_taken,
                        "yellow_cards": stmt.excluded.yellow_cards,
                        "second_yellow_cards": stmt.excluded.second_yellow_cards,
                        "red_cards": stmt.excluded.red_cards,
                        "corner": stmt.excluded.corner,
                        "corner_per_match": stmt.excluded.corner_per_match,
                        "offside": stmt.excluded.offside,
                        "out": stmt.excluded.out,
                        "visitor_total": stmt.excluded.visitor_total,
                        "average_visitors": stmt.excluded.average_visitors,
                        "extra_stats": stmt.excluded.extra_stats,
                        "updated_at": stmt.excluded.updated_at,
                    },
                )
                await self.db.execute(stmt)
                count += 1
            except Exception as e:
                logger.warning(f"Failed to sync team season stats for team {team_id}: {e}")
                continue  # Skip teams without v2 stats

        await self.db.commit()
        logger.info(f"Synced {count} team season stats for season {season_id}")
        return count
