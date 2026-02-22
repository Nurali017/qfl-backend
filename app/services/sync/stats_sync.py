"""
Stats sync service.

Handles synchronization of score tables and team season statistics from SOTA API.
"""
import logging
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from app.models import Game, ScoreTable, SeasonParticipant, TeamSeasonStats
from app.services.sync.base import BaseSyncService, TEAM_SEASON_STATS_FIELDS

logger = logging.getLogger(__name__)


class StatsSyncService(BaseSyncService):
    """
    Service for syncing league tables and team season statistics.

    Handles:
    - Score table (league standings)
    - Team season statistics (92 metrics from v2 API)
    """

    async def sync_score_table(self, season_id: int) -> int:
        """
        Sync league table for a season.

        Args:
            season_id: Season ID to sync

        Returns:
            Number of entries synced
        """
        table_data = await self.client.get_score_table(season_id)
        count = 0

        # Handle different response formats
        # API returns: {"result": "success", "data": {"table": [...]}}
        if isinstance(table_data, list):
            entries = table_data
        elif isinstance(table_data, dict):
            # Try nested data.table first
            data = table_data.get("data", {})
            if isinstance(data, dict):
                entries = data.get("table", [])
            else:
                entries = table_data.get("table", table_data.get("results", []))
        else:
            entries = []

        for idx, entry in enumerate(entries, start=1):
            # Skip non-dict entries
            if not isinstance(entry, dict):
                continue

            # Get team_id - API uses 'id' for team ID
            team_id = entry.get("team_id") or entry.get("id")
            if not team_id:
                continue

            # Parse goals string like "53:19" into scored and conceded
            goals_str = entry.get("goals", "0:0")
            goals_scored, goals_conceded = 0, 0
            if isinstance(goals_str, str) and ":" in goals_str:
                parts = goals_str.split(":")
                goals_scored = int(parts[0]) if parts[0].isdigit() else 0
                goals_conceded = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0

            form_list = entry.get("form", [])
            form_str = "".join(form_list) if isinstance(form_list, list) else (form_list or "")

            # Position is determined by array order (API returns sorted by points)
            stmt = insert(ScoreTable).values(
                season_id=season_id,
                team_id=team_id,
                position=idx,
                games_played=entry.get("matches") or entry.get("games_played"),
                wins=entry.get("wins"),
                draws=entry.get("draws"),
                losses=entry.get("losses"),
                goals_scored=entry.get("goals_scored") or goals_scored,
                goals_conceded=entry.get("goals_conceded") or goals_conceded,
                goal_difference=(goals_scored - goals_conceded) if goals_scored or goals_conceded else entry.get("goal_difference"),
                points=entry.get("points"),
                form=form_str,
                updated_at=datetime.utcnow(),
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["season_id", "team_id"],
                set_={
                    "position": stmt.excluded.position,
                    "games_played": stmt.excluded.games_played,
                    "wins": stmt.excluded.wins,
                    "draws": stmt.excluded.draws,
                    "losses": stmt.excluded.losses,
                    "goals_scored": stmt.excluded.goals_scored,
                    "goals_conceded": stmt.excluded.goals_conceded,
                    "goal_difference": stmt.excluded.goal_difference,
                    "points": stmt.excluded.points,
                    "form": stmt.excluded.form,
                    "updated_at": stmt.excluded.updated_at,
                },
            )
            await self.db.execute(stmt)
            count += 1

        await self.db.commit()
        logger.info(f"Synced score table for season {season_id}: {count} entries")
        return count

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

        count = 0
        for team_id in sorted(team_ids):
            try:
                # Get all metrics from SOTA v2 API
                stats = await self.client.get_team_season_stats_v2(team_id, season_id)

                # Extract extra stats (fields not in our known list)
                extra_stats = {k: v for k, v in stats.items() if k not in TEAM_SEASON_STATS_FIELDS}

                stmt = insert(TeamSeasonStats).values(
                    team_id=team_id,
                    season_id=season_id,
                    # Basic stats
                    games_played=stats.get("games_played"),
                    games_total=stats.get("games_total"),
                    wins=stats.get("win"),
                    draws=stats.get("draw"),
                    losses=stats.get("match_loss"),
                    goals_scored=stats.get("goal"),
                    goals_conceded=stats.get("goals_conceded"),
                    goals_difference=stats.get("goals_difference"),
                    points=stats.get("points"),
                    # xG
                    xg=stats.get("xg"),
                    xg_per_match=stats.get("xg_per_match"),
                    opponent_xg=stats.get("opponent_xg"),
                    # Shots
                    shots=stats.get("shot"),
                    shots_on_goal=stats.get("shots_on_goal"),
                    shots_on_goal_per_match=stats.get("shots_on_goal_per_match"),
                    shots_off_goal=stats.get("shots_off_goal"),
                    shot_per_90=stats.get("shot_per_90"),
                    shot_per_match=stats.get("shot_per_match"),
                    shot_to_goal=stats.get("shot_to_goal"),
                    goal_to_shot_ratio=stats.get("goal_to_shot_ratio"),
                    shots_against_penalty=stats.get("shots_against_penalty"),
                    # Possession
                    possession_avg=stats.get("possession_percent_average"),
                    # Passes
                    passes=stats.get("pass"),
                    pass_per_match=stats.get("pass_per_match"),
                    pass_accuracy_avg=stats.get("pass_ratio"),
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
                    fouls=stats.get("foul"),
                    foul_taken=stats.get("foul_taken"),
                    yellow_cards=stats.get("yellow_cards"),
                    second_yellow_cards=stats.get("second_yellow_cards"),
                    red_cards=stats.get("red_cards"),
                    # Set pieces
                    corners=stats.get("corner"),
                    corner_per_match=stats.get("corner_per_match"),
                    offsides=stats.get("offside"),
                    out=stats.get("out"),
                    # Visitors
                    visitor_total=stats.get("visitor_total"),
                    average_visitors=stats.get("average_visitors"),
                    # Extra stats for unknown fields
                    extra_stats=extra_stats if extra_stats else None,
                    updated_at=datetime.utcnow(),
                )
                stmt = stmt.on_conflict_do_update(
                    index_elements=["team_id", "season_id"],
                    set_={
                        "games_played": stmt.excluded.games_played,
                        "games_total": stmt.excluded.games_total,
                        "wins": stmt.excluded.wins,
                        "draws": stmt.excluded.draws,
                        "losses": stmt.excluded.losses,
                        "goals_scored": stmt.excluded.goals_scored,
                        "goals_conceded": stmt.excluded.goals_conceded,
                        "goals_difference": stmt.excluded.goals_difference,
                        "points": stmt.excluded.points,
                        "xg": stmt.excluded.xg,
                        "xg_per_match": stmt.excluded.xg_per_match,
                        "opponent_xg": stmt.excluded.opponent_xg,
                        "shots": stmt.excluded.shots,
                        "shots_on_goal": stmt.excluded.shots_on_goal,
                        "shots_on_goal_per_match": stmt.excluded.shots_on_goal_per_match,
                        "shots_off_goal": stmt.excluded.shots_off_goal,
                        "shot_per_90": stmt.excluded.shot_per_90,
                        "shot_per_match": stmt.excluded.shot_per_match,
                        "shot_to_goal": stmt.excluded.shot_to_goal,
                        "goal_to_shot_ratio": stmt.excluded.goal_to_shot_ratio,
                        "shots_against_penalty": stmt.excluded.shots_against_penalty,
                        "possession_avg": stmt.excluded.possession_avg,
                        "passes": stmt.excluded.passes,
                        "pass_per_match": stmt.excluded.pass_per_match,
                        "pass_accuracy_avg": stmt.excluded.pass_accuracy_avg,
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
                        "fouls": stmt.excluded.fouls,
                        "foul_taken": stmt.excluded.foul_taken,
                        "yellow_cards": stmt.excluded.yellow_cards,
                        "second_yellow_cards": stmt.excluded.second_yellow_cards,
                        "red_cards": stmt.excluded.red_cards,
                        "corners": stmt.excluded.corners,
                        "corner_per_match": stmt.excluded.corner_per_match,
                        "offsides": stmt.excluded.offsides,
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
