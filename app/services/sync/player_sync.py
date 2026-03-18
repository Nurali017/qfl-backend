"""
Player sync service.

Handles synchronization of player season statistics from SOTA API.
Player profiles (top_role) are managed locally — no longer synced from SOTA.
"""
import logging

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from app.models import Player, PlayerTeam, PlayerSeasonStats
from app.services.sync.base import BaseSyncService, PLAYER_SEASON_STATS_FIELDS
from app.utils.timestamps import utcnow

logger = logging.getLogger(__name__)


class PlayerSyncService(BaseSyncService):
    """
    Service for syncing player statistics.

    Handles:
    - Best players (goals/assists from lightweight endpoint)
    - Player season statistics (50+ metrics from v2 API)
    """

    async def sync_best_players(self, season_id: int) -> int:
        """
        Sync goals and assists from the best_players endpoint (single API call per metric).

        Only updates goals/assists columns in PlayerSeasonStats — does not overwrite
        the other 50+ stat columns that full_sync populates.

        Returns:
            Number of player stats rows upserted
        """
        sota_season_id = await self.get_sota_season_id(season_id)

        # Fetch top scorers, assisters, and clean sheets in three API calls
        try:
            scorers = await self.client.get_best_players(sota_season_id, metric="goal")
        except Exception as e:
            logger.warning("Failed to fetch best scorers for season %d: %s", season_id, e)
            scorers = []

        try:
            assisters = await self.client.get_best_players(sota_season_id, metric="goal_pass")
        except Exception as e:
            logger.warning("Failed to fetch best assisters for season %d: %s", season_id, e)
            assisters = []

        try:
            keepers = await self.client.get_best_players(sota_season_id, metric="dry_match")
        except Exception as e:
            logger.warning("Failed to fetch best keepers for season %d: %s", season_id, e)
            keepers = []

        if not scorers and not assisters and not keepers:
            logger.info("No best_players data for season %d, skipping", season_id)
            return 0

        # Build lookup: sota_id (str) -> (player_id, team_id)
        player_teams_result = await self.db.execute(
            select(PlayerTeam.player_id, PlayerTeam.team_id, Player.sota_id)
            .join(Player, Player.id == PlayerTeam.player_id)
            .where(
                PlayerTeam.season_id == season_id,
                PlayerTeam.is_active == True,
                Player.sota_id.is_not(None),
            )
        )
        lookup: dict[str, tuple[int, int]] = {}
        for player_id, team_id, sota_id in player_teams_result.fetchall():
            lookup[str(sota_id)] = (player_id, team_id)

        if not lookup:
            logger.info("No active player-team mappings for season %d", season_id)
            return 0

        # Merge scorers + assisters + keepers into a combined dict keyed by sota_id
        # API returns: {"id": "uuid", "value": "16", "name": "...", "team_name": "..."}
        # Position index in the response = SOTA's rank (1-based)
        combined: dict[str, dict] = {}
        for rank, p in enumerate(scorers, start=1):
            sid = p.get("id")
            if sid:
                try:
                    entry = combined.setdefault(str(sid), {})
                    entry["goal"] = int(p.get("value", 0))
                    entry["goal_rank"] = rank
                except (ValueError, TypeError):
                    pass
        for rank, p in enumerate(assisters, start=1):
            sid = p.get("id")
            if sid:
                try:
                    entry = combined.setdefault(str(sid), {})
                    entry["goal_pass"] = int(p.get("value", 0))
                    entry["goal_pass_rank"] = rank
                except (ValueError, TypeError):
                    pass
        for rank, p in enumerate(keepers, start=1):
            sid = p.get("id")
            if sid:
                try:
                    entry = combined.setdefault(str(sid), {})
                    entry["dry_match"] = int(p.get("value", 0))
                    entry["dry_match_rank"] = rank
                except (ValueError, TypeError):
                    pass
        count = 0
        now = utcnow()
        for sota_id_str, metrics in combined.items():
            mapping = lookup.get(sota_id_str)
            if not mapping:
                continue
            player_id, team_id = mapping

            values = {
                "player_id": player_id,
                "season_id": season_id,
                "team_id": team_id,
                "updated_at": now,
            }
            update_set: dict = {"updated_at": now}

            for col in ("goal", "goal_pass", "dry_match", "goal_rank", "goal_pass_rank", "dry_match_rank"):
                if col in metrics:
                    values[col] = metrics[col]
                    update_set[col] = metrics[col]

            stmt = insert(PlayerSeasonStats).values(**values)
            stmt = stmt.on_conflict_do_update(
                index_elements=["player_id", "season_id"],
                set_=update_set,
            )
            await self.db.execute(stmt)
            count += 1

        await self.db.commit()
        logger.info("Synced best_players for season %d: %d rows upserted", season_id, count)
        return count

    async def sync_player_season_stats(self, season_id: int) -> int:
        """
        Sync season stats for ALL players in a season from SOTA API v2.

        Uses v2 endpoint which provides 50+ metrics including:
        - xG, xG per 90
        - Duels, aerial/ground duels
        - Dribbles, tackles, interceptions
        - Key passes, progressive passes

        Args:
            season_id: Season ID to sync stats for

        Returns:
            Number of player stats synced
        """
        # Get all players in this season with their team
        player_teams_result = await self.db.execute(
            select(PlayerTeam.player_id, PlayerTeam.team_id, Player.sota_id)
            .join(Player, Player.id == PlayerTeam.player_id)
            .where(
                PlayerTeam.season_id == season_id,
                Player.sota_id.is_not(None),
            )
        )
        player_teams = list(player_teams_result.fetchall())

        if not player_teams:
            return 0

        # Resolve SOTA season ID for API calls (once, outside loop)
        sota_season_id = await self.get_sota_season_id(season_id)

        count = 0
        for player_id, team_id, sota_id in player_teams:
            try:
                # Get all metrics from SOTA v2 API
                stats = await self.client.get_player_season_stats(str(sota_id), sota_season_id)

                # Extract extra stats (fields not in our known list)
                extra_stats = {k: v for k, v in stats.items() if k not in PLAYER_SEASON_STATS_FIELDS}

                stmt = insert(PlayerSeasonStats).values(
                    player_id=player_id,
                    season_id=season_id,
                    team_id=team_id,
                    # Basic stats
                    games_played=stats.get("games_played"),
                    games_starting=stats.get("games_starting"),
                    games_as_subst=stats.get("games_as_subst"),
                    games_be_subst=stats.get("games_be_subst"),
                    games_unused=stats.get("games_unused"),
                    time_on_field_total=stats.get("time_on_field_total"),
                    # Goals & Assists
                    goal=stats.get("goal"),
                    goal_pass=stats.get("goal_pass"),
                    goal_and_assist=stats.get("goal_and_assist"),
                    goal_out_box=stats.get("goal_out_box"),
                    owngoal=stats.get("owngoal"),
                    penalty_success=stats.get("penalty_success"),
                    xg=stats.get("xg"),
                    xg_per_90=stats.get("xg_per_90"),
                    # Shots
                    shot=stats.get("shot"),
                    shots_on_goal=stats.get("shots_on_goal"),
                    shots_blocked_opponent=stats.get("shots_blocked_opponent"),
                    # Passes
                    passes=stats.get("pass"),
                    pass_ratio=stats.get("pass_ratio"),
                    pass_acc=stats.get("pass_acc"),
                    key_pass=stats.get("key_pass"),
                    pass_forward=stats.get("pass_forward"),
                    pass_forward_ratio=stats.get("pass_forward_ratio"),
                    pass_progressive=stats.get("pass_progressive"),
                    pass_cross=stats.get("pass_cross"),
                    pass_cross_acc=stats.get("pass_cross_acc"),
                    pass_cross_ratio=stats.get("pass_cross_ratio"),
                    pass_cross_per_90=stats.get("pass_cross_per_90"),
                    pass_to_box=stats.get("pass_to_box"),
                    pass_to_box_ratio=stats.get("pass_to_box_ratio"),
                    pass_to_3rd=stats.get("pass_to_3rd"),
                    pass_to_3rd_ratio=stats.get("pass_to_3rd_ratio"),
                    # Duels
                    duel=stats.get("duel"),
                    duel_success=stats.get("duel_success"),
                    aerial_duel=stats.get("aerial_duel"),
                    aerial_duel_success=stats.get("aerial_duel_success"),
                    ground_duel=stats.get("ground_duel"),
                    ground_duel_success=stats.get("ground_duel_success"),
                    # Defense
                    tackle=stats.get("tackle"),
                    tackle_per_90=stats.get("tackle_per_90"),
                    interception=stats.get("interception"),
                    recovery=stats.get("recovery"),
                    # Dribbles
                    dribble=stats.get("dribble"),
                    dribble_success=stats.get("dribble_success"),
                    dribble_per_90=stats.get("dribble_per_90"),
                    # Other
                    corner=stats.get("corner"),
                    offside=stats.get("offside"),
                    foul=stats.get("foul"),
                    foul_taken=stats.get("foul_taken"),
                    # Discipline
                    yellow_cards=stats.get("yellow_cards"),
                    second_yellow_cards=stats.get("second_yellow_cards"),
                    red_cards=stats.get("red_cards"),
                    # Goalkeeper
                    goals_conceded=stats.get("goals_conceded"),
                    goals_conceded_penalty=stats.get("goals_conceded_penalty"),
                    goals_conceeded_per_90=stats.get("goals_conceeded_per_90"),
                    save_shot=stats.get("save_shot"),
                    save_shot_ratio=stats.get("save_shot_ratio"),
                    saved_shot_per_90=stats.get("saved_shot_per_90"),
                    save_shot_penalty=stats.get("save_shot_penalty"),
                    save_shot_penalty_success=stats.get("save_shot_penalty_success"),
                    dry_match=stats.get("dry_match"),
                    exit=stats.get("exit"),
                    exit_success=stats.get("exit_success"),
                    # Extra stats for unknown fields
                    extra_stats=extra_stats if extra_stats else None,
                    updated_at=utcnow(),
                )
                stmt = stmt.on_conflict_do_update(
                    index_elements=["player_id", "season_id"],
                    set_={
                        "team_id": stmt.excluded.team_id,
                        "games_played": stmt.excluded.games_played,
                        "games_starting": stmt.excluded.games_starting,
                        "games_as_subst": stmt.excluded.games_as_subst,
                        "games_be_subst": stmt.excluded.games_be_subst,
                        "games_unused": stmt.excluded.games_unused,
                        "time_on_field_total": stmt.excluded.time_on_field_total,
                        "goal": stmt.excluded.goal,
                        "goal_pass": stmt.excluded.goal_pass,
                        "goal_and_assist": stmt.excluded.goal_and_assist,
                        "goal_out_box": stmt.excluded.goal_out_box,
                        "owngoal": stmt.excluded.owngoal,
                        "penalty_success": stmt.excluded.penalty_success,
                        "xg": stmt.excluded.xg,
                        "xg_per_90": stmt.excluded.xg_per_90,
                        "shot": stmt.excluded.shot,
                        "shots_on_goal": stmt.excluded.shots_on_goal,
                        "shots_blocked_opponent": stmt.excluded.shots_blocked_opponent,
                        "passes": stmt.excluded.passes,
                        "pass_ratio": stmt.excluded.pass_ratio,
                        "pass_acc": stmt.excluded.pass_acc,
                        "key_pass": stmt.excluded.key_pass,
                        "pass_forward": stmt.excluded.pass_forward,
                        "pass_forward_ratio": stmt.excluded.pass_forward_ratio,
                        "pass_progressive": stmt.excluded.pass_progressive,
                        "pass_cross": stmt.excluded.pass_cross,
                        "pass_cross_acc": stmt.excluded.pass_cross_acc,
                        "pass_cross_ratio": stmt.excluded.pass_cross_ratio,
                        "pass_cross_per_90": stmt.excluded.pass_cross_per_90,
                        "pass_to_box": stmt.excluded.pass_to_box,
                        "pass_to_box_ratio": stmt.excluded.pass_to_box_ratio,
                        "pass_to_3rd": stmt.excluded.pass_to_3rd,
                        "pass_to_3rd_ratio": stmt.excluded.pass_to_3rd_ratio,
                        "duel": stmt.excluded.duel,
                        "duel_success": stmt.excluded.duel_success,
                        "aerial_duel": stmt.excluded.aerial_duel,
                        "aerial_duel_success": stmt.excluded.aerial_duel_success,
                        "ground_duel": stmt.excluded.ground_duel,
                        "ground_duel_success": stmt.excluded.ground_duel_success,
                        "tackle": stmt.excluded.tackle,
                        "tackle_per_90": stmt.excluded.tackle_per_90,
                        "interception": stmt.excluded.interception,
                        "recovery": stmt.excluded.recovery,
                        "dribble": stmt.excluded.dribble,
                        "dribble_success": stmt.excluded.dribble_success,
                        "dribble_per_90": stmt.excluded.dribble_per_90,
                        "corner": stmt.excluded.corner,
                        "offside": stmt.excluded.offside,
                        "foul": stmt.excluded.foul,
                        "foul_taken": stmt.excluded.foul_taken,
                        "yellow_cards": stmt.excluded.yellow_cards,
                        "second_yellow_cards": stmt.excluded.second_yellow_cards,
                        "red_cards": stmt.excluded.red_cards,
                        "goals_conceded": stmt.excluded.goals_conceded,
                        "goals_conceded_penalty": stmt.excluded.goals_conceded_penalty,
                        "goals_conceeded_per_90": stmt.excluded.goals_conceeded_per_90,
                        "save_shot": stmt.excluded.save_shot,
                        "save_shot_ratio": stmt.excluded.save_shot_ratio,
                        "saved_shot_per_90": stmt.excluded.saved_shot_per_90,
                        "save_shot_penalty": stmt.excluded.save_shot_penalty,
                        "save_shot_penalty_success": stmt.excluded.save_shot_penalty_success,
                        "dry_match": stmt.excluded.dry_match,
                        "exit": stmt.excluded.exit,
                        "exit_success": stmt.excluded.exit_success,
                        "extra_stats": stmt.excluded.extra_stats,
                        "updated_at": stmt.excluded.updated_at,
                    },
                )
                await self.db.execute(stmt)
                count += 1
            except Exception as e:
                logger.warning(f"Failed to sync player season stats for player {player_id}: {e}")
                continue  # Skip players without v2 stats

        await self.db.commit()
        logger.info(f"Synced {count} player season stats for season {season_id}")
        return count
