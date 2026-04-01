"""
Player tour stats sync service.

Syncs cumulative per-tour player statistics from SOTA API v2.
"""
import asyncio
import logging

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from app.models import Player, PlayerTeam
from app.models.player_tour_stats import PlayerTourStats
from app.services.sync.base import BaseSyncService, PLAYER_SEASON_STATS_FIELDS
from app.utils.timestamps import utcnow

logger = logging.getLogger(__name__)

# Fields we store as proper columns (subset of PLAYER_SEASON_STATS_FIELDS)
PLAYER_TOUR_STATS_COLUMNS = {
    "games_played", "time_on_field_total",
    "goal", "goal_pass", "shot",
    "pass", "pass_ratio", "xg",
    "duel", "tackle",
    "yellow_cards", "red_cards",
}


class PlayerTourStatsSyncService(BaseSyncService):
    """Service for syncing per-tour player statistics from SOTA v2."""

    async def sync_tour(self, season_id: int, tour: int) -> int:
        """
        Sync cumulative stats for all players in a season for a given tour.

        Returns:
            Number of player stats rows upserted
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
            logger.info("No players with sota_id for season %d", season_id)
            return 0

        sota_season_ids = await self.get_all_sota_season_ids(season_id)

        count = 0
        for player_id, team_id, sota_id in player_teams:
            try:
                # Try each SOTA season ID (player belongs to one conference)
                stats = {}
                for sid in sota_season_ids:
                    stats = await self.client.get_player_game_stats_v2_by_tour(
                        str(sota_id), sid, tour
                    )
                    if stats and stats.get("games_played"):
                        break

                if not stats:
                    continue

                # Separate known columns from extra stats
                extra_stats = {
                    k: v for k, v in stats.items()
                    if k not in PLAYER_SEASON_STATS_FIELDS
                }

                now = utcnow()
                stmt = insert(PlayerTourStats).values(
                    player_id=player_id,
                    season_id=season_id,
                    team_id=team_id,
                    tour=tour,
                    games_played=stats.get("games_played"),
                    time_on_field_total=stats.get("time_on_field_total"),
                    goal=stats.get("goal"),
                    goal_pass=stats.get("goal_pass"),
                    shot=stats.get("shot"),
                    passes=stats.get("pass"),
                    pass_ratio=stats.get("pass_ratio"),
                    xg=stats.get("xg"),
                    duel=stats.get("duel"),
                    tackle=stats.get("tackle"),
                    yellow_cards=stats.get("yellow_cards"),
                    red_cards=stats.get("red_cards"),
                    extra_stats=extra_stats if extra_stats else None,
                    updated_at=now,
                )
                stmt = stmt.on_conflict_do_update(
                    index_elements=["player_id", "season_id", "tour"],
                    set_={
                        "team_id": stmt.excluded.team_id,
                        "games_played": stmt.excluded.games_played,
                        "time_on_field_total": stmt.excluded.time_on_field_total,
                        "goal": stmt.excluded.goal,
                        "goal_pass": stmt.excluded.goal_pass,
                        "shot": stmt.excluded.shot,
                        "passes": stmt.excluded.passes,
                        "pass_ratio": stmt.excluded.pass_ratio,
                        "xg": stmt.excluded.xg,
                        "duel": stmt.excluded.duel,
                        "tackle": stmt.excluded.tackle,
                        "yellow_cards": stmt.excluded.yellow_cards,
                        "red_cards": stmt.excluded.red_cards,
                        "extra_stats": stmt.excluded.extra_stats,
                        "updated_at": stmt.excluded.updated_at,
                    },
                )
                await self.db.execute(stmt)
                count += 1
            except Exception as e:
                logger.warning(
                    "Failed to sync tour stats for player %d, season %d, tour %d: %s",
                    player_id, season_id, tour, e,
                )
                continue

            await asyncio.sleep(0.15)

        await self.db.commit()
        logger.info(
            "Synced %d player tour stats for season %d, tour %d",
            count, season_id, tour,
        )
        return count

    async def backfill_season(self, season_id: int, max_tour: int) -> dict:
        """
        Backfill tour stats for tours 1..max_tour.

        Returns:
            Dict with per-tour counts
        """
        results = {}
        for tour in range(1, max_tour + 1):
            count = await self.sync_tour(season_id, tour)
            results[f"tour_{tour}"] = count
            logger.info(
                "Backfill tour %d/%d for season %d: %d players",
                tour, max_tour, season_id, count,
            )
        return results
