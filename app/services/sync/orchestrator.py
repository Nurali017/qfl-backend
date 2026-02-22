"""
Sync orchestrator service.

Coordinates sync operations across all sync services,
ensuring correct order of operations and handling dependencies.
"""
import logging
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.season import Season
from app.services.sota_client import SotaClient, get_sota_client
from app.services.sync.reference_sync import ReferenceSyncService
from app.services.sync.player_sync import PlayerSyncService
from app.services.sync.game_sync import GameSyncService
from app.services.sync.lineup_sync import LineupSyncService
from app.services.sync.stats_sync import StatsSyncService

logger = logging.getLogger(__name__)


class SyncOrchestrator:
    """
    Orchestrates sync operations across all sync services.

    Ensures operations are executed in the correct order to
    satisfy foreign key dependencies:
    1. Reference data (tournaments, seasons, teams) - no dependencies
    2. Players - depends on teams
    3. Games - depends on teams, seasons
    4. Statistics - depends on players, games
    """

    def __init__(self, db: AsyncSession, client: SotaClient | None = None):
        """
        Initialize the orchestrator with all sync services.

        Args:
            db: SQLAlchemy async session
            client: Optional SOTA client (uses singleton if not provided)
        """
        self.db = db
        self.client = client or get_sota_client()

        # Initialize specialized sync services
        self.reference = ReferenceSyncService(db, self.client)
        self.player = PlayerSyncService(db, self.client)
        self.game = GameSyncService(db, self.client)
        self.lineup = LineupSyncService(db, self.client)
        self.stats = StatsSyncService(db, self.client)

    async def is_sync_enabled(self, season_id: int) -> bool:
        """
        Check if sync is enabled for a season.

        When sync_enabled=False, our local data is considered the source of truth
        and SOTA must not overwrite it.
        """
        result = await self.db.execute(
            select(Season.sync_enabled).where(Season.id == season_id)
        )
        val = result.scalar_one_or_none()
        return val is True

    async def sync_references(self) -> dict[str, int]:
        """
        Sync all reference data (tournaments, seasons, teams).

        Returns:
            Dict with counts for each entity type
        """
        logger.info("Starting reference data sync")
        results = await self.reference.sync_all()
        logger.info(f"Reference sync complete: {results}")
        return results

    async def sync_players(self, season_id: int) -> int:
        """
        Sync players for a season.

        Args:
            season_id: Season ID to sync players for

        Returns:
            Number of players synced
        """
        if not await self.is_sync_enabled(season_id):
            logger.info(f"Season {season_id}: sync disabled, skipping players sync")
            return 0
        logger.info(f"Syncing players for season {season_id}")
        return await self.player.sync_players(season_id)

    async def sync_player_stats(self, season_id: int) -> int:
        """
        Sync player season statistics.

        Args:
            season_id: Season ID to sync stats for

        Returns:
            Number of player stats synced
        """
        if not await self.is_sync_enabled(season_id):
            logger.info(f"Season {season_id}: sync disabled, skipping player stats sync")
            return 0
        logger.info(f"Syncing player stats for season {season_id}")
        return await self.player.sync_player_season_stats(season_id)

    # ==================== Game sync methods ====================

    async def sync_games(self, season_id: int) -> int:
        """
        Sync games for a season.

        Args:
            season_id: Season ID to sync games for

        Returns:
            Number of games synced
        """
        if not await self.is_sync_enabled(season_id):
            logger.info(f"Season {season_id}: sync disabled, skipping games sync")
            return 0
        logger.info(f"Syncing games for season {season_id}")
        return await self.game.sync_games(season_id)

    async def sync_game_stats(self, game_id: int) -> dict:
        """
        Sync statistics for a specific game.

        Args:
            game_id: Game int ID

        Returns:
            Dict with team and player counts
        """
        return await self.game.sync_game_stats(game_id)

    async def sync_game_events(self, game_id: int) -> dict:
        """
        Sync events for a specific game.

        Args:
            game_id: Game int ID

        Returns:
            Dict with game_id and events_added count
        """
        return await self.game.sync_game_events(game_id)

    async def sync_all_game_events(self, season_id: int | None = None) -> dict:
        """
        Sync events for all games in a season.

        Args:
            season_id: Season ID (uses default if None)

        Returns:
            Dict with sync results
        """
        if season_id is not None and not await self.is_sync_enabled(season_id):
            logger.info(f"Season {season_id}: sync disabled, skipping all game events sync")
            return {"skipped": True, "reason": "sync disabled for season"}
        return await self.game.sync_all_game_events(season_id)

    async def sync_pre_game_lineup(self, game_id: int) -> dict[str, int]:
        """
        Sync pre-game lineup (referees, coaches, lineups) for a game.

        Args:
            game_id: Game int ID

        Returns:
            Dict with synced counts
        """
        return await self.lineup.sync_pre_game_lineup(game_id)

    async def sync_live_positions_and_kits(
        self,
        game_id: int,
        *,
        mode: str = "live_read",
        timeout_seconds: float | None = None,
        auto_commit: bool = True,
        touch_live_sync_timestamp: bool = True,
    ) -> dict:
        return await self.lineup.sync_live_positions_and_kits(
            game_id,
            mode=mode,
            timeout_seconds=timeout_seconds,
            auto_commit=auto_commit,
            touch_live_sync_timestamp=touch_live_sync_timestamp,
        )

    async def backfill_finished_games_positions_and_kits(
        self,
        *,
        season_id: int | None = None,
        batch_size: int = 100,
        limit: int | None = None,
        game_ids: list[str] | None = None,
        timeout_seconds: float | None = None,
    ) -> dict:
        return await self.lineup.backfill_finished_games_positions_and_kits(
            season_id=season_id,
            batch_size=batch_size,
            limit=limit,
            game_ids=game_ids,
            timeout_seconds=timeout_seconds,
        )

    # ==================== Stats sync methods ====================

    async def sync_score_table(self, season_id: int) -> int:
        """
        Sync league table for a season.

        Args:
            season_id: Season ID to sync

        Returns:
            Number of entries synced
        """
        if not await self.is_sync_enabled(season_id):
            logger.info(f"Season {season_id}: sync disabled, skipping score table sync")
            return 0
        logger.info(f"Syncing score table for season {season_id}")
        return await self.stats.sync_score_table(season_id)

    async def sync_team_season_stats(self, season_id: int) -> int:
        """
        Sync team season statistics.

        Args:
            season_id: Season ID to sync

        Returns:
            Number of team stats synced
        """
        if not await self.is_sync_enabled(season_id):
            logger.info(f"Season {season_id}: sync disabled, skipping team season stats sync")
            return 0
        logger.info(f"Syncing team season stats for season {season_id}")
        return await self.stats.sync_team_season_stats(season_id)

    # ==================== Full sync operations ====================

    async def full_sync(self, season_id: int) -> dict[str, Any]:
        """
        Perform full synchronization for a season.

        Syncs in order:
        1. Reference data (tournaments, seasons, teams)
        2. Players
        3. Games
        4. Score table
        5. Team season stats
        6. Player season stats

        Args:
            season_id: Season ID to sync

        Returns:
            Dict with sync results for each operation
        """
        if not await self.is_sync_enabled(season_id):
            logger.info(f"Season {season_id}: sync disabled, skipping full sync")
            return {"skipped": True, "reason": "sync disabled for season"}

        logger.info(f"Starting full sync for season {season_id}")
        results = {}

        # 1. Reference data
        ref_results = await self.sync_references()
        results.update(ref_results)

        # 2. Players
        results["players"] = await self.sync_players(season_id)

        # 3. Games
        results["games"] = await self.sync_games(season_id)

        # 4. Score table
        results["score_table"] = await self.sync_score_table(season_id)

        # 5. Team season stats
        results["team_season_stats"] = await self.sync_team_season_stats(season_id)

        # 6. Player season stats
        results["player_season_stats"] = await self.sync_player_stats(season_id)

        logger.info(f"Full sync complete for season {season_id}: {results}")
        return results

    async def sync_live_stats(self, season_id: int, game_ids: list[int]) -> dict[str, int]:
        """
        Sync statistics for specific games (used for live/recent games).

        Args:
            season_id: Season ID
            game_ids: List of game IDs to sync stats for

        Returns:
            Dict with sync counts
        """
        stats_synced = 0
        for game_id in game_ids:
            await self.sync_game_stats(game_id)
            stats_synced += 1

        return {"games_stats_synced": stats_synced}
