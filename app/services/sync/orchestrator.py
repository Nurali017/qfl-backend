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
from app.services.sync.player_sync import PlayerSyncService
from app.services.sync.game_sync import GameSyncService
from app.services.sync.lineup_sync import LineupSyncService
from app.services.sync.stats_sync import StatsSyncService
from app.services.sync.team_of_week_sync import TeamOfWeekSyncService
from app.services.sync.player_tour_stats_sync import PlayerTourStatsSyncService

logger = logging.getLogger(__name__)


class SyncOrchestrator:
    """
    Orchestrates sync operations across all sync services.

    Teams, players, and score table are managed locally.
    SOTA sync covers: game stats, game events, player/team season stats,
    best players, and live sync.
    """

    def __init__(self, db: AsyncSession, client: SotaClient | None = None):
        self.db = db
        self.client = client or get_sota_client()

        # Initialize specialized sync services
        self.player = PlayerSyncService(db, self.client)
        self.game = GameSyncService(db, self.client)
        self.lineup = LineupSyncService(db, self.client)
        self.stats = StatsSyncService(db, self.client)
        self.team_of_week = TeamOfWeekSyncService(db, self.client)
        self.player_tour = PlayerTourStatsSyncService(db, self.client)

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

    async def sync_player_stats(self, season_id: int, force: bool = False) -> int:
        if not force and not await self.is_sync_enabled(season_id):
            logger.info(f"Season {season_id}: sync disabled, skipping player stats sync")
            return 0
        logger.info(f"Syncing player stats for season {season_id}")
        return await self.player.sync_player_season_stats(season_id)

    async def sync_best_players(self, season_id: int, force: bool = False) -> int:
        """Sync goals + assists from best_players endpoint (lightweight, 2 API calls)."""
        if not force and not await self.is_sync_enabled(season_id):
            logger.info(f"Season {season_id}: sync disabled, skipping best_players sync")
            return 0
        return await self.player.sync_best_players(season_id)

    # ==================== Game sync methods ====================

    async def sync_games(self, season_id: int, force: bool = False) -> int:
        if not force and not await self.is_sync_enabled(season_id):
            logger.info(f"Season {season_id}: sync disabled, skipping games sync")
            return 0
        logger.info(f"Syncing games for season {season_id}")
        return await self.game.sync_games(season_id)

    async def sync_game_stats(self, game_id: int) -> dict:
        return await self.game.sync_game_stats(game_id)

    async def sync_game_events(self, game_id: int) -> dict:
        return await self.game.sync_game_events(game_id)

    async def sync_all_game_events(self, season_id: int | None = None, force: bool = False) -> dict:
        if not force and season_id is not None and not await self.is_sync_enabled(season_id):
            logger.info(f"Season {season_id}: sync disabled, skipping all game events sync")
            return {"skipped": True, "reason": "sync disabled for season"}
        return await self.game.sync_all_game_events(season_id)

    async def sync_pre_game_lineup(self, game_id: int) -> dict[str, int]:
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

    # ==================== Player tour stats sync ====================

    async def sync_player_tour_stats(self, season_id: int, tour: int, force: bool = False) -> int:
        if not force and not await self.is_sync_enabled(season_id):
            logger.info(f"Season {season_id}: sync disabled, skipping player tour stats sync")
            return 0
        logger.info(f"Syncing player tour stats for season {season_id}, tour {tour}")
        return await self.player_tour.sync_tour(season_id, tour)

    async def backfill_player_tour_stats(self, season_id: int, max_tour: int, force: bool = False) -> dict:
        if not force and not await self.is_sync_enabled(season_id):
            logger.info(f"Season {season_id}: sync disabled, skipping player tour stats backfill")
            return {"skipped": True, "reason": "sync disabled for season"}
        logger.info(f"Backfilling player tour stats for season {season_id}, tours 1..{max_tour}")
        return await self.player_tour.backfill_season(season_id, max_tour)

    # ==================== Stats sync methods ====================

    async def sync_team_season_stats(self, season_id: int, force: bool = False) -> int:
        if not force and not await self.is_sync_enabled(season_id):
            logger.info(f"Season {season_id}: sync disabled, skipping team season stats sync")
            return 0
        logger.info(f"Syncing team season stats for season {season_id}")
        return await self.stats.sync_team_season_stats(season_id)

    # ==================== Team of the Week sync ====================

    async def sync_team_of_week(
        self,
        season_id: int,
        force: bool = False,
        tour_keys: list[str] | None = None,
    ) -> dict:
        if not force and not await self.is_sync_enabled(season_id):
            logger.info(f"Season {season_id}: sync disabled, skipping team-of-week sync")
            return {"skipped": True, "reason": "sync disabled for season"}
        from app.config import get_settings
        if season_id not in get_settings().extended_stats_season_ids:
            logger.info(f"Season {season_id}: no extended stats, skipping team-of-week sync")
            return {"skipped": True, "reason": "no extended stats for season"}
        logger.info(f"Syncing team-of-week for season {season_id}")
        return await self.team_of_week.sync_team_of_week(season_id, tour_keys=tour_keys)

    # ==================== Full sync ====================

    async def full_sync(self, season_id: int, force: bool = False) -> dict[str, Any]:
        """
        Perform full synchronization for a season.

        Syncs game stats, game events, team/player season stats, and best players.
        Teams, players, and score table are managed locally.
        """
        if not force and not await self.is_sync_enabled(season_id):
            logger.info(f"Season {season_id}: sync disabled, skipping full sync")
            return {"skipped": True, "reason": "sync disabled for season"}

        logger.info(f"Starting full sync for season {season_id}")
        results: dict[str, Any] = {}

        # 1. Games
        results["games"] = await self.sync_games(season_id, force=force)

        # 2. Team season stats
        results["team_season_stats"] = await self.sync_team_season_stats(season_id, force=force)

        # 3. Player season stats
        results["player_season_stats"] = await self.sync_player_stats(season_id, force=force)

        # 4. Best players
        results["best_players"] = await self.sync_best_players(season_id, force=force)

        # 5. Team of the week
        results["team_of_week"] = await self.sync_team_of_week(season_id, force=force)

        logger.info(f"Full sync complete for season {season_id}: {results}")
        return results

    async def sync_live_stats(self, season_id: int, game_ids: list[int]) -> dict[str, int]:
        stats_synced = 0
        for game_id in game_ids:
            await self.sync_game_stats(game_id)
            stats_synced += 1

        return {"games_stats_synced": stats_synced}
