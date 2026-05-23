"""
Player tour stats sync service.

Syncs cumulative per-tour player statistics from SOTA API v2.
"""
import asyncio
import logging
import time

import httpx
import redis as redis_lib

from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import DBAPIError

# Advisory lock namespace for player_tour_stats writers.
# Separate from player_season_stats / team_season_stats namespaces.
_PLAYER_TOUR_STATS_LOCK_NS = 3
_LOCK_TIMEOUT = "60s"
_OUTER_LOCK_TTL_SECONDS = 7200
# See player_sync._WRITE_CHUNK_SIZE — fetch a chunk (no tx), then persist it.
_WRITE_CHUNK_SIZE = 500

from app.config import get_settings
from app.models import Player, PlayerTeam
from app.models.player_tour_stats import PlayerTourStats
from app.services.sync.base import BaseSyncService, PLAYER_SEASON_STATS_FIELDS
from app.services.sync.guardrails import (
    DeadSeasonCounters,
    SyncTimingMetrics,
    chunked,
    is_dead_season_pair,
    mark_dead_season_pair,
)
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

    @staticmethod
    def _has_useful_stats(stats: dict) -> bool:
        return bool(stats and stats.get("games_played"))

    async def _acquire_tx_locks(self, season_id: int) -> None:
        """Per-tx lock_timeout + advisory_xact_lock; taken once before the write phase."""
        await self.db.execute(text(f"SET LOCAL lock_timeout = '{_LOCK_TIMEOUT}'"))
        await self.db.execute(
            text("SELECT pg_advisory_xact_lock(:ns, :sid)"),
            {"ns": _PLAYER_TOUR_STATS_LOCK_NS, "sid": season_id},
        )

    async def sync_tour(self, season_id: int, tour: int) -> int:
        """
        Sync cumulative stats for all players in a season for a given tour.

        Returns:
            Number of player stats rows upserted
        """
        settings = get_settings()
        # Outer-job mutex keyed on (season, tour). Per-tx advisory lock is
        # released on every batch commit; Redis lease keeps a parallel
        # invocation from slipping in between batches.
        lock_key = f"qfl:sync-lock:player_tour_stats:{season_id}:{tour}"
        redis_client = redis_lib.from_url(settings.redis_url)
        if not redis_client.set(lock_key, "1", nx=True, ex=_OUTER_LOCK_TTL_SECONDS):
            logger.info(
                "sync_tour skipped for season %d tour %d: outer lock held",
                season_id, tour,
            )
            return 0
        try:
            return await self._sync_tour_locked(season_id, tour)
        finally:
            try:
                redis_client.delete(lock_key)
            except Exception as exc:
                logger.warning(
                    "Failed to release outer lock for season %d tour %d: %s",
                    season_id, tour, exc,
                )

    async def _sync_tour_locked(self, season_id: int, tour: int) -> int:
        """Collect-then-write: fetch every player's tour stats from SOTA with no
        transaction open, then persist them under one short write transaction.

        See player_sync._sync_player_season_stats_locked for the rationale —
        keeping HTTP out of the transaction is what avoids idle-in-transaction
        pool starvation and long advisory-lock holds.
        """
        # Phase A — reads, then commit to release the connection for the fetch.
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
            await self.db.rollback()  # leave the session clean for the caller
            return 0
        sota_season_ids = await self.get_all_sota_season_ids(season_id)
        await self.db.commit()

        settings = get_settings()
        timings = SyncTimingMetrics(enabled=settings.debug_sync_timings)

        # Dead-pair state persists across chunks (a pair marked dead early is
        # skipped for the rest of the season).
        dead_counters = {sid: DeadSeasonCounters() for sid in sota_season_ids}
        dead_sota_ids: set[int] = set()
        logged_dead_pairs: set[tuple[int, int]] = set()

        # Fetch a chunk (no tx open), then persist it under one short write tx.
        count = 0
        for chunk in chunked(player_teams, _WRITE_CHUNK_SIZE):
            collected = await self._fetch_tour_stats(
                season_id, tour, chunk, sota_season_ids, timings,
                dead_counters, dead_sota_ids, logged_dead_pairs,
            )
            count += await self._write_tour_stats(season_id, tour, collected, timings)

        timings.log_summary(
            service="player_tour_stats",
            season_id=season_id,
            extra={"tour": tour},
        )
        logger.info(
            "Synced %d player tour stats for season %d, tour %d",
            count, season_id, tour,
        )
        return count

    async def _write_tour_stats(
        self,
        season_id: int,
        tour: int,
        collected: list[tuple[int, int, dict]],
        timings: SyncTimingMetrics,
    ) -> int:
        """Persist one chunk of collected tour stats under a short write tx.

        Skips the advisory lock / transaction entirely when nothing was
        collected.
        """
        if not collected:
            return 0
        await self._acquire_tx_locks(season_id)
        count = 0
        for player_id, team_id, stats in collected:
            stmt = self._build_tour_upsert(player_id, team_id, season_id, tour, stats)
            db_started = time.monotonic()
            try:
                async with self.db.begin_nested():
                    await self.db.execute(stmt)
            except DBAPIError as db_exc:
                logger.warning(
                    "DB error upserting player_tour_stats for player %d season %d tour %d: %s",
                    player_id, season_id, tour, db_exc,
                )
                continue
            timings.add_db(time.monotonic() - db_started)
            count += 1
        await self.db.commit()
        return count

    async def _fetch_tour_stats(
        self,
        season_id: int,
        tour: int,
        player_teams: list,
        sota_season_ids: list[int],
        timings: SyncTimingMetrics,
        dead_counters: dict,
        dead_sota_ids: set[int],
        logged_dead_pairs: set,
    ) -> list[tuple[int, int, dict]]:
        """Fetch tour stats for a chunk of players from SOTA (no DB transaction).

        Returns ``(player_id, team_id, stats)`` only for players with useful
        stats. Keeps the small per-player sleep to stay polite to SOTA.
        Dead-pair state is passed in so it persists across chunks.
        """
        collected: list[tuple[int, int, dict]] = []

        for player_id, team_id, sota_id in player_teams:
            timings.players_processed += 1
            try:
                stats = await self._fetch_one_tour_stats(
                    season_id, tour, sota_id, sota_season_ids,
                    dead_counters, dead_sota_ids, logged_dead_pairs, timings,
                )
            except Exception as e:
                logger.warning(
                    "Failed to fetch tour stats for player %d, season %d, tour %d: %s",
                    player_id, season_id, tour, e,
                )
                stats = {}
            if self._has_useful_stats(stats):
                collected.append((player_id, team_id, stats))

            sleep_started = time.monotonic()
            await asyncio.sleep(0.15)
            timings.add_sleep(time.monotonic() - sleep_started)
        return collected

    async def _fetch_one_tour_stats(
        self,
        season_id: int,
        tour: int,
        sota_id: int,
        sota_season_ids: list[int],
        dead_counters: dict,
        dead_sota_ids: set[int],
        logged_dead_pairs: set,
        timings: SyncTimingMetrics,
    ) -> dict:
        """Try each SOTA season id until useful tour stats are found."""
        stats: dict = {}
        for sid in sota_season_ids:
            pair = (season_id, sid)
            if sid in dead_sota_ids or await is_dead_season_pair(season_id, sid):
                dead_sota_ids.add(sid)
                if pair not in logged_dead_pairs:
                    logger.info(
                        "Skipping dead SOTA pair for player tour stats local=%s sota=%s",
                        season_id, sid,
                    )
                    logged_dead_pairs.add(pair)
                continue

            fetch_started = time.monotonic()
            try:
                stats = await self.client.get_player_game_stats_v2_by_tour(
                    str(sota_id), sid, tour
                )
            except httpx.HTTPStatusError as exc:
                timings.add_fetch(time.monotonic() - fetch_started)
                if exc.response is not None and exc.response.status_code == 404:
                    counters = dead_counters[sid]
                    counters.record_not_found()
                    timings.not_found_count += 1
                    if counters.should_mark_dead():
                        await mark_dead_season_pair(season_id, sid)
                        dead_sota_ids.add(sid)
                        if pair not in logged_dead_pairs:
                            logger.warning(
                                "Marked dead SOTA pair for player tour stats local=%s sota=%s attempted=%s not_found=%s",
                                season_id, sid, counters.attempted, counters.not_found,
                            )
                            logged_dead_pairs.add(pair)
                    continue
                raise
            timings.add_fetch(time.monotonic() - fetch_started)
            if self._has_useful_stats(stats):
                dead_counters[sid].record_success()
                timings.success_count += 1
                break
            dead_counters[sid].record_empty()
        return stats

    def _build_tour_upsert(
        self, player_id: int, team_id: int, season_id: int, tour: int, stats: dict
    ):
        """Build the PlayerTourStats upsert statement for one player."""
        # Separate known columns from extra stats
        extra_stats = {
            k: v for k, v in stats.items()
            if k not in PLAYER_SEASON_STATS_FIELDS
        }

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
            updated_at=utcnow(),
        )
        return stmt.on_conflict_do_update(
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
