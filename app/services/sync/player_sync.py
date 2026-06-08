"""
Player sync service.

Handles synchronization of player season statistics from SOTA API.
Player profiles (top_role) are managed locally — no longer synced from SOTA.
"""
import logging
import time

import httpx
import redis as redis_lib

from sqlalchemy import select, text, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import DBAPIError

# Advisory lock namespaces for player_season_stats writers.
# sync_player_season_stats / sync_extended_stats_for_game touch the full 45+ column set
# and must serialize against each other (NS=1).
# sync_best_players only touches goal/goal_pass/dry_match (+ their *_rank columns);
# it never collides at the column level with the full sync, so it gets its own
# namespace (NS=2). Why: the full sync calls SOTA v2 per-player and can hold the
# transaction "idle" for >60s, which caused best_players to fail by lock_timeout
# for women's / 1L / 2L seasons and leave *_rank=NULL, hiding players from the
# leaderboard endpoint.
_PLAYER_STATS_LOCK_NS = 1
_BEST_PLAYERS_LOCK_NS = 2
# Bound advisory-lock wait so the task doesn't hold a Celery soft-time-limit slot
# blocked on a stuck writer. On timeout asyncpg raises LockNotAvailableError → Celery retry.
_LOCK_TIMEOUT = "60s"
# Outer-job mutex TTL (Redis). sync_player_season_stats / sync_player_tour_stats
# rarely take >30 min even on slow SOTA; 2h is a defensive ceiling so a crashed
# worker can't permanently lock the season.
_OUTER_LOCK_TTL_SECONDS = 7200
# Write-phase chunk size: fetch this many players (no tx open), persist them in
# one short write transaction, then move to the next chunk. Kept small so the
# write tx commits frequently — row locks on player_season_stats are released
# every chunk, so a concurrent sync_best_players (NS=2, not serialized against
# NS=1) isn't blocked long enough to hit lock_timeout. (A larger chunk held
# locks for minutes under contention and timed best_players out.)
_WRITE_CHUNK_SIZE = 50
# Max players to fetch per best_players metric. SOTA defaults to 100, but a
# season can have more scorers than that (e.g. 112 in PL-2026), and the
# leaderboard endpoint filters on *_rank IS NOT NULL — so any scorer beyond
# the top 100 gets *_rank=NULL and silently disappears from the "Голы" table.
# Set comfortably above the active-roster size so every ranked player is kept.
_BEST_PLAYERS_MAX = 1000

from app.models import Player, PlayerTeam, PlayerSeasonStats
from app.services.sync.guardrails import (
    DeadSeasonCounters,
    SyncTimingMetrics,
    chunked,
    is_dead_season_pair,
    mark_dead_season_pair,
)
from app.services.sync.base import BaseSyncService, PLAYER_SEASON_STATS_FIELDS
from app.config import get_settings
from app.utils.timestamps import utcnow

logger = logging.getLogger(__name__)


class PlayerSyncService(BaseSyncService):
    """
    Service for syncing player statistics.

    Handles:
    - Best players (goals/assists from lightweight endpoint)
    - Player season statistics (50+ metrics from v2 API)
    """

    @staticmethod
    def _has_useful_stats(stats: dict) -> bool:
        return bool(stats and stats.get("games_played"))

    async def sync_best_players(self, season_id: int) -> int:
        """
        Sync goals and assists from the best_players endpoint (single API call per metric).

        Only updates goals/assists columns in PlayerSeasonStats — does not overwrite
        the other 50+ stat columns that full_sync populates.

        Returns:
            Number of player stats rows upserted
        """
        # Phase A — reads. Resolve SOTA season ids and the active player→team
        # lookup, then commit so the connection is free during the HTTP fetch.
        # (Decouples HTTP from the advisory-locked transaction — the incident
        # that motivated this refactor explicitly involved sync_best_players.)
        sota_season_ids = await self.get_all_sota_season_ids(season_id)
        player_teams_result = await self.db.execute(
            select(PlayerTeam.player_id, PlayerTeam.team_id, Player.sota_id)
            .join(Player, Player.id == PlayerTeam.player_id)
            .where(
                PlayerTeam.season_id == season_id,
                PlayerTeam.is_active == True,
                Player.sota_id.is_not(None),
            )
        )
        lookup: dict[str, tuple[int, int]] = {
            str(sota_id): (player_id, team_id)
            for player_id, team_id, sota_id in player_teams_result.fetchall()
        }
        await self.db.commit()
        if not lookup:
            logger.info("No active player-team mappings for season %d", season_id)
            return 0

        # Phase B — fetch top scorers, assisters, clean sheets (no transaction open).
        scorers: list = []
        assisters: list = []
        keepers: list = []
        for sota_season_id in sota_season_ids:
            try:
                scorers.extend(await self.client.get_best_players(sota_season_id, metric="goal", max_players=_BEST_PLAYERS_MAX))
            except Exception as e:
                logger.warning("Failed to fetch best scorers for season %d (sota %d): %s", season_id, sota_season_id, e)

            try:
                assisters.extend(await self.client.get_best_players(sota_season_id, metric="goal_pass", max_players=_BEST_PLAYERS_MAX))
            except Exception as e:
                logger.warning("Failed to fetch best assisters for season %d (sota %d): %s", season_id, sota_season_id, e)

            try:
                keepers.extend(await self.client.get_best_players(sota_season_id, metric="dry_match", max_players=_BEST_PLAYERS_MAX))
            except Exception as e:
                logger.warning("Failed to fetch best keepers for season %d (sota %d): %s", season_id, sota_season_id, e)

        if not scorers and not assisters and not keepers:
            logger.info("No best_players data for season %d, skipping", season_id)
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
        # Phase C — write under the best_players advisory lock (NS=2).
        await self.db.execute(text(f"SET LOCAL lock_timeout = '{_LOCK_TIMEOUT}'"))
        await self.db.execute(
            text("SELECT pg_advisory_xact_lock(:ns, :sid)"),
            {"ns": _BEST_PLAYERS_LOCK_NS, "sid": season_id},
        )
        # Track which players we actually upsert so we can null-out ranks only
        # for the "orphaned" rest in a single WHERE NOT IN sweep at the end.
        # Order matters: do the upserts first (each row gets its fresh rank),
        # then issue the targeted reset. The previous version did a blanket
        # UPDATE … WHERE season_id=X before the upsert loop, which grabbed
        # row locks on every player_season_stats row for that season and held
        # them for the full duration of the loop — that's what blocked
        # concurrent writers (sync_player_season_stats) with lock_timeout.
        upserted_player_ids: set[int] = set()
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
            try:
                async with self.db.begin_nested():
                    await self.db.execute(stmt)
                count += 1
                upserted_player_ids.add(player_id)
            except DBAPIError as exc:
                # Fail the whole task — Celery will retry, and the framework
                # rollback restores the prior leaderboard state.
                logger.error(
                    "Aborting best_players sync for season %s after DB error on player %s: %s",
                    season_id, player_id, exc,
                )
                await self.db.rollback()
                raise

        # Targeted reset: only rows NOT just upserted. Holds row locks for a
        # fraction of a second on (typically) a small set, instead of locking
        # the entire season's player_season_stats up front.
        reset_stmt = (
            update(PlayerSeasonStats)
            .where(PlayerSeasonStats.season_id == season_id)
            .values(goal_rank=None, goal_pass_rank=None, dry_match_rank=None)
        )
        if upserted_player_ids:
            reset_stmt = reset_stmt.where(PlayerSeasonStats.player_id.notin_(upserted_player_ids))
        await self.db.execute(reset_stmt)

        await self.db.commit()
        logger.info("Synced best_players for season %d: %d rows upserted", season_id, count)
        return count

    async def _acquire_player_stats_tx_locks(self, season_id: int) -> None:
        """Set per-tx lock_timeout and take the per-season advisory lock.

        Called once at the start of the write phase (after the HTTP fetch phase
        has already collected every player's stats with no transaction open).
        Both SET LOCAL and pg_advisory_xact_lock are transaction-scoped and are
        released by the single commit that ends the write phase. Concurrent
        sync_best_players (NS=2) is on a different namespace; only
        sync_player_season_stats itself can race here (NS=1).
        """
        await self.db.execute(text(f"SET LOCAL lock_timeout = '{_LOCK_TIMEOUT}'"))
        await self.db.execute(
            text("SELECT pg_advisory_xact_lock(:ns, :sid)"),
            {"ns": _PLAYER_STATS_LOCK_NS, "sid": season_id},
        )

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
        # Outer-job mutex. sync_player_stats is invoked from at least three
        # paths (_sync_extended_aggregate_bundle, _sync_season_aggregates, admin
        # force=True). The per-tx advisory lock inside this function is
        # released on every batch commit, which would otherwise let a parallel
        # invocation slip in between batches. The Redis lease guards against
        # that without depending on advisory-lock timing.
        settings = get_settings()
        lock_key = f"qfl:sync-lock:player_season_stats:{season_id}"
        redis_client = redis_lib.from_url(settings.redis_url)
        if not redis_client.set(lock_key, "1", nx=True, ex=_OUTER_LOCK_TTL_SECONDS):
            logger.info(
                "sync_player_season_stats skipped for season %d: outer lock held",
                season_id,
            )
            return 0
        try:
            return await self._sync_player_season_stats_locked(season_id)
        finally:
            try:
                redis_client.delete(lock_key)
            except Exception as exc:
                logger.warning("Failed to release outer lock for season %d: %s", season_id, exc)

    async def _sync_player_season_stats_locked(self, season_id: int) -> int:
        """Collect-then-write: fetch every player's stats from SOTA with no
        transaction open, then persist them under one short write transaction.

        Decoupling HTTP from the transaction is what keeps connections out of
        ``idle in transaction`` and shrinks the advisory-lock hold time to just
        the (fast, local) write phase — see the extended-stats incident notes.
        """
        # Phase A — reads. Resolve the player roster and SOTA season ids, then
        # commit so the connection returns to the pool for the fetch phase.
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
            await self.db.rollback()  # leave the session clean for the caller
            return 0
        # Resolve all SOTA season IDs (usually 1, but 2L has SW+NE)
        sota_season_ids = await self.get_all_sota_season_ids(season_id)
        await self.db.commit()

        settings = get_settings()
        timings = SyncTimingMetrics(enabled=settings.debug_sync_timings)

        # Dead-pair state must persist across chunks so a pair marked dead in an
        # early chunk is skipped for the rest of the season.
        dead_counters = {sid: DeadSeasonCounters() for sid in sota_season_ids}
        dead_sota_ids: set[int] = set()
        logged_dead_pairs: set[tuple[int, int]] = set()

        # Process in chunks: fetch a chunk (no tx open), then persist it under one
        # short write transaction. Bounds RAM and write-tx/advisory-lock hold time.
        count = 0
        for chunk in chunked(player_teams, _WRITE_CHUNK_SIZE):
            collected = await self._fetch_player_season_stats(
                season_id, chunk, sota_season_ids, timings,
                dead_counters, dead_sota_ids, logged_dead_pairs,
            )
            count += await self._write_season_stats(season_id, collected, timings)

        timings.log_summary(service="player_season_stats", season_id=season_id)
        logger.info(f"Synced {count} player season stats for season {season_id}")
        return count

    async def _write_season_stats(
        self,
        season_id: int,
        collected: list[tuple[int, int, dict]],
        timings: SyncTimingMetrics,
    ) -> int:
        """Persist one chunk of collected season stats under a short write tx.

        Skips the advisory lock / transaction entirely when nothing was
        collected, so an all-empty pass never opens a write transaction.
        """
        if not collected:
            return 0
        await self._acquire_player_stats_tx_locks(season_id)
        count = 0
        for player_id, team_id, stats in collected:
            stmt = self._build_season_upsert(player_id, team_id, season_id, stats)
            db_started = time.monotonic()
            try:
                async with self.db.begin_nested():
                    await self.db.execute(stmt)
            except DBAPIError as db_exc:
                logger.warning(
                    "DB error upserting player_season_stats for player %s: %s",
                    player_id, db_exc,
                )
                continue
            timings.add_db(time.monotonic() - db_started)
            count += 1
        await self.db.commit()
        return count

    async def _fetch_player_season_stats(
        self,
        season_id: int,
        player_teams: list,
        sota_season_ids: list[int],
        timings: SyncTimingMetrics,
        dead_counters: dict,
        dead_sota_ids: set[int],
        logged_dead_pairs: set,
    ) -> list[tuple[int, int, dict]]:
        """Fetch season stats for a chunk of players from SOTA (no DB transaction).

        Returns ``(player_id, team_id, stats)`` only for players with useful
        stats. Network errors for a single player are logged and skipped.
        Dead-pair state is passed in so it persists across chunks.
        """
        collected: list[tuple[int, int, dict]] = []

        for player_id, team_id, sota_id in player_teams:
            timings.players_processed += 1
            try:
                stats = await self._fetch_one_player_season_stats(
                    season_id, sota_id, sota_season_ids,
                    dead_counters, dead_sota_ids, logged_dead_pairs, timings,
                )
            except Exception as e:
                logger.warning(f"Failed to fetch player season stats for player {player_id}: {e}")
                continue
            if self._has_useful_stats(stats):
                collected.append((player_id, team_id, stats))
        return collected

    async def _fetch_one_player_season_stats(
        self,
        season_id: int,
        sota_id: int,
        sota_season_ids: list[int],
        dead_counters: dict,
        dead_sota_ids: set[int],
        logged_dead_pairs: set,
        timings: SyncTimingMetrics,
    ) -> dict:
        """Try each SOTA season id (player belongs to one conference) until
        useful stats are found. Returns ``{}`` when none yield stats."""
        stats: dict = {}
        for sid in sota_season_ids:
            pair = (season_id, sid)
            if sid in dead_sota_ids or await is_dead_season_pair(season_id, sid):
                dead_sota_ids.add(sid)
                if pair not in logged_dead_pairs:
                    logger.info(
                        "Skipping dead SOTA pair for player season stats local=%s sota=%s",
                        season_id, sid,
                    )
                    logged_dead_pairs.add(pair)
                continue

            fetch_started = time.monotonic()
            try:
                stats = await self.client.get_player_season_stats(str(sota_id), sid)
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
                                "Marked dead SOTA pair for player season stats local=%s sota=%s attempted=%s not_found=%s",
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

    def _build_season_upsert(self, player_id: int, team_id: int, season_id: int, stats: dict):
        """Build the PlayerSeasonStats upsert statement for one player."""
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
        return stmt.on_conflict_do_update(
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
