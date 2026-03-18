"""
Celery tasks for live match synchronization.

Tasks:
- auto_start_live_games: Auto-start live tracking for games whose scheduled time has passed
- sync_live_game_events: Dispatcher — fans out sync_single_game per active game
- sync_single_game: Sync events, lineup, and stats for one game (tokenized lock)
- auto_end_finished_games: Auto-end games that have been live for over 2h15m
- sync_post_match_protocol: Re-sync events & stats for recently finished games
- post_finish_followup: Post-match pipeline (resync, tour check, extended stats)
- sync_extended_stats_for_game: Game-scoped extended stats sync
- check_finished_without_timestamp: Guardrail — alert on broken finished games
"""
import logging
import time
import uuid
from datetime import timedelta

from sqlalchemy import select, func, case

from app.tasks import celery_app
from app.database import AsyncSessionLocal
from app.models import Game, GameStatus
from app.services.live_sync_service import LiveSyncService
from app.services.sota_client import get_sota_client
from app.services.telegram import send_telegram_message
from app.utils.async_celery import run_async
from app.utils.timestamps import ensure_naive_utc, utcnow

logger = logging.getLogger(__name__)

# Per-game lock TTL — must cover queue wait + worst-case sync (observed peak: 45s)
# 90s = ~30s queue headroom + 60s sync headroom
_LOCK_KEY_PREFIX = "qfl:live-sync:game"
_LOCK_TTL = 90

# Lua script: delete key only if its value matches our token.
# Prevents a late-finishing worker from deleting a lock that was
# already re-acquired by a newer dispatch cycle.
_CAS_DELETE_SCRIPT = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
else
    return 0
end
"""


async def _acquire_token_lock(key: str, ttl: int) -> str | None:
    """SET key <token> NX EX ttl.  Returns token on success, None if held."""
    token = uuid.uuid4().hex
    try:
        from app.utils.live_flag import get_redis
        r = await get_redis()
        ok = await r.set(key, token, nx=True, ex=ttl)
        return token if ok else None
    except Exception:
        # Fail open — return a token so the task proceeds
        return token


async def _release_token_lock(key: str, token: str) -> None:
    """Compare-and-delete: remove key only if it still holds our token."""
    try:
        from app.utils.live_flag import get_redis
        r = await get_redis()
        await r.eval(_CAS_DELETE_SCRIPT, 1, key, token)
    except Exception:
        pass


async def _sync_single_game_impl(game_id: int, token: str):
    """Sync all live data for a single game.

    The dispatcher reserved this game by SET NX with `token` before enqueuing.
    On completion (or failure) we compare-and-delete so only *our* reservation
    is removed.  If the TTL expired and a new cycle already re-reserved the
    key with a different token, our delete is a no-op.
    """
    lock_key = f"{_LOCK_KEY_PREFIX}:{game_id}"
    t0 = time.monotonic()
    try:
        async with AsyncSessionLocal() as db:
            try:
                client = get_sota_client()
                service = LiveSyncService(db, client)

                # Steps run sequentially because all 5 methods mutate the same
                # Game object via shared AsyncSession and call db.commit().
                # asyncio.gather() would cause lost updates.
                # Inter-game parallelism (separate Celery tasks) is the main win;
                # intra-game parallelism requires splitting fetch/apply phases
                # in LiveSyncService (future refactor).
                sync_result = await service.sync_live_events(game_id)
                events_added = sync_result.get("added", 0)

                try:
                    await service.sync_live_time(game_id)
                except Exception as time_err:
                    logger.warning("Failed to sync live time for game %s: %s", game_id, time_err)

                try:
                    await service.sync_live_lineup(game_id)
                except Exception as lineup_err:
                    logger.warning("Failed to sync lineup for game %s: %s", game_id, lineup_err)

                try:
                    await service.sync_live_stats(game_id)
                except Exception as stats_err:
                    logger.warning("Failed to sync stats for game %s: %s", game_id, stats_err)

                try:
                    await service.sync_live_player_stats(game_id)
                except Exception as ps_err:
                    logger.warning("Failed to sync player stats for game %s: %s", game_id, ps_err)

                await db.commit()

                elapsed = time.monotonic() - t0
                if events_added:
                    logger.info("Synced %d new events for game %s", events_added, game_id)
                logger.info("sync_single_game(%s) completed in %.1fs", game_id, elapsed)

                return {
                    "game_id": game_id,
                    "new_events": events_added,
                    "updated_events": sync_result.get("updated", 0),
                    "deleted_events": sync_result.get("deleted", 0),
                    "elapsed": round(elapsed, 1),
                }
            except Exception:
                await db.rollback()
                raise
    except Exception as e:
        elapsed = time.monotonic() - t0
        logger.error("Failed to sync game %s in %.1fs: %s", game_id, elapsed, e)
        return {"game_id": game_id, "error": str(e), "elapsed": round(elapsed, 1)}
    finally:
        await _release_token_lock(lock_key, token)


async def _sync_live_game_events():
    """Dispatcher: get active game IDs, reserve + dispatch per-game tasks.

    For each game, does an atomic SET <key> <token> NX EX 90 *before* calling
    .delay(game_id, token).  This covers the full task lifecycle
    (queued → running → done) with no gap where the game appears unlocked.

    The worker receives the token and does a compare-and-delete in its finally
    block, so a late-finishing worker cannot accidentally remove a reservation
    that belongs to a newer dispatch cycle.

    If a .delay() call fails after SET NX succeeded, we immediately
    compare-and-delete to avoid leaving an orphaned reservation.
    If the task is lost (ack failure, broker crash), the TTL auto-expires.
    """
    from app.utils.live_flag import has_live_games, set_live_flag, clear_live_flag

    if not await has_live_games():
        return {"active_games": 0, "total_new_events": 0, "results": [], "skipped": True}

    async with AsyncSessionLocal() as db:
        try:
            client = get_sota_client()
            service = LiveSyncService(db, client)

            active_games = await service.get_active_games()

            if not active_games:
                await clear_live_flag()
                await db.commit()
                return {"active_games": 0, "total_new_events": 0, "results": []}

            # Refresh flag TTL while games are live
            await set_live_flag()
            await db.commit()

            game_ids = [g.id for g in active_games]
            dispatched = []
            already_locked = []
            for gid in game_ids:
                lock_key = f"{_LOCK_KEY_PREFIX}:{gid}"
                token = await _acquire_token_lock(lock_key, _LOCK_TTL)
                if token is None:
                    # Key already held (queued or running from prior cycle)
                    already_locked.append(gid)
                    continue
                try:
                    sync_single_game.delay(gid, token)
                    dispatched.append(gid)
                except Exception:
                    # .delay() failed — clean up the reservation we just made
                    await _release_token_lock(lock_key, token)
                    logger.exception("Failed to enqueue sync for game %s", gid)

            if dispatched:
                logger.info("Dispatched sync for games: %s", dispatched)
            if already_locked:
                logger.debug("Games still queued/running, skipped: %s", already_locked)

            return {
                "active_games": len(game_ids),
                "dispatched": dispatched,
                "already_locked": already_locked,
            }
        except Exception:
            await db.rollback()
            raise


async def _auto_start_live_games():
    """Find games whose scheduled time has passed and start live tracking."""
    from app.services.game_lifecycle import GameLifecycleService

    async with AsyncSessionLocal() as db:
        try:
            client = get_sota_client()
            service = LiveSyncService(db, client)

            games = await service.get_games_to_start()
            if not games:
                await db.commit()
                return {"started": 0, "results": []}

            lifecycle = GameLifecycleService(db)
            results = []
            for game in games:
                try:
                    result = await lifecycle.start_live(game.id)
                    results.append(result)
                    logger.info("Auto-started live tracking for game %s", game.id)
                except Exception as e:
                    logger.error("Failed to auto-start game %s: %s", game.id, e)
                    results.append({"game_id": game.id, "error": str(e)})

            await db.commit()
            return {
                "started": len([r for r in results if "error" not in r]),
                "results": results,
            }
        except Exception:
            await db.rollback()
            raise


async def _auto_end_finished_games():
    """End games that have been live for over 2h6m.

    Primary source: half1_started_at. Fallback: date + time.
    Routes through GameLifecycleService.finish_live.
    """
    from app.services.game_lifecycle import GameLifecycleService

    async with AsyncSessionLocal() as db:
        try:
            client = get_sota_client()
            service = LiveSyncService(db, client)
            games = await service.get_games_to_end()
            if not games:
                await db.commit()
                return {"ended": 0, "results": []}
            lifecycle = GameLifecycleService(db)
            results = []
            for game in games:
                try:
                    await lifecycle.finish_live(game.id)
                    results.append({"game_id": game.id, "status": "ended"})
                    logger.info("Auto-ended game %s", game.id)
                except Exception as e:
                    logger.error("Failed to auto-end game %s: %s", game.id, e)
                    results.append({"game_id": game.id, "error": str(e)})
            await db.commit()
            return {"ended": len([r for r in results if "status" in r]), "results": results}
        except Exception:
            await db.rollback()
            raise


async def _sync_post_match_protocol():
    """Re-sync events & stats for recently finished games (within 6 hours)."""
    cutoff = utcnow() - timedelta(hours=6)

    async with AsyncSessionLocal() as db:
        try:
            result = await db.execute(
                select(Game).where(
                    Game.status == GameStatus.finished,
                    Game.finished_at.isnot(None),
                    Game.finished_at >= cutoff,
                    Game.sota_id.isnot(None),
                    Game.sync_disabled == False,
                )
            )
            games = list(result.scalars().all())
            if not games:
                return {"synced": 0}

            client = get_sota_client()
            service = LiveSyncService(db, client)
            changes_summary = []

            for game in games:
                try:
                    events = await service.sync_live_events(game.id)
                    await service.sync_live_stats(game.id)
                    await service.sync_live_player_stats(game.id)

                    has_changes = (
                        events.get("added", 0) > 0
                        or events.get("updated", 0) > 0
                        or events.get("deleted", 0) > 0
                    )
                    if has_changes:
                        changes_summary.append({
                            "game_id": game.id,
                            "events": events,
                        })
                except Exception:
                    logger.exception("Post-match sync failed for game %s", game.id)

            if changes_summary:
                lines = ["\U0001f4cb \u041e\u0431\u043d\u043e\u0432\u043b\u0435\u043d\u0438\u0435 \u043f\u0440\u043e\u0442\u043e\u043a\u043e\u043b\u0430 \u043f\u043e\u0441\u043b\u0435 \u043c\u0430\u0442\u0447\u0430\n"]
                for ch in changes_summary:
                    ev = ch["events"]
                    lines.append(
                        f"\U0001f3df Game #{ch['game_id']}: "
                        f"+{ev.get('added', 0)} / ~{ev.get('updated', 0)} / -{ev.get('deleted', 0)} \u0441\u043e\u0431\u044b\u0442\u0438\u0439"
                    )
                await send_telegram_message("\n".join(lines))

            await db.commit()
            return {"synced": len(games), "changes": len(changes_summary)}
        except Exception:
            await db.rollback()
            raise


async def _fetch_pregame_lineups():
    """Fetch lineups from /em/ for games starting within 30 minutes."""
    async with AsyncSessionLocal() as db:
        try:
            client = get_sota_client()
            service = LiveSyncService(db, client)

            games = await service.get_games_for_pregame_lineup()
            if not games:
                await db.commit()
                return {"fetched": 0, "results": []}

            results = []
            for game in games:
                try:
                    result = await service.sync_pregame_lineup(game.id, sota_only=True)
                    results.append(result)
                    lineup_count = result.get("lineup_count", 0)
                    if lineup_count > 0:
                        logger.info(
                            "Pre-fetched lineup for game %s: %d players",
                            game.id, lineup_count,
                        )
                except Exception as e:
                    logger.warning(
                        "Failed to pre-fetch lineup for game %s: %s",
                        game.id, e,
                    )
                    results.append({"game_id": game.id, "error": str(e)})

            await db.commit()
            return {
                "fetched": len([r for r in results if r.get("lineup_count", 0) > 0]),
                "attempted": len(games),
                "results": results,
            }
        except Exception:
            await db.rollback()
            raise


# ==================== Celery Tasks ====================


@celery_app.task(name="app.tasks.live_tasks.auto_start_live_games")
def auto_start_live_games():
    """Celery task: Auto-start live tracking when game time arrives. Runs every 2 minutes."""
    return run_async(_auto_start_live_games())


@celery_app.task(name="app.tasks.live_tasks.sync_live_game_events")
def sync_live_game_events():
    """Celery task: Dispatcher — fans out per-game sync tasks. Runs every 15 seconds."""
    return run_async(_sync_live_game_events())


@celery_app.task(name="app.tasks.live_tasks.sync_single_game")
def sync_single_game(game_id: int, token: str):
    """Celery task: Sync events/lineup/stats for one game. Dispatched by sync_live_game_events."""
    return run_async(_sync_single_game_impl(game_id, token))


@celery_app.task(name="app.tasks.live_tasks.auto_end_finished_games")
def auto_end_finished_games():
    """Celery task: Auto-end games that have been live for over 2h15m. Runs every 5 minutes."""
    return run_async(_auto_end_finished_games())


@celery_app.task(name="app.tasks.live_tasks.sync_post_match_protocol")
def sync_post_match_protocol():
    """Celery task: Re-sync protocol for recently finished games. Runs every 30 minutes."""
    return run_async(_sync_post_match_protocol())


@celery_app.task(name="app.tasks.live_tasks.fetch_pregame_lineups")
def fetch_pregame_lineups():
    """Celery task: Pre-fetch lineups from /em/ for upcoming games. Runs every 3 minutes."""
    return run_async(_fetch_pregame_lineups())


# ==================== Post-finish follow-up ====================


async def _post_finish_followup(game_id: int):
    """Post-match pipeline: resync, tour completion check, extended stats scheduling."""
    from app.utils.live_flag import get_redis

    redis = await get_redis()

    # Dedupe by game_id (5 min TTL)
    dedup_key = f"qfl:post_finish:{game_id}"
    if not await redis.set(dedup_key, "1", nx=True, ex=300):
        logger.info("post_finish_followup(%s) already running, skipping", game_id)
        return {"game_id": game_id, "skipped": True}

    async with AsyncSessionLocal() as db:
        try:
            game = await db.get(Game, game_id)
            if not game:
                return {"error": f"Game {game_id} not found"}

            # 1. Immediate post-match resync
            if game.sota_id and not game.sync_disabled:
                try:
                    client = get_sota_client()
                    svc = LiveSyncService(db, client)
                    await svc.sync_live_events(game_id)
                    await svc.sync_live_stats(game_id)
                    await svc.sync_live_player_stats(game_id)
                    logger.info("Post-finish resync completed for game %s", game_id)
                except Exception:
                    logger.exception("Post-finish resync failed for game %s", game_id)

            # 2. Tour completion check (deduped by season_id/tour)
            if game.season_id and game.tour is not None:
                tour_key = f"qfl:post_finish_tour:{game.season_id}:{game.tour}"
                if await redis.set(tour_key, "1", nx=True, ex=3600):
                    try:
                        from app.tasks.sync_tasks import check_tour_completion
                        check_tour_completion.delay()
                    except Exception:
                        logger.exception("Tour completion check dispatch failed")

                # 2b. Team-of-week sync: all games in tour completed?
                try:
                    TERMINAL = {GameStatus.finished, GameStatus.technical_defeat}
                    tour_check = await db.execute(
                        select(
                            func.count().label("total"),
                            func.count(case(
                                (
                                    Game.status.in_(TERMINAL)
                                    & Game.home_score.isnot(None)
                                    & Game.away_score.isnot(None),
                                    1,
                                ),
                            )).label("completed"),
                        ).where(
                            Game.season_id == game.season_id,
                            Game.tour == game.tour,
                        )
                    )
                    row = tour_check.one()
                    if row.total > 0 and row.completed == row.total:
                        from app.tasks.sync_tasks import _dispatch_tow_sync_for_tours
                        await _dispatch_tow_sync_for_tours(
                            game.season_id, [game.tour], "initial", countdown=60
                        )
                except Exception:
                    logger.exception("Team-of-week immediate sync dispatch failed")

            # 3. Extended stats: immediate if 24h+ old, else schedule
            finished_at = ensure_naive_utc(game.finished_at)
            if finished_at:
                now = utcnow()
                if finished_at <= now - timedelta(hours=24):
                    try:
                        sync_extended_stats_for_game.delay(game_id)
                    except Exception:
                        logger.exception("Extended stats dispatch failed for game %s", game_id)
                else:
                    eta = finished_at + timedelta(hours=24)
                    try:
                        sync_extended_stats_for_game.apply_async(
                            args=[game_id], eta=eta
                        )
                        logger.info(
                            "Scheduled extended stats for game %s at %s", game_id, eta
                        )
                    except Exception:
                        logger.exception("Extended stats schedule failed for game %s", game_id)

            await db.commit()
            return {"game_id": game_id, "status": "completed"}
        except Exception:
            await db.rollback()
            raise


async def _sync_extended_stats_for_game(game_id: int):
    """Sync extended stats for a single game (24h+ post-match)."""
    from app.services.sync import SyncOrchestrator

    aggregate_result = None
    season_id = None
    tour = None

    async with AsyncSessionLocal() as db:
        try:
            game = await db.get(Game, game_id)
            if not game or game.sync_disabled:
                return {"game_id": game_id, "skipped": True}

            orchestrator = SyncOrchestrator(db)
            now = utcnow()

            try:
                r = await orchestrator.sync_game_stats(game_id)
                if r.get("v2_enriched", 0) <= 0:
                    logger.info("Game %s: no v2 data yet, will retry via batch", game_id)
                    await db.commit()
                    return {"game_id": game_id, "synced": False, **r}

                game.extended_stats_synced_at = now
                season_id = game.season_id
                tour = game.tour
            except Exception:
                logger.exception("Extended stats sync failed for game %s", game_id)
                await db.rollback()
                return {"game_id": game_id, "synced": False}

            await db.commit()
        except Exception:
            await db.rollback()
            raise

    if season_id:
        aggregate_result = await _sync_extended_aggregates_for_season(season_id, {tour} if tour is not None else set())

    return {
        "game_id": game_id,
        "synced": True,
        "season_id": season_id,
        "aggregate_result": aggregate_result,
    }


async def _sync_extended_aggregates_for_season(
    season_id: int,
    tours: set[int] | None = None,
) -> dict:
    """Run season aggregate sync in an isolated session.

    This prevents a season-stats failure from rolling back already-synced
    game-level extended stats.
    """
    from app.services.sync import SyncOrchestrator

    tours = tours or set()
    errors: list[str] = []
    team_count = 0
    player_count = 0
    tour_counts: dict[int, int] = {}

    async with AsyncSessionLocal() as db:
        orchestrator = SyncOrchestrator(db)

        try:
            team_count = await orchestrator.sync_team_season_stats(season_id)
        except Exception as exc:
            await db.rollback()
            logger.exception("Team season stats sync failed for season %s", season_id)
            errors.append(f"team_season_stats: {exc}")

        try:
            player_count = await orchestrator.sync_player_stats(season_id)
        except Exception as exc:
            await db.rollback()
            logger.exception("Player season stats sync failed for season %s", season_id)
            errors.append(f"player_season_stats: {exc}")

        for tour in sorted(tours):
            try:
                tour_counts[tour] = await orchestrator.sync_player_tour_stats(season_id, tour)
            except Exception as exc:
                await db.rollback()
                logger.exception(
                    "Player tour stats sync failed for season %s tour %s", season_id, tour
                )
                errors.append(f"player_tour_stats[{tour}]: {exc}")

        try:
            await db.commit()
        except Exception:
            await db.rollback()
            raise

        # Mark completed tours and trigger revalidation
        from app.tasks.tour_readiness import mark_tour_synced, maybe_trigger_tour_revalidation

        season_syncs_ok = not any(
            "team_season_stats" in e or "player_season_stats" in e for e in errors
        )
        marked_tours: list[int] = []
        for tour in sorted(tours):
            tour_sync_ok = not any(f"player_tour_stats[{tour}]" in e for e in errors)
            if season_syncs_ok and tour_sync_ok:
                await mark_tour_synced(db, season_id, tour)
                marked_tours.append(tour)

        if marked_tours:
            await db.commit()
            for tour in marked_tours:
                await maybe_trigger_tour_revalidation(db, season_id, tour)

        # Dispatch team-of-week re-sync with extended stats data
        if marked_tours:
            from app.tasks.sync_tasks import _dispatch_tow_sync_for_tours
            await _dispatch_tow_sync_for_tours(season_id, marked_tours, "extended")

    return {
        "season_id": season_id,
        "teams": team_count,
        "players": player_count,
        "tour_stats": tour_counts,
        "errors": errors,
    }


async def _check_finished_without_timestamp():
    """Guardrail: auto-repair games with status=finished but finished_at=NULL.

    Uses GameLifecycleService.finish_live() repair-tail path.
    Sends a Telegram notification with results.
    """
    from app.services.game_lifecycle import GameLifecycleService

    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(Game.id).where(
                Game.status == GameStatus.finished,
                Game.finished_at.is_(None),
            )
        )
        broken_ids = list(result.scalars().all())

        if not broken_ids:
            return {"broken": 0}

        repaired, failed = [], []
        for gid in broken_ids:
            try:
                service = GameLifecycleService(db)
                await service.finish_live(gid)
                repaired.append(gid)
            except Exception:
                logger.exception("Auto-repair failed for game %s", gid)
                failed.append(gid)

        if repaired or failed:
            msg_parts = [f"🔧 Auto-repaired {len(repaired)} game(s) with broken finished_at."]
            if repaired:
                msg_parts.append("Repaired: " + ", ".join(f"#{gid}" for gid in repaired[:20]))
            if failed:
                msg_parts.append(f"{len(failed)} failed: " + ", ".join(f"#{gid}" for gid in failed[:20]))
            await send_telegram_message("\n".join(msg_parts))

        return {"broken": len(broken_ids), "repaired": len(repaired), "failed": len(failed)}


@celery_app.task(name="app.tasks.live_tasks.post_finish_followup")
def post_finish_followup(game_id: int):
    """Celery task: Post-match pipeline for a single game."""
    return run_async(_post_finish_followup(game_id))


@celery_app.task(name="app.tasks.live_tasks.sync_extended_stats_for_game")
def sync_extended_stats_for_game(game_id: int):
    """Celery task: Sync extended stats for a single game."""
    return run_async(_sync_extended_stats_for_game(game_id))


@celery_app.task(name="app.tasks.live_tasks.check_finished_without_timestamp")
def check_finished_without_timestamp():
    """Celery task: Guardrail — alert on broken finished games. Runs every 10 min."""
    return run_async(_check_finished_without_timestamp())
