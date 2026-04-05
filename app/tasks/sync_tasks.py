import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import redis as redis_lib
from sqlalchemy import select, func, case, exists

from app.tasks import celery_app
from app.database import AsyncSessionLocal
from app.services.sync import SyncOrchestrator
from app.models import Game, GameStatus, GameTeamStats, GamePlayerStats
from app.models.team_of_week import TeamOfWeek
from app.config import get_settings
from app.services.telegram import send_telegram_message
from app.utils.async_celery import run_async
from app.utils.timestamps import utcnow

logger = logging.getLogger(__name__)

settings = get_settings()


async def _sync_team_of_week_for_tour(season_id: int, tour: int) -> dict:
    """Sync team-of-week for a single tour via SyncOrchestrator.

    Returns dict with 'needs_retry' flag based on sync completeness.
    """
    async with AsyncSessionLocal() as db:
        try:
            orchestrator = SyncOrchestrator(db)
            result = await orchestrator.sync_team_of_week(
                season_id, tour_keys=[f"tour_{tour}"]
            )

            # Skipped (disabled season) — no retry needed
            if result.get("skipped"):
                logger.info(
                    "Team-of-week sync skipped (disabled): season=%s tour=%s",
                    season_id, tour,
                )
                return {**result, "needs_retry": False}

            tours_synced = result.get("tours_synced", 0)
            tours_empty = result.get("tours_empty", 0)
            tours_skipped = result.get("tours_skipped", 0)

            # Full success: both locales synced, no empty/skipped
            needs_retry = (
                tours_synced < 2 or tours_empty > 0 or tours_skipped > 0
            )
            logger.info(
                "Team-of-week sync result: season=%s tour=%s synced=%d empty=%d skipped=%d needs_retry=%s",
                season_id, tour, tours_synced, tours_empty, tours_skipped, needs_retry,
            )
            return {**result, "needs_retry": needs_retry}
        except Exception:
            logger.exception(
                "Team-of-week sync error: season=%s tour=%s", season_id, tour
            )
            return {"needs_retry": True, "error": True}


@celery_app.task(
    name="app.tasks.sync_tasks.sync_team_of_week_for_tour",
    bind=True,
    max_retries=3,
    default_retry_delay=600,  # 10 min
)
def sync_team_of_week_for_tour(self, season_id: int, tour: int):
    """Celery task: Sync team-of-week for a specific tour with retries."""
    result = run_async(_sync_team_of_week_for_tour(season_id, tour))
    if result.get("needs_retry"):
        try:
            raise self.retry(
                exc=Exception(f"SOTA partial/empty for s{season_id} t{tour}")
            )
        except self.MaxRetriesExceededError:
            run_async(send_telegram_message(
                f"❌ Team-of-week sync failed after retries: season {season_id} tour {tour}"
            ))
            return {**result, "final_status": "failed"}
    if result.get("tours_synced", 0) > 0:
        run_async(send_telegram_message(
            f"⚽ Team of week synced: season {season_id} tour {tour}"
        ))
    return {**result, "final_status": "success"}


async def _dispatch_tow_sync_for_tours(
    season_id: int, tours: list[int], suffix: str, countdown: int = 0
):
    """Dispatch team-of-week sync for completed tours with Redis dedupe.

    suffix: 'initial' or 'extended' — for separate dedupe keys
    countdown: delay in seconds before task execution
    """
    r = redis_lib.from_url(settings.redis_url)
    for tour in tours:
        key = f"qfl:tow_sync:{season_id}:{tour}:{suffix}"
        if r.set(key, "1", nx=True, ex=86400):
            if countdown > 0:
                sync_team_of_week_for_tour.apply_async(
                    args=[season_id, tour], countdown=countdown
                )
            else:
                sync_team_of_week_for_tour.delay(season_id, tour)
            logger.info(
                "Dispatched team-of-week sync: season=%s tour=%s (%s, countdown=%ds)",
                season_id, tour, suffix, countdown,
            )


async def _sync_extended_aggregate_bundle(
    season_id: int,
    tours: set[int] | None = None,
    *,
    force: bool = False,
) -> dict:
    """Sync season-level aggregates in an isolated transaction bundle.

    A failure here must not roll back already-persisted game-level extended
    stats for individual matches.
    """
    tours = tours or set()
    errors: list[str] = []
    team_count = 0
    player_count = 0
    tour_counts: dict[int, int] = {}

    async with AsyncSessionLocal() as db:
        orchestrator = SyncOrchestrator(db)

        try:
            team_count = await orchestrator.sync_team_season_stats(season_id, force=force)
        except Exception as exc:
            await db.rollback()
            logger.exception("Team season stats failed for season %s", season_id)
            errors.append(f"team_season_stats: {exc}")

        try:
            player_count = await orchestrator.sync_player_stats(season_id, force=force)
        except Exception as exc:
            await db.rollback()
            logger.exception("Player season stats failed for season %s", season_id)
            errors.append(f"player_season_stats: {exc}")

        for tour in sorted(tours):
            try:
                tour_counts[tour] = await orchestrator.sync_player_tour_stats(
                    season_id, tour, force=force
                )
            except Exception as exc:
                await db.rollback()
                logger.exception(
                    "Player tour stats failed for season %s tour %s", season_id, tour
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
    async with AsyncSessionLocal() as db2:
        marked_tours: list[int] = []
        for tour in sorted(tours):
            tour_sync_ok = not any(f"player_tour_stats[{tour}]" in e for e in errors)
            if season_syncs_ok and tour_sync_ok:
                await mark_tour_synced(db2, season_id, tour)
                marked_tours.append(tour)

        if marked_tours:
            await db2.commit()
            for tour in marked_tours:
                await maybe_trigger_tour_revalidation(db2, season_id, tour)

        # Dispatch team-of-week re-sync with extended stats data
        if marked_tours:
            await _dispatch_tow_sync_for_tours(season_id, marked_tours, "extended")

    return {
        "teams": team_count,
        "players": player_count,
        "tour_stats": tour_counts,
        "errors": errors,
    }


async def _sync_games():
    """Sync games for all configured seasons."""
    async with AsyncSessionLocal() as db:
        try:
            orchestrator = SyncOrchestrator(db)
            results = {}
            for season_id in settings.sync_season_ids:
                if not await orchestrator.is_sync_enabled(season_id):
                    logger.info("Season %d: sync disabled, skipping games task", season_id)
                    results[f"season_{season_id}"] = "skipped"
                    continue
                count = await orchestrator.sync_games(season_id)
                results[f"season_{season_id}"] = count
            await db.commit()
            return {"games_synced": results}
        except Exception:
            await db.rollback()
            raise


async def _sync_live_stats():
    """Sync statistics for recent games across all configured seasons."""
    async with AsyncSessionLocal() as db:
        try:
            orchestrator = SyncOrchestrator(db)
            three_days_ago = datetime.now(ZoneInfo("Asia/Almaty")).date() - timedelta(days=3)

            total_synced = 0
            results_by_season = {}

            for season_id in settings.sync_season_ids:
                if not await orchestrator.is_sync_enabled(season_id):
                    logger.info("Season %d: sync disabled, skipping live stats task", season_id)
                    results_by_season[f"season_{season_id}"] = "skipped"
                    continue

                # Derived has_stats: check actual stats records instead of stored flag
                has_stats_filter = (
                    exists().where(GameTeamStats.game_id == Game.id)
                    | exists().where(GamePlayerStats.game_id == Game.id)
                )
                result = await db.execute(
                    select(Game.id).where(
                        Game.season_id == season_id,
                        Game.date >= three_days_ago,
                        has_stats_filter,
                        Game.sync_disabled == False,
                    )
                )
                game_ids = [g[0] for g in result.fetchall()]

                season_synced = 0
                for gid in game_ids:
                    await orchestrator.sync_game_stats(gid)
                    season_synced += 1

                results_by_season[f"season_{season_id}"] = season_synced
                total_synced += season_synced

            await db.commit()
            return {"games_stats_synced": total_synced, "by_season": results_by_season}
        except Exception:
            await db.rollback()
            raise


async def _sync_best_players():
    """Sync goals + assists from best_players endpoint for all configured seasons."""
    async with AsyncSessionLocal() as db:
        try:
            orchestrator = SyncOrchestrator(db)
            total = 0
            results_by_season = {}
            for season_id in settings.sync_season_ids:
                if not await orchestrator.is_sync_enabled(season_id):
                    logger.info("Season %d: sync disabled, skipping best_players task", season_id)
                    results_by_season[f"season_{season_id}"] = "skipped"
                    continue
                count = await orchestrator.sync_best_players(season_id)
                results_by_season[f"season_{season_id}"] = count
                total += count
            await db.commit()
            return {"best_players_synced": total, "by_season": results_by_season}
        except Exception:
            await db.rollback()
            raise


async def _sync_extended_stats():
    """
    Sync extended stats for games finished 24h+ ago that haven't been synced yet.

    After ~24h, SOTA publishes extended data (xG, detailed passes, duels, etc.).
    This task:
    1. Finds finished games without extended_stats_synced_at (24h+ after finish)
    2. Re-syncs game stats with v2 enrichment
    3. Syncs team/player season stats ONLY if new games were processed
    4. Marks games as synced to avoid redundant work
    """
    now = utcnow()
    cutoff = now - timedelta(hours=24)
    season_tours: dict[int, set[int]] = {}
    game_results = []
    game_errors = []

    async with AsyncSessionLocal() as db:
        try:
            orchestrator = SyncOrchestrator(db)

            # 1. Find games finished 24h+ ago, not yet synced, in active seasons only
            result = await db.execute(
                select(Game).where(
                    Game.status == GameStatus.finished,
                    Game.finished_at.isnot(None),
                    Game.finished_at <= cutoff,
                    Game.sota_id.isnot(None),
                    Game.sync_disabled == False,
                    Game.extended_stats_synced_at.is_(None),
                    Game.season_id.in_(settings.sync_season_ids),
                )
            )
            games = list(result.scalars().all())

            if not games:
                return {"extended_stats": "no new games to sync"}

            # 2. Re-sync game stats (with v2 enrichment)
            for game in games:
                try:
                    r = await orchestrator.sync_game_stats(game.id)
                    game_results.append({"game_id": game.id, **r})
                    team_count = r.get("teams", 0)
                    v2_count = r.get("v2_enriched", 0)
                    if team_count > 0 or v2_count > 0:
                        game.extended_stats_synced_at = now
                        if game.season_id:
                            season_tours.setdefault(game.season_id, set())
                            if game.tour is not None:
                                season_tours[game.season_id].add(game.tour)
                    else:
                        logger.info("Game %s: no team stats or v2 data yet, will retry", game.id)
                except Exception as e:
                    logger.warning("Extended game stats failed for game %s: %s", game.id, e)
                    game_errors.append(f"Game {game.id}: {e}")
                    await db.rollback()

            await db.commit()
        except Exception as e:
            await db.rollback()
            try:
                await send_telegram_message(f"❌ Extended stats sync failed:\n{e}")
            except Exception:
                pass
            raise

    # 3. Sync season aggregates in isolated transactions
    season_results = {}
    for season_id, tours in sorted(season_tours.items()):
        result = await _sync_extended_aggregate_bundle(season_id, tours)
        season_results[season_id] = {
            "teams": result["teams"],
            "players": result["players"],
            "tour_stats": result["tour_stats"],
        }
        game_errors.extend(
            [f"Season {season_id}: {err}" for err in result.get("errors", [])]
        )

    # 4. Telegram notification
    if season_results or game_errors:
        lines = ["📊 Extended stats synced (24h+ post-match)"]
        lines.append(f"Games: {len(game_results)} synced")
        for sid, counts in season_results.items():
            lines.append(f"Season {sid}: {counts['teams']} teams, {counts['players']} players")
        if game_errors:
            lines.append(f"⚠️ Errors ({len(game_errors)}):")
            for err in game_errors[:5]:
                lines.append(f"  {err}")
        await send_telegram_message("\n".join(lines))

    return {
        "games_resynced": len(game_results),
        "seasons_synced": season_results,
        "errors": game_errors,
    }


async def _resync_extended_stats(game_ids: list[int]):
    """Resync extended stats for specific games (admin-triggered)."""
    now = utcnow()
    season_tours: dict[int, set[int]] = {}

    async with AsyncSessionLocal() as db:
        try:
            orchestrator = SyncOrchestrator(db)

            result = await db.execute(
                select(Game).where(Game.id.in_(game_ids), Game.sync_disabled == False)
            )
            games = list(result.scalars().all())
            if not games:
                return {"message": "No games found"}

            game_results = []
            game_errors = []
            for game in games:
                try:
                    r = await orchestrator.sync_game_stats(game.id)
                    game_results.append({"game_id": game.id, **r})
                    team_count = r.get("teams", 0)
                    v2_count = r.get("v2_enriched", 0)
                    if team_count > 0 or v2_count > 0:
                        game.extended_stats_synced_at = now
                        if game.season_id:
                            season_tours.setdefault(game.season_id, set())
                            if game.tour is not None:
                                season_tours[game.season_id].add(game.tour)
                    else:
                        logger.info("Game %s: no team stats or v2 data", game.id)
                except Exception as e:
                    logger.warning("Resync failed for game %s: %s", game.id, e)
                    game_errors.append(f"Game {game.id}: {e}")
                    await db.rollback()

            await db.commit()
        except Exception as e:
            await db.rollback()
            try:
                await send_telegram_message(f"❌ Admin resync failed:\n{e}")
            except Exception:
                pass
            raise

    season_results = {}
    for sid, tours in sorted(season_tours.items()):
        result = await _sync_extended_aggregate_bundle(sid, tours, force=True)
        season_results[sid] = {"teams": result["teams"], "players": result["players"]}
        game_errors.extend([f"Season {sid}: {err}" for err in result.get("errors", [])])

    # Telegram notification
    lines = ["🔄 Admin resync extended stats"]
    lines.append(f"Games: {len(game_results)} synced, {len(game_errors)} errors")
    for sid, counts in season_results.items():
        lines.append(f"Season {sid}: {counts['teams']} teams, {counts['players']} players")
    if game_errors:
        for err in game_errors[:5]:
            lines.append(f"  ⚠️ {err}")
    await send_telegram_message("\n".join(lines))

    return {"games_resynced": len(game_results), "errors": game_errors}


@celery_app.task(name="app.tasks.sync_tasks.resync_extended_stats")
def resync_extended_stats_task(game_ids: list[int]):
    """Celery task: Admin-triggered resync of extended stats."""
    return run_async(_resync_extended_stats(game_ids))


@celery_app.task(name="app.tasks.sync_tasks.sync_games")
def sync_games():
    """Celery task: Sync games for all configured seasons."""
    return run_async(_sync_games())


@celery_app.task(name="app.tasks.sync_tasks.sync_live_stats")
def sync_live_stats():
    """Celery task: Sync statistics for recent games across all configured seasons."""
    return run_async(_sync_live_stats())


@celery_app.task(name="app.tasks.sync_tasks.sync_best_players")
def sync_best_players():
    """Celery task: Sync goals + assists from best_players endpoint."""
    return run_async(_sync_best_players())


@celery_app.task(name="app.tasks.sync_tasks.sync_extended_stats")
def sync_extended_stats():
    """Celery task: Sync extended stats 24h+ after match finish."""
    return run_async(_sync_extended_stats())


async def _check_tour_completion():
    """Backstop: pick up any marked tours that missed revalidation.

    Scans TourSyncStatus for tours with markers but no Redis dedupe key,
    and triggers revalidation via the unified helper.
    """
    from app.models.tour_sync_status import TourSyncStatus
    from app.tasks.tour_readiness import maybe_trigger_tour_revalidation

    r = redis_lib.from_url(settings.redis_url)
    triggered = []

    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(TourSyncStatus.season_id, TourSyncStatus.tour)
            .where(TourSyncStatus.season_id.in_(settings.sync_season_ids))
        )
        for row in result.all():
            redis_key = f"tour_reval:{row.season_id}:{row.tour}"
            if not r.exists(redis_key):
                ok = await maybe_trigger_tour_revalidation(db, row.season_id, row.tour)
                if ok:
                    triggered.append({"season_id": row.season_id, "tour": row.tour})

        return {"triggered": triggered}


async def _trigger_stats_revalidation(season_id: int | None = None, tour: int | None = None):
    """Call Next.js on-demand revalidation for stats pages."""
    import httpx

    if not settings.revalidation_secret:
        logger.warning("REVALIDATION_SECRET not set, skipping stats revalidation")
        return {"skipped": "no secret configured"}

    paths = [
        "/kz/stats", "/ru/stats",
        "/kz/stats/overview", "/ru/stats/overview",
        "/kz/stats/players", "/ru/stats/players",
        "/kz/stats/teams", "/ru/stats/teams",
    ]

    url = f"{settings.frontend_internal_url}/api/revalidate"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(url, json={
                "secret": settings.revalidation_secret,
                "paths": paths,
            })
            resp.raise_for_status()
            result = resp.json()
            logger.info("Stats revalidation triggered: %s (season=%s tour=%s)", result, season_id, tour)
            await send_telegram_message(
                f"✅ Stats pages revalidated (season {season_id} tour {tour})"
            )
            return result
    except Exception as e:
        logger.error("Stats revalidation failed: %s", e)
        await send_telegram_message(f"❌ Stats revalidation failed: {e}")
        raise


@celery_app.task(name="app.tasks.sync_tasks.check_tour_completion")
def check_tour_completion():
    """Celery task: Check completed tours, schedule revalidation."""
    return run_async(_check_tour_completion())


@celery_app.task(name="app.tasks.sync_tasks.trigger_stats_revalidation")
def trigger_stats_revalidation(season_id: int | None = None, tour: int | None = None):
    """Celery task: Trigger Next.js on-demand revalidation for stats pages."""
    return run_async(_trigger_stats_revalidation(season_id, tour))


async def _backfill_player_tour_stats(season_id: int, max_tour: int):
    """Backfill player tour stats for a season (all tours 1..max_tour)."""
    async with AsyncSessionLocal() as db:
        try:
            orchestrator = SyncOrchestrator(db)
            results = await orchestrator.backfill_player_tour_stats(
                season_id, max_tour, force=True
            )
            await db.commit()
            return {"season_id": season_id, "max_tour": max_tour, "results": results}
        except Exception:
            await db.rollback()
            raise


@celery_app.task(name="app.tasks.sync_tasks.backfill_player_tour_stats")
def backfill_player_tour_stats_task(season_id: int, max_tour: int):
    """Celery task: Backfill player tour stats for a season."""
    return run_async(_backfill_player_tour_stats(season_id, max_tour))


async def _retry_missing_team_of_week():
    """Retry team-of-week sync for completed tours that are still missing data.

    Finds tours where all games finished within the last 48 hours but
    team-of-week hasn't been synced (for either locale). Dispatches
    sync_team_of_week_for_tour for each missing tour.
    """
    TERMINAL = {GameStatus.finished, GameStatus.technical_defeat}
    cutoff = utcnow() - timedelta(hours=48)
    dispatched = []

    async with AsyncSessionLocal() as db:
        for season_id in settings.sync_season_ids:
            # Find tours with all games finished, where the latest finish is within 48h
            tour_query = await db.execute(
                select(
                    Game.tour,
                    func.count().label("total"),
                    func.count(case(
                        (Game.status.in_(TERMINAL) & Game.home_score.isnot(None) & Game.away_score.isnot(None), 1),
                    )).label("completed"),
                    func.max(Game.finished_at).label("last_finished"),
                ).where(
                    Game.season_id == season_id,
                    Game.tour.isnot(None),
                ).group_by(Game.tour)
            )

            for row in tour_query.all():
                if row.total == 0 or row.completed != row.total:
                    continue
                if row.last_finished is None or row.last_finished < cutoff:
                    continue

                tour_key = f"tour_{row.tour}"

                # Check if both locales exist in team_of_week
                tow_count = await db.execute(
                    select(func.count()).select_from(TeamOfWeek).where(
                        TeamOfWeek.season_id == season_id,
                        TeamOfWeek.tour_key == tour_key,
                    )
                )
                existing = tow_count.scalar()
                if existing >= 2:  # ru + kz
                    continue

                # Missing — dispatch sync
                sync_team_of_week_for_tour.delay(season_id, row.tour)
                dispatched.append({"season_id": season_id, "tour": row.tour, "existing": existing})
                logger.info(
                    "Retry missing team-of-week: season=%s tour=%s (existing=%d/2)",
                    season_id, row.tour, existing,
                )

    if dispatched:
        await send_telegram_message(
            f"🔄 Team-of-week retry: dispatched {len(dispatched)} tour(s)\n"
            + "\n".join(f"  season {d['season_id']} tour {d['tour']}" for d in dispatched)
        )

    return {"dispatched": dispatched}


@celery_app.task(name="app.tasks.sync_tasks.retry_missing_team_of_week")
def retry_missing_team_of_week():
    """Celery task: Retry team-of-week for completed tours missing data (within 48h)."""
    return run_async(_retry_missing_team_of_week())
