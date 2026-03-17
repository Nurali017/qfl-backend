import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import redis
from sqlalchemy import select, func, case, exists

from app.tasks import celery_app
from app.database import AsyncSessionLocal
from app.services.sync import SyncOrchestrator
from app.models import Game, GameStatus, GameTeamStats, GamePlayerStats
from app.config import get_settings
from app.services.telegram import send_telegram_message
from app.utils.async_celery import run_async

logger = logging.getLogger(__name__)

settings = get_settings()


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
    now = datetime.utcnow()
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
                    if r.get("v2_enriched", 0) > 0:
                        game.extended_stats_synced_at = now
                        if game.season_id:
                            season_tours.setdefault(game.season_id, set())
                            if game.tour is not None:
                                season_tours[game.season_id].add(game.tour)
                    else:
                        logger.info("Game %s: no v2 data yet, will retry", game.id)
                except Exception as e:
                    logger.warning("Extended game stats failed for game %s: %s", game.id, e)
                    game_errors.append(f"Game {game.id}: {e}")

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
    now = datetime.utcnow()
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
                    if r.get("v2_enriched", 0) > 0:
                        game.extended_stats_synced_at = now
                        if game.season_id:
                            season_tours.setdefault(game.season_id, set())
                            if game.tour is not None:
                                season_tours[game.season_id].add(game.tour)
                    else:
                        logger.info("Game %s: no v2 data yet", game.id)
                except Exception as e:
                    logger.warning("Resync failed for game %s: %s", game.id, e)
                    game_errors.append(f"Game {game.id}: {e}")

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
    """
    Check if any tour just completed all games, schedule revalidation in 24h.

    A tour is "just completed" if all its games are finished and the last
    game finished within the last hour.
    """
    r = redis.from_url(settings.redis_url)
    now = datetime.utcnow()
    one_hour_ago = now - timedelta(hours=1)

    async with AsyncSessionLocal() as db:
        # Find (season_id, tour) pairs where ALL games are finished
        # and the last game finished within the last hour
        result = await db.execute(
            select(
                Game.season_id,
                Game.tour,
                func.count().label("total"),
                func.count(
                    case((Game.status == GameStatus.finished, 1))
                ).label("finished_count"),
                func.max(Game.finished_at).label("last_finished"),
            )
            .where(
                Game.season_id.in_(settings.sync_season_ids),
                Game.tour.isnot(None),
            )
            .group_by(Game.season_id, Game.tour)
            .having(
                # All games finished
                func.count() == func.count(
                    case((Game.status == GameStatus.finished, 1))
                ),
                # Last game finished within the last hour
                func.max(Game.finished_at) >= one_hour_ago,
            )
        )
        completed_tours = result.fetchall()

        scheduled = []
        for row in completed_tours:
            season_id, tour = row.season_id, row.tour
            redis_key = f"tour_reval:{season_id}:{tour}"

            # Skip if already scheduled
            if r.exists(redis_key):
                logger.info("Revalidation already scheduled for season %d tour %d", season_id, tour)
                continue

            # Mark as scheduled (48h TTL to prevent duplicates)
            r.set(redis_key, "1", ex=48 * 3600)

            # Schedule revalidation in 24h
            trigger_stats_revalidation.apply_async(
                eta=now + timedelta(hours=24),
                kwargs={"season_id": season_id, "tour": tour},
            )
            scheduled.append({"season_id": season_id, "tour": tour})
            logger.info(
                "Scheduled stats revalidation in 24h for season %d tour %d",
                season_id, tour,
            )

        if scheduled:
            await send_telegram_message(
                f"⏳ Stats revalidation scheduled (24h):\n"
                + "\n".join(f"  Season {s['season_id']} Tour {s['tour']}" for s in scheduled)
            )

        return {"scheduled": scheduled}


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
