import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import select

from app.tasks import celery_app
from app.database import AsyncSessionLocal
from app.services.sync import SyncOrchestrator
from app.models import Game, GameStatus
from app.config import get_settings
from app.services.telegram import send_telegram_message
from app.utils.async_celery import run_async

logger = logging.getLogger(__name__)

settings = get_settings()


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

                result = await db.execute(
                    select(Game.id).where(
                        Game.season_id == season_id,
                        Game.date >= three_days_ago,
                        Game.has_stats == True,
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
    async with AsyncSessionLocal() as db:
        try:
            now = datetime.utcnow()
            cutoff = now - timedelta(hours=24)

            orchestrator = SyncOrchestrator(db)
            seasons_to_sync = set()

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
            game_results = []
            game_errors = []
            for game in games:
                try:
                    r = await orchestrator.sync_game_stats(game.id)
                    game_results.append({"game_id": game.id, **r})
                    # Only mark as synced if v2 data was actually received
                    if r.get("v2_enriched", 0) > 0:
                        game.extended_stats_synced_at = now
                        seasons_to_sync.add(game.season_id)
                    else:
                        logger.info("Game %s: no v2 data yet, will retry", game.id)
                except Exception as e:
                    logger.warning("Extended game stats failed for game %s: %s", game.id, e)
                    game_errors.append(f"Game {game.id}: {e}")

            # 3. Sync team + player season stats for affected seasons
            # Also collect (season_id, tour) pairs for player tour stats
            season_tours: dict[int, set[int]] = {}
            for game in games:
                if game.season_id in seasons_to_sync and game.tour is not None:
                    season_tours.setdefault(game.season_id, set()).add(game.tour)

            season_results = {}
            for season_id in seasons_to_sync:
                if not await orchestrator.is_sync_enabled(season_id):
                    continue
                team_count = await orchestrator.sync_team_season_stats(season_id)
                player_count = await orchestrator.sync_player_stats(season_id)
                # Sync player tour stats for each tour with finished games
                tour_counts = {}
                for tour in sorted(season_tours.get(season_id, [])):
                    try:
                        tc = await orchestrator.sync_player_tour_stats(season_id, tour)
                        tour_counts[tour] = tc
                    except Exception as e:
                        logger.warning("Player tour stats failed for season %d tour %d: %s", season_id, tour, e)
                season_results[season_id] = {
                    "teams": team_count,
                    "players": player_count,
                    "tour_stats": tour_counts,
                }

            await db.commit()

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
        except Exception as e:
            await db.rollback()
            try:
                await send_telegram_message(f"❌ Extended stats sync failed:\n{e}")
            except Exception:
                pass
            raise


async def _resync_extended_stats(game_ids: list[int]):
    """Resync extended stats for specific games (admin-triggered)."""
    async with AsyncSessionLocal() as db:
        try:
            now = datetime.utcnow()
            orchestrator = SyncOrchestrator(db)
            seasons_to_sync = set()

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
                        seasons_to_sync.add(game.season_id)
                    else:
                        logger.info("Game %s: no v2 data yet", game.id)
                except Exception as e:
                    logger.warning("Resync failed for game %s: %s", game.id, e)
                    game_errors.append(f"Game {game.id}: {e}")

            # Sync team + player season stats (force=True for admin override)
            season_results = {}
            for sid in seasons_to_sync:
                team_count = await orchestrator.sync_team_season_stats(sid, force=True)
                player_count = await orchestrator.sync_player_stats(sid, force=True)
                season_results[sid] = {"teams": team_count, "players": player_count}

            await db.commit()

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
        except Exception as e:
            await db.rollback()
            try:
                await send_telegram_message(f"❌ Admin resync failed:\n{e}")
            except Exception:
                pass
            raise


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
