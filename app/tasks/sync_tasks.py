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
    Sync extended stats for games finished 24-72h ago.

    After ~24h, SOTA publishes extended data (xG, detailed passes, duels, etc.).
    This task:
    1. Re-syncs game stats with v2 enrichment for recent games
    2. Syncs team season stats (92 metrics) for affected seasons
    3. Syncs player season stats (50+ metrics) for affected seasons
    """
    async with AsyncSessionLocal() as db:
        try:
            now = datetime.utcnow()
            cutoff_start = now - timedelta(hours=72)
            cutoff_end = now - timedelta(hours=24)

            orchestrator = SyncOrchestrator(db)
            seasons_to_sync = set()

            # 1. Find games finished 24-72h ago with SOTA data
            result = await db.execute(
                select(Game).where(
                    Game.status == GameStatus.finished,
                    Game.finished_at.isnot(None),
                    Game.finished_at >= cutoff_start,
                    Game.finished_at <= cutoff_end,
                    Game.sota_id.isnot(None),
                    Game.sync_disabled == False,
                )
            )
            games = list(result.scalars().all())

            if not games:
                return {"extended_stats": "no games in 24-72h window"}

            # 2. Re-sync game stats (with v2 enrichment)
            game_results = []
            game_errors = []
            for game in games:
                try:
                    r = await orchestrator.sync_game_stats(game.id)
                    game_results.append({"game_id": game.id, **r})
                    seasons_to_sync.add(game.season_id)
                except Exception as e:
                    logger.warning("Extended game stats failed for game %s: %s", game.id, e)
                    game_errors.append(f"Game {game.id}: {e}")

            # 3. Sync team + player season stats for affected seasons
            season_results = {}
            for season_id in seasons_to_sync:
                if not await orchestrator.is_sync_enabled(season_id):
                    continue
                team_count = await orchestrator.sync_team_season_stats(season_id)
                player_count = await orchestrator.sync_player_stats(season_id)
                season_results[season_id] = {
                    "teams": team_count,
                    "players": player_count,
                }

            await db.commit()

            # 4. Telegram notification
            if season_results or game_errors:
                lines = ["📊 Extended stats synced (24h+ post-match)"]
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
