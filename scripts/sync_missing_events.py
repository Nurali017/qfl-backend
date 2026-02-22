#!/usr/bin/env python3
"""
Sync missing game events from SOTA API for 2025 seasons.

Finds played games (with scores) that have no events in game_events table,
then calls SyncOrchestrator.sync_game_events() for each.

Usage:
    python -m scripts.sync_missing_events --dry-run
    python -m scripts.sync_missing_events
    python -m scripts.sync_missing_events --season-id 80
"""

from __future__ import annotations

import argparse
import asyncio
import logging

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import AsyncSessionLocal
from app.models.game import Game
from app.models.game_event import GameEvent
from app.services.sync.orchestrator import SyncOrchestrator

LOGGER = logging.getLogger("sync_missing_events")

# 2025 seasons: Premier League, First League, Cup, Second League
DEFAULT_SEASON_IDS = [61, 85, 71, 80]


async def find_games_without_events(
    db: AsyncSession, season_ids: list[int],
) -> list[dict]:
    """Find played games that have no events in game_events table."""
    # Subquery: game IDs that already have events
    has_events_subq = select(GameEvent.game_id).distinct().subquery()

    result = await db.execute(
        select(Game.id, Game.season_id, Game.home_score, Game.away_score)
        .outerjoin(has_events_subq, Game.id == has_events_subq.c.game_id)
        .where(
            Game.season_id.in_(season_ids),
            Game.home_score.isnot(None),       # played games only
            Game.sota_id.isnot(None),           # need SOTA ID for API
            has_events_subq.c.game_id.is_(None),  # no events yet
        )
        .order_by(Game.season_id, Game.id)
    )

    return [
        {
            "id": row.id,
            "season_id": row.season_id,
            "score": f"{row.home_score}-{row.away_score}",
        }
        for row in result.all()
    ]


async def run_sync(args: argparse.Namespace) -> int:
    season_ids = [args.season_id] if args.season_id else DEFAULT_SEASON_IDS

    async with AsyncSessionLocal() as db:
        games = await find_games_without_events(db, season_ids)

        # Group by season for logging
        by_season: dict[int, list] = {}
        for g in games:
            by_season.setdefault(g["season_id"], []).append(g)

        LOGGER.info("Games without events:")
        for sid, sg in sorted(by_season.items()):
            LOGGER.info("  Season %d: %d games", sid, len(sg))
        LOGGER.info("Total: %d games to sync", len(games))

        if not games:
            LOGGER.info("Nothing to sync")
            return 0

        if args.dry_run:
            LOGGER.info("[DRY-RUN] Would sync events for %d games", len(games))
            for sid, sg in sorted(by_season.items()):
                for g in sg:
                    LOGGER.debug(
                        "  Game %d (season %d, score %s)",
                        g["id"], g["season_id"], g["score"],
                    )
            return 0

        orchestrator = SyncOrchestrator(db)
        total_events = 0
        synced = 0
        errors: list[str] = []

        for i, game in enumerate(games, 1):
            try:
                result = await orchestrator.sync_game_events(game["id"])
                events_added = result.get("events_added", 0)
                total_events += events_added

                if "error" not in result:
                    synced += 1
                else:
                    errors.append(f"Game {game['id']}: {result['error']}")

                if i % 10 == 0 or events_added > 0:
                    LOGGER.info(
                        "[%d/%d] Game %d: +%d events",
                        i, len(games), game["id"], events_added,
                    )
            except Exception as e:
                LOGGER.error("Game %d failed: %s", game["id"], e)
                errors.append(f"Game {game['id']}: {e}")

        LOGGER.info("=== Summary ===")
        LOGGER.info("Games synced: %d / %d", synced, len(games))
        LOGGER.info("Total events added: %d", total_events)
        if errors:
            LOGGER.warning("Errors (%d):", len(errors))
            for err in errors:
                LOGGER.warning("  %s", err)

    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync missing game events from SOTA API for 2025 seasons",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Only show what would be synced, do not call SOTA API",
    )
    parser.add_argument(
        "--season-id", type=int, default=None,
        help="Sync specific season only (default: all 2025 seasons)",
    )
    parser.add_argument(
        "--verbose", action="store_true",
        help="Enable debug logs",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    raise SystemExit(asyncio.run(run_sync(args)))


if __name__ == "__main__":
    main()
