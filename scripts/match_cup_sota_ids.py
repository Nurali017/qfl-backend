"""Match Cup 2026 games with SOTA API and assign sota_id.

Thin CLI wrapper around :func:`app.services.cup_sota_setup.setup_cup_sota`.

Steps (implemented in the service):
  1. Discover SOTA season ID for Cup 2026 (by listing all seasons)
  2. Fetch games from SOTA for that season
  3. Match local cup games (season_id=202) with SOTA games by team names + date
  4. Update sota_id in the database
  5. Optionally enable sync_enabled on the season

Usage:
  docker exec qfl-backend python scripts/match_cup_sota_ids.py
  docker exec qfl-backend python scripts/match_cup_sota_ids.py --dry-run
  docker exec qfl-backend python scripts/match_cup_sota_ids.py --sota-season-id 176
"""
import argparse
import asyncio
import logging

from app.database import AsyncSessionLocal
from app.services.cup_sota_setup import setup_cup_sota
from app.services.sota_client import SotaClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

CUP_SEASON_ID = 202


async def main(
    sota_season_id: int | None = None,
    dry_run: bool = False,
    enable_sync: bool = True,
    season_id: int = CUP_SEASON_ID,
) -> None:
    client = SotaClient()
    try:
        await client.ensure_authenticated()
        async with AsyncSessionLocal() as db:
            result = await setup_cup_sota(
                db,
                client,
                season_id=season_id,
                sota_season_id=sota_season_id,
                dry_run=dry_run,
                enable_sync=enable_sync,
            )
            logger.info("Result: %s", result.message)
            if result.unmatched:
                logger.warning("%d local games could not be matched:", len(result.unmatched))
                for unmatched in result.unmatched:
                    logger.warning(
                        "  game %d: %s vs %s [%s]",
                        unmatched.local_game_id,
                        unmatched.home,
                        unmatched.away,
                        unmatched.game_date,
                    )
    finally:
        if hasattr(client, "close"):
            await client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Match Cup 2026 games with SOTA")
    parser.add_argument(
        "--sota-season-id",
        type=int,
        default=None,
        help="SOTA season ID for Cup 2026 (auto-discovered if not set)",
    )
    parser.add_argument(
        "--season-id",
        type=int,
        default=CUP_SEASON_ID,
        help=f"Local cup season ID (default: {CUP_SEASON_ID})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't save changes, just show what would be matched",
    )
    parser.add_argument(
        "--no-enable-sync",
        action="store_true",
        help="Don't enable sync_enabled on the season",
    )
    args = parser.parse_args()

    asyncio.run(
        main(
            sota_season_id=args.sota_season_id,
            dry_run=args.dry_run,
            enable_sync=not args.no_enable_sync,
            season_id=args.season_id,
        )
    )
