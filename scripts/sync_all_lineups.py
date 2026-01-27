#!/usr/bin/env python3
"""
Script to sync amplua/field_position data for all games from SOTA live endpoint.
"""
import asyncio
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import select
from app.database import AsyncSessionLocal
from app.models import Game
from app.services.sync_service import SyncService


async def sync_all_lineup_positions():
    """Sync amplua data for all games that have lineups."""
    async with AsyncSessionLocal() as db:
        sync_service = SyncService(db)

        # Get all games with lineups
        result = await db.execute(
            select(Game.id, Game.date, Game.home_team_id, Game.away_team_id)
            .where(Game.has_lineup == True)
            .order_by(Game.date.desc())
        )
        games = result.all()

        print(f"Found {len(games)} games with lineups")

        success_count = 0
        failed_count = 0

        for game_id, game_date, home_id, away_id in games:
            game_id_str = str(game_id)
            try:
                result = await sync_service.sync_live_lineup_positions(game_id_str)
                positions_updated = result.get("positions_updated", 0)
                players_added = result.get("players_added", 0)

                if positions_updated > 0 or players_added > 0:
                    print(f"✓ {game_date} ({game_id_str[:8]}...): updated={positions_updated}, added={players_added}")
                    success_count += 1
                else:
                    print(f"- {game_date} ({game_id_str[:8]}...): no live data available")

            except Exception as e:
                print(f"✗ {game_date} ({game_id_str[:8]}...): {str(e)[:50]}")
                failed_count += 1

        print(f"\nDone! Success: {success_count}, No data: {len(games) - success_count - failed_count}, Failed: {failed_count}")


if __name__ == "__main__":
    asyncio.run(sync_all_lineup_positions())
