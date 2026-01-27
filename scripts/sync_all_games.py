#!/usr/bin/env python3
"""
Sync all games (full_sync) for all seasons from SOTA API.

Usage:
    python scripts/sync_all_games.py
    python scripts/sync_all_games.py --season-ids 84 85
    python scripts/sync_all_games.py --games-only
"""
import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import select, func
from app.database import AsyncSessionLocal
from app.models import Game, Season, Tournament
from app.services.sync_service import SyncService


async def main():
    parser = argparse.ArgumentParser(description="Sync all seasons from SOTA")
    parser.add_argument("--season-ids", nargs="+", type=int, help="Specific season IDs")
    parser.add_argument("--games-only", action="store_true", help="Only sync games (skip stats/lineups)")
    args = parser.parse_args()

    # Get seasons to sync
    if args.season_ids:
        season_ids = args.season_ids
    else:
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(Season.id, Season.name, Tournament.name.label("tournament"))
                .join(Tournament, Season.tournament_id == Tournament.id)
                .order_by(Season.id)
            )
            rows = result.all()
            season_ids = [r[0] for r in rows]
            print("Available seasons:")
            for r in rows:
                print(f"  {r[0]:4d} | {r[1]:10s} | {r[2]}")
            print()

    for sid in season_ids:
        print(f"{'='*60}")
        print(f"Syncing season_id={sid}")
        print(f"{'='*60}")

        try:
            async with AsyncSessionLocal() as db:
                sync_service = SyncService(db)
                if args.games_only:
                    count = await sync_service.sync_games(sid)
                    print(f"  Games synced: {count}")
                else:
                    results = await sync_service.full_sync(sid)
                    for key, val in results.items():
                        print(f"  {key}: {val}")
        except Exception as e:
            print(f"  ERROR: {e}")

        print()

    # Summary
    print(f"{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(Season.id, Tournament.name, func.count(Game.id))
            .join(Tournament, Season.tournament_id == Tournament.id)
            .outerjoin(Game, Game.season_id == Season.id)
            .group_by(Season.id, Tournament.name)
            .order_by(func.count(Game.id).desc())
        )
        for r in result.all():
            print(f"  {r[0]:4d} | {r[1]:40s} | {r[2]} matches")


if __name__ == "__main__":
    asyncio.run(main())
