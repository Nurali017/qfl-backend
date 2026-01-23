#!/usr/bin/env python3
"""
Cleanup script for player photos.

This script:
1. Removes all player photos from MinIO storage (player_photos/ folder)
2. Clears photo_url field in the players table

Usage:
    # Dry run (preview without changes)
    python scripts/cleanup_player_photos.py --dry-run

    # Full cleanup
    python scripts/cleanup_player_photos.py
"""

import argparse
import asyncio
import sys
from pathlib import Path

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import update
from app.database import AsyncSessionLocal
from app.models.player import Player
from app.minio_client import get_minio_client
from app.config import get_settings

settings = get_settings()


async def cleanup_minio_photos(dry_run: bool = False) -> int:
    """Remove all player photos from MinIO storage."""
    client = get_minio_client()
    bucket = settings.minio_bucket
    prefix = "player_photos/"

    # List all objects with prefix
    objects = client.list_objects(bucket, prefix=prefix, recursive=True)
    object_list = list(objects)

    count = len(object_list)
    print(f"Found {count} photos in MinIO ({prefix})")

    if count == 0:
        return 0

    if dry_run:
        print("[DRY RUN] Would delete the following files:")
        for obj in object_list[:10]:
            print(f"  - {obj.object_name}")
        if count > 10:
            print(f"  ... and {count - 10} more")
        return count

    # Delete all objects
    deleted = 0
    for obj in object_list:
        try:
            client.remove_object(bucket, obj.object_name)
            deleted += 1
        except Exception as e:
            print(f"  Error deleting {obj.object_name}: {e}")

    print(f"Deleted {deleted} photos from MinIO")
    return deleted


async def cleanup_db_photo_urls(dry_run: bool = False) -> int:
    """Clear photo_url field in players table."""
    async with AsyncSessionLocal() as session:
        # Count players with photos
        from sqlalchemy import select, func

        result = await session.execute(
            select(func.count(Player.id)).where(Player.photo_url.isnot(None))
        )
        count = result.scalar()

        print(f"Found {count} players with photo_url in database")

        if count == 0:
            return 0

        if dry_run:
            print(f"[DRY RUN] Would clear photo_url for {count} players")
            return count

        # Clear all photo_urls
        await session.execute(
            update(Player).values(photo_url=None)
        )
        await session.commit()

        print(f"Cleared photo_url for {count} players")
        return count


async def main():
    parser = argparse.ArgumentParser(description="Cleanup player photos from MinIO and database")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without executing")
    args = parser.parse_args()

    print("=" * 70)
    print("Player Photos Cleanup Script")
    print("=" * 70)

    if args.dry_run:
        print("MODE: DRY RUN (no changes will be made)")
    else:
        print("MODE: FULL CLEANUP (will delete all player photos)")
        confirm = input("\nAre you sure you want to proceed? (yes/no): ")
        if confirm.lower() != "yes":
            print("Aborted.")
            return

    print()

    # Step 1: Cleanup MinIO
    print("[1/2] Cleaning up MinIO storage...")
    minio_count = await cleanup_minio_photos(args.dry_run)

    print()

    # Step 2: Cleanup database
    print("[2/2] Cleaning up database...")
    db_count = await cleanup_db_photo_urls(args.dry_run)

    print()
    print("=" * 70)
    print("Summary:")
    print(f"  MinIO photos {'would be ' if args.dry_run else ''}deleted: {minio_count}")
    print(f"  DB records {'would be ' if args.dry_run else ''}cleared: {db_count}")
    print("=" * 70)

    if args.dry_run:
        print("\nTo execute cleanup, run without --dry-run flag")


if __name__ == "__main__":
    asyncio.run(main())
