#!/usr/bin/env python3
"""Find specific players on kffleague.kz and update their photos."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.database import AsyncSessionLocal
from app.models.player import Player
from sqlalchemy import select
from scripts.scrape_player_photos import PlayerPhotoScraper


async def main():
    """Find and update missing player photos."""

    # Players without photos
    missing_ids = [
        '9703402c-4d52-4ab7-be37-6fa228906e00',  # Satpaev Dastan (Kairat)
    ]

    async with AsyncSessionLocal() as session:
        print("=" * 70)
        print("Поиск игроков без фото")
        print("=" * 70)
        print()

        # Get players without photos
        result = await session.execute(
            select(Player).where(Player.photo_url.is_(None))
        )
        players_without_photos = result.scalars().all()

        print(f"Всего игроков без фото: {len(players_without_photos)}")
        print()

        # Show first 20
        print("Первые 20 игроков без фото:")
        for i, p in enumerate(players_without_photos[:20], 1):
            print(f"  {i}. {p.last_name} {p.first_name}")

        print()
        print("=" * 70)
        print("Для обновления фото запустите:")
        print("  python3 scripts/scrape_player_photos.py")
        print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
