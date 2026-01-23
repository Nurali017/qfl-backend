#!/usr/bin/env python3
"""Test that photo_url is preserved during sync."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.database import AsyncSessionLocal
from app.models.player import Player
from sqlalchemy import select


async def main():
    """Test photo preservation."""

    async with AsyncSessionLocal() as session:
        # Get a few players with photos
        result = await session.execute(
            select(Player).where(Player.photo_url.isnot(None)).limit(10)
        )
        players_before = result.scalars().all()

        print("=" * 70)
        print("ТЕСТ СОХРАНЕНИЯ ФОТО ПОСЛЕ SYNC")
        print("=" * 70)
        print()
        print(f"Проверяем {len(players_before)} игроков с фото:")
        print()

        for p in players_before:
            print(f"  - {p.last_name} {p.first_name}")
            print(f"    ID: {p.id}")
            print(f"    Photo: {p.photo_url[:60]}..." if len(p.photo_url) > 60 else f"    Photo: {p.photo_url}")
            print()

        print("=" * 70)
        print("ВАЖНО!")
        print("=" * 70)
        print()
        print("После запуска sync (например, /api/sync/players),")
        print("фото этих игроков ДОЛЖНЫ ОСТАТЬСЯ на месте.")
        print()
        print("Запустите этот скрипт снова после sync, чтобы проверить:")
        print("  python3 scripts/test_photo_preservation.py")


if __name__ == "__main__":
    asyncio.run(main())
