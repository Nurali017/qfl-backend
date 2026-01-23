#!/usr/bin/env python3
"""Test fuzzy matching for complex player names."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.database import AsyncSessionLocal
from scripts.scrape_player_photos import PlayerPhotoScraper


async def test_matching():
    """Test fuzzy matching on problematic names."""

    # Test cases: (first_name_from_site, last_name_from_site)
    test_cases = [
        # Geoffrey - попробуем разные варианты как на сайте
        ("Джеффри", "Чинеду"),
        ("Geoffrey", "Chinedu"),
        ("Chinedu", "Geoffrey"),  # Swapped

        # da Costa Monteiro
        ("Jorge", "Gabriel"),
        ("Gabriel", "Jorge"),
        ("Monteiro", "Jorge"),
    ]

    scraper = PlayerPhotoScraper(dry_run=True)

    print("=" * 80)
    print("Testing Fuzzy Matching Algorithm")
    print("=" * 80)
    print()

    async with AsyncSessionLocal() as session:
        for first, last in test_cases:
            print(f"Testing: {first} {last}")
            player = await scraper.find_player_in_db(session, first, last)

            if player:
                print(f"  ✅ FOUND: {player.last_name} {player.first_name}")
                print(f"     ID: {player.id}")
            else:
                print(f"  ❌ NOT FOUND")
            print()


if __name__ == "__main__":
    asyncio.run(test_matching())
