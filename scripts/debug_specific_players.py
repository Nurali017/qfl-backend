#!/usr/bin/env python3
"""Debug specific players to see if they can be found on kffleague.kz"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.database import AsyncSessionLocal
from app.models.player import Player
from sqlalchemy import select
from scripts.scrape_player_photos import PlayerPhotoScraper, transliterate


async def main():
    """Test specific players."""

    # Players to test (from different categories)
    test_player_ids = [
        # Portuguese/Brazilian compound names
        ("Macedo Moraes", "Everton"),           # Ordabasy
        ("Machado Mata", "Luis Carlos"),        # Kairat
        ("Santos Filho", "Edmilson de Paula"),  # Kairat

        # Georgian names
        ("Lobjanidze", "Elguja"),               # Jenis
        ("Tsitskishvili", "Zaza"),              # Atyrau

        # Regular foreign names
        ("Ivkovic", "Djordje"),                 # Aktobe - Serbian
        ("Prce", "Franjo"),                     # Okzhetpes - Croatian

        # Kazakh names
        ("Abdashym", "Anuar"),                  # Ordabasy
        ("Tokmagambetov", "Dauren"),            # Turan
    ]

    scraper = PlayerPhotoScraper(dry_run=True)

    async with AsyncSessionLocal() as session:
        print("=" * 80)
        print("ПОИСК КОНКРЕТНЫХ ИГРОКОВ В БД")
        print("=" * 80)
        print()

        for last_name, first_name in test_player_ids:
            print(f"Testing: {last_name} {first_name}")

            # Try exact match first
            result = await session.execute(
                select(Player).where(
                    Player.last_name == last_name,
                    Player.first_name == first_name,
                )
            )
            player = result.scalar_one_or_none()

            if player:
                print(f"  ✅ Found in DB: {player.last_name} {player.first_name} (ID: {player.id})")
                print(f"     Photo: {player.photo_url}")

                # Try fuzzy matching with different name variations
                print(f"  Testing fuzzy matching strategies:")

                # Strategy: Try reversed
                matched = await scraper.find_player_in_db(session, first_name, last_name)
                if matched:
                    print(f"    - Reversed names: ✅ {matched.last_name} {matched.first_name}")
                else:
                    print(f"    - Reversed names: ❌")

                # Strategy: Try transliterated (if Cyrillic in original)
                first_translit = transliterate(first_name)
                last_translit = transliterate(last_name)
                if first_translit != first_name or last_translit != last_name:
                    matched = await scraper.find_player_in_db(session, first_translit, last_translit)
                    if matched:
                        print(f"    - Transliterated: ✅ {matched.last_name} {matched.first_name}")
                    else:
                        print(f"    - Transliterated: ❌")

                # Strategy: Try parts of compound name
                if " " in last_name:
                    parts = last_name.split()
                    # Try first part as last name
                    matched = await scraper.find_player_in_db(session, first_name, parts[0])
                    if matched:
                        print(f"    - First part of last name ({parts[0]}): ✅ {matched.last_name} {matched.first_name}")
                    else:
                        print(f"    - First part of last name ({parts[0]}): ❌")

                    # Try last part as last name
                    matched = await scraper.find_player_in_db(session, first_name, parts[-1])
                    if matched:
                        print(f"    - Last part of last name ({parts[-1]}): ✅ {matched.last_name} {matched.first_name}")
                    else:
                        print(f"    - Last part of last name ({parts[-1]}): ❌")
            else:
                print(f"  ❌ NOT found in DB with exact match")

            print()


if __name__ == "__main__":
    asyncio.run(main())
