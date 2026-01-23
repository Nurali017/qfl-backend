#!/usr/bin/env python3
"""Debug detailed matching for specific problematic players."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.database import AsyncSessionLocal
from scripts.scrape_player_photos import PlayerPhotoScraper, transliterate
from fuzzywuzzy import fuzz


async def main():
    """Test matching for specific names from site."""

    # Names from kffleague.kz site (Cyrillic)
    test_cases = [
        # From unmatched list
        ("Маседо", "Эвертон"),                        # Should be: Macedo Moraes Everton
        ("Фернандес", "Жоао Паулино Морейра"),         # Should be: Moreira Fernandes Joao Paulino
        ("Абдуллаев", "Тимур"),
        ("Зинабдинов", "Ерасыл"),

        # From matched list (should work)
        ("Диас", "Алан"),
        ("Малый", "Сергей"),
        ("Чанаджа", "Дарио"),
    ]

    scraper = PlayerPhotoScraper(dry_run=True)

    print("=" * 80)
    print("ДЕТАЛЬНАЯ ПРОВЕРКА MATCHING")
    print("=" * 80)
    print()

    async with AsyncSessionLocal() as session:
        for first, last in test_cases:
            print(f"Scraped (Cyrillic): {first} {last}")

            # Transliterate
            first_latin = transliterate(first)
            last_latin = transliterate(last)
            print(f"Transliterated:     {first_latin} {last_latin}")

            # Try to find in DB
            player = await scraper.find_player_in_db(session, first, last)

            if player:
                print(f"✅ MATCHED with:    {player.first_name} {player.last_name}")
                print(f"   ID: {player.id}")
                print(f"   Photo: {player.photo_url or 'None'}")

                # Calculate fuzzy scores
                scraped_full = f"{first_latin} {last_latin}".lower()
                db_full = f"{player.first_name} {player.last_name}".lower()

                token_sort = fuzz.token_sort_ratio(scraped_full, db_full)
                partial = fuzz.partial_ratio(scraped_full, db_full)
                token_set = fuzz.token_set_ratio(scraped_full, db_full)

                print(f"   Scores: token_sort={token_sort}, partial={partial}, token_set={token_set}")
            else:
                print(f"❌ NOT MATCHED")

                # Try to find similar players manually
                from app.models.player import Player
                from sqlalchemy import select, func

                # Search by partial name
                parts = (first_latin + " " + last_latin).split()
                if parts:
                    result = await session.execute(
                        select(Player).where(
                            func.lower(Player.last_name).like(f"%{parts[0][:4].lower()}%")
                        ).limit(5)
                    )
                    candidates = result.scalars().all()

                    if candidates:
                        print(f"   Possible candidates:")
                        for c in candidates:
                            scraped_full = f"{first_latin} {last_latin}".lower()
                            db_full = f"{c.first_name} {c.last_name}".lower()
                            score = max(
                                fuzz.token_sort_ratio(scraped_full, db_full),
                                fuzz.partial_ratio(scraped_full, db_full)
                            )
                            print(f"     - {c.first_name} {c.last_name} (score: {score})")

            print()


if __name__ == "__main__":
    asyncio.run(main())
