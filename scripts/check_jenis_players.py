#!/usr/bin/env python3
"""Check if Jenis players are on kffleague.kz"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from playwright.async_api import async_playwright
from scripts.scrape_player_photos import PlayerPhotoScraper, transliterate
from fuzzywuzzy import fuzz


# Jenis players without photos from DB
JENIS_PLAYERS = [
    ("Abalora", "Razak"),
    ("Carmo Teixeira", "Goncalo Filipe"),
    ("Daci", "Erdon"),
    ("Dos Reis Batalha", "Rui Pedro"),
    ("Hueco Martinez", "Miguel Angel"),
    ("Lobjanidze", "Elguja"),
    ("Novak", "Jacob"),
    ("Nowak", "Krystian"),
    ("Venizelou", "Konstantinos"),
    ("Yakudi", "Khamza"),
]


async def main():
    """Check if Jenis players are on the website."""

    scraper = PlayerPhotoScraper(dry_run=True)

    print("=" * 80)
    print("ПРОВЕРКА ИГРОКОВ JENIS НА САЙТЕ kffleague.kz")
    print("=" * 80)
    print()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        )
        page = await context.new_page()

        # Get all team URLs
        team_urls = await scraper.get_team_urls(page)

        # Find Jenis
        jenis_url = None
        for url in team_urls:
            players = await scraper.scrape_team(page, url)
            if players and players[0].team_name_db.lower() in ["jenis", "zhenis"]:
                jenis_url = url
                print(f"✅ Найдена команда Jenis: {url}")
                print(f"   Название на сайте: {players[0].team_name_kff}")
                print(f"   Спарсено игроков: {len(players)}")
                print()
                print("=" * 80)
                print("ИГРОКИ НА САЙТЕ:")
                print("=" * 80)
                for i, p in enumerate(players, 1):
                    print(f"{i:2}. {p.first_name:25} {p.last_name:30}")

                print()
                print("=" * 80)
                print("СОПОСТАВЛЕНИЕ С ИГРОКАМИ БЕЗ ФОТО:")
                print("=" * 80)
                print()

                # Try to match each DB player
                for db_last, db_first in JENIS_PLAYERS:
                    print(f"БД: {db_last:25} {db_first:20}")

                    best_match = None
                    best_score = 0

                    for scraped in players:
                        # Transliterate
                        scraped_first = transliterate(scraped.first_name)
                        scraped_last = transliterate(scraped.last_name)

                        # Compare
                        scraped_full = f"{scraped_first} {scraped_last}".lower()
                        db_full = f"{db_first} {db_last}".lower()

                        score = max(
                            fuzz.token_sort_ratio(scraped_full, db_full),
                            fuzz.partial_ratio(scraped_full, db_full)
                        )

                        if score > best_score:
                            best_score = score
                            best_match = (scraped, score)

                    if best_match and best_match[1] >= 65:
                        scraped, score = best_match
                        print(f"  ✅ Возможно: {scraped.first_name:25} {scraped.last_name:30} (score: {score})")
                        print(f"     URL: {scraped.photo_url}")
                    else:
                        if best_match:
                            print(f"  ⚠️ Лучший: {best_match[0].first_name:25} {best_match[0].last_name:30} (score: {best_score} - ниже порога)")
                        else:
                            print(f"  ❌ НЕ НАЙДЕН на сайте")

                    print()

                break

        if not jenis_url:
            print("❌ Команда Jenis не найдена на сайте!")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
