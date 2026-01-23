#!/usr/bin/env python3
"""Debug scraping for Ordabasy team specifically."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from playwright.async_api import async_playwright
from scripts.scrape_player_photos import PlayerPhotoScraper, BASE_URL, normalize_team_name
from app.database import AsyncSessionLocal


async def main():
    """Scrape Ordabasy and see what we get."""

    print("=" * 80)
    print("ПОИСК КОМАНДЫ ORDABASY НА САЙТЕ")
    print("=" * 80)
    print()

    scraper = PlayerPhotoScraper(dry_run=True)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)  # headless=False to see what's happening
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        )
        page = await context.new_page()

        # Get all team URLs
        team_urls = await scraper.get_team_urls(page)
        print(f"Найдено команд: {len(team_urls)}")
        print()

        # Find Ordabasy
        ordabasy_url = None
        for url in team_urls:
            if "ordabasy" in url.lower() or "ордабасы" in url.lower():
                ordabasy_url = url
                print(f"✅ Найдена Ordabasy: {url}")
                break

        if not ordabasy_url:
            # Try searching manually
            print("Не найдена в списке команд, попробуем найти вручную...")
            for url in team_urls[:5]:
                print(f"  Checking: {url}")

        if ordabasy_url:
            print()
            print("=" * 80)
            print("ПАРСИНГ СОСТАВА ORDABASY")
            print("=" * 80)
            print()

            # Scrape this team
            players = await scraper.scrape_team(page, ordabasy_url)

            print(f"\nВсего спарсено игроков: {len(players)}")
            print()

            # Show all scraped players
            print("Спарсенные игроки:")
            for i, p in enumerate(players, 1):
                print(f"  {i:2}. {p.first_name:20} {p.last_name:25}")

            print()
            print("=" * 80)
            print("СОПОСТАВЛЕНИЕ С БД")
            print("=" * 80)
            print()

            # Now try to match with DB
            async with AsyncSessionLocal() as session:
                matched = []
                unmatched = []

                for scraped in players:
                    player = await scraper.find_player_in_db(
                        session, scraped.first_name, scraped.last_name
                    )

                    if player:
                        matched.append({
                            'scraped': f"{scraped.first_name} {scraped.last_name}",
                            'db': f"{player.first_name} {player.last_name}",
                            'has_photo': player.photo_url is not None,
                            'photo_url': scraped.photo_url
                        })
                    else:
                        unmatched.append({
                            'scraped': f"{scraped.first_name} {scraped.last_name}",
                            'photo_url': scraped.photo_url
                        })

                print(f"Совпало: {len(matched)}/{len(players)}")
                print(f"Не совпало: {len(unmatched)}/{len(players)}")
                print()

                if matched:
                    print("СОВПАВШИЕ ИГРОКИ:")
                    for m in matched:
                        photo_status = "✅ Есть фото" if m['has_photo'] else "❌ Нет фото"
                        print(f"  {photo_status} | Scraped: {m['scraped']:30} -> DB: {m['db']}")

                print()

                if unmatched:
                    print("НЕ СОВПАВШИЕ ИГРОКИ:")
                    for u in unmatched:
                        print(f"  ❌ Scraped: {u['scraped']:30} | URL: {u['photo_url']}")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
