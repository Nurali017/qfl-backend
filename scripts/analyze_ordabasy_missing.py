#!/usr/bin/env python3
"""Analyze specifically Ordabasy players without photos."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.database import AsyncSessionLocal
from app.models.player import Player
from app.models.player_team import PlayerTeam
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from playwright.async_api import async_playwright
from scripts.scrape_player_photos import PlayerPhotoScraper, transliterate
from fuzzywuzzy import fuzz


async def main():
    """Analyze Ordabasy missing photos."""

    print("=" * 80)
    print("АНАЛИЗ ИГРОКОВ ORDABASY БЕЗ ФОТО")
    print("=" * 80)
    print()

    # Step 1: Get Ordabasy players without photos from DB
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Player)
            .where(Player.photo_url.is_(None))
            .options(selectinload(Player.player_teams).selectinload(PlayerTeam.team))
        )
        all_missing = result.scalars().all()

        ordabasy_missing = []
        for player in all_missing:
            for pt in player.player_teams:
                if pt.team and pt.team.name == "Ordabasy":
                    ordabasy_missing.append(player)
                    break

        print(f"Игроков Ordabasy без фото в БД: {len(ordabasy_missing)}")
        print()
        for i, p in enumerate(ordabasy_missing, 1):
            print(f"  {i:2}. {p.last_name:25} {p.first_name:20}")

    # Step 2: Scrape Ordabasy from site
    print()
    print("=" * 80)
    print("ПАРСИНГ ORDABASY С САЙТА")
    print("=" * 80)
    print()

    scraper = PlayerPhotoScraper(dry_run=True)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        )
        page = await context.new_page()

        # Get all team URLs
        team_urls = await scraper.get_team_urls(page)

        # Scrape all teams to find Ordabasy
        ordabasy_scraped = []
        for url in team_urls:
            players = await scraper.scrape_team(page, url)
            if players and players[0].team_name_db == "Ordabasy":
                ordabasy_scraped = players
                print(f"✅ Найдена Ordabasy: {url}")
                print(f"   Команда на сайте: {players[0].team_name_kff}")
                print(f"   Спарсено игроков: {len(players)}")
                break
            await asyncio.sleep(1)

        await browser.close()

    if not ordabasy_scraped:
        print("❌ Ordabasy не найдена на сайте!")
        return

    # Step 3: Try to match scraped players with missing players
    print()
    print("=" * 80)
    print("СОПОСТАВЛЕНИЕ")
    print("=" * 80)
    print()

    print("Игроки на сайте kffleague.kz:")
    for i, sp in enumerate(ordabasy_scraped, 1):
        print(f"  {i:2}. {sp.first_name:25} {sp.last_name:30}")

    print()
    print("=" * 80)
    print("ПРОВЕРКА КАЖДОГО ИГРОКА БЕЗ ФОТО")
    print("=" * 80)
    print()

    async with AsyncSessionLocal() as session:
        for db_player in ordabasy_missing:
            print(f"DB: {db_player.last_name} {db_player.first_name}")

            # Try to find this player in scraped data
            found = None
            best_score = 0

            for scraped in ordabasy_scraped:
                # Transliterate scraped name
                first_latin = transliterate(scraped.first_name)
                last_latin = transliterate(scraped.last_name)

                # Compare with DB
                scraped_full = f"{first_latin} {last_latin}".lower()
                db_full = f"{db_player.first_name} {db_player.last_name}".lower()

                score = max(
                    fuzz.token_sort_ratio(scraped_full, db_full),
                    fuzz.partial_ratio(scraped_full, db_full)
                )

                if score > best_score:
                    best_score = score
                    found = (scraped, score)

            if found and found[1] >= 65:
                scraped, score = found
                first_latin = transliterate(scraped.first_name)
                last_latin = transliterate(scraped.last_name)
                print(f"  ✅ Возможно это: {scraped.first_name} {scraped.last_name}")
                print(f"     Transliterated: {first_latin} {last_latin}")
                print(f"     Score: {score}")
                print(f"     Photo URL: {scraped.photo_url}")
            else:
                print(f"  ❌ НЕ НАЙДЕН на сайте")
                if found:
                    print(f"     Best match: {found[0].first_name} {found[0].last_name} (score: {found[1]})")

            print()


if __name__ == "__main__":
    asyncio.run(main())
