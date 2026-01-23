#!/usr/bin/env python3
"""Simply list all teams found on kffleague.kz"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from playwright.async_api import async_playwright
from scripts.scrape_player_photos import PlayerPhotoScraper, BASE_URL


async def main():
    """Get list of teams."""

    scraper = PlayerPhotoScraper(dry_run=True)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        )
        page = await context.new_page()

        # Get team URLs
        team_urls = await scraper.get_team_urls(page)

        print(f"Найдено команд: {len(team_urls)}")
        print()

        # Get team names
        from scripts.scrape_player_photos import normalize_team_name

        for i, url in enumerate(team_urls, 1):
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=20000)
                await page.wait_for_timeout(500)

                team_name_elem = await page.query_selector("h1, .team-name, .club-title")
                if team_name_elem:
                    team_name = await team_name_elem.inner_text()
                    team_name = team_name.strip()
                    team_db = normalize_team_name(team_name)
                    print(f"{i:2}. {team_name:30} ({team_db:20}) - {url}")
                else:
                    print(f"{i:2}. (no name)                     - {url}")
            except Exception as e:
                print(f"{i:2}. ERROR: {e} - {url}")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
