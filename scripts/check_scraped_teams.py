#!/usr/bin/env python3
"""Check which teams were actually scraped from kffleague.kz"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from playwright.async_api import async_playwright
from scripts.scrape_player_photos import PlayerPhotoScraper, BASE_URL, normalize_team_name


async def main():
    """Get list of teams from kffleague.kz"""

    scraper = PlayerPhotoScraper(dry_run=True)

    print("=" * 80)
    print("КОМАНДЫ НА САЙТЕ kffleague.kz")
    print("=" * 80)
    print()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        )
        page = await context.new_page()

        # Get team URLs from homepage
        team_urls = await scraper.get_team_urls(page)

        print(f"Всего найдено команд: {len(team_urls)}")
        print()

        # Get team names
        teams_on_site = []
        for i, team_url in enumerate(team_urls, 1):
            try:
                await page.goto(team_url, wait_until="domcontentloaded", timeout=30000)
                await page.wait_for_timeout(1000)

                # Get team name
                team_name_elem = await page.query_selector("h1, .team-name, .club-title")
                if team_name_elem:
                    team_name_kff = await team_name_elem.inner_text()
                    team_name_kff = team_name_kff.strip()
                    team_name_db = normalize_team_name(team_name_kff)

                    teams_on_site.append({
                        'kff': team_name_kff,
                        'db': team_name_db,
                        'url': team_url
                    })

                    print(f"{i:2}. {team_name_kff:30} -> {team_name_db:20}")
                else:
                    print(f"{i:2}. (не удалось получить имя) - {team_url}")

            except Exception as e:
                print(f"{i:2}. ERROR: {team_url} - {e}")

            await asyncio.sleep(1)  # Rate limiting

        await browser.close()

    # Now check which DB teams are NOT on the site
    print()
    print("=" * 80)
    print("ПРОВЕРКА КОМАНД БЕЗ ФОТО")
    print("=" * 80)
    print()

    from app.database import AsyncSessionLocal
    from app.models.player import Player
    from app.models.player_team import PlayerTeam
    from sqlalchemy import select
    from sqlalchemy.orm import selectinload

    async with AsyncSessionLocal() as session:
        # Get players without photos
        result = await session.execute(
            select(Player)
            .where(Player.photo_url.is_(None))
            .options(selectinload(Player.player_teams).selectinload(PlayerTeam.team))
        )
        players_without_photos = result.scalars().all()

        # Get unique teams
        teams_without_photos = set()
        for player in players_without_photos:
            for pt in player.player_teams:
                if pt.team:
                    teams_without_photos.add(pt.team.name)

        teams_on_site_names = {t['db'] for t in teams_on_site}

        print(f"Команды в БД с игроками без фото: {len(teams_without_photos)}")
        print(f"Команды на сайте: {len(teams_on_site_names)}")
        print()

        missing_teams = teams_without_photos - teams_on_site_names
        if missing_teams:
            print(f"Команды БЕЗ страницы на kffleague.kz ({len(missing_teams)}):")
            for team in sorted(missing_teams):
                # Count players
                count = sum(
                    1 for p in players_without_photos
                    for pt in p.player_teams
                    if pt.team and pt.team.name == team
                )
                print(f"  - {team:20} ({count} игроков без фото)")
        else:
            print("✅ Все команды есть на сайте!")

        print()
        present_teams = teams_without_photos & teams_on_site_names
        if present_teams:
            print(f"Команды ЕСТЬ на сайте, но игроки не совпали ({len(present_teams)}):")
            for team in sorted(present_teams):
                count = sum(
                    1 for p in players_without_photos
                    for pt in p.player_teams
                    if pt.team and pt.team.name == team
                )
                print(f"  - {team:20} ({count} игроков без фото)")


if __name__ == "__main__":
    asyncio.run(main())
