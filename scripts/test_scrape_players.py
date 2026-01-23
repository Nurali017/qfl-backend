#!/usr/bin/env python3
"""
Test script to scrape player photos from ALL teams on kffleague.kz.
Maps team names from Russian/Kazakh to English for DB matching.

Usage:
    python scripts/test_scrape_players.py
"""

import asyncio
from dataclasses import dataclass
from playwright.async_api import async_playwright

BASE_URL = "https://kffleague.kz"

# Mapping: KFF site name (Russian/Kazakh) -> DB name (English)
TEAM_NAME_MAP = {
    # Premier League
    "астана": "Astana",
    "кайрат": "Kairat",
    "тобол": "Tobol",
    "актобе": "Aktobe",
    "ордабасы": "Ordabasy",
    "кызылжар": "Kyzylzhar",
    "жетысу": "Jetisu",
    "атырау": "Atyrau",
    "женис": "Jenis",
    "жеңіс": "Jenis",
    "кайсар": "Kaysar",
    "елімай": "Elimai",
    "елимай": "Elimai",
    "туран": "Turan",
    "окжетпес": "Okzhetpes",
    "шахтер": "Shakhter",
    "каспий": "Caspiy",
    "улытау": "Ulytau",
    "тараз": "Taraz",
    "экибастуз": "Ekibastuz",
    "алтай": "Altai",
    "мақтаарал": "Maqtaaral",
    "ертіс": "Ertis",
    "арыс": "Arys",
    "жетысай": "Jetisay",
    "хан тенгри": "Khan Tengri",
    "онтустик": "Ontustik",
    "тұркістан": "Turkistan",
    "түркістан": "Turkistan",
    "туркестан": "Turkistan",
    "талас": "Talas",
    "жайык": "Jaiyq",
    "жас қыран": "Jas Qyran",
    "жейран": "Jeyran",
    "sd family": "SD Family",
    "акас": "AKAS",
    # Women
    "биик": "BIIK Shymkent W",
    "биик-шымкент": "BIIK Shymkent W",
    "томирис": "Tomiris-Turan W",
    "томирис-туран": "Tomiris-Turan W",
    # Youth suffix
    "жастар": "Zhastar",
}


def normalize_team_name(kff_name: str) -> str:
    """
    Convert KFF site team name (Russian/Kazakh) to DB name (English).
    Example: "КАЙСАР М" -> "Kaysar M"
    """
    name = kff_name.strip().lower()

    # Check for suffix (М = M for youth teams, W for women)
    suffix = ""
    if name.endswith(" м") or name.endswith("-м"):
        suffix = " M"
        name = name.replace(" м", "").replace("-м", "").strip()
    elif name.endswith(" w") or name.endswith("-w"):
        suffix = " W"
        name = name.replace(" w", "").replace("-w", "").strip()
    elif "жастар" in name:
        suffix = "-Zhastar"
        name = name.replace("жастар", "").replace("-", "").strip()

    # Look up in map
    db_name = TEAM_NAME_MAP.get(name)

    if db_name:
        return db_name + suffix

    # Fallback: return original with suffix
    return kff_name.strip() + suffix


@dataclass
class ParsedPlayer:
    """Player data parsed from kffleague.kz"""
    first_name: str
    last_name: str
    photo_url: str
    team_name_kff: str  # Original name from site
    team_name_db: str   # Normalized for DB matching


async def scrape_all_teams():
    """Scrape players from all team pages."""
    print("=" * 60)
    print("KFF League Player Photo Scraper - ALL TEAMS")
    print("=" * 60)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080}
        )
        page = await context.new_page()

        # Step 1: Get list of all teams
        print("\n[1] Getting list of teams...")
        await page.goto(f"{BASE_URL}/ru", wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(3000)

        team_links = await page.query_selector_all('a[href^="/ru/team/"]')
        team_urls = set()
        for link in team_links:
            href = await link.get_attribute("href")
            if href:
                full_url = f"{BASE_URL}{href}" if not href.startswith("http") else href
                team_urls.add(full_url)

        team_urls = list(team_urls)
        print(f"    Found {len(team_urls)} teams")

        # Step 2: Scrape teams (test: first 3)
        all_players = []
        test_teams = team_urls[:3]

        print(f"\n[2] Scraping {len(test_teams)} teams (test mode)...")

        for i, team_url in enumerate(test_teams, 1):
            print(f"\n    [{i}/{len(test_teams)}] {team_url}")

            try:
                await page.goto(team_url, wait_until="domcontentloaded", timeout=30000)
                await page.wait_for_timeout(3000)

                # Get team name from site
                team_name_elem = await page.query_selector("h1, .team-name, .club-title")
                team_name_kff = await team_name_elem.inner_text() if team_name_elem else "Unknown"
                team_name_kff = team_name_kff.strip()

                # Normalize to DB name
                team_name_db = normalize_team_name(team_name_kff)

                print(f"        KFF: '{team_name_kff}' -> DB: '{team_name_db}'")

                # Find player images
                images = await page.query_selector_all('img[src*="/upload/people/"]')
                print(f"        Found {len(images)} player images")

                for img in images:
                    src = await img.get_attribute("src")
                    if not src:
                        continue

                    full_src = src if src.startswith("http") else f"{BASE_URL}{src}"

                    # Skip duplicates
                    if any(p.photo_url == full_src for p in all_players):
                        continue

                    # Get player name
                    name_text = await img.get_attribute("alt") or ""

                    if not name_text:
                        try:
                            container = await img.evaluate_handle(
                                "el => el.closest('.player-card, .player, .squad-item, [class*=\"player\"]')"
                            )
                            if container:
                                name_elem = await container.query_selector(
                                    ".player-name, .name, h3, h4, span, p"
                                )
                                if name_elem:
                                    name_text = await name_elem.inner_text()
                        except:
                            pass

                    if not name_text:
                        try:
                            parent = await img.evaluate_handle("el => el.parentElement")
                            if parent:
                                text = await parent.inner_text()
                                lines = [l.strip() for l in text.split('\n') if l.strip()]
                                for line in lines:
                                    if 2 < len(line) < 40 and not line.isdigit():
                                        name_text = line
                                        break
                        except:
                            pass

                    # Parse name
                    name_text = name_text.strip()
                    parts = name_text.split() if name_text else []

                    if len(parts) >= 2:
                        last_name = parts[0]
                        first_name = " ".join(parts[1:])
                    elif len(parts) == 1:
                        last_name = parts[0]
                        first_name = ""
                    else:
                        last_name = "Unknown"
                        first_name = ""

                    player = ParsedPlayer(
                        first_name=first_name,
                        last_name=last_name,
                        photo_url=full_src,
                        team_name_kff=team_name_kff,
                        team_name_db=team_name_db
                    )
                    all_players.append(player)

                print(f"        Added {len([p for p in all_players if p.team_name_kff == team_name_kff])} players")

            except Exception as e:
                print(f"        Error: {e}")
                continue

            await asyncio.sleep(1)

        # Summary
        print("\n" + "=" * 60)
        print("RESULTS")
        print("=" * 60)
        print(f"\nTotal players: {len(all_players)}")

        # Group by team
        teams = {}
        for p in all_players:
            key = f"{p.team_name_kff} -> {p.team_name_db}"
            if key not in teams:
                teams[key] = []
            teams[key].append(p)

        for team, players in teams.items():
            print(f"\n{team}: {len(players)} players")
            for p in players[:2]:
                print(f"  - {p.last_name} {p.first_name}")
            if len(players) > 2:
                print(f"  ... and {len(players) - 2} more")

        print("\n[3] Browser closing in 10 seconds...")
        await page.wait_for_timeout(10000)
        await browser.close()

        return all_players


if __name__ == "__main__":
    asyncio.run(scrape_all_teams())
