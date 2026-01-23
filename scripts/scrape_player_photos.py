#!/usr/bin/env python3
"""
Scrape player photos from kffleague.kz and upload to MinIO.

Workflow:
1. Get all team pages from kffleague.kz
2. Scrape player photos and names from each team using .player-card elements
3. Match players with DB by first_name + last_name (using Cyrillic comparison)
4. Download photos and upload to MinIO (player_photos category)
5. Update photo_url in players table

Usage:
    # Dry run (no DB changes)
    python scripts/scrape_player_photos.py --dry-run

    # Full run
    python scripts/scrape_player_photos.py

    # Limit teams for testing
    python scripts/scrape_player_photos.py --limit-teams 5
"""

import argparse
import asyncio
import io
import re
import sys
from dataclasses import dataclass
from pathlib import Path

import httpx
from playwright.async_api import async_playwright
from sqlalchemy import select, func, or_
from fuzzywuzzy import fuzz

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.database import AsyncSessionLocal
from app.models.player import Player
from app.minio_client import get_minio_client, get_public_url
from app.config import get_settings

settings = get_settings()

BASE_URL = "https://kffleague.kz"


def normalize_name(name: str) -> str:
    """Normalize name for comparison: lowercase, strip, collapse spaces."""
    return re.sub(r'\s+', ' ', name.strip().lower())


def cyrillic_to_latin(text: str) -> str:
    """Transliterate Cyrillic to Latin for fallback comparison."""
    translit_map = {
        'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'yo',
        'ж': 'zh', 'з': 'z', 'и': 'i', 'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm',
        'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u',
        'ф': 'f', 'х': 'kh', 'ц': 'ts', 'ч': 'ch', 'ш': 'sh', 'щ': 'shch',
        'ъ': '', 'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu', 'я': 'ya',
        # Kazakh specific
        'ә': 'a', 'ғ': 'g', 'қ': 'q', 'ң': 'n', 'ө': 'o', 'ұ': 'u', 'ү': 'u',
        'һ': 'h', 'і': 'i',
    }
    result = []
    for char in text.lower():
        result.append(translit_map.get(char, char))
    return ''.join(result)


@dataclass
class ScrapedPlayer:
    """Player data scraped from kffleague.kz"""
    first_name: str  # Имя (e.g., "Бауыржан")
    last_name: str   # Фамилия (e.g., "ИСЛАМХАН")
    photo_url: str
    team_name: str
    jersey_number: str = ""


class PlayerPhotoScraper:
    """Scraper for player photos from kffleague.kz"""

    def __init__(self, dry_run: bool = False, limit_teams: int | None = None):
        self.dry_run = dry_run
        self.limit_teams = limit_teams
        self.stats = {
            "teams_scraped": 0,
            "players_scraped": 0,
            "players_matched": 0,
            "photos_uploaded": 0,
            "photos_skipped": 0,
            "errors": 0,
        }
        self.unmatched: list[str] = []

    async def get_team_urls(self, page) -> list[dict]:
        """Get all team page URLs from the main page."""
        print("      Navigating to main page...")
        await page.goto(f"{BASE_URL}/ru", wait_until="networkidle", timeout=60000)
        await page.wait_for_timeout(2000)

        # Extract unique team URLs with names
        teams = await page.evaluate("""
            () => {
                const links = document.querySelectorAll('a[href*="/ru/team/"]');
                const teamsMap = new Map();
                links.forEach(link => {
                    const href = link.href;
                    const match = href.match(/\\/ru\\/team\\/(\\d+)/);
                    if (match) {
                        const id = match[1];
                        if (!teamsMap.has(id)) {
                            teamsMap.set(id, {
                                id: id,
                                url: href,
                                name: link.textContent?.trim() || ''
                            });
                        }
                    }
                });
                return Array.from(teamsMap.values());
            }
        """)

        return teams

    async def scrape_team(self, page, team_info: dict, retries: int = 3) -> list[ScrapedPlayer]:
        """Scrape players from a team page using .player-card elements."""
        players = []
        team_url = team_info['url']

        for attempt in range(retries):
            try:
                await page.goto(team_url, wait_until="networkidle", timeout=60000)
                await page.wait_for_timeout(2000)
                break
            except Exception as e:
                if attempt < retries - 1:
                    print(f" [retry {attempt + 1}]", end="", flush=True)
                    await asyncio.sleep(5)
                else:
                    print(f" Error: {e}")
                    self.stats["errors"] += 1
                    return players

        try:
            # Get team name from page header
            team_name = await page.evaluate("""
                () => {
                    const h1 = document.querySelector('h1');
                    return h1?.textContent?.trim() || '';
                }
            """)

            if not team_name:
                team_name = team_info.get('name', 'Unknown')

            # Extract players from .player-card elements
            player_data = await page.evaluate("""
                () => {
                    const cards = document.querySelectorAll('.player-card');
                    const players = [];

                    cards.forEach(card => {
                        const img = card.querySelector('img');
                        const nameEl = card.querySelector('.player-name');
                        const numberEl = card.querySelector('.player-number');

                        if (img && nameEl) {
                            // Get photo URL
                            let photoUrl = img.src || img.getAttribute('data-src') || '';
                            if (photoUrl && !photoUrl.startsWith('http')) {
                                photoUrl = 'https://kffleague.kz' + photoUrl;
                            }

                            // Get name text and clean it
                            // Format on site: "ФАМИЛИЯ\\nИмя" (e.g., "ИСЛАМХАН\\nБауыржан")
                            const nameText = nameEl.textContent?.trim() || '';

                            // Get jersey number
                            const number = numberEl?.textContent?.trim() || '';

                            if (photoUrl && nameText && photoUrl.includes('/upload/people/')) {
                                players.push({
                                    name: nameText,
                                    photo: photoUrl,
                                    number: number
                                });
                            }
                        }
                    });

                    return players;
                }
            """)

            # Process each player
            seen_photos = set()
            for p in player_data:
                # Skip duplicates
                if p['photo'] in seen_photos:
                    continue
                seen_photos.add(p['photo'])

                # Parse name: "ФАМИЛИЯ\nИмя" -> last_name="Фамилия", first_name="Имя"
                name_text = p['name']
                # Replace newlines and multiple spaces
                name_parts = [part.strip() for part in re.split(r'[\n\r]+', name_text) if part.strip()]

                if len(name_parts) >= 2:
                    # First part is last name (uppercase), second is first name
                    last_name = name_parts[0].title()  # Convert from UPPERCASE to Title Case
                    first_name = ' '.join(name_parts[1:])
                elif len(name_parts) == 1:
                    # Single name - try to split by space
                    parts = name_parts[0].split()
                    if len(parts) >= 2:
                        last_name = parts[0].title()
                        first_name = ' '.join(parts[1:])
                    else:
                        last_name = parts[0].title()
                        first_name = ""
                else:
                    continue

                player = ScrapedPlayer(
                    first_name=first_name,
                    last_name=last_name,
                    photo_url=p['photo'],
                    team_name=team_name,
                    jersey_number=p['number']
                )
                players.append(player)

        except Exception as e:
            print(f" Parse error: {e}")
            self.stats["errors"] += 1

        return players

    async def find_player_in_db(self, session, scraped: ScrapedPlayer) -> Player | None:
        """
        Find player in DB by name comparison.

        Site format: ФАМИЛИЯ + Имя (Cyrillic)
        DB format: first_name + last_name (could be Cyrillic or Latin)

        Strategies:
        1. Direct Cyrillic match (first_name_kz, last_name_kz)
        2. Direct match (first_name, last_name)
        3. Fuzzy match with high threshold
        """
        first_name = scraped.first_name
        last_name = scraped.last_name

        # Strategy 1: Match using Kazakh name fields (Cyrillic)
        if first_name and last_name:
            result = await session.execute(
                select(Player).where(
                    func.lower(Player.first_name_kz) == first_name.lower(),
                    func.lower(Player.last_name_kz) == last_name.lower(),
                )
            )
            players = result.scalars().all()
            if len(players) == 1:
                return players[0]

            # Try swapped (some names might be in different order)
            result = await session.execute(
                select(Player).where(
                    func.lower(Player.first_name_kz) == last_name.lower(),
                    func.lower(Player.last_name_kz) == first_name.lower(),
                )
            )
            players = result.scalars().all()
            if len(players) == 1:
                return players[0]

        # Strategy 2: Match using main name fields
        if first_name and last_name:
            result = await session.execute(
                select(Player).where(
                    func.lower(Player.first_name) == first_name.lower(),
                    func.lower(Player.last_name) == last_name.lower(),
                )
            )
            players = result.scalars().all()
            if len(players) == 1:
                return players[0]

            # Try swapped
            result = await session.execute(
                select(Player).where(
                    func.lower(Player.first_name) == last_name.lower(),
                    func.lower(Player.last_name) == first_name.lower(),
                )
            )
            players = result.scalars().all()
            if len(players) == 1:
                return players[0]

        # Strategy 3: Transliterate and match
        first_latin = cyrillic_to_latin(first_name)
        last_latin = cyrillic_to_latin(last_name)

        if first_latin and last_latin:
            result = await session.execute(
                select(Player).where(
                    func.lower(Player.first_name) == first_latin.lower(),
                    func.lower(Player.last_name) == last_latin.lower(),
                )
            )
            players = result.scalars().all()
            if len(players) == 1:
                return players[0]

            # Try swapped
            result = await session.execute(
                select(Player).where(
                    func.lower(Player.first_name) == last_latin.lower(),
                    func.lower(Player.last_name) == first_latin.lower(),
                )
            )
            players = result.scalars().all()
            if len(players) == 1:
                return players[0]

        # Strategy 4: Partial last name match
        if len(last_name) >= 4:
            # Try first 4+ characters of last name
            result = await session.execute(
                select(Player).where(
                    or_(
                        func.lower(Player.last_name).startswith(last_name[:4].lower()),
                        func.lower(Player.last_name_kz).startswith(last_name[:4].lower()),
                        func.lower(Player.last_name).startswith(last_latin[:4].lower()),
                    )
                )
            )
            candidates = result.scalars().all()

            # Filter by first name similarity
            for candidate in candidates:
                # Check if first names match partially
                cand_first = (candidate.first_name or "").lower()
                cand_first_kz = (candidate.first_name_kz or "").lower()
                scraped_first = first_name.lower()
                scraped_first_latin = first_latin.lower()

                if (scraped_first and (
                    scraped_first in cand_first or
                    scraped_first in cand_first_kz or
                    scraped_first_latin in cand_first or
                    cand_first in scraped_first or
                    cand_first_kz in scraped_first
                )):
                    return candidate

        # Strategy 5: Fuzzy matching as last resort
        scraped_full = f"{first_name} {last_name}".lower()
        scraped_full_latin = f"{first_latin} {last_latin}".lower()

        # Get candidates with any partial match
        name_parts = [p for p in (first_name, last_name, first_latin, last_latin) if p and len(p) >= 3]
        if name_parts:
            conditions = []
            for part in name_parts[:4]:
                part_lower = part.lower()
                conditions.extend([
                    func.lower(Player.first_name).contains(part_lower),
                    func.lower(Player.last_name).contains(part_lower),
                    func.lower(Player.first_name_kz).contains(part_lower),
                    func.lower(Player.last_name_kz).contains(part_lower),
                ])

            result = await session.execute(
                select(Player).where(or_(*conditions)).limit(50)
            )
            candidates = result.scalars().all()

            best_match = None
            best_score = 0

            for player in candidates:
                if not player.first_name or not player.last_name:
                    continue

                # Try both orderings
                db_full = f"{player.first_name} {player.last_name}".lower()
                db_full_kz = f"{player.first_name_kz or ''} {player.last_name_kz or ''}".lower()

                # Calculate fuzzy scores
                scores = [
                    fuzz.token_sort_ratio(scraped_full, db_full),
                    fuzz.token_sort_ratio(scraped_full, db_full_kz),
                    fuzz.token_sort_ratio(scraped_full_latin, db_full),
                ]

                score = max(scores)

                if score > best_score and score >= 75:  # Higher threshold for fuzzy
                    best_score = score
                    best_match = player

            if best_match:
                return best_match

        return None

    async def download_photo(self, photo_url: str) -> bytes | None:
        """Download photo from URL."""
        try:
            async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
                response = await client.get(photo_url)
                response.raise_for_status()
                return response.content
        except Exception as e:
            print(f"      Download failed: {photo_url[:50]}... - {e}")
            return None

    async def upload_to_minio(self, photo_data: bytes, player_id: str) -> str | None:
        """Upload photo to MinIO and return URL."""
        try:
            client = get_minio_client()
            bucket = settings.minio_bucket

            object_name = f"player_photos/{player_id}.webp"
            data = io.BytesIO(photo_data)

            client.put_object(
                bucket_name=bucket,
                object_name=object_name,
                data=data,
                length=len(photo_data),
                content_type="image/webp",
            )

            return get_public_url(object_name)
        except Exception as e:
            print(f"      MinIO upload failed: {e}")
            return None

    async def run(self):
        """Main scraping workflow."""
        print("=" * 70)
        print("KFF League Player Photo Scraper v2.0")
        print("=" * 70)
        if self.dry_run:
            print("MODE: DRY RUN (no changes will be made)")
        print()

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) "
                           "Chrome/120.0.0.0 Safari/537.36",
                viewport={"width": 1920, "height": 1080}
            )
            page = await context.new_page()

            # Step 1: Get team URLs
            print("[1/4] Getting team list...")
            teams = await self.get_team_urls(page)
            print(f"      Found {len(teams)} unique teams")

            # Apply limit if set
            if self.limit_teams:
                teams = teams[:self.limit_teams]
                print(f"      Limited to {len(teams)} teams for testing")

            # Step 2: Scrape all teams
            print(f"\n[2/4] Scraping teams...")
            all_players: list[ScrapedPlayer] = []

            for i, team in enumerate(teams, 1):
                team_display = team.get('name', team['id'])[:20]
                print(f"      [{i}/{len(teams)}] {team_display}", end="", flush=True)
                players = await self.scrape_team(page, team)
                all_players.extend(players)
                self.stats["teams_scraped"] += 1
                print(f" -> {len(players)} players")
                await asyncio.sleep(2)  # Rate limiting

            self.stats["players_scraped"] = len(all_players)
            print(f"\n      Total scraped: {len(all_players)} players")

            await browser.close()

        # Step 3: Match and upload
        print(f"\n[3/4] Matching players with DB and uploading photos...")

        async with AsyncSessionLocal() as session:
            for i, scraped in enumerate(all_players, 1):
                if i % 50 == 0:
                    print(f"      Progress: {i}/{len(all_players)}")

                # Find player in DB
                player = await self.find_player_in_db(session, scraped)

                if not player:
                    self.unmatched.append(
                        f"{scraped.last_name} {scraped.first_name} ({scraped.team_name})"
                    )
                    continue

                self.stats["players_matched"] += 1

                # Skip if already has our MinIO photo
                if player.photo_url and "player_photos/" in player.photo_url:
                    self.stats["photos_skipped"] += 1
                    continue

                if self.dry_run:
                    print(f"      [DRY] Would upload: {scraped.last_name} {scraped.first_name} -> {player.first_name} {player.last_name}")
                    continue

                # Download photo
                photo_data = await self.download_photo(scraped.photo_url)
                if not photo_data:
                    self.stats["errors"] += 1
                    continue

                # Upload to MinIO
                new_url = await self.upload_to_minio(photo_data, str(player.id))
                if not new_url:
                    self.stats["errors"] += 1
                    continue

                # Update player
                player.photo_url = new_url
                self.stats["photos_uploaded"] += 1

            if not self.dry_run:
                await session.commit()
                print("      Committed changes to DB")

        # Step 4: Summary
        print("\n[4/4] Summary")
        print("=" * 70)
        print(f"  Teams scraped:    {self.stats['teams_scraped']}")
        print(f"  Players scraped:  {self.stats['players_scraped']}")
        print(f"  Players matched:  {self.stats['players_matched']}")
        print(f"  Photos uploaded:  {self.stats['photos_uploaded']}")
        print(f"  Photos skipped:   {self.stats['photos_skipped']} (already have)")
        print(f"  Errors:           {self.stats['errors']}")
        print(f"  Unmatched:        {len(self.unmatched)}")
        print("=" * 70)

        if self.unmatched:
            print(f"\nUnmatched players ({len(self.unmatched)} total, showing first 30):")
            for name in self.unmatched[:30]:
                print(f"  - {name}")
            if len(self.unmatched) > 30:
                print(f"  ... and {len(self.unmatched) - 30} more")


def main():
    parser = argparse.ArgumentParser(description="Scrape player photos from kffleague.kz")
    parser.add_argument("--dry-run", action="store_true", help="Run without making changes")
    parser.add_argument("--limit-teams", type=int, help="Limit number of teams (for testing)")
    args = parser.parse_args()

    scraper = PlayerPhotoScraper(dry_run=args.dry_run, limit_teams=args.limit_teams)
    asyncio.run(scraper.run())


if __name__ == "__main__":
    main()
