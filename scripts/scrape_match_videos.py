#!/usr/bin/env python3
"""
Scrape YouTube video URLs from kffleague.kz match pages and update games table.

Workflow:
1. Iterate through match pages on kffleague.kz
2. Extract match info (date, teams, score) and YouTube URL from OBZOR tab
3. Match to games in DB by date + team names
4. Update video_url field (only if empty, unless --force)

Usage:
    # Dry run (no DB changes)
    python scripts/scrape_match_videos.py --dry-run

    # Scrape specific match IDs
    python scripts/scrape_match_videos.py --match-ids 691 692 693

    # Scrape range of IDs
    python scripts/scrape_match_videos.py --from-id 600 --to-id 750

    # Force overwrite existing video_url
    python scripts/scrape_match_videos.py --from-id 690 --to-id 695 --force
"""

import argparse
import asyncio
import re
import sys
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

from playwright.async_api import async_playwright
from sqlalchemy import select
from sqlalchemy.orm import selectinload

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.database import AsyncSessionLocal
from app.models.game import Game
from app.models.team import Team

BASE_URL = "https://kffleague.kz"

# Team name mapping (KFF site Russian name -> DB name)
# Copied from test_scrape_players.py and extended
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
    # First League
    "экибастуз": "Ekibastuz",
    "махтаарал": "Makhtaaral",
    "кызыл-жар": "Kyzyl-Zhar",
    "байконур": "Baikonur",
    "академия оф": "Akademiya OF",
    "академия онтустик": "Akademiya Ontustik",
}


@dataclass
class ParsedMatch:
    """Match data scraped from kffleague.kz"""
    source_id: int          # Match ID on kffleague.kz
    source_url: str         # Full URL
    match_date: date | None # Match date
    home_team_name: str     # Team name (Russian)
    away_team_name: str     # Team name (Russian)
    home_score: int | None  # Score
    away_score: int | None
    video_url: str | None   # YouTube embed URL


def normalize_team_name(kff_name: str) -> str:
    """
    Convert KFF site team name to DB-compatible format.

    Example: "Жетысу" -> "Jetisu"
    """
    name = kff_name.strip().lower()

    # Direct mapping
    if name in TEAM_NAME_MAP:
        return TEAM_NAME_MAP[name]

    # Return original if not found
    return kff_name.strip()


def match_team_names(kff_name: str, db_team: Team) -> bool:
    """
    Check if KFF team name matches DB team.
    Checks: team.name, team.name_kz, normalized variants.
    """
    kff_lower = kff_name.strip().lower()
    kff_normalized = normalize_team_name(kff_name).lower()

    # Check all DB name variants
    for db_name in [db_team.name, getattr(db_team, 'name_kz', None), getattr(db_team, 'name_en', None)]:
        if db_name:
            db_lower = db_name.lower()
            # Direct match
            if kff_lower == db_lower:
                return True
            # Normalized match
            if kff_normalized == db_lower:
                return True
            # Partial match (for cases like "ФК Жетысу" vs "Жетысу")
            if kff_lower in db_lower or db_lower in kff_lower:
                return True

    return False


async def parse_match_page(page, match_id: int) -> ParsedMatch | None:
    """
    Parse a single match page to extract:
    - Date
    - Team names
    - Score
    - YouTube video URL (from OBZOR tab)

    URL: https://kffleague.kz/ru/match/{match_id}
    """
    url = f"{BASE_URL}/ru/match/{match_id}"

    try:
        await page.goto(url, wait_until="networkidle", timeout=30000)
        await page.wait_for_timeout(2000)

        # Extract match info - use page title as primary source for team names
        match_data = await page.evaluate("""
            () => {
                // Check if real 404 page (has specific 404 styling/message)
                const h1 = document.querySelector('h1');
                if (h1 && h1.textContent.includes('404')) {
                    return { is404: true };
                }

                // Get teams from page title (most reliable)
                // Format: "Жетысу - Кызылжар | Қазақстан Премьер-лигасы"
                let homeTeam = null;
                let awayTeam = null;
                const titleParts = document.title.split(' | ')[0];
                if (titleParts && titleParts.includes(' - ')) {
                    const teams = titleParts.split(' - ');
                    if (teams.length === 2) {
                        homeTeam = teams[0].trim();
                        awayTeam = teams[1].trim();
                    }
                }

                // Date - look for pattern DD.MM in the visible content
                let matchDate = null;
                const bodyText = document.body.innerText;
                const dateMatch = bodyText.match(/(\\d{2})\\.(\\d{2})(?:\\.(\\d{4}))?/);
                if (dateMatch) {
                    const day = dateMatch[1];
                    const month = dateMatch[2];
                    const year = dateMatch[3] || '2025';
                    matchDate = `${day}.${month}.${year}`;
                }

                // Score - try to find from score element first
                let homeScore = null;
                let awayScore = null;
                const scoreEl = document.querySelector('.score, [class*="score"]');
                if (scoreEl) {
                    const scoreText = scoreEl.textContent.replace(/\\s+/g, '');
                    const scoreMatch = scoreText.match(/(\\d+):(\\d+)/);
                    if (scoreMatch) {
                        homeScore = parseInt(scoreMatch[1], 10);
                        awayScore = parseInt(scoreMatch[2], 10);
                    }
                }
                // Fallback: find pattern in text, but avoid time patterns
                if (homeScore === null) {
                    const allMatches = bodyText.match(/(\\d+)\\s*:\\s*(\\d+)/g) || [];
                    for (const m of allMatches) {
                        const parts = m.split(':').map(p => parseInt(p.trim()));
                        // Skip if looks like time (XX:00, XX:30, XX:45)
                        if (parts[1] === 0 || parts[1] === 30 || parts[1] === 45) continue;
                        // Skip if first part is typical hour (12-23)
                        if (parts[0] >= 12 && parts[0] <= 23) continue;
                        homeScore = parts[0];
                        awayScore = parts[1];
                        break;
                    }
                }

                return {
                    is404: false,
                    matchDate,
                    homeTeam,
                    awayTeam,
                    homeScore,
                    awayScore
                };
            }
        """)

        # Check for 404
        if match_data.get('is404'):
            return None

        # Get team names
        home_team = match_data.get('homeTeam', '') or ''
        away_team = match_data.get('awayTeam', '') or ''

        if not home_team or not away_team:
            return None

        # Click on OBZOR tab to get video
        video_url = None
        try:
            # Try to click ОБЗОР tab
            obzor_tab = await page.query_selector('text=ОБЗОР')
            if obzor_tab:
                await obzor_tab.click()
                await page.wait_for_timeout(2000)

            # Extract YouTube URL
            video_url = await page.evaluate("""
                () => {
                    const ytIframe = document.querySelector('iframe[src*="youtube"], iframe[data-src*="youtube"]');
                    if (ytIframe) {
                        return ytIframe.src || ytIframe.getAttribute('data-src');
                    }
                    return null;
                }
            """)
        except Exception:
            pass  # Video tab might not exist

        # Parse date
        match_date = None
        if match_data.get('matchDate'):
            try:
                match_date = datetime.strptime(match_data['matchDate'], '%d.%m.%Y').date()
            except ValueError:
                pass

        return ParsedMatch(
            source_id=match_id,
            source_url=url,
            match_date=match_date,
            home_team_name=home_team,
            away_team_name=away_team,
            home_score=match_data.get('homeScore'),
            away_score=match_data.get('awayScore'),
            video_url=video_url
        )

    except Exception as e:
        print(f"  ERROR parsing match {match_id}: {e}")
        return None


async def find_matching_game(db, parsed: ParsedMatch) -> Game | None:
    """
    Find matching game in DB by:
    1. Date (exact match)
    2. Team names (fuzzy match)

    Returns matched Game or None.
    """
    if not parsed.match_date:
        return None

    # Query games on the same date with teams loaded
    result = await db.execute(
        select(Game)
        .where(Game.date == parsed.match_date)
        .options(selectinload(Game.home_team), selectinload(Game.away_team))
    )
    games = result.scalars().all()

    if not games:
        return None

    # Find matching game by team names
    for game in games:
        if not game.home_team or not game.away_team:
            continue

        home_match = match_team_names(parsed.home_team_name, game.home_team)
        away_match = match_team_names(parsed.away_team_name, game.away_team)

        if home_match and away_match:
            # Additional check: score should match if available
            if parsed.home_score is not None and parsed.away_score is not None:
                if game.home_score != parsed.home_score or game.away_score != parsed.away_score:
                    continue  # Score mismatch
            return game

    return None


async def update_video_url(db, game: Game, video_url: str, dry_run: bool = False, force: bool = False) -> bool:
    """
    Update video_url for a game.
    Skip if game already has video_url (unless force=True).

    Returns True if updated, False otherwise.
    """
    if game.video_url and not force:
        return False  # Already has video

    if not dry_run:
        game.video_url = video_url
        await db.commit()

    return True


async def main():
    parser = argparse.ArgumentParser(description="Scrape YouTube videos from kffleague.kz matches")
    parser.add_argument("--match-ids", nargs="+", type=int, help="Specific match IDs to scrape")
    parser.add_argument("--from-id", type=int, default=600, help="Start of match ID range")
    parser.add_argument("--to-id", type=int, default=750, help="End of match ID range")
    parser.add_argument("--dry-run", action="store_true", help="Run without making DB changes")
    parser.add_argument("--force", action="store_true", help="Overwrite existing video_url")
    args = parser.parse_args()

    print("=" * 70)
    print("KFF League Match Video Scraper")
    print("=" * 70)
    if args.dry_run:
        print("MODE: DRY RUN (no changes will be made)")
    if args.force:
        print("MODE: FORCE (will overwrite existing video_url)")
    print()

    # Determine match IDs to process
    if args.match_ids:
        match_ids = args.match_ids
    else:
        match_ids = list(range(args.from_id, args.to_id + 1))

    print(f"Processing {len(match_ids)} match IDs: {match_ids[0]} - {match_ids[-1]}")
    print()

    # Statistics
    stats = {
        "total": len(match_ids),
        "parsed": 0,
        "with_video": 0,
        "matched": 0,
        "updated": 0,
        "skipped_existing": 0,
        "not_found": 0,
        "errors": 0,
    }

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080}
        )
        page = await context.new_page()

        async with AsyncSessionLocal() as db:
            for i, match_id in enumerate(match_ids, 1):
                print(f"[{i}/{len(match_ids)}] Match ID {match_id}...")

                # Parse match page
                parsed = await parse_match_page(page, match_id)

                if not parsed:
                    print(f"  -> Not found or parse error")
                    stats["not_found"] += 1
                    continue

                stats["parsed"] += 1
                print(f"  -> {parsed.home_team_name} vs {parsed.away_team_name} ({parsed.match_date})")
                print(f"     Score: {parsed.home_score}:{parsed.away_score}")

                if parsed.video_url:
                    stats["with_video"] += 1
                    print(f"     Video: {parsed.video_url[:60]}...")
                else:
                    print(f"     Video: None")
                    continue  # No video to update

                # Find matching game in DB
                game = await find_matching_game(db, parsed)

                if not game:
                    print(f"     DB Match: NOT FOUND")
                    continue

                stats["matched"] += 1
                print(f"     DB Match: {game.id}")

                if game.video_url and not args.force:
                    print(f"     -> Skipped (already has video)")
                    stats["skipped_existing"] += 1
                    continue

                # Update video_url
                updated = await update_video_url(db, game, parsed.video_url, args.dry_run, args.force)

                if updated:
                    stats["updated"] += 1
                    if args.dry_run:
                        print(f"     -> [DRY] Would update video_url")
                    else:
                        print(f"     -> Updated video_url")

                # Small delay to avoid rate limiting
                await asyncio.sleep(0.5)

        await browser.close()

    # Print summary
    print()
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Total match IDs:     {stats['total']}")
    print(f"Successfully parsed: {stats['parsed']}")
    print(f"With video:          {stats['with_video']}")
    print(f"Matched in DB:       {stats['matched']}")
    print(f"Updated:             {stats['updated']}")
    print(f"Skipped (existing):  {stats['skipped_existing']}")
    print(f"Not found (404):     {stats['not_found']}")
    print()


if __name__ == "__main__":
    asyncio.run(main())
