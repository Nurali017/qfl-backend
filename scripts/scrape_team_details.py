#!/usr/bin/env python3
"""
Scrape team details from kffleague.kz team pages.

Scrapes:
- City
- Stadium name
- Official website
- Coaching staff with photos

Usage:
    # Dry run (no DB changes)
    python scripts/scrape_team_details.py --dry-run

    # Full run
    python scripts/scrape_team_details.py

    # Limit teams for testing
    python scripts/scrape_team_details.py --limit-teams 3
"""

import argparse
import asyncio
import re
import sys
from dataclasses import dataclass
from pathlib import Path

import httpx
from playwright.async_api import async_playwright
from sqlalchemy import select
from sqlalchemy.orm import selectinload

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.database import AsyncSessionLocal
from app.models.team import Team
from app.models.stadium import Stadium
from app.models.coach import Coach, TeamCoach, CoachRole
from app.minio_client import get_minio_client, get_public_url
from app.config import get_settings

settings = get_settings()

BASE_URL = "https://kffleague.kz"


@dataclass
class ScrapedCoach:
    """Coach data scraped from kffleague.kz"""
    first_name: str
    last_name: str
    role: str
    photo_url: str | None


@dataclass
class ScrapedTeamDetails:
    """Team details scraped from kffleague.kz"""
    team_id: int
    team_name: str
    city: str | None
    stadium_name: str | None
    website: str | None
    head_coach_name: str | None
    coaches: list[ScrapedCoach]


def parse_role(role_text: str) -> CoachRole:
    """Parse coach role from Russian text."""
    role_lower = role_text.lower()
    if "главный тренер" in role_lower:
        return CoachRole.head_coach
    elif "ассистент" in role_lower:
        return CoachRole.assistant
    elif "вратар" in role_lower:
        return CoachRole.goalkeeper_coach
    elif "физ" in role_lower or "подготов" in role_lower:
        return CoachRole.fitness_coach
    else:
        return CoachRole.other


def parse_name(name_text: str) -> tuple[str, str]:
    """Parse name into first_name and last_name from format 'Фамилия Имя' (kffleague.kz format)."""
    parts = name_text.strip().split()
    if len(parts) >= 2:
        # On kffleague.kz, format is always "Фамилия Имя"
        last_name = parts[0]
        first_name = ' '.join(parts[1:])
        return first_name, last_name
    elif len(parts) == 1:
        return "", parts[0]
    return "", ""


class TeamDetailsScraper:
    """Scraper for team details from kffleague.kz"""

    def __init__(self, dry_run: bool = False, limit_teams: int | None = None):
        self.dry_run = dry_run
        self.limit_teams = limit_teams
        self.stats = {
            "teams_processed": 0,
            "stadiums_created": 0,
            "coaches_created": 0,
            "coaches_updated": 0,
            "photos_uploaded": 0,
            "errors": 0,
        }

    async def get_team_urls(self, page) -> list[dict]:
        """Get all team page URLs from the main page."""
        print("  Navigating to main page...")
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
                                id: parseInt(id),
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

    async def scrape_team_details(self, page, team_info: dict) -> ScrapedTeamDetails | None:
        """Scrape details from a team page."""
        team_url = team_info['url']
        team_id = team_info['id']

        try:
            await page.goto(team_url, wait_until="networkidle", timeout=60000)
            await page.wait_for_timeout(2000)

            # Extract team details from header
            details = await page.evaluate("""
                () => {
                    const result = {
                        team_name: '',
                        city: null,
                        stadium: null,
                        website: null,
                        head_coach: null
                    };

                    // Team name from h1
                    const h1 = document.querySelector('h1');
                    result.team_name = h1?.textContent?.trim() || '';

                    // Get all text content in header area
                    const headerTexts = document.querySelectorAll('.team-info, .team-header, [class*="team"]');

                    // Look for specific patterns
                    const allText = document.body.innerText;

                    // City - usually first line under team name or in a specific element
                    const cityEl = document.querySelector('h1 + *');
                    if (cityEl && cityEl.textContent && !cityEl.textContent.includes(':')) {
                        result.city = cityEl.textContent.trim();
                    }

                    // Find all label:value pairs
                    const labels = document.querySelectorAll('*');
                    labels.forEach(el => {
                        const text = el.textContent?.trim() || '';

                        // Head coach
                        if (text.startsWith('Главный тренер:')) {
                            const value = text.replace('Главный тренер:', '').trim();
                            if (value) result.head_coach = value;
                        }

                        // Stadium
                        if (text.startsWith('Стадион:')) {
                            const value = text.replace('Стадион:', '').trim();
                            if (value) result.stadium = value;
                        }

                        // Website
                        if (text.startsWith('Официальный сайт:')) {
                            const link = el.querySelector('a');
                            if (link) {
                                result.website = link.href;
                            }
                        }
                    });

                    // Alternative: look for links with specific patterns
                    const siteLink = document.querySelector('a[href*="fckairat"], a[href*="fcaktobe"], a[href*="fctobol"], a[href*="fc"]');
                    if (siteLink && !result.website) {
                        const href = siteLink.href;
                        if (!href.includes('kffleague.kz')) {
                            result.website = href;
                        }
                    }

                    return result;
                }
            """)

            # Click on "ТРЕНЕРСКИЙ ШТАБ" tab to get coaches
            coaches = []
            try:
                # Find and click the coaching staff tab
                staff_tab = await page.query_selector('button:has-text("Тренерский штаб"), [role="tab"]:has-text("Тренерский штаб"), a:has-text("Тренерский штаб")')
                if staff_tab:
                    await staff_tab.click()
                    await page.wait_for_timeout(2000)

                    # Extract coaches from .coach-card elements
                    coaches_data = await page.evaluate("""
                        () => {
                            const coaches = [];

                            // Find all coach cards in the #coach panel
                            const coachCards = document.querySelectorAll('#coach .coach-card, .coach-card');

                            coachCards.forEach(card => {
                                const img = card.querySelector('img.coach-photo, img');
                                const nameEl = card.querySelector('.coach-name');
                                const roleEl = card.querySelector('.coach-position');

                                if (nameEl) {
                                    const name = nameEl.textContent?.trim() || '';
                                    const role = roleEl?.textContent?.trim() || '';
                                    const photo = img?.src || null;

                                    if (name.length > 2) {
                                        coaches.push({
                                            name: name,
                                            role: role,
                                            photo: photo
                                        });
                                    }
                                }
                            });

                            // Dedupe by name
                            const seen = new Set();
                            return coaches.filter(c => {
                                const key = c.name.toLowerCase();
                                if (seen.has(key)) return false;
                                seen.add(key);
                                return c.name.length > 0;
                            });
                        }
                    """)

                    for c in coaches_data:
                        first_name, last_name = parse_name(c['name'])
                        if last_name:
                            print(f"      Found coach: {first_name} {last_name} - {c['role']}")
                            coaches.append(ScrapedCoach(
                                first_name=first_name,
                                last_name=last_name,
                                role=c['role'],
                                photo_url=c['photo'] if c['photo'] else None
                            ))

            except Exception as e:
                print(f"    Warning: Could not get coaches: {e}")

            return ScrapedTeamDetails(
                team_id=team_id,
                team_name=details['team_name'],
                city=details['city'],
                stadium_name=details['stadium'],
                website=details['website'],
                head_coach_name=details['head_coach'],
                coaches=coaches
            )

        except Exception as e:
            print(f"  Error scraping team {team_id}: {e}")
            self.stats["errors"] += 1
            return None

    async def upload_coach_photo(self, photo_url: str, coach_id: int) -> str | None:
        """Download and upload coach photo to MinIO."""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(photo_url, timeout=30)
                if response.status_code != 200:
                    return None

                content = response.content
                content_type = response.headers.get('content-type', 'image/jpeg')

                # Determine extension
                if 'png' in content_type:
                    ext = 'png'
                elif 'webp' in content_type:
                    ext = 'webp'
                else:
                    ext = 'jpg'

                # Upload to MinIO
                minio_client = get_minio_client()
                object_name = f"coach_photos/{coach_id}.{ext}"

                import io
                minio_client.put_object(
                    settings.minio_bucket,
                    object_name,
                    io.BytesIO(content),
                    length=len(content),
                    content_type=content_type
                )

                return get_public_url(object_name)

        except Exception as e:
            print(f"    Error uploading photo: {e}")
            return None

    async def process_team(self, session, team_details: ScrapedTeamDetails) -> None:
        """Process scraped team details and update database."""
        # Find team in DB by name (fuzzy match)
        team_name = team_details.team_name.strip()

        # Try exact match first
        result = await session.execute(
            select(Team).where(Team.name == team_name)
        )
        team = result.scalar_one_or_none()

        # If not found, try case-insensitive contains search
        if not team:
            result = await session.execute(
                select(Team).where(Team.name.ilike(f"%{team_name}%"))
            )
            teams = result.scalars().all()
            if len(teams) == 1:
                team = teams[0]
            elif len(teams) > 1:
                # Try more specific match
                for t in teams:
                    if t.name.lower() == team_name.lower():
                        team = t
                        break

        if not team:
            print(f"    Team '{team_name}' not found in DB, skipping")
            return

        print(f"    Found DB team: {team.name} (id={team.id})")

        # Update city
        if team_details.city and not team.city:
            team.city = team_details.city
            print(f"    Updated city: {team_details.city}")

        # Update website
        if team_details.website and not team.website:
            team.website = team_details.website
            print(f"    Updated website: {team_details.website}")

        # Handle stadium
        if team_details.stadium_name and not team.stadium_id:
            # Check if stadium exists
            stadium_result = await session.execute(
                select(Stadium).where(Stadium.name == team_details.stadium_name)
            )
            stadium = stadium_result.scalar_one_or_none()

            if not stadium:
                # Create new stadium
                stadium = Stadium(
                    name=team_details.stadium_name,
                    city=team_details.city
                )
                session.add(stadium)
                await session.flush()
                self.stats["stadiums_created"] += 1
                print(f"    Created stadium: {team_details.stadium_name}")

            team.stadium_id = stadium.id

        # Process coaches
        for coach_data in team_details.coaches:
            # Find or create coach
            coach_result = await session.execute(
                select(Coach).where(
                    Coach.first_name == coach_data.first_name,
                    Coach.last_name == coach_data.last_name
                )
            )
            coach = coach_result.scalar_one_or_none()

            if not coach:
                coach = Coach(
                    first_name=coach_data.first_name,
                    last_name=coach_data.last_name
                )
                session.add(coach)
                await session.flush()
                self.stats["coaches_created"] += 1
                print(f"    Created coach: {coach_data.first_name} {coach_data.last_name}")

            # Upload photo if available and not already set
            if coach_data.photo_url and not coach.photo_url and not self.dry_run:
                photo_url = await self.upload_coach_photo(coach_data.photo_url, coach.id)
                if photo_url:
                    coach.photo_url = photo_url
                    self.stats["photos_uploaded"] += 1
                    print(f"    Uploaded photo for {coach_data.first_name} {coach_data.last_name}")

            # Create team-coach association if not exists
            assoc_result = await session.execute(
                select(TeamCoach).where(
                    TeamCoach.team_id == team.id,
                    TeamCoach.coach_id == coach.id
                )
            )
            assoc = assoc_result.scalar_one_or_none()

            if not assoc:
                role = parse_role(coach_data.role)
                assoc = TeamCoach(
                    team_id=team.id,
                    coach_id=coach.id,
                    role=role,
                    is_active=True
                )
                session.add(assoc)
                print(f"    Linked coach {coach_data.last_name} to team as {role.value}")

        self.stats["teams_processed"] += 1

    async def run(self) -> None:
        """Run the scraper."""
        print("=" * 60)
        print("Team Details Scraper")
        print("=" * 60)
        print(f"Mode: {'DRY RUN' if self.dry_run else 'LIVE'}")
        print()

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            # Get team URLs
            print("Getting team URLs...")
            teams = await self.get_team_urls(page)
            print(f"Found {len(teams)} teams")

            if self.limit_teams:
                teams = teams[:self.limit_teams]
                print(f"Limited to {len(teams)} teams")

            print()

            async with AsyncSessionLocal() as session:
                for i, team_info in enumerate(teams, 1):
                    print(f"[{i}/{len(teams)}] Processing team {team_info['id']}: {team_info.get('name', 'Unknown')}")

                    details = await self.scrape_team_details(page, team_info)
                    if details:
                        print(f"    City: {details.city}")
                        print(f"    Stadium: {details.stadium_name}")
                        print(f"    Website: {details.website}")
                        print(f"    Coaches: {len(details.coaches)}")

                        if not self.dry_run:
                            await self.process_team(session, details)
                            await session.commit()

                    print()

            await browser.close()

        # Print stats
        print("=" * 60)
        print("Summary")
        print("=" * 60)
        for key, value in self.stats.items():
            print(f"  {key}: {value}")


async def main():
    parser = argparse.ArgumentParser(description="Scrape team details from kffleague.kz")
    parser.add_argument("--dry-run", action="store_true", help="Don't make DB changes")
    parser.add_argument("--limit-teams", type=int, help="Limit number of teams to process")
    args = parser.parse_args()

    scraper = TeamDetailsScraper(dry_run=args.dry_run, limit_teams=args.limit_teams)
    await scraper.run()


if __name__ == "__main__":
    asyncio.run(main())
