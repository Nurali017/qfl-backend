"""
Extract color palettes from all team logos and update the database.

This script:
1. Gets all teams from the database
2. Downloads their logos from MinIO
3. Extracts dominant colors using ColorThief
4. Updates team records with color information

Usage:
    python scripts/extract_team_colors.py

    # With options:
    python scripts/extract_team_colors.py --force  # Re-extract even if colors exist
    python scripts/extract_team_colors.py --team-id 5  # Extract for specific team
"""

import asyncio
import sys
import argparse
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import select
from app.database import AsyncSessionLocal
from app.models import Team
from app.services.file_storage import FileStorageService
from app.services.color_palette import ColorPaletteService


async def extract_colors_for_team(db, team: Team, force: bool = False) -> bool:
    """Extract and save colors for a single team."""

    # Skip if colors already exist (unless force=True)
    if not force and team.primary_color:
        print(f"⏭️  {team.name}: colors already extracted (use --force to re-extract)")
        return False

    # Check if logo exists
    if not team.logo_url:
        print(f"⚠️  {team.name}: no logo URL")
        return False

    try:
        # Extract team name from logo_url for FileStorageService
        # logo_url format: "http://localhost:9000/qfl-files/public/team-logos/aktobe.webp"
        team_name_from_url = team.logo_url.split('/')[-1].rsplit('.', 1)[0]

        # Download logo from MinIO
        logo_data = await FileStorageService.get_team_logo(team_name_from_url)

        if not logo_data:
            print(f"⚠️  {team.name}: logo not found in MinIO")
            return False

        logo_bytes, metadata = logo_data

        # Extract colors
        colors = ColorPaletteService.extract_team_colors(logo_bytes)

        # Update team in database
        team.primary_color = colors["primary_color"]
        team.secondary_color = colors["secondary_color"]
        team.accent_color = colors["accent_color"]
        team.colors_updated_at = datetime.utcnow()

        await db.commit()

        print(f"✅ {team.name}: {colors['primary_color']} | {colors['secondary_color']} | {colors['accent_color']}")
        return True

    except Exception as e:
        print(f"❌ {team.name}: error - {e}")
        await db.rollback()
        return False


async def extract_all_team_colors(force: bool = False, team_id: int | None = None):
    """Extract colors for all teams or a specific team."""

    async with AsyncSessionLocal() as db:
        # Build query
        query = select(Team)
        if team_id:
            query = query.where(Team.id == team_id)

        result = await db.execute(query.order_by(Team.name))
        teams = result.scalars().all()

        if not teams:
            print("No teams found")
            return

        print(f"Found {len(teams)} teams")
        print("=" * 60)

        success_count = 0
        skip_count = 0
        error_count = 0

        for team in teams:
            result = await extract_colors_for_team(db, team, force)
            if result is True:
                success_count += 1
            elif result is False and team.primary_color:
                skip_count += 1
            else:
                error_count += 1

        print("=" * 60)
        print(f"Results:")
        print(f"  ✅ Extracted: {success_count}")
        print(f"  ⏭️  Skipped: {skip_count}")
        print(f"  ❌ Errors: {error_count}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract team colors from logos")
    parser.add_argument("--force", action="store_true",
                       help="Re-extract colors even if they already exist")
    parser.add_argument("--team-id", type=int,
                       help="Extract colors for specific team ID only")

    args = parser.parse_args()

    asyncio.run(extract_all_team_colors(force=args.force, team_id=args.team_id))
