"""
Link existing players to countries based on country_name.

Usage:
    python scripts/link_players_to_countries.py
"""

import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import select, update

from app.database import AsyncSessionLocal
from app.models import Player, Country


async def build_country_cache(db) -> dict[str, int]:
    """Build a cache mapping country names to IDs."""
    result = await db.execute(select(Country))
    countries = result.scalars().all()

    cache = {}
    for c in countries:
        if c.name:
            cache[c.name.lower().strip()] = c.id
        if c.name_kz:
            cache[c.name_kz.lower().strip()] = c.id
        if c.name_en:
            cache[c.name_en.lower().strip()] = c.id

    return cache


async def link_players():
    """Link all players without country_id to their countries."""
    print("Linking players to countries...")

    async with AsyncSessionLocal() as db:
        # Build country cache
        cache = await build_country_cache(db)
        print(f"Loaded {len(cache)} country name mappings")

        # Get players without country_id but with country_name
        result = await db.execute(
            select(Player).where(
                Player.country_id.is_(None),
                Player.country_name.isnot(None)
            )
        )
        players = result.scalars().all()

        print(f"Found {len(players)} players without country_id")

        linked = 0
        not_found = []

        for player in players:
            country_id = None

            # Try Russian name first
            if player.country_name:
                country_id = cache.get(player.country_name.lower().strip())

            # Try English name
            if not country_id and player.country_name_en:
                country_id = cache.get(player.country_name_en.lower().strip())

            if country_id:
                player.country_id = country_id
                linked += 1
            else:
                country_names = f"{player.country_name} / {player.country_name_en}"
                if country_names not in not_found:
                    not_found.append(country_names)

        await db.commit()

        print(f"\nLinked: {linked} players")
        if not_found:
            print(f"\nCountries not found ({len(not_found)}):")
            for name in sorted(set(not_found)):
                print(f"  - {name}")


if __name__ == "__main__":
    asyncio.run(link_players())
