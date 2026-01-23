#!/usr/bin/env python3
"""Analyze players without photos."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.database import AsyncSessionLocal
from app.models.player import Player
from app.models.player_team import PlayerTeam
from app.models.team import Team
from sqlalchemy import select, func
from sqlalchemy.orm import selectinload


async def main():
    """Find players without photos and analyze them."""

    async with AsyncSessionLocal() as session:
        # Get all players
        result = await session.execute(select(Player))
        all_players = result.scalars().all()

        # Get players without photos with eager loading
        result = await session.execute(
            select(Player)
            .where(Player.photo_url.is_(None))
            .options(selectinload(Player.player_teams).selectinload(PlayerTeam.team))
        )
        players_without_photos = result.scalars().all()

        # Get players with photos
        result = await session.execute(
            select(Player).where(Player.photo_url.isnot(None))
        )
        players_with_photos = result.scalars().all()

        print("=" * 80)
        print("СТАТИСТИКА ПО ФОТОГРАФИЯМ")
        print("=" * 80)
        print(f"Всего игроков:     {len(all_players)}")
        print(f"С фотографиями:    {len(players_with_photos)} ({len(players_with_photos)/len(all_players)*100:.1f}%)")
        print(f"Без фотографий:    {len(players_without_photos)} ({len(players_without_photos)/len(all_players)*100:.1f}%)")
        print()

        # Group by team
        print("=" * 80)
        print("ПО КОМАНДАМ (топ-10 команд с наибольшим числом игроков без фото)")
        print("=" * 80)

        teams_missing = {}
        for player in players_without_photos:
            # Get team from player_teams relationship
            if player.player_teams:
                for pt in player.player_teams:
                    team_name = pt.team.name if pt.team else "Unknown"
                    if team_name not in teams_missing:
                        teams_missing[team_name] = []
                    teams_missing[team_name].append(player)

        # Sort by count
        sorted_teams = sorted(teams_missing.items(), key=lambda x: len(x[1]), reverse=True)

        for team_name, players in sorted_teams[:10]:
            print(f"\n{team_name}: {len(players)} игроков")
            for p in players[:5]:  # Show first 5
                print(f"  - {p.last_name} {p.first_name}")
            if len(players) > 5:
                print(f"  ... и еще {len(players) - 5}")

        # Analyze name patterns
        print()
        print("=" * 80)
        print("АНАЛИЗ ИМЕН (первые 50 игроков без фото)")
        print("=" * 80)
        print()

        for i, player in enumerate(players_without_photos[:50], 1):
            team_names = [pt.team.name for pt in player.player_teams if pt.team]
            team_str = ", ".join(team_names) if team_names else "No team"

            print(f"{i:2}. {player.last_name:20} {player.first_name:20} ({team_str})")

        if len(players_without_photos) > 50:
            print(f"\n... и еще {len(players_without_photos) - 50} игроков")


if __name__ == "__main__":
    asyncio.run(main())
