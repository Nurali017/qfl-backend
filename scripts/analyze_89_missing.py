#!/usr/bin/env python3
"""Detailed analysis of 89 players without photos."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.database import AsyncSessionLocal
from app.models.player import Player
from sqlalchemy import select, func


async def main():
    """Analyze players without photos."""

    async with AsyncSessionLocal() as session:
        # Get players without photos
        result = await session.execute(
            select(Player).where(Player.photo_url.is_(None))
        )
        players_no_photo = result.scalars().all()

        print("=" * 100)
        print(f"ДЕТАЛЬНЫЙ АНАЛИЗ {len(players_no_photo)} ИГРОКОВ БЕЗ ФОТО")
        print("=" * 100)
        print()

        # Group by team
        from app.models.player_team import PlayerTeam
        from sqlalchemy.orm import selectinload

        result = await session.execute(
            select(Player)
            .where(Player.photo_url.is_(None))
            .options(selectinload(Player.player_teams).selectinload(PlayerTeam.team))
        )
        players = result.scalars().all()

        # Organize by team
        by_team = {}
        no_team = []

        for player in players:
            if player.player_teams:
                for pt in player.player_teams:
                    if pt.team:
                        team_name = pt.team.name
                        if team_name not in by_team:
                            by_team[team_name] = []
                        by_team[team_name].append(player)
            else:
                no_team.append(player)

        # Sort teams by number of players
        sorted_teams = sorted(by_team.items(), key=lambda x: len(x[1]), reverse=True)

        # Print all teams
        print("РАСПРЕДЕЛЕНИЕ ПО КОМАНДАМ:")
        print("-" * 100)
        total_shown = 0
        for team_name, team_players in sorted_teams:
            print(f"\n{team_name}: {len(team_players)} игроков")
            print("  " + "-" * 96)

            for i, p in enumerate(team_players, 1):
                # Analyze name
                name_info = []
                if " " in p.last_name:
                    name_info.append("Compound surname")
                if " " in p.first_name:
                    name_info.append("Compound name")

                full_name = f"{p.first_name} {p.last_name}"
                if len(full_name) > 30:
                    name_info.append("Long name")

                # Check nationality
                country = p.country_name or p.country_code or "?"

                info_str = f" [{', '.join(name_info)}]" if name_info else ""

                print(f"  {i:2}. {p.last_name:25} {p.first_name:20} ({country:15}){info_str}")

            total_shown += len(team_players)

        if no_team:
            print(f"\n\nИгроки БЕЗ КОМАНДЫ: {len(no_team)}")
            print("  " + "-" * 96)
            for i, p in enumerate(no_team, 1):
                print(f"  {i:2}. {p.last_name:25} {p.first_name:20}")

        # Statistics
        print()
        print("=" * 100)
        print("СТАТИСТИКА")
        print("=" * 100)
        print(f"Всего команд:              {len(sorted_teams)}")
        print(f"Всего игроков без фото:    {len(players)}")
        print()

        # Analyze by characteristics
        foreign_players = [p for p in players if p.country_code and p.country_code != "KZ"]
        compound_surname = [p for p in players if " " in (p.last_name or "")]
        compound_first = [p for p in players if " " in (p.first_name or "")]
        long_names = [p for p in players if len(f"{p.first_name} {p.last_name}") > 30]

        print("ХАРАКТЕРИСТИКИ:")
        print(f"  Иностранцы:                {len(foreign_players)} ({len(foreign_players)/len(players)*100:.1f}%)")
        print(f"  Составная фамилия:         {len(compound_surname)} ({len(compound_surname)/len(players)*100:.1f}%)")
        print(f"  Составное имя:             {len(compound_first)} ({len(compound_first)/len(players)*100:.1f}%)")
        print(f"  Длинные имена (>30 симв):  {len(long_names)} ({len(long_names)/len(players)*100:.1f}%)")
        print()

        # Top nationalities
        from collections import Counter
        countries = Counter(p.country_code or "Unknown" for p in players)
        print("ТОП НАЦИОНАЛЬНОСТИ:")
        for country, count in countries.most_common(10):
            country_name = next((p.country_name for p in players if (p.country_code or "Unknown") == country and p.country_name), country)
            print(f"  {country_name:20} {count:3} игрока")

        print()
        print("=" * 100)
        print("ВОЗМОЖНЫЕ ПРИЧИНЫ:")
        print("=" * 100)
        print("1. Игрок не представлен на kffleague.kz (команды нижних лиг)")
        print("2. Имя на сайте написано по-другому (не совпадает fuzzy matching)")
        print("3. Игрок недавно добавлен и еще не появился на сайте")
        print("4. Ошибка при скрапинге команды (timeout/network error)")

if __name__ == "__main__":
    asyncio.run(main())
