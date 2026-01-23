#!/usr/bin/env python3
"""Analyze why specific players don't have photos."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.database import AsyncSessionLocal
from app.models.player import Player
from app.models.player_team import PlayerTeam
from sqlalchemy import select
from sqlalchemy.orm import selectinload


async def main():
    """Check why players don't have photos."""

    async with AsyncSessionLocal() as session:
        # Get players without photos
        result = await session.execute(
            select(Player)
            .where(Player.photo_url.is_(None))
            .options(selectinload(Player.player_teams).selectinload(PlayerTeam.team))
        )
        players_no_photo = result.scalars().all()

        # Group by team
        by_team = {}
        for player in players_no_photo:
            for pt in player.player_teams:
                if pt.team:
                    team_name = pt.team.name
                    if team_name not in by_team:
                        by_team[team_name] = []
                    by_team[team_name].append(player)

        # Show top teams
        print("=" * 100)
        print("ТОП-5 КОМАНД С НАИБОЛЬШИМ ЧИСЛОМ ИГРОКОВ БЕЗ ФОТО")
        print("=" * 100)
        print()

        sorted_teams = sorted(by_team.items(), key=lambda x: len(x[1]), reverse=True)

        for team_name, players in sorted_teams[:5]:
            print(f"\n{team_name}: {len(players)} игроков")
            print("-" * 100)
            print(f"{'#':<3} {'Фамилия':<25} {'Имя':<20} {'Обновлен':<12} {'Примечание':<30}")
            print("-" * 100)

            for i, p in enumerate(players, 1):
                # Check if name looks like it might be missing from website
                updated = p.updated_at.strftime("%Y-%m-%d") if p.updated_at else "Unknown"

                # Categorize
                notes = []

                # Check if name has special characters (compound names)
                if " " in p.last_name or " " in p.first_name:
                    notes.append("Составное имя")

                # Check if name is very long
                full_name = f"{p.first_name} {p.last_name}"
                if len(full_name) > 30:
                    notes.append("Длинное имя")

                # Check if recently updated (might not be on website yet)
                if p.updated_at and p.updated_at.year >= 2024:
                    notes.append("Недавно обновлен")

                # Check if name has non-Latin characters
                if any(ord(c) > 127 for c in full_name):
                    notes.append("Не латиница")

                note_str = ", ".join(notes) if notes else "-"

                print(f"{i:<3} {p.last_name:<25} {p.first_name:<20} {updated:<12} {note_str:<30}")

        # Summary statistics
        print()
        print("=" * 100)
        print("СТАТИСТИКА ПО КАТЕГОРИЯМ")
        print("=" * 100)
        print()

        total = len(players_no_photo)
        compound_names = sum(1 for p in players_no_photo if " " in p.last_name or " " in p.first_name)
        long_names = sum(1 for p in players_no_photo if len(f"{p.first_name} {p.last_name}") > 30)
        non_latin = sum(1 for p in players_no_photo if any(ord(c) > 127 for c in f"{p.first_name} {p.last_name}"))
        recent = sum(1 for p in players_no_photo if p.updated_at and p.updated_at.year >= 2024)

        print(f"Всего без фото:           {total}")
        print(f"Составные имена:          {compound_names} ({compound_names/total*100:.1f}%)")
        print(f"Длинные имена (>30):      {long_names} ({long_names/total*100:.1f}%)")
        print(f"Не латиница:              {non_latin} ({non_latin/total*100:.1f}%)")
        print(f"Обновлены в 2024+:        {recent} ({recent/total*100:.1f}%)")

        print()
        print("=" * 100)
        print("ВОЗМОЖНЫЕ ПРИЧИНЫ ОТСУТСТВИЯ ФОТО")
        print("=" * 100)
        print()
        print("1. Игрок не представлен на сайте kffleague.kz")
        print("   - Команды нижних лиг (Jenis, Turan, Ulytau, Elimai)")
        print("   - Недавно добавленные игроки")
        print()
        print("2. Имя на сайте сильно отличается от БД")
        print("   - Разная транслитерация (особенно для составных имен)")
        print("   - Сокращенные/полные версии имени")
        print()
        print("3. Технические проблемы при скрапинге")
        print("   - Timeout или network errors")
        print("   - Изменена структура страницы")


if __name__ == "__main__":
    asyncio.run(main())
