import asyncio
from sqlalchemy import text
from app.database import AsyncSessionLocal


async def main():
    async with AsyncSessionLocal() as session:
        # 1. Overall stats
        result = await session.execute(text('''
            SELECT
                COUNT(*) as total,
                COUNT(photo_url) as with_photo,
                COUNT(*) - COUNT(photo_url) as without_photo,
                ROUND(COUNT(photo_url)::numeric / NULLIF(COUNT(*), 0) * 100, 1) as coverage_pct
            FROM players
        '''))
        row = result.fetchone()
        print('=' * 60)
        print('1. ОБЩАЯ СТАТИСТИКА ПО ФОТО ИГРОКОВ')
        print('=' * 60)
        print(f'  Всего игроков:   {row.total}')
        print(f'  С фото:          {row.with_photo}')
        print(f'  Без фото:        {row.without_photo}')
        print(f'  Покрытие:        {row.coverage_pct}%')

        # 2. Breakdown by championship (2025 season only)
        # Use season_participants to get real team→division mapping
        result = await session.execute(text('''
            SELECT
                c.name as championship,
                COUNT(DISTINCT p.id) as total_players,
                COUNT(DISTINCT p.id) FILTER (WHERE p.photo_url IS NULL) as without_photo,
                COUNT(DISTINCT p.id) FILTER (WHERE p.photo_url IS NOT NULL) as with_photo,
                ROUND(
                    COUNT(DISTINCT p.id) FILTER (WHERE p.photo_url IS NOT NULL)::numeric
                    / NULLIF(COUNT(DISTINCT p.id), 0) * 100, 1
                ) as coverage_pct
            FROM players p
            JOIN player_teams pt ON pt.player_id = p.id
            JOIN season_participants sp ON sp.team_id = pt.team_id AND sp.season_id = pt.season_id
            JOIN seasons s ON s.id = sp.season_id
            JOIN championships c ON c.id = s.championship_id
            WHERE s.name = '2025'
            GROUP BY c.id, c.name
            ORDER BY total_players DESC
        '''))
        rows = result.fetchall()
        print()
        print('=' * 60)
        print('2. РАЗБИВКА ПО ЧЕМПИОНАТАМ (сезон 2025)')
        print('=' * 60)
        print(f'  {"Чемпионат":<35} {"Всего":>6} {"Без фото":>9} {"Покрытие":>9}')
        print(f'  {"-"*35} {"-"*6} {"-"*9} {"-"*9}')
        for row in rows:
            print(f'  {row.championship:<35} {row.total_players:>6} {row.without_photo:>9} {row.coverage_pct:>8}%')

        # 3. Top-10 teams with most players without photo (2025)
        result = await session.execute(text('''
            SELECT
                t.name as team,
                c.name as championship,
                COUNT(DISTINCT p.id) as total_players,
                COUNT(DISTINCT p.id) FILTER (WHERE p.photo_url IS NULL) as without_photo
            FROM players p
            JOIN player_teams pt ON pt.player_id = p.id
            JOIN season_participants sp ON sp.team_id = pt.team_id AND sp.season_id = pt.season_id
            JOIN teams t ON t.id = pt.team_id
            JOIN seasons s ON s.id = sp.season_id
            JOIN championships c ON c.id = s.championship_id
            WHERE s.name = '2025'
            GROUP BY t.id, t.name, c.name
            HAVING COUNT(DISTINCT p.id) FILTER (WHERE p.photo_url IS NULL) > 0
            ORDER BY without_photo DESC
            LIMIT 10
        '''))
        rows = result.fetchall()
        print()
        print('=' * 60)
        print('3. ТОП-10 КОМАНД БЕЗ ФОТО (сезон 2025)')
        print('=' * 60)
        print(f'  {"Команда":<25} {"Чемпионат":<25} {"Всего":>6} {"Без фото":>9}')
        print(f'  {"-"*25} {"-"*25} {"-"*6} {"-"*9}')
        for row in rows:
            print(f'  {row.team:<25} {row.championship:<25} {row.total_players:>6} {row.without_photo:>9}')

        # 4. Breakdown by position (amplua) — using top_role from players
        result = await session.execute(text('''
            SELECT
                COALESCE(p.top_role, 'Не указана') as position,
                COUNT(DISTINCT p.id) as total_players,
                COUNT(DISTINCT p.id) FILTER (WHERE p.photo_url IS NULL) as without_photo,
                COUNT(DISTINCT p.id) FILTER (WHERE p.photo_url IS NOT NULL) as with_photo,
                ROUND(
                    COUNT(DISTINCT p.id) FILTER (WHERE p.photo_url IS NOT NULL)::numeric
                    / NULLIF(COUNT(DISTINCT p.id), 0) * 100, 1
                ) as coverage_pct
            FROM players p
            JOIN player_teams pt ON pt.player_id = p.id
            JOIN season_participants sp ON sp.team_id = pt.team_id AND sp.season_id = pt.season_id
            JOIN seasons s ON s.id = sp.season_id
            WHERE s.name = '2025'
            GROUP BY COALESCE(p.top_role, 'Не указана')
            ORDER BY total_players DESC
        '''))
        rows = result.fetchall()
        print()
        print('=' * 60)
        print('4. РАЗБИВКА ПО ПОЗИЦИЯМ (сезон 2025)')
        print('=' * 60)
        print(f'  {"Позиция":<25} {"Всего":>6} {"Без фото":>9} {"Покрытие":>9}')
        print(f'  {"-"*25} {"-"*6} {"-"*9} {"-"*9}')
        for row in rows:
            print(f'  {row.position:<25} {row.total_players:>6} {row.without_photo:>9} {row.coverage_pct:>8}%')


asyncio.run(main())
