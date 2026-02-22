import asyncio
from sqlalchemy import text
from app.database import AsyncSessionLocal

async def main():
    async with AsyncSessionLocal() as session:
        result = await session.execute(text('''
            WITH field_eligible AS (
                SELECT g.id as game_id, g.date, c.name as champ_name, s.name as season_name,
                       g.home_team_id, g.away_team_id,
                       ht.name as home_name, at.name as away_name,
                       g.home_formation, g.away_formation
                FROM games g
                JOIN seasons s ON s.id = g.season_id
                JOIN championships c ON c.id = s.championship_id
                LEFT JOIN teams ht ON ht.id = g.home_team_id
                LEFT JOIN teams at ON at.id = g.away_team_id
                WHERE COALESCE(c.legacy_id, c.id) IN (1, 2, 3)
                  AND g.date >= '2025-06-01'
                  AND g.is_technical = false
            ),
            team_validity AS (
                SELECT gl.game_id, gl.team_id,
                       COUNT(*) FILTER (WHERE gl.lineup_type = 'starter') as starter_count,
                       COUNT(*) FILTER (WHERE gl.lineup_type = 'starter' AND gl.amplua IS NOT NULL AND gl.field_position IS NOT NULL) as valid_starters
                FROM game_lineups gl
                WHERE gl.game_id IN (SELECT game_id FROM field_eligible)
                GROUP BY gl.game_id, gl.team_id
            ),
            field_valid_games AS (
                SELECT fe.game_id
                FROM field_eligible fe
                JOIN team_validity tv_home ON tv_home.game_id = fe.game_id AND tv_home.team_id = fe.home_team_id AND tv_home.valid_starters >= 11
                JOIN team_validity tv_away ON tv_away.game_id = fe.game_id AND tv_away.team_id = fe.away_team_id AND tv_away.valid_starters >= 11
            ),
            duplicates AS (
                SELECT gl.game_id, gl.team_id, gl.amplua, gl.field_position, COUNT(*) as dup_count,
                       STRING_AGG(CONCAT(p.last_name, ' #', gl.shirt_number), ', ' ORDER BY gl.shirt_number) as players
                FROM game_lineups gl
                LEFT JOIN players p ON p.id = gl.player_id
                WHERE gl.game_id IN (SELECT game_id FROM field_valid_games)
                  AND gl.lineup_type = 'starter'
                  AND gl.amplua IS NOT NULL
                GROUP BY gl.game_id, gl.team_id, gl.amplua, gl.field_position
                HAVING COUNT(*) > 1
            )
            SELECT d.game_id, fe.date, fe.champ_name, fe.home_name, fe.away_name,
                   t.name as team_name, d.amplua, d.field_position, d.dup_count, d.players
            FROM duplicates d
            JOIN field_eligible fe ON fe.game_id = d.game_id
            JOIN teams t ON t.id = d.team_id
            ORDER BY d.game_id, d.team_id, d.amplua
        '''))
        rows = result.fetchall()
        
        if not rows:
            print('No field-mode games with duplicate positions!')
            return
        
        current_game = None
        game_count = 0
        for row in rows:
            if row.game_id != current_game:
                current_game = row.game_id
                game_count += 1
                print(f'')
                print(f'=== Game {row.game_id} | {row.date} | {row.home_name} vs {row.away_name} ({row.champ_name}) ===')
            print(f'  {row.team_name}: {row.dup_count}x {row.amplua}/{row.field_position} -> {row.players}')
        
        print(f'\nTotal field-mode games with visual overlaps: {game_count}')

asyncio.run(main())
