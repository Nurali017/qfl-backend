import asyncio
from sqlalchemy import text
from app.database import AsyncSessionLocal

async def main():
    async with AsyncSessionLocal() as session:
        result = await session.execute(text('''
            WITH field_eligible AS (
                SELECT g.id as game_id
                FROM games g
                JOIN seasons s ON s.id = g.season_id
                JOIN championships c ON c.id = s.championship_id
                WHERE COALESCE(c.legacy_id, c.id) IN (1, 2, 3)
                  AND g.date >= '2025-06-01'
                  AND g.is_technical = false
            ),
            team_validity AS (
                SELECT gl.game_id, gl.team_id,
                       COUNT(*) FILTER (WHERE gl.lineup_type = 'starter' AND gl.amplua IS NOT NULL AND gl.field_position IS NOT NULL) as valid_starters
                FROM game_lineups gl
                WHERE gl.game_id IN (SELECT game_id FROM field_eligible)
                GROUP BY gl.game_id, gl.team_id
            ),
            dup_counts AS (
                SELECT gl.game_id, gl.team_id, COUNT(*) as total_dups
                FROM game_lineups gl
                WHERE gl.game_id IN (SELECT game_id FROM field_eligible)
                  AND gl.lineup_type = 'starter'
                  AND gl.amplua IS NOT NULL
                GROUP BY gl.game_id, gl.team_id, gl.amplua, gl.field_position
                HAVING COUNT(*) > 1
            ),
            worst_games AS (
                SELECT game_id, team_id, SUM(total_dups) as total_overlap
                FROM dup_counts
                GROUP BY game_id, team_id
                ORDER BY total_overlap DESC
                LIMIT 3
            )
            SELECT gl.game_id, t.name as team_name, 
                   gl.amplua, gl.field_position, 
                   p.last_name, gl.shirt_number,
                   gl.lineup_type
            FROM game_lineups gl
            JOIN worst_games wg ON wg.game_id = gl.game_id AND wg.team_id = gl.team_id
            JOIN teams t ON t.id = gl.team_id
            LEFT JOIN players p ON p.id = gl.player_id
            WHERE gl.lineup_type = 'starter'
            ORDER BY gl.game_id, gl.team_id, 
                CASE gl.amplua 
                    WHEN 'Gk' THEN 0 WHEN 'D' THEN 1 WHEN 'DM' THEN 2 
                    WHEN 'M' THEN 3 WHEN 'AM' THEN 4 WHEN 'F' THEN 5 
                END,
                CASE gl.field_position
                    WHEN 'L' THEN 0 WHEN 'LC' THEN 1 WHEN 'C' THEN 2
                    WHEN 'RC' THEN 3 WHEN 'R' THEN 4
                END
        '''))
        rows = result.fetchall()
        
        current_game = None
        for row in rows:
            if row.game_id != current_game:
                current_game = row.game_id
                print(f'\n=== Game {row.game_id} - {row.team_name} full XI ===')
            print(f'  {row.amplua or "NULL"}/{row.field_position or "NULL"} | {row.last_name} #{row.shirt_number}')

asyncio.run(main())
