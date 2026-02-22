"""
Analyze field_position collisions: starters with same (amplua, field_position)
on the same team in the same game — they would overlap on the field visualization.

Usage:
    cd backend && python -m scripts.analyze_lineup_collisions
"""

import asyncio
from sqlalchemy import text
from app.database import AsyncSessionLocal


async def main():
    async with AsyncSessionLocal() as s:

        # 1) How many team-sides have duplicate (amplua, field_position)?
        r = await s.execute(text("""
            SELECT count(DISTINCT game_id::text || '-' || team_id::text) AS affected_sides
            FROM (
                SELECT gl.game_id, gl.team_id, gl.amplua, gl.field_position, count(*) AS cnt
                FROM game_lineups gl
                JOIN games g ON g.id = gl.game_id
                WHERE g.has_lineup = true
                  AND gl.lineup_type = 'starter'
                  AND gl.amplua IS NOT NULL
                  AND gl.field_position IS NOT NULL
                  AND gl.amplua <> 'Gk'
                GROUP BY gl.game_id, gl.team_id, gl.amplua, gl.field_position
                HAVING count(*) > 1
            ) sub
        """))
        dup_sides = r.scalar()

        # Total sides with complete data
        r2 = await s.execute(text("""
            SELECT count(DISTINCT gl.game_id::text || '-' || gl.team_id::text)
            FROM game_lineups gl
            JOIN games g ON g.id = gl.game_id
            WHERE g.has_lineup = true
              AND gl.lineup_type = 'starter'
              AND gl.amplua IS NOT NULL
              AND gl.field_position IS NOT NULL
        """))
        total_sides = r2.scalar()

        print("=" * 65)
        print("  FIELD POSITION COLLISION ANALYSIS")
        print("=" * 65)
        print(f"\n  Team-sides with complete amplua+field_position: {total_sides}")
        print(f"  Team-sides with DUPLICATE (amplua, field_position):  {dup_sides}")
        if total_sides:
            print(f"  Percentage with overlaps: {dup_sides/total_sides*100:.1f}%")

        # 2) Which combos collide most?
        r3 = await s.execute(text("""
            SELECT amplua, field_position, cnt, count(*) AS occurrences
            FROM (
                SELECT gl.game_id, gl.team_id, gl.amplua, gl.field_position, count(*) AS cnt
                FROM game_lineups gl
                JOIN games g ON g.id = gl.game_id
                WHERE g.has_lineup = true
                  AND gl.lineup_type = 'starter'
                  AND gl.amplua IS NOT NULL
                  AND gl.field_position IS NOT NULL
                  AND gl.amplua <> 'Gk'
                GROUP BY gl.game_id, gl.team_id, gl.amplua, gl.field_position
                HAVING count(*) > 1
            ) sub
            GROUP BY amplua, field_position, cnt
            ORDER BY occurrences DESC
        """))
        print(f"\n{'  Amplua':<10} {'FPos':<6} {'Dups':<5} {'Occurrences'}")
        print(f"  {'─'*8} {'─'*5} {'─'*4} {'─'*11}")
        for row in r3.fetchall():
            print(f"  {row[0]:<8} {row[1]:<6} {row[2]:<5} {row[3]}")

        # 3) Per-league breakdown of collision rate
        r4 = await s.execute(text("""
            WITH sides AS (
                SELECT DISTINCT gl.game_id, gl.team_id
                FROM game_lineups gl
                JOIN games g ON g.id = gl.game_id
                WHERE g.has_lineup = true
                  AND gl.lineup_type = 'starter'
                  AND gl.amplua IS NOT NULL
                  AND gl.field_position IS NOT NULL
            ),
            dup_sides AS (
                SELECT DISTINCT gl.game_id, gl.team_id
                FROM game_lineups gl
                JOIN games g ON g.id = gl.game_id
                WHERE g.has_lineup = true
                  AND gl.lineup_type = 'starter'
                  AND gl.amplua IS NOT NULL
                  AND gl.field_position IS NOT NULL
                  AND gl.amplua <> 'Gk'
                GROUP BY gl.game_id, gl.team_id, gl.amplua, gl.field_position
                HAVING count(*) > 1
            )
            SELECT t.name AS tournament, se.name AS season,
                   count(DISTINCT s.game_id::text || '-' || s.team_id::text) AS total_sides,
                   count(DISTINCT d.game_id::text || '-' || d.team_id::text) AS dup_sides
            FROM sides s
            JOIN games g ON g.id = s.game_id
            JOIN seasons se ON se.id = g.season_id
            JOIN tournaments t ON t.id = se.tournament_id
            LEFT JOIN dup_sides d ON d.game_id = s.game_id AND d.team_id = s.team_id
            GROUP BY t.name, se.name
            ORDER BY t.name, se.name
        """))
        print(f"\n{'─'*65}")
        print("  PER-LEAGUE COLLISION RATE")
        print(f"{'─'*65}")
        print(f"  {'Tournament':<32} {'Season':<8} {'Total':<7} {'Dups':<6} {'%'}")
        print(f"  {'─'*32} {'─'*7} {'─'*6} {'─'*5} {'─'*5}")
        for row in r4.fetchall():
            tourn, season, tot, dups = row
            pct = f"{dups/tot*100:.0f}%" if tot else "—"
            print(f"  {tourn:<32} {season:<8} {tot:<7} {dups:<6} {pct}")

        # 4) Game 93 specifically
        r5 = await s.execute(text("""
            SELECT t.name, gl.amplua, gl.field_position,
                   gl.shirt_number,
                   p.last_name
            FROM game_lineups gl
            JOIN teams t ON t.id = gl.team_id
            JOIN players p ON p.id = gl.player_id
            WHERE gl.game_id = 93
              AND gl.lineup_type = 'starter'
            ORDER BY t.name,
                     CASE gl.amplua WHEN 'Gk' THEN 0 WHEN 'D' THEN 1 WHEN 'DM' THEN 2
                                    WHEN 'M' THEN 3 WHEN 'AM' THEN 4 WHEN 'F' THEN 5 END,
                     gl.field_position
        """))
        print(f"\n{'─'*65}")
        print("  GAME 93: Атырау vs Елимай — стартёры по линиям")
        print(f"{'─'*65}")
        print(f"  {'Team':<12} {'Amplua':<6} {'FPos':<5} {'#':<4} {'Player'}")
        print(f"  {'─'*12} {'─'*6} {'─'*5} {'─'*4} {'─'*20}")
        for row in r5.fetchall():
            marker = " <<<" if row[1] in ('D', 'DM', 'F') and row[2] == 'C' else ""
            print(f"  {row[0]:<12} {row[1]:<6} {row[2]:<5} {row[3]:<4} {row[4]}{marker}")


if __name__ == "__main__":
    asyncio.run(main())
