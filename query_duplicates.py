"""
Find matches where starters on the same team have duplicate (amplua, field_position) combinations.

Usage:
    cd backend && python3 query_duplicates.py
"""
import asyncio
from sqlalchemy import text
from app.database import AsyncSessionLocal

DATE_FILTER = "2025-07-01"


async def main():
    async with AsyncSessionLocal() as session:
        # ── Step 1: Count ──
        count_result = await session.execute(text(f"""
            WITH dup_games AS (
                SELECT DISTINCT gl.game_id
                FROM game_lineups gl
                JOIN games g ON g.id = gl.game_id
                WHERE gl.lineup_type = 'starter'
                  AND gl.amplua IS NOT NULL
                  AND gl.field_position IS NOT NULL
                  AND g.date >= '{DATE_FILTER}'
                GROUP BY gl.game_id, gl.team_id, gl.amplua, gl.field_position
                HAVING COUNT(*) > 1
            )
            SELECT COUNT(*) as total FROM dup_games
        """))
        total = count_result.scalar()
        print(f"Total matches with duplicate (amplua, field_position) among starters (>= {DATE_FILTER}): {total}")
        print("=" * 100)

        # ── Step 2: 10 examples ──
        result = await session.execute(text(f"""
            WITH dup_teams AS (
                SELECT gl.game_id, gl.team_id, gl.amplua, gl.field_position, COUNT(*) as dup_count
                FROM game_lineups gl
                JOIN games g ON g.id = gl.game_id
                WHERE gl.lineup_type = 'starter'
                  AND gl.amplua IS NOT NULL
                  AND gl.field_position IS NOT NULL
                  AND g.date >= '{DATE_FILTER}'
                GROUP BY gl.game_id, gl.team_id, gl.amplua, gl.field_position
                HAVING COUNT(*) > 1
            ),
            dup_game_ids AS (
                SELECT DISTINCT game_id FROM dup_teams
            ),
            ranked_games AS (
                SELECT g.id as game_id, g.date, g.tour,
                       g.home_score, g.away_score,
                       g.home_formation, g.away_formation,
                       g.lineup_source, g.lineup_render_mode,
                       ht.name as home_name, at2.name as away_name,
                       c.name as champ_name, s.name as season_name,
                       ROW_NUMBER() OVER (ORDER BY g.date DESC, g.id DESC) as rn
                FROM dup_game_ids dg
                JOIN games g ON g.id = dg.game_id
                LEFT JOIN teams ht ON ht.id = g.home_team_id
                LEFT JOIN teams at2 ON at2.id = g.away_team_id
                LEFT JOIN seasons s ON s.id = g.season_id
                LEFT JOIN championships c ON c.id = s.championship_id
            )
            SELECT rg.game_id, rg.date, rg.tour, rg.home_name, rg.away_name,
                   rg.home_score, rg.away_score,
                   rg.home_formation, rg.away_formation,
                   rg.lineup_source, rg.lineup_render_mode,
                   rg.champ_name, rg.season_name,
                   dt.team_id, t.name as dup_team_name,
                   dt.amplua, dt.field_position, dt.dup_count
            FROM ranked_games rg
            JOIN dup_teams dt ON dt.game_id = rg.game_id
            JOIN teams t ON t.id = dt.team_id
            WHERE rg.rn <= 10
            ORDER BY rg.date DESC, rg.game_id DESC, dt.team_id, dt.amplua, dt.field_position
        """))
        rows = result.fetchall()

        if not rows:
            print("No matches with duplicate (amplua, field_position) found!")
            return

        current_game = None
        for row in rows:
            if row.game_id != current_game:
                current_game = row.game_id
                print(f"\n{'=' * 100}")
                print(f"Game #{row.game_id} | {row.date} | Tour {row.tour}")
                print(f"  {row.home_name} {row.home_score}-{row.away_score} {row.away_name}")
                print(f"  Championship: {row.champ_name} / {row.season_name}")
                print(f"  Formations: {row.home_formation} vs {row.away_formation}")
                print(f"  Lineup source: {row.lineup_source} | Render mode: {row.lineup_render_mode}")
                print(f"  Duplicate positions found:")
            print(f"    {row.dup_team_name}: {row.dup_count}x players at {row.amplua}/{row.field_position}")

        # ── Step 3: Detailed view (first 5) ──
        print(f"\n\n{'#' * 100}")
        print("DETAILED VIEW: Players in duplicate positions (first 5 matches)")
        print(f"{'#' * 100}")

        detail_result = await session.execute(text(f"""
            WITH dup_game_ids AS (
                SELECT DISTINCT gl.game_id
                FROM game_lineups gl
                JOIN games g ON g.id = gl.game_id
                WHERE gl.lineup_type = 'starter'
                  AND gl.amplua IS NOT NULL
                  AND gl.field_position IS NOT NULL
                  AND g.date >= '{DATE_FILTER}'
                GROUP BY gl.game_id, gl.team_id, gl.amplua, gl.field_position
                HAVING COUNT(*) > 1
            ),
            ranked_games AS (
                SELECT game_id, ROW_NUMBER() OVER (ORDER BY game_id DESC) as rn
                FROM dup_game_ids
            ),
            target_games AS (
                SELECT game_id FROM ranked_games WHERE rn <= 5
            ),
            starters_with_dup_flag AS (
                SELECT gl.game_id, gl.team_id, gl.player_id,
                       gl.amplua, gl.field_position, gl.shirt_number,
                       COUNT(*) OVER (
                           PARTITION BY gl.game_id, gl.team_id, gl.amplua, gl.field_position
                       ) as pos_count
                FROM game_lineups gl
                WHERE gl.game_id IN (SELECT game_id FROM target_games)
                  AND gl.lineup_type = 'starter'
            )
            SELECT g.id as game_id, g.date,
                   ht.name as home_name, at2.name as away_name,
                   g.home_formation, g.away_formation,
                   c.name as champ_name,
                   t.name as team_name,
                   s.amplua, s.field_position, s.shirt_number,
                   p.first_name, p.last_name,
                   CASE WHEN s.pos_count > 1 AND s.amplua IS NOT NULL AND s.field_position IS NOT NULL
                        THEN true ELSE false END as is_duplicate
            FROM starters_with_dup_flag s
            JOIN games g ON g.id = s.game_id
            LEFT JOIN players p ON p.id = s.player_id
            LEFT JOIN teams t ON t.id = s.team_id
            LEFT JOIN teams ht ON ht.id = g.home_team_id
            LEFT JOIN teams at2 ON at2.id = g.away_team_id
            LEFT JOIN seasons se ON se.id = g.season_id
            LEFT JOIN championships c ON c.id = se.championship_id
            ORDER BY g.date DESC, g.id DESC, s.team_id,
                CASE s.amplua
                    WHEN 'Gk' THEN 0 WHEN 'D' THEN 1 WHEN 'DM' THEN 2
                    WHEN 'M' THEN 3 WHEN 'AM' THEN 4 WHEN 'F' THEN 5
                    ELSE 6
                END,
                CASE s.field_position
                    WHEN 'L' THEN 0 WHEN 'LC' THEN 1 WHEN 'C' THEN 2
                    WHEN 'RC' THEN 3 WHEN 'R' THEN 4
                    ELSE 5
                END,
                s.shirt_number
        """))
        detail_rows = detail_result.fetchall()

        current_game = None
        current_team = None
        for row in detail_rows:
            if row.game_id != current_game:
                current_game = row.game_id
                current_team = None
                print(f"\n{'=' * 80}")
                print(f"Game #{row.game_id} | {row.date} | {row.home_name} vs {row.away_name}")
                print(f"  {row.champ_name} | Formations: {row.home_formation} vs {row.away_formation}")

            if row.team_name != current_team:
                current_team = row.team_name
                print(f"\n  --- {row.team_name} Starting XI ---")

            dup_marker = " << DUPLICATE" if row.is_duplicate else ""
            pos = f"{row.amplua or '?'}/{row.field_position or '?'}"
            print(f"    #{row.shirt_number or '?':>3}  {pos:<8}  {row.last_name or '?'} {row.first_name or ''}{dup_marker}")

        # ── Step 4: By lineup source ──
        print(f"\n\n{'#' * 100}")
        print("BREAKDOWN: Duplicates by lineup_source")
        print(f"{'#' * 100}")

        source_result = await session.execute(text(f"""
            WITH dup_games AS (
                SELECT DISTINCT gl.game_id
                FROM game_lineups gl
                JOIN games g ON g.id = gl.game_id
                WHERE gl.lineup_type = 'starter'
                  AND gl.amplua IS NOT NULL
                  AND gl.field_position IS NOT NULL
                  AND g.date >= '{DATE_FILTER}'
                GROUP BY gl.game_id, gl.team_id, gl.amplua, gl.field_position
                HAVING COUNT(*) > 1
            )
            SELECT g.lineup_source, COUNT(*) as cnt
            FROM dup_games dg
            JOIN games g ON g.id = dg.game_id
            GROUP BY g.lineup_source
            ORDER BY cnt DESC
        """))
        for row in source_result.fetchall():
            print(f"  {row.lineup_source or 'NULL':<25} {row.cnt} matches")

        # ── Step 5: Most common duplicates ──
        print(f"\n\n{'#' * 100}")
        print("MOST COMMON DUPLICATE POSITIONS")
        print(f"{'#' * 100}")

        common_result = await session.execute(text(f"""
            SELECT gl.amplua, gl.field_position, COUNT(*) as occurrence_count
            FROM game_lineups gl
            JOIN games g ON g.id = gl.game_id
            WHERE gl.lineup_type = 'starter'
              AND gl.amplua IS NOT NULL
              AND gl.field_position IS NOT NULL
              AND g.date >= '{DATE_FILTER}'
            GROUP BY gl.game_id, gl.team_id, gl.amplua, gl.field_position
            HAVING COUNT(*) > 1
        """))
        from collections import Counter
        pair_counts = Counter()
        for row in common_result.fetchall():
            pair_counts[(row.amplua, row.field_position)] += 1

        for (amplua, field_pos), count in pair_counts.most_common(15):
            print(f"  {amplua}/{field_pos:<5}  appears as duplicate in {count} team-games")


asyncio.run(main())
