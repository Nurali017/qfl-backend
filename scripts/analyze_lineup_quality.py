"""
Lineup data quality analysis script.
Read-only — only SELECT queries.

Usage:
    cd backend && python -m scripts.analyze_lineup_quality
"""

import asyncio
from sqlalchemy import text
from app.database import AsyncSessionLocal


QUERIES = {
    # ── Game-level stats ──
    "total_games": """
        SELECT count(*) FROM games
    """,
    "games_with_lineup": """
        SELECT count(*) FROM games WHERE has_lineup = true
    """,
    "missing_home_formation": """
        SELECT count(*) FROM games
        WHERE has_lineup = true AND home_formation IS NULL
    """,
    "missing_away_formation": """
        SELECT count(*) FROM games
        WHERE has_lineup = true AND away_formation IS NULL
    """,
    "missing_home_kit_color": """
        SELECT count(*) FROM games
        WHERE has_lineup = true AND home_kit_color IS NULL
    """,
    "missing_away_kit_color": """
        SELECT count(*) FROM games
        WHERE has_lineup = true AND away_kit_color IS NULL
    """,

    # ── Starter counts per side ──
    "games_home_starters_not_11": """
        SELECT count(*) FROM (
            SELECT g.id, count(gl.id) AS cnt
            FROM games g
            JOIN game_lineups gl ON gl.game_id = g.id
                AND gl.team_id = g.home_team_id
                AND gl.lineup_type = 'starter'
            WHERE g.has_lineup = true
            GROUP BY g.id
            HAVING count(gl.id) != 11
        ) sub
    """,
    "games_away_starters_not_11": """
        SELECT count(*) FROM (
            SELECT g.id, count(gl.id) AS cnt
            FROM games g
            JOIN game_lineups gl ON gl.game_id = g.id
                AND gl.team_id = g.away_team_id
                AND gl.lineup_type = 'starter'
            WHERE g.has_lineup = true
            GROUP BY g.id
            HAVING count(gl.id) != 11
        ) sub
    """,
    "games_with_zero_starters": """
        SELECT count(*) FROM games g
        WHERE g.has_lineup = true
        AND NOT EXISTS (
            SELECT 1 FROM game_lineups gl
            WHERE gl.game_id = g.id AND gl.lineup_type = 'starter'
        )
    """,

    # ── Starter field data quality ──
    "starters_missing_amplua": """
        SELECT count(*) FROM game_lineups gl
        JOIN games g ON g.id = gl.game_id
        WHERE g.has_lineup = true
            AND gl.lineup_type = 'starter'
            AND gl.amplua IS NULL
    """,
    "starters_missing_field_position": """
        SELECT count(*) FROM game_lineups gl
        JOIN games g ON g.id = gl.game_id
        WHERE g.has_lineup = true
            AND gl.lineup_type = 'starter'
            AND gl.field_position IS NULL
    """,
    "starters_missing_shirt_number": """
        SELECT count(*) FROM game_lineups gl
        JOIN games g ON g.id = gl.game_id
        WHERE g.has_lineup = true
            AND gl.lineup_type = 'starter'
            AND gl.shirt_number IS NULL
    """,

    # ── Goalkeeper check ──
    "games_home_no_gk": """
        SELECT count(*) FROM games g
        WHERE g.has_lineup = true
        AND EXISTS (
            SELECT 1 FROM game_lineups gl
            WHERE gl.game_id = g.id AND gl.team_id = g.home_team_id
                AND gl.lineup_type = 'starter'
        )
        AND NOT EXISTS (
            SELECT 1 FROM game_lineups gl
            WHERE gl.game_id = g.id AND gl.team_id = g.home_team_id
                AND gl.lineup_type = 'starter' AND gl.amplua = 'Gk'
        )
    """,
    "games_away_no_gk": """
        SELECT count(*) FROM games g
        WHERE g.has_lineup = true
        AND EXISTS (
            SELECT 1 FROM game_lineups gl
            WHERE gl.game_id = g.id AND gl.team_id = g.away_team_id
                AND gl.lineup_type = 'starter'
        )
        AND NOT EXISTS (
            SELECT 1 FROM game_lineups gl
            WHERE gl.game_id = g.id AND gl.team_id = g.away_team_id
                AND gl.lineup_type = 'starter' AND gl.amplua = 'Gk'
        )
    """,
}

# Fully renderable = has_lineup, both formations, both kit colors,
# exactly 11 starters each side, all starters have amplua + field_position + shirt_number,
# exactly 1 GK each side.
FULLY_RENDERABLE_QUERY = """
    SELECT count(*) FROM games g
    WHERE g.has_lineup = true
      AND g.home_formation IS NOT NULL
      AND g.away_formation IS NOT NULL
      AND g.home_kit_color IS NOT NULL
      AND g.away_kit_color IS NOT NULL
      -- exactly 11 home starters
      AND (SELECT count(*) FROM game_lineups gl
           WHERE gl.game_id = g.id AND gl.team_id = g.home_team_id
             AND gl.lineup_type = 'starter') = 11
      -- exactly 11 away starters
      AND (SELECT count(*) FROM game_lineups gl
           WHERE gl.game_id = g.id AND gl.team_id = g.away_team_id
             AND gl.lineup_type = 'starter') = 11
      -- all home starters have amplua, field_position, shirt_number
      AND NOT EXISTS (
          SELECT 1 FROM game_lineups gl
          WHERE gl.game_id = g.id AND gl.team_id = g.home_team_id
            AND gl.lineup_type = 'starter'
            AND (gl.amplua IS NULL OR gl.field_position IS NULL OR gl.shirt_number IS NULL)
      )
      -- all away starters have amplua, field_position, shirt_number
      AND NOT EXISTS (
          SELECT 1 FROM game_lineups gl
          WHERE gl.game_id = g.id AND gl.team_id = g.away_team_id
            AND gl.lineup_type = 'starter'
            AND (gl.amplua IS NULL OR gl.field_position IS NULL OR gl.shirt_number IS NULL)
      )
      -- exactly 1 home GK
      AND (SELECT count(*) FROM game_lineups gl
           WHERE gl.game_id = g.id AND gl.team_id = g.home_team_id
             AND gl.lineup_type = 'starter' AND gl.amplua = 'Gk') = 1
      -- exactly 1 away GK
      AND (SELECT count(*) FROM game_lineups gl
           WHERE gl.game_id = g.id AND gl.team_id = g.away_team_id
             AND gl.lineup_type = 'starter' AND gl.amplua = 'Gk') = 1
"""

# Breakdown of distinct amplua values actually in the data
PER_LEAGUE_BREAKDOWN = """
    SELECT
        s.id AS season_id,
        t.name AS tournament,
        s.name AS season,
        count(g.id) AS total_games,
        count(g.id) FILTER (WHERE g.has_lineup) AS has_lineup,
        count(g.id) FILTER (
            WHERE g.has_lineup
              AND g.home_formation IS NOT NULL
              AND g.away_formation IS NOT NULL
              AND g.home_kit_color IS NOT NULL
              AND g.away_kit_color IS NOT NULL
              AND (SELECT count(*) FROM game_lineups gl
                   WHERE gl.game_id = g.id AND gl.team_id = g.home_team_id
                     AND gl.lineup_type = 'starter') = 11
              AND (SELECT count(*) FROM game_lineups gl
                   WHERE gl.game_id = g.id AND gl.team_id = g.away_team_id
                     AND gl.lineup_type = 'starter') = 11
              AND NOT EXISTS (
                  SELECT 1 FROM game_lineups gl
                  WHERE gl.game_id = g.id AND gl.lineup_type = 'starter'
                    AND (gl.amplua IS NULL OR gl.field_position IS NULL OR gl.shirt_number IS NULL)
              )
              AND (SELECT count(*) FROM game_lineups gl
                   WHERE gl.game_id = g.id AND gl.team_id = g.home_team_id
                     AND gl.lineup_type = 'starter' AND gl.amplua = 'Gk') = 1
              AND (SELECT count(*) FROM game_lineups gl
                   WHERE gl.game_id = g.id AND gl.team_id = g.away_team_id
                     AND gl.lineup_type = 'starter' AND gl.amplua = 'Gk') = 1
        ) AS fully_ok,
        count(g.id) FILTER (WHERE g.has_lineup AND g.home_formation IS NULL) AS no_formation,
        count(g.id) FILTER (WHERE g.has_lineup AND g.home_kit_color IS NULL) AS no_kit_color
    FROM games g
    JOIN seasons s ON s.id = g.season_id
    JOIN tournaments t ON t.id = s.tournament_id
    GROUP BY s.id, t.name, s.name
    ORDER BY t.name, s.name
"""

PER_LEAGUE_STARTER_ISSUES = """
    SELECT
        s.id AS season_id,
        t.name AS tournament,
        s.name AS season,
        count(DISTINCT g.id) FILTER (
            WHERE (SELECT count(*) FROM game_lineups gl2
                   WHERE gl2.game_id = g.id AND gl2.team_id = g.home_team_id
                     AND gl2.lineup_type = 'starter') != 11
               OR (SELECT count(*) FROM game_lineups gl2
                   WHERE gl2.game_id = g.id AND gl2.team_id = g.away_team_id
                     AND gl2.lineup_type = 'starter') != 11
        ) AS starters_not_11,
        count(*) FILTER (WHERE gl.amplua IS NULL) AS missing_amplua,
        count(*) FILTER (WHERE gl.field_position IS NULL) AS missing_fpos,
        count(DISTINCT g.id) FILTER (
            WHERE NOT EXISTS (
                SELECT 1 FROM game_lineups gl3
                WHERE gl3.game_id = g.id AND gl3.team_id = g.home_team_id
                  AND gl3.lineup_type = 'starter' AND gl3.amplua = 'Gk'
            )
            OR NOT EXISTS (
                SELECT 1 FROM game_lineups gl3
                WHERE gl3.game_id = g.id AND gl3.team_id = g.away_team_id
                  AND gl3.lineup_type = 'starter' AND gl3.amplua = 'Gk'
            )
        ) AS no_gk
    FROM games g
    JOIN seasons s ON s.id = g.season_id
    JOIN tournaments t ON t.id = s.tournament_id
    JOIN game_lineups gl ON gl.game_id = g.id AND gl.lineup_type = 'starter'
    WHERE g.has_lineup = true
    GROUP BY s.id, t.name, s.name
    ORDER BY t.name, s.name
"""

AMPLUA_DISTRIBUTION = """
    SELECT gl.amplua, count(*) AS cnt
    FROM game_lineups gl
    JOIN games g ON g.id = gl.game_id
    WHERE g.has_lineup = true AND gl.lineup_type = 'starter'
    GROUP BY gl.amplua
    ORDER BY cnt DESC
"""

FIELD_POSITION_DISTRIBUTION = """
    SELECT gl.field_position, count(*) AS cnt
    FROM game_lineups gl
    JOIN games g ON g.id = gl.game_id
    WHERE g.has_lineup = true AND gl.lineup_type = 'starter'
    GROUP BY gl.field_position
    ORDER BY cnt DESC
"""


async def main():
    async with AsyncSessionLocal() as session:
        results = {}
        for name, sql in QUERIES.items():
            row = await session.execute(text(sql))
            results[name] = row.scalar()

        # Fully renderable
        row = await session.execute(text(FULLY_RENDERABLE_QUERY))
        fully_renderable = row.scalar()

        # Per-league breakdown
        league_rows = (await session.execute(text(PER_LEAGUE_BREAKDOWN))).fetchall()
        league_issues = (await session.execute(text(PER_LEAGUE_STARTER_ISSUES))).fetchall()

        # Distributions
        amplua_rows = (await session.execute(text(AMPLUA_DISTRIBUTION))).fetchall()
        fp_rows = (await session.execute(text(FIELD_POSITION_DISTRIBUTION))).fetchall()

    # ── Report ──
    total = results["total_games"]
    with_lineup = results["games_with_lineup"]
    broken = with_lineup - fully_renderable

    print("=" * 60)
    print("  LINEUP DATA QUALITY REPORT")
    print("=" * 60)

    print(f"\n{'Total games:':<40} {total:>6}")
    print(f"{'Games with has_lineup=True:':<40} {with_lineup:>6}")
    print(f"{'Fully renderable lineups:':<40} {fully_renderable:>6}")
    print(f"{'Broken / incomplete lineups:':<40} {broken:>6}")
    if with_lineup:
        pct = fully_renderable / with_lineup * 100
        print(f"{'Renderable %:':<40} {pct:>5.1f}%")

    print("\n" + "-" * 60)
    print("  GAME-LEVEL ISSUES (among has_lineup=True games)")
    print("-" * 60)
    print(f"{'Missing home_formation:':<40} {results['missing_home_formation']:>6}")
    print(f"{'Missing away_formation:':<40} {results['missing_away_formation']:>6}")
    print(f"{'Missing home_kit_color:':<40} {results['missing_home_kit_color']:>6}")
    print(f"{'Missing away_kit_color:':<40} {results['missing_away_kit_color']:>6}")

    print("\n" + "-" * 60)
    print("  STARTER COUNT ISSUES")
    print("-" * 60)
    print(f"{'Games with 0 starters (lineup empty):':<40} {results['games_with_zero_starters']:>6}")
    print(f"{'Games home starters != 11:':<40} {results['games_home_starters_not_11']:>6}")
    print(f"{'Games away starters != 11:':<40} {results['games_away_starters_not_11']:>6}")

    print("\n" + "-" * 60)
    print("  STARTER FIELD DATA ISSUES (individual rows)")
    print("-" * 60)
    print(f"{'Starters missing amplua:':<40} {results['starters_missing_amplua']:>6}")
    print(f"{'Starters missing field_position:':<40} {results['starters_missing_field_position']:>6}")
    print(f"{'Starters missing shirt_number:':<40} {results['starters_missing_shirt_number']:>6}")

    print("\n" + "-" * 60)
    print("  GOALKEEPER ISSUES")
    print("-" * 60)
    print(f"{'Games home side no GK:':<40} {results['games_home_no_gk']:>6}")
    print(f"{'Games away side no GK:':<40} {results['games_away_no_gk']:>6}")

    print("\n" + "=" * 120)
    print("  PER-LEAGUE BREAKDOWN")
    print("=" * 120)
    hdr = f"  {'Tournament':<30} {'Season':<25} {'Total':>6} {'Lineup':>7} {'OK':>5} {'OK%':>6} {'NoForm':>7} {'NoKit':>6}"
    print(hdr)
    print("  " + "-" * 116)
    for r in league_rows:
        sid, tourn, season, total, has_lu, ok, no_form, no_kit = r
        pct_str = f"{ok/has_lu*100:.0f}%" if has_lu else "—"
        print(f"  {tourn:<30} {season:<25} {total:>6} {has_lu:>7} {ok:>5} {pct_str:>6} {no_form:>7} {no_kit:>6}")

    print("\n" + "-" * 120)
    print("  PER-LEAGUE STARTER ISSUES (among has_lineup games)")
    print("-" * 120)
    hdr2 = f"  {'Tournament':<30} {'Season':<25} {'!=11':>6} {'NoAmpl':>7} {'NoFPos':>7} {'NoGK':>6}"
    print(hdr2)
    print("  " + "-" * 116)
    for r in league_issues:
        sid, tourn, season, not11, no_amp, no_fp, no_gk = r
        print(f"  {tourn:<30} {season:<25} {not11:>6} {no_amp:>7} {no_fp:>7} {no_gk:>6}")

    print("\n" + "-" * 60)
    print("  AMPLUA DISTRIBUTION (starters)")
    print("-" * 60)
    for val, cnt in amplua_rows:
        print(f"  {str(val):<20} {cnt:>6}")

    print("\n" + "-" * 60)
    print("  FIELD_POSITION DISTRIBUTION (starters)")
    print("-" * 60)
    for val, cnt in fp_rows:
        print(f"  {str(val):<20} {cnt:>6}")

    print("\n" + "=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
