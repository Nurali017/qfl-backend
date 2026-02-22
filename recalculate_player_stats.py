"""
Recalculate player_season_stats for a given season from game-level data.

After the Second League migration merged seasons 80/81/157 into season 80,
player_season_stats for players from seasons 81 and 157 were deleted.
This script recalculates all player_season_stats for the season from:
  - game_player_stats: games_played, minutes, shots, passes, etc.
  - game_events: goals, assists, yellow_cards, red_cards

Usage:
    python3 recalculate_player_stats.py --season-id 80 --dry-run   # preview
    python3 recalculate_player_stats.py --season-id 80 --apply     # apply
"""
import argparse
import asyncio
from sqlalchemy import text
from app.database import AsyncSessionLocal


async def main(season_id: int, apply: bool):
    async with AsyncSessionLocal() as session:
        # 1. Aggregate per-game stats by player
        gps_result = await session.execute(text("""
            SELECT
                gps.player_id,
                COUNT(*) AS games_played,
                SUM(CASE WHEN gps.started = true THEN 1 ELSE 0 END) AS games_starting,
                SUM(COALESCE(gps.minutes_played, 0)) AS minutes_played,
                SUM(gps.shots) AS shots,
                SUM(gps.shots_on_goal) AS shots_on_goal,
                SUM(gps.passes) AS passes,
                SUM(gps.duel) AS duels,
                SUM(gps.tackle) AS tackle,
                SUM(gps.corner) AS corner,
                SUM(gps.offside) AS offside,
                SUM(gps.foul) AS foul
            FROM game_player_stats gps
            JOIN games g ON g.id = gps.game_id
            WHERE g.season_id = :season_id
            GROUP BY gps.player_id
        """), {"season_id": season_id})
        gps_rows = gps_result.fetchall()

        # Build player stats dict
        players = {}
        for row in gps_rows:
            players[row.player_id] = {
                "games_played": row.games_played,
                "games_starting": row.games_starting,
                "minutes_played": row.minutes_played,
                "shots": row.shots,
                "shots_on_goal": row.shots_on_goal,
                "passes": row.passes,
                "duels": row.duels,
                "tackle": row.tackle,
                "corner": row.corner,
                "offside": row.offside,
                "foul": row.foul,
                "goals": 0,
                "assists": 0,
                "yellow_cards": 0,
                "red_cards": 0,
            }

        # 2. Get primary team for each player (team with most games)
        team_result = await session.execute(text("""
            SELECT player_id, team_id FROM (
                SELECT gps.player_id, gps.team_id, COUNT(*) AS cnt,
                       ROW_NUMBER() OVER (
                           PARTITION BY gps.player_id ORDER BY COUNT(*) DESC
                       ) AS rn
                FROM game_player_stats gps
                JOIN games g ON g.id = gps.game_id
                WHERE g.season_id = :season_id
                GROUP BY gps.player_id, gps.team_id
            ) sub WHERE rn = 1
        """), {"season_id": season_id})
        player_teams = {row.player_id: row.team_id for row in team_result.fetchall()}

        # 3. Get goals from game_events
        goals_result = await session.execute(text("""
            SELECT ge.player_id, COUNT(*) AS goals
            FROM game_events ge
            JOIN games g ON g.id = ge.game_id
            WHERE g.season_id = :season_id
              AND ge.event_type = 'goal'
              AND ge.player_id IS NOT NULL
            GROUP BY ge.player_id
        """), {"season_id": season_id})
        for row in goals_result.fetchall():
            if row.player_id in players:
                players[row.player_id]["goals"] = row.goals
            else:
                players[row.player_id] = _empty_stats(goals=row.goals)

        # 4. Get assists from game_events
        assists_result = await session.execute(text("""
            SELECT ge.assist_player_id AS player_id, COUNT(*) AS assists
            FROM game_events ge
            JOIN games g ON g.id = ge.game_id
            WHERE g.season_id = :season_id
              AND ge.event_type = 'goal'
              AND ge.assist_player_id IS NOT NULL
            GROUP BY ge.assist_player_id
        """), {"season_id": season_id})
        for row in assists_result.fetchall():
            if row.player_id in players:
                players[row.player_id]["assists"] = row.assists
            else:
                players[row.player_id] = _empty_stats(assists=row.assists)

        # 5. Get yellow cards from game_events
        yellows_result = await session.execute(text("""
            SELECT ge.player_id, COUNT(*) AS yellow_cards
            FROM game_events ge
            JOIN games g ON g.id = ge.game_id
            WHERE g.season_id = :season_id
              AND ge.event_type = 'yellow_card'
              AND ge.player_id IS NOT NULL
            GROUP BY ge.player_id
        """), {"season_id": season_id})
        for row in yellows_result.fetchall():
            if row.player_id in players:
                players[row.player_id]["yellow_cards"] = row.yellow_cards

        # 6. Get red cards from game_events
        reds_result = await session.execute(text("""
            SELECT ge.player_id, COUNT(*) AS red_cards
            FROM game_events ge
            JOIN games g ON g.id = ge.game_id
            WHERE g.season_id = :season_id
              AND ge.event_type = 'red_card'
              AND ge.player_id IS NOT NULL
            GROUP BY ge.player_id
        """), {"season_id": season_id})
        for row in reds_result.fetchall():
            if row.player_id in players:
                players[row.player_id]["red_cards"] = row.red_cards

        # Calculate goal_and_assist
        for stats in players.values():
            stats["goal_and_assist"] = stats["goals"] + stats["assists"]

        # 7. Get existing player_season_stats for comparison
        existing_result = await session.execute(text("""
            SELECT player_id FROM player_season_stats
            WHERE season_id = :season_id
        """), {"season_id": season_id})
        existing_ids = {row.player_id for row in existing_result.fetchall()}

        new_count = sum(1 for pid in players if pid not in existing_ids)
        update_count = sum(1 for pid in players if pid in existing_ids)

        # Get player names for display
        player_ids = list(players.keys())
        names_result = await session.execute(text("""
            SELECT id, first_name, last_name FROM players
            WHERE id = ANY(:ids)
        """), {"ids": player_ids})
        player_names = {
            row.id: f"{row.first_name or ''} {row.last_name or ''}".strip()
            for row in names_result.fetchall()
        }

        # Get team names for display
        team_ids = list(set(player_teams.values()))
        team_names_result = await session.execute(text("""
            SELECT id, name FROM teams WHERE id = ANY(:ids)
        """), {"ids": team_ids})
        team_names = {row.id: row.name for row in team_names_result.fetchall()}

        # Summary
        print("=" * 70)
        print(f"RECALCULATE player_season_stats FOR SEASON {season_id}")
        print("=" * 70)
        print(f"  Total players found in game data: {len(players)}")
        print(f"  → New records (INSERT):           {new_count}")
        print(f"  → Existing records (UPDATE):      {update_count}")
        print()

        # Top scorers preview
        sorted_by_goals = sorted(
            players.items(),
            key=lambda x: (x[1]["goals"], x[1]["assists"]),
            reverse=True,
        )
        print("-" * 70)
        print("TOP 15 SCORERS (preview)")
        print("-" * 70)
        header = (
            f"  {'Player':<30} {'Team':<20} {'G':>3} {'A':>3} "
            f"{'G+A':>4} {'GP':>3} {'Min':>5}"
        )
        print(header)
        print(f"  {'-'*30} {'-'*20} {'-'*3} {'-'*3} {'-'*4} {'-'*3} {'-'*5}")
        for pid, stats in sorted_by_goals[:15]:
            name = player_names.get(pid, f"id={pid}")
            tid = player_teams.get(pid)
            tname = team_names.get(tid, "?") if tid else "?"
            marker = " *NEW*" if pid not in existing_ids else ""
            print(
                f"  {name:<30} {tname:<20} {stats['goals']:>3} "
                f"{stats['assists']:>3} {stats['goal_and_assist']:>4} "
                f"{stats['games_played']:>3} {stats['minutes_played']:>5}{marker}"
            )
        print()

        if not apply:
            print("*** DRY RUN — no changes made. Use --apply to execute. ***")
            return

        # 8. UPSERT into player_season_stats
        print("Applying changes...")
        upserted = 0
        for pid, stats in players.items():
            team_id = player_teams.get(pid)
            await session.execute(text("""
                INSERT INTO player_season_stats (
                    player_id, season_id, team_id,
                    games_played, games_starting, minutes_played,
                    goals, assists, goal_and_assist,
                    shots, shots_on_goal, passes, duels, tackle,
                    corner, offside, foul,
                    yellow_cards, red_cards,
                    updated_at
                ) VALUES (
                    :player_id, :season_id, :team_id,
                    :games_played, :games_starting, :minutes_played,
                    :goals, :assists, :goal_and_assist,
                    :shots, :shots_on_goal, :passes, :duels, :tackle,
                    :corner, :offside, :foul,
                    :yellow_cards, :red_cards,
                    NOW()
                )
                ON CONFLICT (player_id, season_id) DO UPDATE SET
                    team_id = EXCLUDED.team_id,
                    games_played = EXCLUDED.games_played,
                    games_starting = EXCLUDED.games_starting,
                    minutes_played = EXCLUDED.minutes_played,
                    goals = EXCLUDED.goals,
                    assists = EXCLUDED.assists,
                    goal_and_assist = EXCLUDED.goal_and_assist,
                    shots = EXCLUDED.shots,
                    shots_on_goal = EXCLUDED.shots_on_goal,
                    passes = EXCLUDED.passes,
                    duels = EXCLUDED.duels,
                    tackle = EXCLUDED.tackle,
                    corner = EXCLUDED.corner,
                    offside = EXCLUDED.offside,
                    foul = EXCLUDED.foul,
                    yellow_cards = EXCLUDED.yellow_cards,
                    red_cards = EXCLUDED.red_cards,
                    updated_at = NOW()
            """), {
                "player_id": pid,
                "season_id": season_id,
                "team_id": team_id,
                "games_played": stats["games_played"],
                "games_starting": stats["games_starting"],
                "minutes_played": stats["minutes_played"],
                "goals": stats["goals"],
                "assists": stats["assists"],
                "goal_and_assist": stats["goal_and_assist"],
                "shots": stats["shots"],
                "shots_on_goal": stats["shots_on_goal"],
                "passes": stats["passes"],
                "duels": stats["duels"],
                "tackle": stats["tackle"],
                "corner": stats["corner"],
                "offside": stats["offside"],
                "foul": stats["foul"],
                "yellow_cards": stats["yellow_cards"],
                "red_cards": stats["red_cards"],
            })
            upserted += 1

        await session.commit()
        print(
            f"Done. {upserted} records upserted "
            f"({new_count} new, {update_count} updated)."
        )


def _empty_stats(**overrides):
    """Return a zeroed-out stats dict with optional overrides."""
    base = {
        "games_played": 0, "games_starting": 0, "minutes_played": 0,
        "shots": 0, "shots_on_goal": 0, "passes": 0, "duels": 0,
        "tackle": 0, "corner": 0, "offside": 0, "foul": 0,
        "goals": 0, "assists": 0, "yellow_cards": 0, "red_cards": 0,
    }
    base.update(overrides)
    return base


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Recalculate player_season_stats from game-level data"
    )
    parser.add_argument(
        "--season-id", type=int, required=True, help="Season ID to recalculate"
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--dry-run", action="store_true", default=True,
        help="Preview changes (default)",
    )
    group.add_argument(
        "--apply", action="store_true", help="Apply changes"
    )
    args = parser.parse_args()
    asyncio.run(main(season_id=args.season_id, apply=args.apply))
