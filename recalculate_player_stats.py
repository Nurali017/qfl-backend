"""
Recalculate player_season_stats for a given season from game-level data.

After the Second League migration merged seasons 80/81/157 into season 80,
player_season_stats for players from seasons 81 and 157 were deleted.
This script recalculates all player_season_stats for the season from:
  - game_events: goals, yellow_cards, red_cards (aggregated by player_name
    to include events where player_id is NULL)
  - game_player_stats: games_played, minutes, shots, passes, etc.

Many game_events have player_id=NULL but still carry player_name,
player_number, and team_name. The script resolves player_id via:
  1) Events that DO have player_id for the same player_name
  2) player_number + team_id lookup in player_teams table

Usage:
    python3 recalculate_player_stats.py --season-id 80 --dry-run   # preview
    python3 recalculate_player_stats.py --season-id 80 --apply     # apply
"""
import argparse
import asyncio
from collections import defaultdict
from sqlalchemy import text
from app.database import AsyncSessionLocal


# Fallback team name mapping for names not resolvable from event data.
# Event team_name (Kazakh) → team_id in DB (Russian names).
TEAM_NAME_FALLBACK = {
    "SD Family Ж": 317,
    "Жайық": 633,
    "Жас Қыран": 632,
    "Қайсар Ж": 332,
    "Ордабасы Ж": 620,
    "Ұлытау Ж": 635,
}


async def main(season_id: int, apply: bool):
    async with AsyncSessionLocal() as session:
        # ── Step 1: Build team_name → team_id mapping ──────────────────
        # From events that have both team_name and team_id
        tm_result = await session.execute(text("""
            SELECT DISTINCT ge.team_name, ge.team_id
            FROM game_events ge
            JOIN games g ON g.id = ge.game_id
            WHERE g.season_id = :sid AND ge.team_name IS NOT NULL AND ge.team_id IS NOT NULL
        """), {"sid": season_id})
        team_map = {row.team_name: row.team_id for row in tm_result.fetchall()}

        # From game_player_stats: match event player_id → gps team_id
        tm2_result = await session.execute(text("""
            SELECT DISTINCT ge.team_name, gps.team_id
            FROM game_events ge
            JOIN game_player_stats gps ON gps.game_id = ge.game_id
                AND gps.player_id = ge.player_id
            JOIN games g ON g.id = ge.game_id
            WHERE g.season_id = :sid
              AND ge.team_name IS NOT NULL AND ge.team_id IS NULL
              AND ge.player_id IS NOT NULL AND gps.team_id IS NOT NULL
        """), {"sid": season_id})
        for row in tm2_result.fetchall():
            team_map.setdefault(row.team_name, row.team_id)

        # Add hardcoded fallbacks for remaining
        for k, v in TEAM_NAME_FALLBACK.items():
            team_map.setdefault(k, v)

        # Reverse map: team_id → team_name (from DB)
        all_team_ids = list(set(team_map.values()))
        tn_result = await session.execute(text(
            "SELECT id, name FROM teams WHERE id = ANY(:ids)"
        ), {"ids": all_team_ids})
        team_id_to_name = {row.id: row.name for row in tn_result.fetchall()}

        print(f"Team mapping: {len(team_map)} event team names resolved")

        # ── Step 2: Get ALL goal events (including NULL player_id) ─────
        goals_result = await session.execute(text("""
            SELECT ge.player_name, ge.player_id, ge.player_number,
                   ge.team_name, ge.team_id
            FROM game_events ge
            JOIN games g ON g.id = ge.game_id
            WHERE g.season_id = :sid AND ge.event_type = 'goal'
              AND ge.player_name IS NOT NULL
        """), {"sid": season_id})
        goal_rows = goals_result.fetchall()

        # ── Step 3: Get ALL yellow/red card events ─────────────────────
        cards_result = await session.execute(text("""
            SELECT ge.player_name, ge.player_id, ge.player_number,
                   ge.team_name, ge.team_id, ge.event_type
            FROM game_events ge
            JOIN games g ON g.id = ge.game_id
            WHERE g.season_id = :sid
              AND ge.event_type IN ('yellow_card', 'red_card')
              AND ge.player_name IS NOT NULL
        """), {"sid": season_id})
        card_rows = cards_result.fetchall()

        # ── Step 4: Aggregate by player_name ───────────────────────────
        # For each player_name, collect: goals, yellows, reds,
        # known player_ids, player_numbers, team_names
        player_data = {}  # player_name → dict

        def ensure_player(pname):
            if pname not in player_data:
                player_data[pname] = {
                    "goals": 0, "yellow_cards": 0, "red_cards": 0,
                    "known_player_ids": [],
                    "player_numbers": set(),
                    "team_names": set(),
                }

        for row in goal_rows:
            ensure_player(row.player_name)
            d = player_data[row.player_name]
            d["goals"] += 1
            if row.player_id is not None:
                d["known_player_ids"].append(row.player_id)
            if row.player_number is not None:
                d["player_numbers"].add(row.player_number)
            if row.team_name is not None:
                d["team_names"].add(row.team_name)

        for row in card_rows:
            ensure_player(row.player_name)
            d = player_data[row.player_name]
            if row.event_type == "yellow_card":
                d["yellow_cards"] += 1
            elif row.event_type == "red_card":
                d["red_cards"] += 1
            if row.player_id is not None:
                d["known_player_ids"].append(row.player_id)
            if row.player_number is not None:
                d["player_numbers"].add(row.player_number)
            if row.team_name is not None:
                d["team_names"].add(row.team_name)

        # ── Step 5: Resolve player_name → player_id ────────────────────
        # Strategy A: Use most common player_id from events
        name_to_pid = {}
        for pname, d in player_data.items():
            if d["known_player_ids"]:
                # Most common player_id
                from collections import Counter
                pid = Counter(d["known_player_ids"]).most_common(1)[0][0]
                name_to_pid[pname] = pid

        # Strategy B: player_number + team_id → player_teams
        unresolved = [pn for pn in player_data if pn not in name_to_pid]
        if unresolved:
            # Build lookup: (team_id, number) → player_id
            pt_result = await session.execute(text("""
                SELECT pt.player_id, pt.team_id, pt.number
                FROM player_teams pt
                WHERE pt.season_id = :sid AND pt.number IS NOT NULL
            """), {"sid": season_id})
            pt_lookup = {}
            for row in pt_result.fetchall():
                pt_lookup[(row.team_id, row.number)] = row.player_id

            for pname in list(unresolved):
                d = player_data[pname]
                resolved = False
                for num in d["player_numbers"]:
                    for tname in d["team_names"]:
                        tid = team_map.get(tname)
                        if tid and (tid, num) in pt_lookup:
                            name_to_pid[pname] = pt_lookup[(tid, num)]
                            resolved = True
                            break
                    if resolved:
                        break

        still_unresolved = [pn for pn in player_data if pn not in name_to_pid]

        # ── Step 6: Build final player stats dict (keyed by player_id) ─
        players = {}  # player_id → stats
        player_team_ids = {}  # player_id → team_id

        for pname, d in player_data.items():
            pid = name_to_pid.get(pname)
            if pid is None:
                continue

            if pid not in players:
                players[pid] = _empty_stats()

            players[pid]["goals"] += d["goals"]
            players[pid]["yellow_cards"] += d["yellow_cards"]
            players[pid]["red_cards"] += d["red_cards"]

            # Resolve team_id
            if pid not in player_team_ids:
                for tname in d["team_names"]:
                    tid = team_map.get(tname)
                    if tid:
                        player_team_ids[pid] = tid
                        break

        # ── Step 7: Merge game_player_stats ────────────────────────────
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
            WHERE g.season_id = :sid
            GROUP BY gps.player_id
        """), {"sid": season_id})

        for row in gps_result.fetchall():
            pid = row.player_id
            if pid not in players:
                players[pid] = _empty_stats()
            players[pid]["games_played"] = row.games_played
            players[pid]["games_starting"] = row.games_starting
            players[pid]["minutes_played"] = row.minutes_played
            players[pid]["shots"] = row.shots
            players[pid]["shots_on_goal"] = row.shots_on_goal
            players[pid]["passes"] = row.passes
            players[pid]["duels"] = row.duels
            players[pid]["tackle"] = row.tackle
            players[pid]["corner"] = row.corner
            players[pid]["offside"] = row.offside
            players[pid]["foul"] = row.foul

        # Get primary team from gps for players not yet having team
        gps_team_result = await session.execute(text("""
            SELECT player_id, team_id FROM (
                SELECT gps.player_id, gps.team_id, COUNT(*) AS cnt,
                       ROW_NUMBER() OVER (
                           PARTITION BY gps.player_id ORDER BY COUNT(*) DESC
                       ) AS rn
                FROM game_player_stats gps
                JOIN games g ON g.id = gps.game_id
                WHERE g.season_id = :sid
                GROUP BY gps.player_id, gps.team_id
            ) sub WHERE rn = 1
        """), {"sid": season_id})
        for row in gps_team_result.fetchall():
            player_team_ids.setdefault(row.player_id, row.team_id)

        # Calculate goal_and_assist
        for stats in players.values():
            stats["goal_and_assist"] = stats["goals"] + stats["assists"]

        # ── Step 8: Existing records & display ─────────────────────────
        existing_result = await session.execute(text(
            "SELECT player_id FROM player_season_stats WHERE season_id = :sid"
        ), {"sid": season_id})
        existing_ids = {row.player_id for row in existing_result.fetchall()}

        new_count = sum(1 for pid in players if pid not in existing_ids)
        update_count = sum(1 for pid in players if pid in existing_ids)

        # Player names for display
        pids = list(players.keys())
        names_result = await session.execute(text(
            "SELECT id, first_name, last_name FROM players WHERE id = ANY(:ids)"
        ), {"ids": pids})
        player_names = {
            row.id: f"{row.first_name or ''} {row.last_name or ''}".strip()
            for row in names_result.fetchall()
        }

        # Summary
        print("=" * 70)
        print(f"RECALCULATE player_season_stats FOR SEASON {season_id}")
        print("=" * 70)
        print(f"  Players with events (by name):  {len(player_data)}")
        print(f"  Resolved to player_id:          {len(name_to_pid)}")
        print(f"  Unresolved (skipped):           {len(still_unresolved)}")
        print(f"  Total player records:           {len(players)}")
        print(f"  → New records (INSERT):         {new_count}")
        print(f"  → Existing records (UPDATE):    {update_count}")
        print()

        if still_unresolved:
            print("-" * 70)
            print(f"UNRESOLVED PLAYERS ({len(still_unresolved)}):")
            print("-" * 70)
            for pname in sorted(still_unresolved):
                d = player_data[pname]
                nums = ",".join(str(n) for n in d["player_numbers"])
                teams = ",".join(d["team_names"])
                print(f"  {pname} #{nums} ({teams}): "
                      f"{d['goals']}g {d['yellow_cards']}y {d['red_cards']}r")
            print()

        # Top scorers preview
        sorted_by_goals = sorted(
            players.items(),
            key=lambda x: (x[1]["goals"], x[1]["goal_and_assist"]),
            reverse=True,
        )
        print("-" * 70)
        print("TOP 20 SCORERS (preview)")
        print("-" * 70)
        header = (
            f"  {'Player':<30} {'Team':<20} {'G':>3} {'A':>3} "
            f"{'G+A':>4} {'GP':>3} {'Min':>5}"
        )
        print(header)
        print(f"  {'-'*30} {'-'*20} {'-'*3} {'-'*3} {'-'*4} {'-'*3} {'-'*5}")
        for pid, stats in sorted_by_goals[:20]:
            name = player_names.get(pid, f"id={pid}")
            tid = player_team_ids.get(pid)
            tname = team_id_to_name.get(tid, "?") if tid else "?"
            marker = " *NEW*" if pid not in existing_ids else ""
            print(
                f"  {name:<30} {tname:<20} {stats['goals']:>3} "
                f"  {stats['assists']:>3} {stats['goal_and_assist']:>4} "
                f"{stats['games_played']:>3} {stats['minutes_played']:>5}{marker}"
            )
        print()

        if not apply:
            print("*** DRY RUN — no changes made. Use --apply to execute. ***")
            return

        # ── Step 9: UPSERT ─────────────────────────────────────────────
        print("Applying changes...")
        upserted = 0
        for pid, stats in players.items():
            team_id = player_team_ids.get(pid)
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
