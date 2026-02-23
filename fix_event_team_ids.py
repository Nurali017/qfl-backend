"""
Fix game_events with team_id = NULL.

Attempts to recover team_id using 3 strategies:
  A) player_id → game_lineups.team_id
  B) player_id → game_player_stats.team_id
  C) team_name → TeamNameMatcher (fuzzy match against home/away team names)

Usage:
  python fix_event_team_ids.py          # dry-run (report only)
  python fix_event_team_ids.py --apply  # apply changes
"""

import argparse
import asyncio
import sys
from collections import defaultdict

from sqlalchemy import select, update
from sqlalchemy.orm import selectinload

from app.database import AsyncSessionLocal
from app.models.game import Game
from app.models.game_event import GameEvent
from app.models.game_lineup import GameLineup
from app.models.game_player_stats import GamePlayerStats
from app.utils.team_name_matcher import TeamNameMatcher


async def fix_event_team_ids(apply: bool) -> None:
    async with AsyncSessionLocal() as session:
        # 1. Find all events with team_id IS NULL
        result = await session.execute(
            select(GameEvent).where(GameEvent.team_id.is_(None))
        )
        null_events = result.scalars().all()

        if not null_events:
            print("No game_events with team_id = NULL found.")
            return

        print(f"Found {len(null_events)} events with team_id = NULL\n")

        # Group events by game_id for efficient batch processing
        events_by_game: dict[int, list[GameEvent]] = defaultdict(list)
        for ev in null_events:
            events_by_game[ev.game_id].append(ev)

        print(f"Across {len(events_by_game)} games\n")

        # Counters
        fixed_by_lineup = 0
        fixed_by_player_stats = 0
        fixed_by_team_name = 0
        still_null = 0
        updates: list[tuple[int, int]] = []  # (event_id, team_id)

        for game_id, events in events_by_game.items():
            # Preload lineup lookup: player_id → team_id
            lineup_result = await session.execute(
                select(GameLineup.player_id, GameLineup.team_id)
                .where(GameLineup.game_id == game_id)
            )
            lineup_map: dict[int, int] = {
                row.player_id: row.team_id for row in lineup_result
            }

            # Preload player_stats lookup: player_id → team_id
            stats_result = await session.execute(
                select(GamePlayerStats.player_id, GamePlayerStats.team_id)
                .where(GamePlayerStats.game_id == game_id)
            )
            stats_map: dict[int, int] = {
                row.player_id: row.team_id
                for row in stats_result
                if row.team_id is not None
            }

            # Preload game with team relationships for TeamNameMatcher
            game_result = await session.execute(
                select(Game)
                .options(
                    selectinload(Game.home_team),
                    selectinload(Game.away_team),
                )
                .where(Game.id == game_id)
            )
            game = game_result.scalar_one_or_none()
            matcher = TeamNameMatcher.from_game(game) if game else None

            for ev in events:
                resolved_team_id = None
                method = None

                # Strategy A: player_id → game_lineups
                if ev.player_id and ev.player_id in lineup_map:
                    resolved_team_id = lineup_map[ev.player_id]
                    method = "lineup"

                # Strategy B: player_id → game_player_stats
                if resolved_team_id is None and ev.player_id and ev.player_id in stats_map:
                    resolved_team_id = stats_map[ev.player_id]
                    method = "player_stats"

                # Also try player2_id for strategies A & B (substitutions)
                if resolved_team_id is None and ev.player2_id:
                    if ev.player2_id in lineup_map:
                        resolved_team_id = lineup_map[ev.player2_id]
                        method = "lineup"
                    elif ev.player2_id in stats_map:
                        resolved_team_id = stats_map[ev.player2_id]
                        method = "player_stats"

                # Strategy C: team_name → TeamNameMatcher
                if resolved_team_id is None and matcher and ev.team_name:
                    resolved_team_id = matcher.match(ev.team_name)
                    if resolved_team_id:
                        method = "team_name"

                if resolved_team_id is not None:
                    updates.append((ev.id, resolved_team_id))
                    if method == "lineup":
                        fixed_by_lineup += 1
                    elif method == "player_stats":
                        fixed_by_player_stats += 1
                    elif method == "team_name":
                        fixed_by_team_name += 1
                else:
                    still_null += 1
                    print(
                        f"  [UNRESOLVED] event_id={ev.id} game_id={ev.game_id} "
                        f"type={ev.event_type.value} player_id={ev.player_id} "
                        f"team_name={ev.team_name!r}"
                    )

        # Summary
        total_fixed = fixed_by_lineup + fixed_by_player_stats + fixed_by_team_name
        print(f"\n{'='*50}")
        print(f"SUMMARY")
        print(f"{'='*50}")
        print(f"Total NULL team_id events: {len(null_events)}")
        print(f"Fixable:                   {total_fixed}")
        print(f"  - via game_lineups:      {fixed_by_lineup}")
        print(f"  - via game_player_stats: {fixed_by_player_stats}")
        print(f"  - via team_name match:   {fixed_by_team_name}")
        print(f"Still unresolved:          {still_null}")

        if not updates:
            print("\nNothing to update.")
            return

        if not apply:
            print(f"\nDry-run mode. Run with --apply to update {total_fixed} events.")
            return

        # Apply updates
        print(f"\nApplying {total_fixed} updates...")
        for event_id, team_id in updates:
            await session.execute(
                update(GameEvent)
                .where(GameEvent.id == event_id)
                .values(team_id=team_id)
            )
        await session.commit()
        print("Done. Changes committed.")


def main():
    parser = argparse.ArgumentParser(description="Fix game_events with NULL team_id")
    parser.add_argument(
        "--apply", action="store_true",
        help="Apply changes (default is dry-run)",
    )
    args = parser.parse_args()
    asyncio.run(fix_event_team_ids(apply=args.apply))


if __name__ == "__main__":
    main()
