"""
Script to clean duplicate game events from the database.

Finds and removes duplicate events based on:
- game_id, half, minute, event_type, player_id (if available)
- game_id, half, minute, event_type, normalized player_name (fallback)

Keeps the oldest event (lowest ID) and removes newer duplicates.
"""
import argparse
import asyncio
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from sqlalchemy import select, delete

from app.database import AsyncSessionLocal
from app.models import Game, GameEvent

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Clean duplicate game events")
    parser.add_argument("--season-id", type=int, default=None, help="Limit processing to specific season")
    parser.add_argument("--dry-run", action="store_true", help="Preview duplicates (default behavior)")
    parser.add_argument("--apply", action="store_true", help="Delete found duplicates")
    parser.add_argument("--output", type=str, default=None, help="Write JSON report to file")
    return parser.parse_args()


async def find_and_clean_duplicates(
    dry_run: bool = True,
    season_id: int | None = None,
    output: str | None = None,
) -> dict:
    """
    Find and remove duplicate game events.

    Args:
        dry_run: If True, only report duplicates without deleting
        season_id: Optional season filter
        output: Optional JSON report path
    """
    async with AsyncSessionLocal() as db:
        # Get all events ordered by game_id, half, minute
        query = (
            select(GameEvent).order_by(
                GameEvent.game_id,
                GameEvent.half,
                GameEvent.minute,
                GameEvent.id
            )
        )
        if season_id is not None:
            query = query.join(Game, Game.id == GameEvent.game_id).where(Game.season_id == season_id)

        result = await db.execute(query)
        all_events = list(result.scalars().all())

        logger.info(f"Total events in database: {len(all_events)}")

        # Track seen signatures and duplicates
        seen_signatures = {}
        duplicates_to_delete = []

        for event in all_events:
            # Normalize player name
            normalized_name = event.player_name.strip().lower() if event.player_name else ""

            # Create signature based on player_id if available, otherwise use name
            if event.player_id:
                sig = (
                    str(event.game_id),
                    event.half,
                    event.minute,
                    event.event_type.value,
                    str(event.player_id)
                )
            else:
                sig = (
                    str(event.game_id),
                    event.half,
                    event.minute,
                    event.event_type.value,
                    normalized_name
                )

            # Also check by name signature to catch cases where player_id differs
            name_sig = (
                str(event.game_id),
                event.half,
                event.minute,
                event.event_type.value,
                normalized_name
            ) if normalized_name else None

            # Check if we've seen this event before
            is_duplicate = False

            if sig in seen_signatures:
                is_duplicate = True
                original_id = seen_signatures[sig]
            elif name_sig and name_sig in seen_signatures:
                is_duplicate = True
                original_id = seen_signatures[name_sig]

            if is_duplicate:
                duplicates_to_delete.append({
                    'id': event.id,
                    'original_id': original_id,
                    'game_id': str(event.game_id),
                    'half': event.half,
                    'minute': event.minute,
                    'event_type': event.event_type.value,
                    'player_name': event.player_name,
                    'created_at': event.created_at
                })
            else:
                # Mark this signature as seen
                seen_signatures[sig] = event.id
                if name_sig:
                    seen_signatures[name_sig] = event.id

        logger.info(f"Found {len(duplicates_to_delete)} duplicate events")

        if duplicates_to_delete:
            # Group by game for better reporting
            by_game = {}
            for dup in duplicates_to_delete:
                game_id = dup['game_id']
                if game_id not in by_game:
                    by_game[game_id] = []
                by_game[game_id].append(dup)

            logger.info(f"Duplicates found in {len(by_game)} games:")
            for game_id, dups in by_game.items():
                logger.info(f"  Game {game_id}: {len(dups)} duplicates")
                for dup in dups:
                    logger.info(
                        f"    - ID {dup['id']} (duplicate of {dup['original_id']}): "
                        f"{dup['event_type']} by {dup['player_name']} "
                        f"at {dup['half']}H {dup['minute']}' "
                        f"(created {dup['created_at']})"
                    )

            if not dry_run:
                # Delete duplicates
                duplicate_ids = [dup['id'] for dup in duplicates_to_delete]
                await db.execute(
                    delete(GameEvent).where(GameEvent.id.in_(duplicate_ids))
                )
                await db.commit()
                logger.info(f"✅ Deleted {len(duplicate_ids)} duplicate events")
            else:
                logger.info("🔍 DRY RUN - No events were deleted")
                logger.info("Run with dry_run=False to actually delete duplicates")
        else:
            logger.info("✅ No duplicates found!")

        summary = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "season_id": season_id,
            "mode": "dry-run" if dry_run else "apply",
            "total_events_scanned": len(all_events),
            "duplicates_found": len(duplicates_to_delete),
            "duplicates_deleted": 0 if dry_run else len(duplicates_to_delete),
            "games_affected": len({d["game_id"] for d in duplicates_to_delete}),
            "duplicates": duplicates_to_delete,
        }

        if output:
            output_path = Path(output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
            logger.info(f"Report written to: {output_path}")

        return summary


async def main():
    """Main entry point."""
    args = parse_args()
    apply = bool(args.apply)
    if apply:
        logger.warning("🚨 RUNNING IN APPLY MODE - Duplicates will be removed!")
    else:
        logger.info("🔍 Running in DRY RUN mode")

    await find_and_clean_duplicates(
        dry_run=not apply,
        season_id=args.season_id,
        output=args.output,
    )


if __name__ == "__main__":
    asyncio.run(main())
