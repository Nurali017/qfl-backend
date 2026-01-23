"""
Script to clean duplicate game events from the database.

Finds and removes duplicate events based on:
- game_id, half, minute, event_type, player_id (if available)
- game_id, half, minute, event_type, normalized player_name (fallback)

Keeps the oldest event (lowest ID) and removes newer duplicates.
"""
import asyncio
import logging
from uuid import UUID
from sqlalchemy import select, and_, or_, func, delete
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import AsyncSessionLocal
from app.models import GameEvent

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def find_and_clean_duplicates(dry_run: bool = True):
    """
    Find and remove duplicate game events.

    Args:
        dry_run: If True, only report duplicates without deleting
    """
    async with AsyncSessionLocal() as db:
        # Get all events ordered by game_id, half, minute
        result = await db.execute(
            select(GameEvent).order_by(
                GameEvent.game_id,
                GameEvent.half,
                GameEvent.minute,
                GameEvent.id
            )
        )
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

        return len(duplicates_to_delete)


async def main():
    """Main entry point."""
    import sys

    dry_run = True
    if len(sys.argv) > 1 and sys.argv[1] == "--delete":
        dry_run = False
        logger.warning("🚨 RUNNING IN DELETE MODE - Duplicates will be removed!")
    else:
        logger.info("🔍 Running in DRY RUN mode")
        logger.info("Use --delete flag to actually remove duplicates")

    await find_and_clean_duplicates(dry_run=dry_run)


if __name__ == "__main__":
    asyncio.run(main())
