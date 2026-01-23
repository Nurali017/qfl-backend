"""
Merge duplicate referees in the database.

Usage:
    python scripts/merge_referee_duplicates.py
"""

import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import select, update, delete

from app.database import AsyncSessionLocal
from app.models import Referee
from app.models.game_referee import GameReferee


# Mapping of duplicate referee IDs to canonical referee ID
# Format: duplicate_id -> canonical_id
DUPLICATE_MAP = {
    # Абдуллаев Багдат (133) -> Абдуллаев Бағдат (54)
    133: 54,
    # Арынханов Нұрдаулет (68) -> Арынханов Нұрдәулет (35)
    68: 35,
    # Белоградов Дмитирий (111) -> Белоградов Дмитрий (14)
    111: 14,
    # Быстровв Дмитрий (122) -> Быстров Дмитрий (23)
    122: 23,
    # Жақсылық Төребек (100) -> Жақсылык Төребек (52)
    100: 52,
    # Калачев Сергей (99) -> Калачёв Сергей (48)
    99: 48,
    # Ісмұратов Арман (103) -> Есмұратов Арман (41)
    103: 41,
    # Дмитрий Будниченко (142) -> Будниченко Дмитрий (28)
    142: 28,
    # Артем Кучин (140) -> needs to find if Кучин Артем exists
    # Дмитрий Филоненко (141) -> needs to find if Филоненко Дмитрий exists
}


async def find_more_duplicates(db):
    """Find referees with swapped first/last names."""
    result = await db.execute(select(Referee))
    referees = result.scalars().all()

    # Check for swapped names
    by_names = {}
    for r in referees:
        # Normal order
        key1 = f"{r.first_name}|{r.last_name}".lower()
        # Swapped order
        key2 = f"{r.last_name}|{r.first_name}".lower()

        if key1 in by_names:
            print(f"Possible duplicate: {r.id} ({r.first_name} {r.last_name}) matches {by_names[key1]}")
        if key2 in by_names:
            print(f"Swapped names: {r.id} ({r.first_name} {r.last_name}) matches {by_names[key2]}")

        by_names[key1] = (r.id, r.first_name, r.last_name)


async def merge_duplicates():
    """Merge duplicate referees."""
    print("Merging duplicate referees...")

    async with AsyncSessionLocal() as db:
        # First, find more duplicates
        print("\n=== Checking for swapped names ===")
        await find_more_duplicates(db)

        print("\n=== Merging known duplicates ===")

        for dup_id, canonical_id in DUPLICATE_MAP.items():
            # Check if duplicate exists
            result = await db.execute(select(Referee).where(Referee.id == dup_id))
            dup = result.scalar_one_or_none()

            if not dup:
                print(f"  Duplicate {dup_id} not found, skipping")
                continue

            # Check if canonical exists
            result = await db.execute(select(Referee).where(Referee.id == canonical_id))
            canonical = result.scalar_one_or_none()

            if not canonical:
                print(f"  Canonical {canonical_id} not found, skipping")
                continue

            print(f"  Merging '{dup.first_name} {dup.last_name}' (ID {dup_id}) -> '{canonical.first_name} {canonical.last_name}' (ID {canonical_id})")

            # Update game_referees to point to canonical
            await db.execute(
                update(GameReferee)
                .where(GameReferee.referee_id == dup_id)
                .values(referee_id=canonical_id)
            )

            # Delete duplicate referee
            await db.execute(delete(Referee).where(Referee.id == dup_id))

        await db.commit()

        # Count remaining
        result = await db.execute(select(Referee))
        remaining = len(result.scalars().all())
        print(f"\nRemaining referees: {remaining}")


if __name__ == "__main__":
    asyncio.run(merge_duplicates())
