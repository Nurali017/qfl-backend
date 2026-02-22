#!/usr/bin/env python3
"""
One-time backfill for season_participants.

Sources (UNION DISTINCT):
- score_table.team_id
- team_season_stats.team_id
- player_season_stats.team_id
- games.home_team_id
- games.away_team_id

For inserted rows:
- group_name = NULL
- sort_order = score_table.position if available, else 999

Usage:
    python -m scripts.backfill_season_participants --season-id 81 --dry-run
    python -m scripts.backfill_season_participants --season-id 81 --apply
"""

from __future__ import annotations

import argparse
import asyncio
import logging

from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.database import AsyncSessionLocal
from app.models import SeasonParticipant

LOGGER = logging.getLogger("backfill_season_participants")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill season_participants from existing season data sources",
    )
    parser.add_argument("--season-id", type=int, required=True, help="Target season ID")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview inserted rows without writing changes (default mode)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply INSERT ... ON CONFLICT DO NOTHING",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    return parser.parse_args()


async def collect_candidates(season_id: int) -> list[dict]:
    """Collect candidate team IDs and derived sort order for a season."""
    query = text(
        """
        WITH candidate_team_ids AS (
            SELECT team_id
            FROM score_table
            WHERE season_id = :season_id
              AND team_id IS NOT NULL
            UNION
            SELECT team_id
            FROM team_season_stats
            WHERE season_id = :season_id
              AND team_id IS NOT NULL
            UNION
            SELECT team_id
            FROM player_season_stats
            WHERE season_id = :season_id
              AND team_id IS NOT NULL
            UNION
            SELECT home_team_id AS team_id
            FROM games
            WHERE season_id = :season_id
              AND home_team_id IS NOT NULL
            UNION
            SELECT away_team_id AS team_id
            FROM games
            WHERE season_id = :season_id
              AND away_team_id IS NOT NULL
        )
        SELECT c.team_id, COALESCE(st.position, 999) AS sort_order
        FROM candidate_team_ids c
        LEFT JOIN score_table st
            ON st.season_id = :season_id
           AND st.team_id = c.team_id
        ORDER BY COALESCE(st.position, 999), c.team_id
        """
    )

    async with AsyncSessionLocal() as db:
        result = await db.execute(query, {"season_id": season_id})
        return [
            {"team_id": int(row.team_id), "sort_order": int(row.sort_order)}
            for row in result.all()
        ]


async def get_existing_team_ids(season_id: int) -> set[int]:
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(SeasonParticipant.team_id).where(SeasonParticipant.season_id == season_id)
        )
        return {int(row[0]) for row in result.all()}


async def apply_insert(season_id: int, rows: list[dict]) -> int:
    if not rows:
        return 0

    payload = [
        {
            "team_id": row["team_id"],
            "season_id": season_id,
            "group_name": None,
            "sort_order": row["sort_order"],
        }
        for row in rows
    ]

    before = await get_existing_team_ids(season_id)

    async with AsyncSessionLocal() as db:
        stmt = (
            pg_insert(SeasonParticipant)
            .values(payload)
            .on_conflict_do_nothing(index_elements=["team_id", "season_id"])
        )
        await db.execute(stmt)
        await db.commit()

    after = await get_existing_team_ids(season_id)
    return max(len(after) - len(before), 0)


async def run(args: argparse.Namespace) -> int:
    if args.dry_run and args.apply:
        LOGGER.error("Use either --dry-run or --apply, not both")
        return 2

    dry_run = args.dry_run or not args.apply
    season_id = args.season_id

    candidates = await collect_candidates(season_id)
    existing = await get_existing_team_ids(season_id)
    new_rows = [row for row in candidates if row["team_id"] not in existing]

    LOGGER.info("Season %s candidate teams: %d", season_id, len(candidates))
    LOGGER.info("Season %s existing participants: %d", season_id, len(existing))
    LOGGER.info("Season %s rows to insert: %d", season_id, len(new_rows))

    if new_rows:
        preview_limit = 30
        LOGGER.info("Preview (first %d rows):", min(preview_limit, len(new_rows)))
        for row in new_rows[:preview_limit]:
            LOGGER.info(
                "  team_id=%s season_id=%s group_name=NULL sort_order=%s",
                row["team_id"],
                season_id,
                row["sort_order"],
            )

    if dry_run:
        LOGGER.info("[DRY-RUN] No changes were written")
        return 0

    inserted = await apply_insert(season_id, new_rows)
    LOGGER.info("Inserted rows: %d", inserted)
    return 0


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    raise SystemExit(asyncio.run(run(args)))


if __name__ == "__main__":
    main()
